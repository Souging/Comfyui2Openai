//! ComfyUI Backend Communication & Request Processing Module
//!
//! This module handles the translation between OpenAI API format and ComfyUI format,
//! manages image generation requests, and retrieves generated images from the backend.

use crate::ws::WebSocketManager;
use axum::{
    body::{Body, Bytes},
    extract::{Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method},
    response::Response as AxumResponse,
};
use base64::{Engine as _, engine::general_purpose};
use log::{debug, error, info, warn};
use rand::Rng;
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::time::timeout;

/// Metadata for ComfyUI output images
///
/// Used to construct the query string for retrieving images.
#[derive(Debug, Clone, Serialize)]
struct ImageFile {
    /// Filename in the ComfyUI output directory
    filename: String,
    /// Subfolder within the output directory
    subfolder: String,
    /// Image type (e.g., "output" for final result, "temp" for preview)
    #[serde(rename = "type")]
    type_field: String,
}

// =================================================================================
//  Section: Workflow Loading
// =================================================================================

/// Workflow Loader
///
/// Responsible for loading ComfyUI workflow JSON files from a specified directory.
/// These files define the specific image generation process and are referenced by the `model` name in API requests.
pub struct WorkflowsLoader;

impl WorkflowsLoader {
    /// Scans and loads all JSON files from the folder
    ///
    /// Uses the filename (without extension) as the key. This allows clients to specify the workflow
    /// using the `model` field in the request.
    ///
    /// # Arguments
    /// * `folder_path` - Path to the directory containing workflow JSON files
    ///
    /// # Returns
    /// - `HashMap<String, Value>`: Map of Workflow Name -> JSON Content
    /// - `String`: Error message if directory doesn't exist or JSON parsing fails
    ///
    /// # Example
    /// ```no_run
    /// let workflows = WorkflowsLoader::load_from_folder("./workflows")?;
    /// // Access workflow with: workflows.get("animagine-xl-4")
    /// ```
    pub fn load_from_folder(folder_path: &str) -> Result<HashMap<String, Value>, String> {
        let path = Path::new(folder_path);

        // Verify folder existence
        if !path.exists() {
            return Err(format!("Workflows folder does not exist: {}", folder_path));
        }

        // Verify it is a directory
        if !path.is_dir() {
            return Err(format!(
                "Workflows path is not a directory: {}",
                folder_path
            ));
        }

        let mut workflows = HashMap::new();

        // Read all entries in the directory
        let entries =
            fs::read_dir(path).map_err(|e| format!("Failed to read workflows directory: {}", e))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let file_path = entry.path();

            // Process only JSON files
            if file_path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("json"))
                .unwrap_or(false)
            {
                // Extract filename as workflow name
                let filename = file_path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .ok_or_else(|| format!("Failed to get filename for: {:?}", file_path))?
                    .to_string();

                // Read JSON content
                let file_content = fs::read_to_string(&file_path).map_err(|e| {
                    format!("Failed to read JSON file {}: {}", file_path.display(), e)
                })?;

                // Parse JSON
                let json_value: Value = serde_json::from_str(&file_content).map_err(|e| {
                    format!("Failed to parse JSON from {}: {}", file_path.display(), e)
                })?;

                info!("✅ Loaded workflow: {}", filename);
                workflows.insert(filename, json_value);
            }
        }

        info!("📦 Successfully loaded {} workflow(s)", workflows.len());
        Ok(workflows)
    }
}

use crate::proxy::{ProxyError, ProxyState, handle_request_error, handle_timeout_error};

// =================================================================================
//  Section: Request Handling (Main Logic)
// =================================================================================

/// Handles OpenAI image generation requests and proxies them to ComfyUI
///
/// This is the core logic entry point for image generation. The flow is as follows:
/// 1. Read the OpenAI format request body.
/// 2. Convert it to ComfyUI prompt format (injecting parameters into the workflow).
/// 3. Send the task to the backend (`/prompt`).
/// 4. Wait for task completion via WebSocket.
/// 5. Retrieve the generated images and encode them as Base64.
/// 6. Return the response in OpenAI format.
pub async fn generations_response(
    State(state): State<Arc<ProxyState>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
    req_id: &str,
) -> Result<AxumResponse, ProxyError> {
    // Construct backend ComfyUI prompt submission URL
    let target_base: String = format!("{}:{}", state.backend_url, state.backend_port);
    let method = Method::POST;
    let target_url: String = format!("http://{}/prompt", target_base);

    info!("┃ [STEP 1/5] 📥 Processing Request [{}]", req_id);
    debug!("┃ [{}] 🎯 Proxying {} / -> {}", req_id, method, target_url);

    // Construct query string (rarely used, kept for compatibility)
    let query_string = if params.is_empty() {
        String::new()
    } else {
        let mut query = String::with_capacity(256);
        query.push('?');
        for (i, (k, v)) in params.iter().enumerate() {
            if i > 0 {
                query.push('&');
            }
            query.push_str(k);
            query.push('=');
            query.push_str(v);
        }
        query
    };
    let full_url = format!("{}{}", target_url, query_string);

    // Read request body, limiting max size
    let body_bytes = match axum::body::to_bytes(body, state.max_payload_size_mb * 1024 * 1024).await
    {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("❌ Failed to read body: {}", e);
            return Err(ProxyError::Internal(format!(
                "Failed to read request body: {}",
                e
            )));
        }
    };

    // Transform OpenAI API request format to ComfyUI format
    let processed_body = if !body_bytes.is_empty() {
        debug!("┃ [{}] 🔧 Generating comfyui request body...", req_id);
        match create_json_payload(
            body_bytes,
            state.workflows.clone(),
            state.backend_client_id.clone(),
            req_id,
        )
        .await
        {
            Ok(modified) => {
                debug!("┃ [{}] ✅ Body modified successfully", req_id);
                modified
            }
            Err(e) => {
                warn!("┃ [{}] ❌ Failed to modify body: {:?}", req_id, e);
                return Err(e);
            }
        }
    } else {
        body_bytes
    };

    // Prepare HTTP Headers for the backend
    let mut upstream_headers = reqwest::header::HeaderMap::new();

    // Set Content-Type for JSON payload
    if !processed_body.is_empty() {
        upstream_headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
    }

    // Forward Authorization headers if present
    if let Some(auth) = headers.get("authorization") {
        if let Ok(auth_value) = reqwest::header::HeaderValue::from_bytes(auth.as_bytes()) {
            upstream_headers.insert(reqwest::header::AUTHORIZATION, auth_value);
        }
    }

    debug!(
        "┃ [{}] 🚀 Requesting ComfyUI /prompt ({} bytes)",
        req_id,
        processed_body.len()
    );

    // Build the request to the backend
    let request_builder = state
        .client
        .request(method.clone(), &full_url)
        .headers(upstream_headers)
        .body(processed_body);

    // Send request (with timeout protection)
    let request_future = request_builder.send();
    let timeout_duration = Duration::from_secs(state.timeout);

    // Execute request
    let upstream_response = match tokio::time::timeout(timeout_duration, request_future).await {
        Ok(Ok(response)) => {
            debug!(
                "┃ [{}] ✅ Got response from backend: {} - Headers: {:?}",
                req_id,
                response.status(),
                response.headers()
            );
            response
        }
        Ok(Err(e)) => {
            return Err(handle_request_error(e, &full_url));
        }
        Err(_) => {
            return Err(handle_timeout_error(&full_url, timeout_duration));
        }
    };

    // Detect response type (Streaming not yet supported)
    let _is_streaming = upstream_response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.contains("text/event-stream")
                || v.contains("application/x-ndjson")
                || v.contains("text/plain")
        })
        .unwrap_or(false);

    // Handle ComfyUI response
    handle_regular_response(
        upstream_response,
        target_base,
        headers,
        state.use_ws,
        &state.client,
        &state.ws_manager,
        req_id,
    )
    .await
}

/// Handles OpenAI video generation requests and proxies them to ComfyUI
///
/// This mirrors `generations_response` but returns video assets in OpenAI-like format
/// through the `/v1/videos/generations` endpoint.
pub async fn video_generations_response(
    State(state): State<Arc<ProxyState>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
    req_id: &str,
) -> Result<AxumResponse, ProxyError> {
    let target_base: String = format!("{}:{}", state.backend_url, state.backend_port);
    let method = Method::POST;
    let target_url: String = format!("http://{}/prompt", target_base);

    info!("┃ [STEP 1/5] 📥 Processing OpenAI video request...");

    let body_bytes = axum::body::to_bytes(body, state.max_payload_size_mb * 1024 * 1024)
        .await
        .map_err(|e| ProxyError::Internal(format!("Failed to read request body: {}", e)))?;

    let transformed_body = create_json_payload(
        body_bytes,
        state.workflows.clone(),
        state.backend_client_id.clone(),
        req_id,
    )
    .await?;

    let mut upstream_headers = reqwest::header::HeaderMap::new();
    for (name, value) in headers.iter() {
        if name.as_str().eq_ignore_ascii_case("host")
            || name.as_str().eq_ignore_ascii_case("content-length")
        {
            continue;
        }
        if let (Ok(req_name), Ok(req_value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            upstream_headers.insert(req_name, req_value);
        }
    }

    if let Some(content_type) = reqwest::header::HeaderValue::from_str("application/json").ok() {
        upstream_headers.insert(reqwest::header::CONTENT_TYPE, content_type);
    }

    let request_builder = state
        .client
        .request(method, &target_url)
        .headers(upstream_headers)
        .query(&params)
        .body(transformed_body);

    let timeout_duration = Duration::from_secs(state.timeout);
    let request_future = request_builder.send();

    let upstream_response = match tokio::time::timeout(timeout_duration, request_future).await {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => return Err(handle_request_error(e, &target_url)),
        Err(_) => return Err(handle_timeout_error(&target_url, timeout_duration)),
    };

    handle_video_response(
        upstream_response,
        target_base,
        headers,
        state.use_ws,
        &state.client,
        &state.ws_manager,
        req_id,
    )
    .await
}

// =================================================================================
//  Section: Payload Transformation
// =================================================================================

/// Transforms OpenAI API request into ComfyUI prompt request
///
/// This function executes the key translation layer between OpenAI image generation specs
/// and ComfyUI workflows.
///
/// # Mapping Logic
/// - `model` (OpenAI) -> Workflow Name (ComfyUI) - Used to look up specific workflow JSON
/// - `prompt` (OpenAI) -> Positive Prompt (ComfyUI node input)
/// - `negative_prompt` (OpenAI Extension) -> Negative Prompt
/// - `seed` (OpenAI) -> Random Seed (KSampler node)
/// - `size` (OpenAI) -> Image Size (EmptyLatentImage node)
/// - `n` (OpenAI) -> Batch Size / Number of images
///
/// # Arguments
/// * `body` - Original request body from OpenAI API call (Bytes)
/// * `workflows` - Map of available ComfyUI workflow definitions
/// * `client_id` - Client ID used for WebSocket pairing
///
/// # Returns
/// - Serialized ComfyUI prompt JSON (Bytes), ready to be submitted to backend
/// - `ProxyError`: If validation fails or transformation errors occur
async fn create_json_payload(
    body: Bytes,
    workflows: Arc<HashMap<String, Value>>,
    client_id: String,
    req_id: &str,
) -> Result<Bytes, ProxyError> {
    // Return empty if body is empty
    if body.is_empty() {
        return Ok(body);
    }
    let mut rng = rand::rng();

    // Parse input OpenAI format JSON
    let json: Value = serde_json::from_slice(&body)
        .map_err(|e| ProxyError::Json(format!("Failed to parse JSON: {}", e)))?;

    // Initialize ComfyUI request structure
    let mut workflow_use = serde_json::json!({
        "prompt": "",
        "client_id": ""
    });

    // Process OpenAI request fields
    if let Some(openai_request) = json.as_object() {
        // Extract and validate model/workflow name
        if let Some(model_name) = openai_request.get("model").and_then(|v| v.as_str()) {
            if let Some(workflow) = workflows.get(model_name) {
                // debug!("┃ [{}] 📦 Retrieved workflow '{}'", req_id, model_name); // Reduce noise
                info!("┃ [STEP 2/5] 🧩 Loading Workflow: '{}'", model_name);
                // Use selected workflow as the base prompt graph
                workflow_use["prompt"] = workflow.clone();
            } else {
                return Err(ProxyError::Json(format!(
                    "Workflow '{}' not found",
                    model_name
                )));
            }
        } else {
            return Err(ProxyError::Json(format!(
                "Failed to get model name from JSON"
            )));
        }

        // Modify parameters in the workflow
        if let Some(obj) = workflow_use.as_object_mut() {
            // Set Client ID for WebSocket pairing
            obj.insert("client_id".to_string(), Value::String(client_id.clone()));

            // Iterate and modify workflow nodes, injecting OpenAI request parameters
            if let Some(workflow_prompt) = workflow_use
                .get_mut("prompt")
                .and_then(|v| v.as_object_mut())
            {
                for (_node_id, node_data) in workflow_prompt {
                    if let Some(class_type_ref) = node_data["class_type"].as_str() {
                        let class_type = class_type_ref.to_string();
                        match class_type.as_str() {
                            // Handle Random Seed (Schema: KSampler)
                            "KSampler" | "easy seed" => {
                                if let Some(inputs_data_sampler) =
                                    node_data["inputs"].as_object_mut()
                                {
                                    // Inject Seed
                                    if let Some(seed_data) =
                                        openai_request.get("seed").and_then(|v| v.as_i64())
                                    {
                                        debug!("┃ [{}] ✏️ Requested seed: {}", req_id, seed_data);
                                        inputs_data_sampler.insert(
                                            "seed".to_string(),
                                            serde_json::json!(seed_data),
                                        );
                                    } else {
                                        // Generate random seed if not requested
                                        let random_number: u64 = rng.random_range(0..1_000_000_000);
                                        debug!(
                                            "┃ [{}] No seed in JSON, using random seed: {}",
                                            req_id, random_number
                                        );
                                        inputs_data_sampler.insert(
                                            "seed".to_string(),
                                            serde_json::json!(random_number),
                                        );
                                    }
                                }
                            }
                            // Handle Image Size and Batch Size
                            "EmptyLatentImage"
                            | "EmptySD3LatentImage"
                            | "EmptyFlux2LatentImage"
                            | "EmptyHunyuanLatentVideo"
                            | "VHS_VideoCombine" => {
                                if let Some(inputs_data_size) = node_data["inputs"].as_object_mut()
                                {
                                    // Parse and set image size (Format: WIDTHxHEIGHT)
                                    if let Some(size_data) =
                                        openai_request.get("size").and_then(|v| v.as_str())
                                    {
                                        debug!(
                                            "┃ [{}] ✏️ Requested image size: {}",
                                            req_id, size_data
                                        );
                                        let size_data_split: Vec<i64> = size_data
                                            .split('x')
                                            .map(|p| p.parse().unwrap_or(512))
                                            .collect();

                                        inputs_data_size.insert(
                                            "width".to_string(),
                                            serde_json::json!(
                                                size_data_split.get(0).unwrap_or(&1024).clone()
                                            ),
                                        );
                                        inputs_data_size.insert(
                                            "height".to_string(),
                                            serde_json::json!(
                                                size_data_split.get(1).unwrap_or(&1024).clone()
                                            ),
                                        );
                                    }

                                    // Set batch_size (number of images)
                                    if let Some(copies_num_data) =
                                        openai_request.get("n").and_then(|v| v.as_i64())
                                    {
                                        debug!(
                                            "┃ [{}] ✏️ Requested n (batch_size): {}",
                                            req_id, copies_num_data
                                        );
                                        inputs_data_size.insert(
                                            "batch_size".to_string(),
                                            serde_json::json!(copies_num_data),
                                        );
                                    }
                                }
                            }
                            // Handle Text Prompts
                            "CLIPTextEncode" | "CR Text" | "easy promptLine" => {
                                // Check if it's Positive or Negative prompt node
                                if let Some(meta_data) = node_data["_meta"].as_object() {
                                    if let Some(title) = meta_data["title"].as_str() {
                                        // Inject Positive Prompt
                                        // Matches common node titles for positive prompts (English & Chinese)
                                        if title == "Positive Prompt"
                                            || title == "CLIP文本编码"
                                            || title == "🔤 CR Text"
                                            || title == "提示词行"
                                        {
                                            if let Some(inputs_data) =
                                                node_data["inputs"].as_object_mut()
                                            {
                                                if let Some(prompt_input) = openai_request
                                                    .get("prompt")
                                                    .and_then(|v| v.as_str())
                                                {
                                                    debug!(
                                                        "┃ [{}] ✏️ Requested prompt: {}",
                                                        req_id, prompt_input
                                                    );

                                                    // Determine field name based on class_type
                                                    let key = match class_type.as_str() {
                                                        "easy promptLine" => "prompt",
                                                        _ => "text",
                                                    };

                                                    inputs_data.insert(
                                                        key.to_string(),
                                                        Value::String(prompt_input.to_string()),
                                                    );
                                                } else {
                                                    return Err(ProxyError::Json(format!(
                                                        "Failed to get prompt from JSON"
                                                    )));
                                                }
                                            }
                                        }
                                        // Inject Negative Prompt (Optional)
                                        // Matches common node titles for negative prompts
                                        else if title == "Negative Prompt" || title == "条件零化"
                                        {
                                            if let Some(inputs_data) =
                                                node_data["inputs"].as_object_mut()
                                            {
                                                if let Some(neg_prompt_input) = openai_request
                                                    .get("negative_prompt")
                                                    .and_then(|v| v.as_str())
                                                {
                                                    debug!(
                                                        "┃ [{}] ✏️ Requested negative prompt: {}",
                                                        req_id, neg_prompt_input
                                                    );
                                                    inputs_data.insert(
                                                        "text".to_string(),
                                                        Value::String(neg_prompt_input.to_string()),
                                                    );
                                                } else {
                                                    debug!(
                                                        "┃ [{}] No negative_prompt in JSON, using workflow default",
                                                        req_id
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // Skip irrelevant nodes
                            _ => continue,
                        }
                    }
                }
            }
        }

        // info!("┃ [{}] 🔧 JSON payload generated", req_id);
    }

    // Serialize modified ComfyUI prompt to JSON bytes
    let modified_json = serde_json::to_vec(&workflow_use)
        .map_err(|e| ProxyError::Json(format!("Failed to serialize JSON: {}", e)))?;

    Ok(Bytes::from(modified_json))
}

// =================================================================================
//  Section: Response Handling & Image Retrieval
// =================================================================================

/// Handles the response from ComfyUI backend
///
/// This function:
/// 1. Extracts the `prompt_id` from the backend response.
/// 2. Waits for job completion via WebSocket.
/// 3. Retrieves generated images from the backend.
/// 4. Encodes images as base64.
/// 5. Returns response in OpenAI API format.
async fn handle_regular_response(
    upstream_response: reqwest::Response,
    target_base: String,
    _headers: HeaderMap,
    use_ws: bool,
    client: &Client,
    ws_manager: &Option<Arc<WebSocketManager>>,
    req_id: &str,
) -> Result<AxumResponse, ProxyError> {
    let status = upstream_response.status();
    let headers = upstream_response.headers().clone();

    debug!(
        "┃ [{}] 📄 Handling regular response with status: {}",
        req_id, status
    );

    // Read response body from backend
    let body_bytes = upstream_response
        .bytes()
        .await
        .map_err(|e| ProxyError::Upstream(format!("Failed to read response body: {}", e)))?;

    debug!(
        "┃ [{}] 📥 Response body: {} bytes",
        req_id,
        body_bytes.len()
    );

    // Parse backend response as JSON
    let json: Value = serde_json::from_slice(&body_bytes)
        .map_err(|e| ProxyError::Json(format!("Failed to parse JSON: {}", e)))?;

    debug!("┃ [{}] 📝 Received response: {}", req_id, json.to_string());

    // Extract the prompt_id which identifies this job in ComfyUI
    let prompt_id = json.get("prompt_id").and_then(|v| v.as_str());
    if let Some(pid) = prompt_id {
        debug!(
            "┃ [{}] 📝 Found prompt_id in response: {}. Waiting for job completion...",
            req_id, pid
        );
        info!("┃ [STEP 3/5] ⏳ Waiting for Job Completion (ID: {})", pid);

        // Block until the job completes via WebSocket
        if use_ws {
            if let Some(manager) = ws_manager {
                match timeout(
                    Duration::from_secs(600),
                    manager.wait_for_job_completion(pid),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        warn!("┃ [{}] ⚠️ Failed to wait for job completion: {}", req_id, e)
                    }
                    Err(_) => warn!(
                        "┃ [{}] ⚠️ Job completion wait timed out after 600 seconds",
                        req_id
                    ),
                }
            } else {
                warn!(
                    "┃ [{}] ⚠️ WebSocket manager is not initialized but use_ws is true",
                    req_id
                );
            }
        } else {
            loop {
                // Poll the queue until the job is finished (not in queue)
                let is_done = check_queue(
                    target_base.clone(),
                    Some(pid),
                    headers.clone(),
                    client,
                    req_id,
                )
                .await?;

                if is_done {
                    debug!(
                        "┃ [{}] ⚡ Job {} completed (not found in queue)",
                        req_id, pid
                    );
                    break;
                }

                tokio::time::sleep(Duration::from_millis(2000)).await;
            }
        }
    }

    // Fetch generated images from ComfyUI backend and prepare response
    let image_response_json =
        retrieve_image_from_history(target_base, prompt_id, headers.clone(), client, req_id)
            .await?;

    // Serialize the image response JSON to bytes
    let output_json = serde_json::to_vec(&image_response_json)
        .map_err(|e| ProxyError::Json(format!("Failed to serialize JSON: {}", e)))?;
    let output_body_bytes = Bytes::from(output_json);
    debug!(
        "┃ [{}] ✏️ JSON regular response: {} bytes",
        req_id,
        output_body_bytes.len()
    );

    // Copy and update response headers
    let mut response_headers = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers.iter() {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_str(name.as_str()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            if name.as_str() == "content-length" {
                // Update content-length to match the final response size
                if let Ok(value) =
                    HeaderValue::from_str(format!("{}", output_body_bytes.len()).as_str())
                {
                    response_headers.insert(name, value);
                }
            } else {
                response_headers.insert(name, value);
            }
        }
    }

    // Build the final HTTP response
    let mut response = AxumResponse::builder().status(status.as_u16());

    for (name, value) in response_headers.iter() {
        response = response.header(name, value);
    }

    debug!("┃ [{}] ✅ Regular response built successfully", req_id);

    response
        .body(Body::from(output_body_bytes))
        .map_err(|e| ProxyError::Internal(format!("Failed to build regular response: {}", e)))
}

/// Handles the response from ComfyUI backend and returns video assets
async fn handle_video_response(
    upstream_response: reqwest::Response,
    target_base: String,
    _headers: HeaderMap,
    use_ws: bool,
    client: &Client,
    ws_manager: &Option<Arc<WebSocketManager>>,
    req_id: &str,
) -> Result<AxumResponse, ProxyError> {
    let status = upstream_response.status();
    let headers = upstream_response.headers().clone();

    let body_bytes = upstream_response
        .bytes()
        .await
        .map_err(|e| ProxyError::Upstream(format!("Failed to read response body: {}", e)))?;

    let json: Value = serde_json::from_slice(&body_bytes)
        .map_err(|e| ProxyError::Json(format!("Failed to parse JSON: {}", e)))?;

    let prompt_id = json.get("prompt_id").and_then(|v| v.as_str());
    if let Some(pid) = prompt_id {
        info!(
            "┃ [STEP 3/5] ⏳ Waiting for Video Job Completion (ID: {})",
            pid
        );
        if use_ws {
            if let Some(manager) = ws_manager {
                match timeout(
                    Duration::from_secs(600),
                    manager.wait_for_job_completion(pid),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => warn!(
                        "┃ [{}] ⚠️ Failed to wait for video completion: {}",
                        req_id, e
                    ),
                    Err(_) => warn!(
                        "┃ [{}] ⚠️ Video completion wait timed out after 600 seconds",
                        req_id
                    ),
                }
            }
        } else {
            loop {
                let is_done = check_queue(
                    target_base.clone(),
                    Some(pid),
                    headers.clone(),
                    client,
                    req_id,
                )
                .await?;
                if is_done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2000)).await;
            }
        }
    }

    let video_response_json =
        retrieve_video_from_history(target_base, prompt_id, headers.clone(), client, req_id)
            .await?;

    let output_json = serde_json::to_vec(&video_response_json)
        .map_err(|e| ProxyError::Json(format!("Failed to serialize JSON: {}", e)))?;
    let output_body_bytes = Bytes::from(output_json);

    let mut response_headers = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers.iter() {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_str(name.as_str()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            if name.as_str() == "content-length" {
                if let Ok(value) =
                    HeaderValue::from_str(format!("{}", output_body_bytes.len()).as_str())
                {
                    response_headers.insert(name, value);
                }
            } else {
                response_headers.insert(name, value);
            }
        }
    }

    let mut response = AxumResponse::builder().status(status.as_u16());
    for (name, value) in response_headers.iter() {
        response = response.header(name, value);
    }

    response
        .body(Body::from(output_body_bytes))
        .map_err(|e| ProxyError::Internal(format!("Failed to build video response: {}", e)))
}

/// Retrieves generated videos from ComfyUI backend
async fn retrieve_video_from_history(
    target_base: String,
    prompt_id: Option<&str>,
    headers: HeaderMap,
    client: &Client,
    _req_id: &str,
) -> Result<Value, ProxyError> {
    let prompt_id = match prompt_id {
        Some(id) => id,
        None => return Err(ProxyError::Upstream("No prompt_id received.".to_string())),
    };

    let history_url: String = format!("http://{}/history/{}", target_base, prompt_id);

    let mut upstream_headers = reqwest::header::HeaderMap::new();
    if let Some(auth) = headers.get("authorization") {
        if let Ok(auth_value) = reqwest::header::HeaderValue::from_bytes(auth.as_bytes()) {
            upstream_headers.insert(reqwest::header::AUTHORIZATION, auth_value);
        }
    }

    let upstream_response = client
        .request(Method::GET, &history_url)
        .headers(upstream_headers.clone())
        .send()
        .await?;

    let response_body = upstream_response.bytes().await.map_err(|e| {
        ProxyError::Upstream(format!("Failed to read history response body: {}", e))
    })?;
    let history_json: Value = serde_json::from_slice(&response_body)
        .map_err(|e| ProxyError::Json(format!("Failed to parse history JSON: {}", e)))?;

    let mut image_files: Vec<ImageFile> = Vec::new();
    if let Some(prompt_history) = history_json.get(prompt_id).and_then(|v| v.as_object()) {
        if let Some(outputs) = prompt_history.get("outputs").and_then(|v| v.as_object()) {
            for (_node_id, out_node_data) in outputs {
                for key in ["gifs", "videos", "images"] {
                    if let Some(all_assets) = out_node_data.get(key).and_then(|v| v.as_array()) {
                        for asset_data in all_assets {
                            let type_field = asset_data["type"].as_str().unwrap_or("output");
                            if type_field == "output" {
                                if let Some(filename) = asset_data["filename"].as_str() {
                                    let subfolder = asset_data["subfolder"].as_str().unwrap_or("");
                                    image_files.push(ImageFile {
                                        filename: filename.to_string(),
                                        subfolder: subfolder.to_string(),
                                        type_field: type_field.to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut response_data: Vec<serde_json::Value> = Vec::new();
    for image_file_data in image_files {
        let view_query = serde_urlencoded::to_string(&image_file_data).map_err(|e| {
            ProxyError::Json(format!("Failed to serialize video data query: {}", e))
        })?;

        let view_url: String = format!("http://{}/view?{}", target_base, view_query);
        let view_response = client
            .request(Method::GET, &view_url)
            .headers(upstream_headers.clone())
            .send()
            .await?;

        let asset_bytes = view_response.bytes().await?;
        let b64_asset = general_purpose::STANDARD.encode(asset_bytes);
        let mime_type = if image_file_data.filename.ends_with(".mp4") {
            "video/mp4"
        } else if image_file_data.filename.ends_with(".webm") {
            "video/webm"
        } else if image_file_data.filename.ends_with(".gif") {
            "image/gif"
        } else {
            "application/octet-stream"
        };

        response_data.push(serde_json::json!({
            "b64_json": b64_asset,
            "mime_type": mime_type
        }));
    }

    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProxyError::Internal("Failed to get current time".to_string()))?
        .as_secs() as i64;

    Ok(serde_json::json!({
        "data": response_data,
        "created": created
    }))
}

/// Retrieves generated images from ComfyUI backend
///
/// This function:
/// 1. Queries the ComfyUI history endpoint for the given prompt_id.
/// 2. Extracts image metadata from the job outputs.
/// 3. Downloads each image from the view endpoint.
/// 4. Base64 encodes the images.
/// 5. Formats the response in OpenAI API standards.
///
/// # Arguments
/// * `target_base` - ComfyUI backend address (host:port)
/// * `prompt_id` - The job ID to retrieve results for
/// * `headers` - Original request headers (may contain auth)
/// * `client` - HTTP client for backend communication
///
/// # Returns
/// - JSON response in OpenAI image generation format with base64 encoded images.
/// - `ProxyError` if history lookup or image retrieval fails.
async fn retrieve_image_from_history(
    target_base: String,
    prompt_id: Option<&str>,
    headers: HeaderMap,
    client: &Client,
    req_id: &str,
) -> Result<Value, ProxyError> {
    // Validate that we have a prompt_id
    let prompt_id = match prompt_id {
        Some(id) => id,
        None => {
            error!("⚠️ No prompt_id received!");
            return Err(ProxyError::Upstream(format!("No prompt_id received.",)));
        }
    };

    // Construct URL to ComfyUI history endpoint
    let history_url: String = format!("http://{}/history/{}", target_base, prompt_id);

    info!(
        "┃ [STEP 4/5] 🔍 Checking History/Validation (ID: {})",
        prompt_id
    );
    debug!(
        "┃ [{}] 🔍 Checking history at {} for {}",
        req_id, target_base, prompt_id
    );

    // Prepare headers for backend requests
    let mut upstream_headers = reqwest::header::HeaderMap::new();

    // Forward authorization headers if present
    if let Some(auth) = headers.get("authorization") {
        if let Ok(auth_value) = reqwest::header::HeaderValue::from_bytes(auth.as_bytes()) {
            upstream_headers.insert(reqwest::header::AUTHORIZATION, auth_value);
        }
    }

    // Build request to history endpoint
    let request_builder = client
        .request(Method::GET, &history_url)
        .headers(upstream_headers.clone());

    // Query history with timeout protection
    let request_future = request_builder.send();
    let timeout_duration = Duration::from_secs(5);

    let upstream_response = match tokio::time::timeout(timeout_duration, request_future).await {
        Ok(Ok(response)) => {
            debug!(
                "┃ [{}] ✅ Got response from history backend: {} - Headers: {:?}",
                req_id,
                response.status(),
                response.headers()
            );
            response
        }
        Ok(Err(e)) => {
            return Err(handle_request_error(e, &history_url));
        }
        Err(_) => {
            return Err(handle_timeout_error(&history_url, timeout_duration));
        }
    };

    // Parse history response
    let response_body = upstream_response.bytes().await.map_err(|e| {
        ProxyError::Upstream(format!("Failed to read history response body: {}", e))
    })?;
    let history_json: Value = serde_json::from_slice(&response_body)
        .map_err(|e| ProxyError::Json(format!("Failed to parse history JSON: {}", e)))?;

    // Extract image metadata from job outputs
    let mut image_files: Vec<ImageFile> = Vec::new();
    if let Some(prompt_hist) = history_json.get(prompt_id).and_then(|v| v.as_object()) {
        if let Some(out_nodes) = prompt_hist.get("outputs").and_then(|v| v.as_object()) {
            // Iterate through output nodes looking for generated images
            for (_node_id, out_node_data) in out_nodes {
                if let Some(all_images_data) =
                    out_node_data.get("images").and_then(|v| v.as_array())
                {
                    // Process each image in the node output
                    for image_data in all_images_data {
                        if let Some(type_field) = image_data["type"].as_str() {
                            // Only include output images (not temporary/preview)
                            if type_field == "output" {
                                if let Some(filename) = image_data["filename"].as_str() {
                                    let subfolder = image_data["subfolder"].as_str().unwrap_or("");
                                    debug!(
                                        "┃ [{}] Found image: {} (subfolder: {})",
                                        req_id, filename, subfolder
                                    );
                                    image_files.push(ImageFile {
                                        filename: filename.to_string(),
                                        subfolder: subfolder.to_string(),
                                        type_field: type_field.to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    } else {
        error!("┃ [{}] ⚠️ No prompt_id history found", req_id);
        return Err(ProxyError::Upstream(
            format!("No prompt_id history found.",),
        ));
    }

    debug!(
        "┃ [{}] 📦 Collected {} image files",
        req_id,
        image_files.len()
    );
    let mut response_data: Vec<serde_json::Value> = Vec::new();

    // Download and encode each generated image
    info!("┃ [STEP 5/5] ⬇️  Retrieving & Encoding Images");
    for image_file_data in image_files {
        // Construct query parameters for the view endpoint
        let view_query = serde_urlencoded::to_string(&image_file_data).map_err(|e| {
            ProxyError::Json(format!("Failed to serialize image data query: {}", e))
        })?;

        let view_url: String = format!("http://{}/view?{}", target_base, view_query);

        // Build request to image view endpoint
        let request_builder = client
            .request(Method::GET, &view_url)
            .headers(upstream_headers.clone());

        debug!(
            "┃ [{}] ⏳ Sending view request to backend: {}",
            req_id, view_query
        );

        // Download image with timeout
        let request_future = request_builder.send();
        let timeout_duration = Duration::from_secs(5);

        let view_response = match tokio::time::timeout(timeout_duration, request_future).await {
            Ok(Ok(response)) => {
                debug!(
                    "┃ [{}] ✅ Got response from view backend: {} - Headers: {:?}",
                    req_id,
                    response.status(),
                    response.headers()
                );
                response
            }
            Ok(Err(e)) => {
                return Err(handle_request_error(e, &view_url));
            }
            Err(_) => {
                return Err(handle_timeout_error(&view_url, timeout_duration));
            }
        };

        // Read image bytes and encode as base64
        let image_bytes = view_response.bytes().await?;
        debug!("┃ [{}] 📋 Read {} image bytes", req_id, image_bytes.len());
        let b64_image = general_purpose::STANDARD.encode(image_bytes);
        response_data.push(serde_json::json!({
            "b64_json": b64_image
        }));
    }

    // Create OpenAI API format response with timestamp
    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ProxyError::Internal("Failed to get current time".to_string()))?
        .as_secs() as i64;

    Ok(serde_json::json!({
        "data": response_data,
        "created": created
    }))
}

// =================================================================================
//  Section: Queue Management
// =================================================================================

/// Checks a job state from ComfyUI backend
///
/// This function:
/// 1. Queries the ComfyUI queue endpoint for the given prompt_id.
/// 2. Looks for the given job ID in both queues (pending and running).
/// 3. Returns true if not found (implying the job is finished).
///
/// # Arguments
/// * `target_base` - ComfyUI backend address (host:port)
/// * `prompt_id` - The job ID to check
/// * `headers` - Original request headers (may contain auth)
/// * `client` - HTTP client for backend communication
///
/// # Returns
/// - `true` if the given job id is NOT in the queues (hence finished).
/// - `ProxyError` if queue lookup fails.
async fn check_queue(
    target_base: String,
    prompt_id: Option<&str>,
    headers: HeaderMap,
    client: &Client,
    req_id: &str,
) -> Result<bool, ProxyError> {
    // Validate that we have a prompt_id
    let prompt_id = match prompt_id {
        Some(id) => id,
        None => {
            error!("⚠️ No prompt_id received!");
            return Err(ProxyError::Upstream(format!("No prompt_id received.",)));
        }
    };

    // Construct URL to ComfyUI history endpoint
    let history_url: String = format!("http://{}/queue", target_base);

    debug!("┃ [{}] 🔍 Checking queue at {}", req_id, target_base);

    // Prepare headers for backend requests
    let mut upstream_headers = reqwest::header::HeaderMap::new();

    // Forward authorization headers if present
    if let Some(auth) = headers.get("authorization") {
        if let Ok(auth_value) = reqwest::header::HeaderValue::from_bytes(auth.as_bytes()) {
            upstream_headers.insert(reqwest::header::AUTHORIZATION, auth_value);
        }
    }

    // Log headers for debugging
    debug!("┃ [{}] 📋 Headers to send (if any):", req_id);
    for (name, value) in upstream_headers.iter() {
        debug!(
            "┃ [{}]    {}: {}",
            req_id,
            name,
            value.to_str().unwrap_or("[unprintable]")
        );
    }

    // Build request to history endpoint
    let request_builder = client
        .request(Method::GET, &history_url)
        .headers(upstream_headers.clone());

    debug!("┃ [{}] ⏳ Sending queue request to backend...", req_id);

    // Query history with timeout protection
    let request_future = request_builder.send();
    let timeout_duration = Duration::from_secs(5);

    let upstream_response = match tokio::time::timeout(timeout_duration, request_future).await {
        Ok(Ok(response)) => {
            debug!(
                "┃ [{}] ✅ Got response from queue backend: {} - Headers: {:?}",
                req_id,
                response.status(),
                response.headers()
            );
            response
        }
        Ok(Err(e)) => {
            return Err(handle_request_error(e, &history_url));
        }
        Err(_) => {
            return Err(handle_timeout_error(&history_url, timeout_duration));
        }
    };

    // Parse history response
    let response_body = upstream_response
        .bytes()
        .await
        .map_err(|e| ProxyError::Upstream(format!("Failed to read queue response body: {}", e)))?;
    let queu_json: Value = serde_json::from_slice(&response_body)
        .map_err(|e| ProxyError::Json(format!("Failed to parse queue JSON: {}", e)))?;

    // Check running queue
    if let Some(queue_running) = queu_json.get("queue_running").and_then(|v| v.as_array()) {
        for queue_elem in queue_running.iter() {
            if queue_elem[1] == prompt_id {
                return Ok(false);
            }
        }
    }
    // Check pending queue
    if let Some(queue_pending) = queu_json.get("queue_pending").and_then(|v| v.as_array()) {
        for queue_elem in queue_pending.iter() {
            if queue_elem[1] == prompt_id {
                return Ok(false);
            }
        }
    }

    Ok(true)
}
