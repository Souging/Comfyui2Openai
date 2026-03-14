//! ComfyUI Backend WebSocket Connection Manager
//!
//! This module manages a persistent WebSocket connection to the ComfyUI backend,
//! used for tracking task execution progress and completion status. Tasks are uniquely identified by `prompt_id`.
//! The manager uses a Circular Buffer to cache recently completed tasks, preventing infinite memory growth.
//!
//! The connection includes an automatic reconnection mechanism (exponential backoff strategy) to handle temporary network fluctuations or connection hangs.

// use axum::http::HeaderMap; (removed unused)
use log::{debug, error, info, warn};
// use reqwest::Client; (removed unused)
use futures::stream::{SplitStream, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Maximum number of completed job IDs to keep in memory
/// Uses a circular buffer to prevent infinite memory growth
const COMPLETED_JOBS_CAPACITY: usize = 100;

/// Reconnection configuration constants
/// Controls retry behavior when WebSocket connection fails
const INITIAL_RECONNECT_DELAY_SECS: u64 = 2;
const MAX_RECONNECT_DELAY_SECS: u64 = 60;
const RECONNECT_BACKOFF_MULTIPLIER: f64 = 1.5;

/// A circular buffer for efficiently tracking recently completed task IDs
///
/// Stores up to `COMPLETED_JOBS_CAPACITY` job IDs in a fixed-size buffer.
/// When the buffer is full, new entries overwrite the oldest ones.
/// This design allows us to check if a task completed recently without memory leaks.
struct CompletedJobsBuffer {
    /// Fixed-size vector of completed job IDs
    jobs: Vec<String>,
    /// Current write position in the circular buffer
    index: usize,
    /// Flag indicating if the buffer has been filled at least once (start overwriting)
    is_full: bool,
}

impl CompletedJobsBuffer {
    /// Creates a new empty circular buffer
    fn new() -> Self {
        CompletedJobsBuffer {
            jobs: Vec::with_capacity(COMPLETED_JOBS_CAPACITY),
            index: 0,
            is_full: false,
        }
    }

    /// Adds a completed job ID to the buffer
    ///
    /// If buffer is not full, appends to the end.
    /// If full, overwrites the oldest entry and advances the index.
    fn add(&mut self, job_id: String) {
        if self.jobs.len() < COMPLETED_JOBS_CAPACITY {
            // Buffer not full, append directly
            self.jobs.push(job_id);
        } else {
            // Buffer full, overwrite oldest entry
            self.is_full = true;
            self.jobs[self.index] = job_id;
            self.index = (self.index + 1) % COMPLETED_JOBS_CAPACITY;
        }
    }

    /// Checks if a job ID is in the completed buffer
    ///
    /// Returns true if job_id is found.
    /// Note: Only tracks recent tasks (up to COMPLETED_JOBS_CAPACITY).
    fn contains(&self, job_id: &str) -> bool {
        self.jobs.iter().any(|id| id == job_id)
    }
}

/// Manages the persistent WebSocket connection to the ComfyUI backend
///
/// Maintains a connection to `ws://backend:port/ws?clientId=X` for receiving
/// task execution progress and completion messages. Uses a background task to process incoming messages and update task status.
///
/// Includes automatic reconnection with exponential backoff to handle temporary network issues or hanging connections.
pub struct WebSocketManager {
    /// Circular buffer of recently completed job IDs (thread-safe)
    completed_jobs: Mutex<CompletedJobsBuffer>,
    /// Backend URL for reconnection attempts
    backend_url: String,
    /// Backend port for reconnection attempts
    backend_port: String,
    /// Client ID for reconnection attempts
    client_id: String,
}

impl WebSocketManager {
    /// Creates a new WebSocket manager and establishes connection to ComfyUI backend
    ///
    /// This function performs the following:
    /// 1. Connects to `ws://backend:port/ws?clientId=clientId`
    /// 2. Starts a background message listening task
    /// 3. Returns the manager wrapped in Arc for thread-safe sharing
    ///
    /// # Arguments
    /// * `backend_url` - ComfyUI backend host
    /// * `backend_port` - ComfyUI backend port
    /// * `client_id` - Unique client identifier for this connection
    ///
    /// # Returns
    /// - `Result<Arc<Self>, Error>`: Manager wrapped in Arc on success, or error on failure
    ///
    /// # Error Handling
    /// Connection is required for task tracking. If initialization fails, the program should likely exit.
    pub async fn new(
        backend_url: String,
        backend_port: String,
        client_id: String,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let ws_url = format!(
            "ws://{}:{}/ws?clientId={}",
            backend_url, backend_port, client_id
        );

        debug!("🔌 Connecting to backend WebSocket at: {}", ws_url);

        // Establish WebSocket connection
        let (ws_stream, _) = connect_async(&ws_url).await?;
        info!("✅ Connected to backend WebSocket");

        // Split stream into write and read halves (currently only using read)
        let (_write, read) = ws_stream.split();

        let manager = Arc::new(WebSocketManager {
            completed_jobs: Mutex::new(CompletedJobsBuffer::new()),
            backend_url: backend_url.clone(),
            backend_port: backend_port.clone(),
            client_id: client_id.clone(),
        });

        // Start background task to listen for WebSocket messages
        // This task runs concurrently and updates the completed_jobs buffer
        // It includes reconnection logic to handle disconnects or hangs
        let manager_clone = Arc::clone(&manager);
        tokio::spawn(WebSocketManager::message_listener(read, manager_clone));

        Ok(manager)
    }

    /// Background task listening for WebSocket messages from ComfyUI
    ///
    /// Processes incoming messages, looking for "executing" events with `null` node values,
    /// which indicate task completion. When a task completes, it updates the `completed_jobs` buffer.
    ///
    /// ComfyUI WebSocket message format example:
    /// ```json
    /// {
    ///   "type": "executing",
    ///   "data": {
    ///     "prompt_id": "...",
    ///     "node": null          // null = task completed, otherwise executing node ID
    ///   }
    /// }
    /// ```
    ///
    /// This task runs indefinitely and automatically reconnects if the connection is lost.
    /// Uses exponential backoff strategy to gracefully handle temporary network and server unavailability.
    async fn message_listener(
        mut read: SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
        manager: Arc<WebSocketManager>,
    ) {
        let mut retry_delay = Duration::from_secs(INITIAL_RECONNECT_DELAY_SECS);
        let mut retry_count = 0u32;

        loop {
            loop {
                // Use timeout detection to prevent hanging connections (no data sent) from waiting indefinitely
                match tokio::time::timeout(Duration::from_secs(60), read.next()).await {
                    Ok(Some(Ok(msg))) => {
                        // Received successful message, reset retry state
                        retry_count = 0;
                        retry_delay = Duration::from_secs(INITIAL_RECONNECT_DELAY_SECS);

                        // Only process text messages
                        if let Message::Text(text) = msg {
                            // Parse JSON message
                            if let Ok(json) = serde_json::from_str::<Value>(&text) {
                                if let Some(msg_type) = json.get("type").and_then(|v| v.as_str()) {
                                    // Look for execution status messages
                                    if msg_type == "executing" {
                                        if let Some(data) =
                                            json.get("data").and_then(|v| v.as_object())
                                        {
                                            if let Some(prompt_id) =
                                                data.get("prompt_id").and_then(|v| v.as_str())
                                            {
                                                // Check if node is null (indicates completion)
                                                if let Some(node) = data.get("node") {
                                                    if node.is_null() {
                                                        // Job completed
                                                        info!(
                                                            "✅ WebSocket: Job Completed for prompt_id: {}",
                                                            prompt_id
                                                        );
                                                        // Add to completed buffer
                                                        let mut jobs =
                                                            manager.completed_jobs.lock().await;
                                                        jobs.add(prompt_id.to_string());
                                                    } else {
                                                        // Node still executing
                                                    }
                                                } else {
                                                    // In some setups, missing node might also mean completion
                                                    debug!(
                                                        "✅ Job finished (node null/missing) for: {}",
                                                        prompt_id
                                                    );
                                                    let mut jobs =
                                                        manager.completed_jobs.lock().await;
                                                    jobs.add(prompt_id.to_string());
                                                }
                                            }
                                        }
                                    } else if msg_type == "execution_success" {
                                        if let Some(data) =
                                            json.get("data").and_then(|v| v.as_object())
                                        {
                                            if let Some(prompt_id) =
                                                data.get("prompt_id").and_then(|v| v.as_str())
                                            {
                                                debug!(
                                                    "✅ Execution success received for: {}",
                                                    prompt_id
                                                );
                                                let mut jobs = manager.completed_jobs.lock().await;
                                                jobs.add(prompt_id.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(Some(Err(e))) => {
                        // WebSocket error occurred
                        error!("❌ WebSocket error: {}", e);
                        break; // Exit inner loop to trigger reconnection
                    }
                    Ok(None) => {
                        // Server closed connection
                        error!("❌ WebSocket connection closed by server");
                        break; // Exit inner loop to trigger reconnection
                    }
                    Err(_) => {
                        // Timeout
                        break; // Exit inner loop to trigger reconnection
                    }
                }
            }

            // Reconnection logic: Retry with exponential backoff
            loop {
                retry_count += 1;
                warn!(
                    "🔄 Attempting to reconnect to WebSocket (attempt #{}, delay: {}s)",
                    retry_count,
                    retry_delay.as_secs()
                );

                // Wait for specified delay
                tokio::time::sleep(retry_delay).await;

                // Build WebSocket URL
                let ws_url = format!(
                    "ws://{}:{}/ws?clientId={}",
                    manager.backend_url, manager.backend_port, manager.client_id
                );

                // Attempt to reconnect
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        debug!("✅ Reconnected to backend WebSocket");
                        let (_write, new_read) = ws_stream.split();

                        // Reconnection successful, reset retry state
                        retry_count = 0;
                        retry_delay = Duration::from_secs(INITIAL_RECONNECT_DELAY_SECS);

                        // Continue listening with new connection
                        read = new_read;
                        break; // Exit reconnection loop, resume main message loop
                    }
                    Err(e) => {
                        error!("❌ Reconnection failed: {}", e);

                        // Calculate next retry delay (exponential backoff)
                        let next_delay_secs = (retry_delay.as_secs_f64()
                            * RECONNECT_BACKOFF_MULTIPLIER)
                            .min(MAX_RECONNECT_DELAY_SECS as f64);
                        retry_delay = Duration::from_secs_f64(next_delay_secs);

                        warn!("⏱️ Next reconnection attempt in {}s", retry_delay.as_secs());

                        // Continue loop to retry
                    }
                }
            }
        }
    }

    /// Waits for a job to complete by polling the `completed_jobs` buffer
    ///
    /// This function blocks (checking every 500ms) until the specified `job_id` appears in the buffer.
    /// Since the background listener updates the buffer in real-time, this effectively waits for the WebSocket message.
    ///
    /// # Arguments
    /// * `prompt_id` - The job ID to wait for
    ///
    /// # Returns
    /// - `Ok(())` when job completion is detected
    /// - `Error` if an error occurs (currently not used)
    ///
    /// # Blocking Warning
    /// This function runs in an async context but will delay the current async task.
    /// Should only be called within an async Handler.
    pub async fn wait_for_job_completion(
        &self,
        prompt_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("⏳ Waiting for job completion for prompt_id: {}", prompt_id);

        let mut check_count = 0;
        loop {
            {
                // 1. Check completion status received via WebSocket (short lock)
                let jobs = self.completed_jobs.lock().await;
                if jobs.contains(prompt_id) {
                    return Ok(());
                }
            }

            // 2. Fallback: Poll History API every 2 seconds (4 ticks)
            // Used to prevent infinite waiting if WebSocket misses a message
            check_count += 1;
            if check_count % 4 == 0 {
                let history_url = format!(
                    "http://{}:{}/history/{}",
                    self.backend_url, self.backend_port, prompt_id
                );
                // Silent poll: Log only on success or error to reduce noise

                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(3))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new());

                match client.get(&history_url).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<serde_json::Value>().await {
                                Ok(json) => {
                                    if json.get(prompt_id).is_some() {
                                        debug!(
                                            "💡 Fallback: Job completion detected via History API for {}",
                                            prompt_id
                                        );
                                        let mut jobs = self.completed_jobs.lock().await;
                                        jobs.add(prompt_id.to_string());
                                        return Ok(());
                                    }
                                    // Else: Still waiting, keep silent
                                }
                                Err(e) => warn!("⚠️ Fallback: Failed to parse history JSON: {}", e),
                            }
                        } else {
                            // Only warn on non-404 errors (404 is normal for ongoing tasks)
                            if resp.status() != 404 {
                                warn!(
                                    "⚠️ Fallback: History API returned status: {}",
                                    resp.status()
                                );
                            }
                        }
                    }
                    Err(e) => warn!("⚠️ Fallback request failed: {}", e),
                }
            }

            // Sleep briefly to avoid busy-waiting and yield CPU
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
