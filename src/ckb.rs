use ckb_jsonrpc_types::Uint32;
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{debug, info, instrument, warn};

use crate::config::RpcConfig;

#[derive(Debug)]
pub enum CkbError {
    Config(String),
    Http(String),
    HttpStatus { status: StatusCode, body: String },
    JsonRpcError(String),
    ParseError(String),
}

impl fmt::Display for CkbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CkbError::Config(e) => write!(f, "CKB RPC configuration error: {}", e),
            CkbError::Http(e) => write!(f, "CKB RPC HTTP error: {}", e),
            CkbError::HttpStatus { status, body } => {
                write!(f, "CKB RPC returned HTTP {}: {}", status, body)
            }
            CkbError::JsonRpcError(e) => write!(f, "CKB JSON-RPC error: {}", e),
            CkbError::ParseError(e) => write!(f, "CKB RPC parse error: {}", e),
        }
    }
}

impl Error for CkbError {}

pub type Result<T> = std::result::Result<T, CkbError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexerTip {
    pub block_hash: String,
    pub block_number: u64,
}

#[derive(Clone)]
pub struct CkbClient {
    client: Client,
    rpc_urls: Arc<Vec<String>>,
    id_counter: Arc<AtomicU64>,
    max_retries: u32,
    retry_interval: Duration,
}

impl CkbClient {
    pub fn new(config: &RpcConfig) -> Result<Self> {
        if config.urls.is_empty() {
            return Err(CkbError::Config(
                "at least one HTTP RPC URL must be configured".to_string(),
            ));
        }

        for url in &config.urls {
            let parsed = reqwest::Url::parse(url)
                .map_err(|e| CkbError::Config(format!("invalid RPC URL '{}': {}", url, e)))?;
            if parsed.scheme() != "http" && parsed.scheme() != "https" {
                return Err(CkbError::Config(format!(
                    "RPC URL '{}' must use http or https",
                    url
                )));
            }
        }

        let client = Client::builder()
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| CkbError::Http(e.to_string()))?;

        Ok(Self {
            client,
            rpc_urls: Arc::new(config.urls.clone()),
            id_counter: Arc::new(AtomicU64::new(1)),
            max_retries: config.max_retries,
            retry_interval: Duration::from_millis(config.retry_interval_millis),
        })
    }

    pub fn rpc_urls(&self) -> &[String] {
        self.rpc_urls.as_slice()
    }

    fn get_next_id(&self) -> u64 {
        self.id_counter.fetch_add(1, Ordering::Relaxed)
    }

    #[instrument(skip(self), fields(rpc_url = %rpc_url, code_hash = %code_hash, hash_type = %hash_type, limit = %limit))]
    pub async fn get_cells_by_type_script_code_hash(
        &self,
        rpc_url: &str,
        code_hash: &str,
        hash_type: &str,
        limit: u32,
        cursor: Option<&str>,
        block_range: (u64, u64),
    ) -> Result<Value> {
        if let Some(cursor_value) = cursor {
            debug!(cursor = %cursor_value, "Continuing HTTP cell query");
        } else {
            info!(rpc_url = %rpc_url, "Starting HTTP cell query");
        }

        let search_key = json!({
            "script": {
                "code_hash": code_hash,
                "hash_type": hash_type,
                "args": "0x"
            },
            "script_type": "type",
            "script_search_mode": "prefix",
            "filter": {
                "block_range": [
                    format!("0x{:x}", block_range.0),
                    format!("0x{:x}", block_range.1)
                ]
            },
            "with_data": true
        });

        // The integrated CKB Indexer accepts four get_cells parameters.
        let params = json!([search_key, "asc", Uint32::from(limit), cursor]);
        self.call_rpc_at(rpc_url, "get_cells", params).await
    }

    pub async fn get_indexer_tip(&self, rpc_url: &str) -> Result<IndexerTip> {
        let result = self
            .call_rpc_at(rpc_url, "get_indexer_tip", json!([]))
            .await?;
        let block_hash = result
            .get("block_hash")
            .and_then(Value::as_str)
            .ok_or_else(|| CkbError::ParseError("indexer tip has no block_hash".to_string()))?
            .to_string();
        let block_number = result
            .get("block_number")
            .and_then(Value::as_str)
            .ok_or_else(|| CkbError::ParseError("indexer tip has no block_number".to_string()))
            .and_then(parse_hex_u64)?;

        Ok(IndexerTip {
            block_hash,
            block_number,
        })
    }

    pub async fn get_block_by_number(&self, rpc_url: &str, block_number: u64) -> Result<Value> {
        let result = self
            .call_rpc_at(
                rpc_url,
                "get_block_by_number",
                json!([format!("0x{:x}", block_number)]),
            )
            .await?;
        if result.is_null() {
            return Err(CkbError::ParseError(format!(
                "block {} was not found",
                block_number
            )));
        }
        Ok(result)
    }

    #[instrument(skip(self, params), fields(rpc_url = %rpc_url, method = %method))]
    pub async fn call_rpc_at(&self, rpc_url: &str, method: &str, params: Value) -> Result<Value> {
        let mut attempt = 0;

        loop {
            let request_body = json!({
                "id": self.get_next_id(),
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            });

            match self.send_request(rpc_url, &request_body).await {
                Ok(response) => return parse_json_rpc_response(response),
                Err(error) if attempt < self.max_retries && error.is_retryable() => {
                    attempt += 1;
                    let delay = self.retry_interval.saturating_mul(attempt);
                    warn!(
                        rpc_url = %rpc_url,
                        method = %method,
                        attempt,
                        delay_ms = delay.as_millis(),
                        error = %error,
                        "Retrying CKB HTTP RPC request"
                    );
                    sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn send_request(&self, rpc_url: &str, body: &Value) -> Result<Value> {
        let response = self
            .client
            .post(rpc_url)
            .json(body)
            .send()
            .await
            .map_err(|e| CkbError::Http(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(CkbError::HttpStatus { status, body });
        }

        response
            .json::<Value>()
            .await
            .map_err(|e| CkbError::ParseError(e.to_string()))
    }

    pub fn parse_cells_response(&self, response: &Value) -> Result<(Vec<Value>, Option<String>)> {
        let objects = response
            .get("objects")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                CkbError::ParseError("invalid get_cells result: missing objects array".to_string())
            })?
            .clone();

        let last_cursor = response
            .get("last_cursor")
            .and_then(Value::as_str)
            .map(String::from);

        Ok((objects, last_cursor))
    }
}

impl CkbError {
    fn is_retryable(&self) -> bool {
        match self {
            CkbError::Http(_) => true,
            CkbError::HttpStatus { status, .. } => {
                *status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
            }
            _ => false,
        }
    }
}

fn parse_json_rpc_response(response: Value) -> Result<Value> {
    if let Some(error) = response.get("error") {
        return Err(CkbError::JsonRpcError(error.to_string()));
    }

    response
        .get("result")
        .cloned()
        .ok_or_else(|| CkbError::ParseError("JSON-RPC response is missing result".to_string()))
}

fn parse_hex_u64(value: &str) -> Result<u64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    u64::from_str_radix(value, 16)
        .map_err(|e| CkbError::ParseError(format!("invalid hexadecimal block number: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_rpc_result() {
        let response = json!({"jsonrpc": "2.0", "id": 1, "result": {"objects": []}});
        assert_eq!(
            parse_json_rpc_response(response).unwrap(),
            json!({"objects": []})
        );
    }

    #[test]
    fn rejects_json_rpc_error() {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32602, "message": "invalid params"}
        });
        assert!(matches!(
            parse_json_rpc_response(response),
            Err(CkbError::JsonRpcError(_))
        ));
    }

    #[test]
    fn parses_hex_block_number() {
        assert_eq!(parse_hex_u64("0x10").unwrap(), 16);
    }
}
