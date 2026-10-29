use ckb_sdk::NetworkType;
use serde::Deserialize;
use std::error::Error;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub rpc: RpcConfig,
    pub ckb: CkbConfig,
    pub spore_sync: SporeSyncConfig,
    pub database: Option<DatabaseConfig>,
    pub dob_decoder: Option<crate::dob_decoder::DobDecoderConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SporeSyncConfig {
    pub clusters: Vec<ClusterSyncConfig>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct ClusterSyncConfig {
    pub cluster_id: String,
    #[serde(default)]
    pub sync_start_block: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RpcConfig {
    pub urls: Vec<String>,
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_interval_millis")]
    pub retry_interval_millis: u64,
}

fn default_connect_timeout_secs() -> u64 {
    5
}
fn default_request_timeout_secs() -> u64 {
    30
}
fn default_max_retries() -> u32 {
    2
}
fn default_retry_interval_millis() -> u64 {
    1_000
}

#[derive(Debug, Deserialize, Clone)]
pub struct CkbConfig {
    pub type_script_code_hash: String,
    pub type_script_hash_type: String,
    pub query_limit: u32,
    pub query_interval_secs: u64,
    pub network_type: NetworkTypeConfig,
    #[serde(default)]
    pub sync_start_block: u64,
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
    #[serde(default = "default_overlap_blocks")]
    pub overlap_blocks: u64,
    #[serde(default = "default_block_batch_size")]
    pub block_batch_size: u64,
}

fn default_confirmations() -> u64 {
    12
}

fn default_overlap_blocks() -> u64 {
    24
}

fn default_block_batch_size() -> u64 {
    100_000
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum NetworkTypeConfig {
    Mainnet,
    Testnet,
    Devnet,
}

impl From<NetworkTypeConfig> for NetworkType {
    fn from(config: NetworkTypeConfig) -> Self {
        match config {
            NetworkTypeConfig::Mainnet => NetworkType::Mainnet,
            NetworkTypeConfig::Testnet => NetworkType::Testnet,
            NetworkTypeConfig::Devnet => NetworkType::Dev,
        }
    }
}

impl NetworkTypeConfig {
    pub fn as_str(&self) -> &'static str {
        match self {
            NetworkTypeConfig::Mainnet => "mainnet",
            NetworkTypeConfig::Testnet => "testnet",
            NetworkTypeConfig::Devnet => "devnet",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SporeFilterConfig {
    pub name: String,
    pub enabled: bool,
    pub cluster_id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub enabled: bool,
    pub url: String,
    pub max_connections: u32,
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let config_builder = config::Config::builder()
            .add_source(config::File::with_name("config/default.toml"))
            .add_source(config::File::with_name("config/local.toml").required(false))
            .add_source(config::Environment::with_prefix("LAMBDA_NEXT").separator("__"));

        let config = config_builder.build()?;
        Ok(config.try_deserialize()?)
    }
}
