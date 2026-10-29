use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use molecule::prelude::*;
use serde_json::Value;
use spore_types::{generated::spore::SporeData, NativeNFTData};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tracing::{debug, error, info, trace, warn};

// Add imports for CKB address handling
use ckb_jsonrpc_types::Script;
use ckb_sdk::{Address, AddressPayload, NetworkType};
use ckb_types::{core::ScriptHashType, packed, prelude::*, H256};

use crate::config::{CkbConfig, SporeFilterConfig};
use crate::db::{DbSporeData, ServerDecodeResult};
use std::str::FromStr;

/// Represents a Spore cell with its decoded data
#[derive(Debug, Clone)]
pub struct SporeCell {
    /// The raw cell data from CKB
    pub raw_cell: Value,
    /// The decoded Spore data
    pub spore_data: Option<NativeNFTData>,
    /// The cluster ID if present
    pub cluster_id: Option<String>,
    /// The Type ID of the cell
    pub type_id: Option<String>,
    /// The network type to use for address formatting
    pub network_type: NetworkType,
}

/// Cache for decoded Spore cells to prevent repeated decoding
pub struct SporeCache {
    /// Cached Spore data by Type ID
    cache: HashMap<String, NativeNFTData>,
}

impl SporeCache {
    /// Create a new empty cache
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Get cached Spore data if available
    pub fn get(&self, type_id: &str) -> Option<&NativeNFTData> {
        self.cache.get(type_id)
    }

    /// Store decoded Spore data in the cache
    pub fn insert(&mut self, type_id: String, data: NativeNFTData) {
        self.cache.insert(type_id, data);
    }

    /// Get the number of entries in the cache
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Remove an entry from the cache
    pub fn remove(&mut self, type_id: &str) -> Option<NativeNFTData> {
        self.cache.remove(type_id)
    }
}

// Global cache instance
lazy_static! {
    static ref SPORE_CACHE: Arc<StdMutex<SporeCache>> = Arc::new(StdMutex::new(SporeCache::new()));
}

/// Extract the Type ID from a cell
fn extract_type_id(cell: &Value) -> Option<String> {
    cell.get("output")
        .and_then(|output| output.get("type"))
        .and_then(|type_script| type_script.get("args"))
        .and_then(|args| args.as_str())
        .map(|args_str| {
            // Take the first 32 bytes (64 hex characters + 0x prefix)
            let args_str = args_str.trim_start_matches("0x");
            if args_str.len() >= 64 {
                format!("0x{}", &args_str[0..64])
            } else {
                format!("0x{}", args_str)
            }
        })
}

/// Decodes the molecule-encoded SporeData from a cell, using cache when available
pub fn decode_spore_data(cell: &Value) -> Result<NativeNFTData> {
    // Extract Type ID for cache lookup
    let type_id =
        extract_type_id(cell).ok_or_else(|| anyhow!("Cell has no Type ID for cache lookup"))?;

    // Try to get from cache first
    let cache = SPORE_CACHE.lock().unwrap();
    if let Some(cached_data) = cache.get(&type_id) {
        debug!("Cache hit for Type ID {}", type_id);
        return Ok(cached_data.clone());
    }
    drop(cache); // Release the lock before decoding

    // Get data field from the cell
    let data_hex = cell
        .get("output_data")
        .and_then(|data| data.as_str())
        .ok_or_else(|| anyhow!("Cell has no data field"))?;

    // Remove 0x prefix if present
    let data_hex = data_hex.trim_start_matches("0x");

    // Convert hex to bytes
    let data = hex::decode(data_hex).map_err(|e| {
        error!("Failed to decode hex data: {}", e);
        anyhow!("Failed to decode hex data: {}", e)
    })?;

    // Parse SporeData from bytes
    let spore_data = SporeData::from_compatible_slice(&data).map_err(|e| {
        error!("Failed to parse SporeData: {}", e);
        anyhow!("Failed to parse SporeData: {}", e)
    })?;

    // Convert to native representation (manually)
    let native_data = NativeNFTData {
        content_type: String::from_utf8_lossy(spore_data.content_type().unpack()).to_string(),
        content: spore_data.content().unpack().to_vec(),
        cluster_id: spore_data
            .cluster_id()
            .to_opt()
            .map(|id| id.unpack().to_vec()),
    };

    // Store in cache using a clone of the type_id to avoid moving it
    let mut cache = SPORE_CACHE.lock().unwrap();
    cache.insert(type_id.clone(), native_data.clone());
    debug!(
        "Added cell to cache with Type ID {}, total cached: {}",
        type_id,
        cache.len()
    );

    Ok(native_data)
}

/// Extract cluster ID from a spore NFT data
pub fn extract_cluster_id(spore_data: &NativeNFTData) -> Option<String> {
    spore_data.cluster_id.as_ref().map(|id| {
        // Convert bytes to hex string
        format!("0x{}", hex::encode(id))
    })
}

/// Filter cells based on configured criteria
pub fn apply_filter(
    cells: Vec<Value>,
    filter: &SporeFilterConfig,
    network_type: NetworkType,
) -> Vec<SporeCell> {
    let cells_len = cells.len();
    debug!("Applying filter '{}' to {} cells", filter.name, cells_len);

    let cluster_id_filter = &filter.cluster_id;

    let mut filtered_cells = Vec::new();

    // Process each cell
    for cell in cells {
        // Extract the Type ID from the cell
        let type_id = match extract_type_id(&cell) {
            Some(id) => id,
            None => {
                trace!("Cell has no Type ID, skipping");
                continue;
            }
        };

        // Decode the cell data - Always decode basic spore data regardless of skip_decoding
        let spore_data = match decode_spore_data(&cell) {
            Ok(data) => Some(data),
            Err(e) => {
                debug!("Failed to decode cell with Type ID {}: {}", type_id, e);
                None
            }
        };

        // Extract cluster ID from the data
        let cluster_id = match &spore_data {
            Some(data) => extract_cluster_id(data),
            None => None,
        };

        // Check if this cell's cluster ID matches our filter
        let cluster_id_matches = cluster_id
            .as_ref()
            .is_some_and(|cell_cluster_id| cell_cluster_id == cluster_id_filter);

        // Skip if cluster ID doesn't match
        if !cluster_id_matches {
            continue;
        }

        // Add the matching cell to our result set
        filtered_cells.push(SporeCell {
            raw_cell: cell,
            spore_data,
            cluster_id,
            type_id: Some(type_id),
            network_type,
        });
    }

    if !filtered_cells.is_empty() {
        trace!(
            "Filter '{}' selected {} cells out of {}",
            filter.name,
            filtered_cells.len(),
            cells_len
        );
    }

    filtered_cells
}

/// Process all cells using all enabled filters
pub async fn process_cells_with_filters(
    cells: &[Value],
    filters: &[SporeFilterConfig],
    ckb_config: &CkbConfig,
    db: Option<&Arc<crate::db::SporeDb>>,
) -> Result<()> {
    if cells.is_empty() {
        info!("No cells to process");
        return Ok(());
    }

    // Get only enabled filters
    let enabled_filters: Vec<_> = filters.iter().filter(|f| f.enabled).collect();
    if enabled_filters.is_empty() {
        info!("No enabled filters found, skipping processing");
        return Ok(());
    }

    debug!(
        "Processing {} cells with {} enabled filters",
        cells.len(),
        enabled_filters.len()
    );

    // Get the configured network type
    let network_type: NetworkType = ckb_config.network_type.clone().into();
    debug!("Using network type: {:?}", network_type);

    // Process each filter independently
    for filter in enabled_filters {
        debug!("Applying filter: {}", filter.name);

        // Create a fresh copy of the cells for each filter
        // This avoids the borrow after move issue
        let cells_for_filter = cells.to_vec();

        // Apply the filter
        let filtered_cells = apply_filter(cells_for_filter, filter, network_type);

        if !filtered_cells.is_empty() {
            trace!(
                "Filter '{}' selected {} cells for processing",
                filter.name,
                filtered_cells.len()
            );

            // Process the filtered cells
            process_spore_cells(filtered_cells, db).await?;
        } else {
            debug!("No cells matched filter: {}", filter.name);
        }
    }

    Ok(())
}

/// Process a single spore cell
///
/// This function is a placeholder that should be customized based on specific requirements.
/// The user should implement this function to perform their desired operations on each spore cell.
pub async fn process_single_spore(
    cell: &SporeCell,
    db: Option<&Arc<crate::db::SporeDb>>,
) -> Result<()> {
    debug!(
        "Processing spore cell: {}",
        cell.type_id.as_deref().unwrap_or("None")
    );
    trace!("Spore data: {:?}", cell.spore_data);
    trace!("Cluster ID: {:?}", cell.cluster_id);
    trace!("Type ID: {:?}", cell.type_id);
    trace!("Raw cell: {:?}", cell.raw_cell);
    trace!("--------------------------------");

    // If we have a database and a type ID, check optimization rules
    if let (Some(db), Some(type_id)) = (db, &cell.type_id) {
        // Standard processing for cells with spore_data
        if let Some(spore_data) = &cell.spore_data {
            // Extract required fields for partial update
            let owner = if let Some(lock) = cell.raw_cell.get("output").and_then(|o| o.get("lock"))
            {
                if let Some(owner) = SporeCell::extract_owner_from_lock(lock, cell.network_type) {
                    owner
                } else {
                    // If we can't extract the owner, fall back to regular processing
                    return process_regular_spore(cell, Some(db)).await;
                }
            } else {
                // If lock script is missing, fall back to regular processing
                return process_regular_spore(cell, Some(db)).await;
            };

            // Extract capacity for partial update
            let capacity = cell
                .raw_cell
                .get("output")
                .and_then(|output| output.get("capacity"))
                .and_then(|capacity| capacity.as_str())
                .unwrap_or("0")
                .to_string();

            // extract tx hash and index for partial update
            let tx_hash = cell
                .raw_cell
                .get("out_point")
                .and_then(|out_point| out_point.get("tx_hash"))
                .and_then(|tx_hash| tx_hash.as_str())
                .unwrap_or("unknown")
                .to_string();

            let index = cell
                .raw_cell
                .get("out_point")
                .and_then(|out_point| out_point.get("index"))
                .and_then(|index| index.as_str())
                .and_then(|index_str| {
                    // Handle hex string format (0x prefix)
                    if index_str.starts_with("0x") {
                        u32::from_str_radix(&index_str[2..], 16).ok()
                    } else {
                        // Fallback to parsing as decimal
                        index_str.parse::<u32>().ok()
                    }
                })
                .unwrap_or(0);

            // Situation #1: DOB spore with full data already
            if spore_data.content_type.starts_with("dob/") {
                // Check if this DOB spore is already fully populated
                match db.is_dob_spore_fully_populated(type_id).await {
                    Ok(true) => {
                        // DOB spore is fully populated - do only partial update
                        debug!(
                            "DOB spore is fully populated - performing partial update for: {}",
                            type_id
                        );
                        db.update_spore_owner_capacity(type_id, &owner, &capacity, &tx_hash, index)
                            .await
                            .map_err(|e| anyhow!("failed to update spore {}: {}", type_id, e))?;
                        return Ok(());
                    }
                    Ok(false) => {
                        // DOB spore exists but missing some data - proceed with full update
                        debug!("DOB spore needs full update: {}", type_id);
                    }
                    Err(e) => return Err(anyhow!("failed to check DOB spore {}: {}", type_id, e)),
                }
            }
            // Situation #2: Generic spore (non-DOB)
            else if !spore_data.content_type.starts_with("dob/") {
                // Check if record exists
                match db.get_spore_by_id(type_id).await {
                    Ok(Some(_)) => {
                        // Generic spore exists - do only partial update
                        debug!(
                            "Generic spore exists - performing partial update for: {}",
                            type_id
                        );
                        db.update_spore_owner_capacity(type_id, &owner, &capacity, &tx_hash, index)
                            .await
                            .map_err(|e| anyhow!("failed to update spore {}: {}", type_id, e))?;
                        return Ok(());
                    }
                    Ok(None) => {
                        // Generic spore doesn't exist - proceed with full update
                        debug!("Generic spore needs full insert: {}", type_id);
                    }
                    Err(e) => return Err(anyhow!("failed to check spore {}: {}", type_id, e)),
                }
            }
        } else {
            // If we get here with spore_data=None, that means decoding failed for some reason
            // Just log an error and return
            error!(
                "Spore data decoding failed for cell with type_id: {}",
                type_id
            );
            return Ok(());
        }
    }

    // Fall back to regular processing if optimization rules don't apply
    return process_regular_spore(cell, db).await;
}

/// Regular spore processing without optimizations (original implementation)
async fn process_regular_spore(
    cell: &SporeCell,
    db: Option<&Arc<crate::db::SporeDb>>,
) -> Result<()> {
    // Convert to SporeData and store in database if DB is configured
    if let Some(db) = db {
        if let Some(spore_data) = cell.to_spore_data() {
            debug!("Storing spore in database: {}", spore_data.id);
            db.upsert_spore(&spore_data)
                .await
                .map_err(|e| anyhow!("failed to store spore {}: {}", spore_data.id, e))?;
        }
    }

    Ok(())
}

/// Process a list of spore cells
///
/// This function is a placeholder for user-defined processing logic
pub async fn process_spore_cells(
    cells: Vec<SporeCell>,
    db: Option<&Arc<crate::db::SporeDb>>,
) -> Result<()> {
    info!("Processing {} spore cells", cells.len());

    // First, identify DOB spores that need decoding
    let config =
        crate::config::Config::load().map_err(|e| anyhow!("Failed to load config: {}", e))?;
    // Separate DOB spores that need decoding
    let mut dob_spores = Vec::new();
    if config.dob_decoder.is_some() && config.dob_decoder.as_ref().unwrap().enabled {
        for (index, cell) in cells.iter().enumerate() {
            if let Some(data) = &cell.spore_data {
                // Only process DOB spores that haven't been decoded yet
                if data.content_type.starts_with("dob/") {
                    if let Some(type_id) = &cell.type_id {
                        // Check if already decoded by looking up in DB
                        let already_decoded = if let Some(db) = db {
                            if let Ok(Some(spore)) = db.get_spore_by_id(type_id).await {
                                spore.dob_decode_output.is_some()
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        if !already_decoded {
                            // Convert Vec<u8> to hex String for the JSON-RPC API
                            let content_str = hex::encode(&data.content);
                            dob_spores.push((type_id.clone(), content_str));
                        }
                    }
                }
            }
        }
    }

    // Process DOB spores in batches if there are any
    let mut decoded_results = std::collections::HashMap::new();
    if !dob_spores.is_empty() && config.dob_decoder.is_some() {
        let decoder_config = config.dob_decoder.unwrap();
        let decoder = crate::dob_decoder::DobDecoder::new(decoder_config.clone());

        // Split into batches
        let batches = crate::dob_decoder::batch_items(dob_spores, decoder_config.batch_size);

        for (batch_idx, batch) in batches.into_iter().enumerate() {
            debug!("Processing DOB batch {}, size: {}", batch_idx, batch.len());

            // Send batch request
            let results = decoder.decode_batch(batch).await;

            // Store results
            for (id, result) in results {
                decoded_results.insert(id, result);
            }
        }
    }

    // Process all cells as before, but now with decode results available
    for (index, cell) in cells.iter().enumerate() {
        match &cell.spore_data {
            Some(data) => {
                debug!(
                    "Cell {}: Type ID: {}, Content Type: {}, Cluster ID: {}",
                    index,
                    cell.type_id.as_deref().unwrap_or("None"),
                    data.content_type,
                    cell.cluster_id.as_deref().unwrap_or("None")
                );

                // Process the individual cell with decoded DOB data if available
                process_single_spore_with_dob(cell, db, &decoded_results)
                    .await
                    .map_err(|e| anyhow!("failed to process spore cell {}: {}", index, e))?;
            }
            None => {
                // This case should be rare now that we're always decoding basic spore data
                if let Some(type_id) = &cell.type_id {
                    warn!(
                        "Cell {} with Type ID {} could not be decoded as a valid spore",
                        index, type_id
                    );
                } else {
                    warn!("Cell {} could not be decoded as a valid spore", index);
                }
            }
        }
    }

    Ok(())
}

/// Process a single spore cell with potential DOB decode results
///
/// This is a helper function that calls the original process_single_spore
/// but with DOB decode results if available
async fn process_single_spore_with_dob(
    cell: &SporeCell,
    db: Option<&Arc<crate::db::SporeDb>>,
    dob_results: &std::collections::HashMap<
        String,
        Result<crate::db::ServerDecodeResult, Box<dyn std::error::Error + Send + Sync>>,
    >,
) -> Result<()> {
    // NOTE: We're wrapping process_single_spore to avoid modifying it directly,
    // per the requirement to preserve its behavior.
    // DOB decode results are passed to the database separately when creating the DbSporeData.

    // Process the spore normally
    process_single_spore(cell, db).await?;

    // If we have DOB results and a database, update the record with the DOB data
    if let (Some(db), Some(type_id)) = (db, &cell.type_id) {
        if let Some(dob_result) = dob_results.get(type_id) {
            if let Ok(server_result) = dob_result {
                if let Some(mut spore_data) = cell.to_spore_data() {
                    // The JSON-RPC response already contains properly structured DOB decode output,
                    // use it directly as provided by the server
                    spore_data.dob_decode_output = Some(server_result.clone());

                    // Update the database
                    debug!(
                        "Updating spore in database with DOB decode output: {}",
                        spore_data.id
                    );
                    db.upsert_spore(&spore_data).await.map_err(|e| {
                        anyhow!(
                            "failed to store DOB decode output for {}: {}",
                            spore_data.id,
                            e
                        )
                    })?;
                }
            } else if let Err(error) = dob_result {
                // A decoder failure must not block the block-range checkpoint. The basic
                // Spore data has already been stored by process_single_spore above; keep
                // its DOB output empty and continue with the remaining cells.
                warn!(
                    type_id = %type_id,
                    error = %error,
                    "DOB decoding failed; stored basic Spore data and continuing"
                );
            }
        }
    }

    Ok(())
}

impl SporeCell {
    /// Convert SporeCell to SporeData for database storage
    pub fn to_spore_data(&self) -> Option<DbSporeData> {
        // Extract the Type ID
        let type_id = self.type_id.as_ref()?;

        // Extract cell details
        let tx_hash = self
            .raw_cell
            .get("out_point")
            .and_then(|out_point| out_point.get("tx_hash"))
            .and_then(|tx_hash| tx_hash.as_str())
            .unwrap_or("unknown")
            .to_string();

        let index = self
            .raw_cell
            .get("out_point")
            .and_then(|out_point| out_point.get("index"))
            .and_then(|index| index.as_str())
            .and_then(|index_str| {
                // Handle hex string format (0x prefix)
                if index_str.starts_with("0x") {
                    u32::from_str_radix(&index_str[2..], 16).ok()
                } else {
                    // Fallback to parsing as decimal
                    index_str.parse::<u32>().ok()
                }
            })
            .unwrap_or(0);

        // Extract capacity
        let capacity = self
            .raw_cell
            .get("output")
            .and_then(|output| output.get("capacity"))
            .and_then(|capacity| capacity.as_str())
            .unwrap_or("0")
            .to_string();

        // Get owner from lock script
        let lock = self
            .raw_cell
            .get("output")
            .and_then(|output| output.get("lock"));

        let owner = if let Some(lock) = lock {
            Self::extract_owner_from_lock(lock, self.network_type)
                .unwrap_or_else(|| "unknown".to_string())
        } else {
            "unknown".to_string()
        };

        // Get content type and content
        debug!("Determining content type");
        let content_type = match &self.spore_data {
            Some(spore) => {
                debug!("Using content_type from spore_data: {}", spore.content_type);
                spore.content_type.to_string()
            }
            None => {
                debug!("No spore_data present, using default content type");
                "plain/text".to_string()
            }
        };

        debug!("Preparing content");
        let content = match &self.spore_data {
            Some(spore) => {
                let content_bytes = &spore.content;
                debug!("Content size: {} bytes", content_bytes.len());

                // For text content, try to convert to string
                if content_type.starts_with("text/") || content_type.contains("json") {
                    match String::from_utf8(content_bytes.clone()) {
                        Ok(s) => {
                            debug!("Converted binary content to UTF-8 string");
                            s
                        }
                        Err(_) => {
                            debug!("Failed to convert binary content to UTF-8, using hex encoding");
                            hex::encode(content_bytes)
                        }
                    }
                } else {
                    // For binary content, use hex encoding
                    debug!("Using hex encoding for binary content");
                    hex::encode(content_bytes)
                }
            }
            None => {
                debug!("No spore_data present, content will be empty");
                String::new()
            }
        };

        // Get cluster ID or use empty string
        debug!("Determining cluster_id");
        let cluster_id = match &self.cluster_id {
            Some(id) => {
                debug!("Using cluster_id from cell: {}", id);
                id.clone()
            }
            None => {
                debug!("No cluster_id present, using empty string");
                String::new()
            }
        };

        // Convert network type
        let network_type_str = match self.network_type {
            NetworkType::Mainnet => Some("mainnet".to_string()),
            NetworkType::Testnet => Some("testnet".to_string()),
            _ => None,
        };

        // DOB decoding is now handled externally in process_spore_cells
        debug!("Successfully created DbSporeData");
        Some(DbSporeData {
            id: type_id.clone(),
            cluster_id,
            content_type,
            content,
            tx_hash,
            index,
            owner,
            capacity,
            dob_decode_output: None, // Will be populated separately if needed
            network_type: network_type_str,
        })
    }

    /// Extract owner address from lock script
    fn extract_owner_from_lock(lock: &Value, network_type: NetworkType) -> Option<String> {
        // Extract code_hash from lock script
        let code_hash = lock.get("code_hash").and_then(|ch| ch.as_str())?;

        // Extract hash_type from lock script
        let hash_type_str = lock.get("hash_type").and_then(|ht| ht.as_str())?;

        // Convert hash_type string to ScriptHashType
        let hash_type = match hash_type_str {
            "type" => ScriptHashType::Type,
            "data" => ScriptHashType::Data,
            "data1" => ScriptHashType::Data1,
            "data2" => ScriptHashType::Data2,
            _ => {
                error!("Invalid hash_type: {}", hash_type_str);
                return None;
            }
        };

        // Extract args from lock script
        let args = lock
            .get("args")
            .and_then(|a| a.as_str())?
            .trim_start_matches("0x");

        // Parse code_hash as H256
        let code_hash_bytes = match H256::from_str(code_hash.trim_start_matches("0x")) {
            Ok(hash) => hash,
            Err(_) => return None,
        };

        // Parse args as bytes
        let args_bytes = match hex::decode(args) {
            Ok(bytes) => bytes,
            Err(_) => return None,
        };

        // Create owner address
        let owner = Address::new(
            network_type,
            AddressPayload::Full {
                hash_type,
                code_hash: code_hash_bytes.pack(),
                args: args_bytes.into(),
            },
            true, // Use short address format
        )
        .to_string();

        Some(owner)
    }
}
