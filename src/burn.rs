use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tracing::{info, warn};

use crate::ckb::CkbClient;
use crate::db::SporeDb;
use crate::utils::{AppError, Result};

const BURN_SYNC_SCOPE: &str = "__spore_burn_scan__";

pub async fn sync_burned_spores(
    client: &CkbClient,
    rpc_url: &str,
    db: &Arc<SporeDb>,
    network_type: &str,
    cluster_ids: &[String],
    spore_code_hash: &str,
    spore_hash_type: &str,
    safe_tip: u64,
) -> Result<usize> {
    let state = db
        .get_cluster_sync_state(network_type, BURN_SYNC_SCOPE)
        .await
        .map_err(db_read_error)?;

    let Some((last_scanned, _)) = state else {
        db.update_cluster_sync_state(network_type, BURN_SYNC_SCOPE, safe_tip, None)
            .await
            .map_err(db_write_error)?;
        info!(network_type, safe_tip, "Initialized Spore burn checkpoint");
        return Ok(0);
    };

    let mut burned_count = 0;
    for block_number in last_scanned.saturating_add(1)..=safe_tip {
        let block = client.get_block_by_number(rpc_url, block_number).await?;
        burned_count += process_block(
            db,
            network_type,
            cluster_ids,
            spore_code_hash,
            spore_hash_type,
            &block,
        )
        .await?;
        let block_hash = block
            .get("header")
            .and_then(|header| header.get("hash"))
            .and_then(Value::as_str);
        db.update_cluster_sync_state(network_type, BURN_SYNC_SCOPE, block_number, block_hash)
            .await
            .map_err(db_write_error)?;
    }

    if burned_count > 0 {
        info!(
            network_type,
            burned_count, "Spore burn synchronization completed"
        );
    }
    Ok(burned_count)
}

async fn process_block(
    db: &Arc<SporeDb>,
    network_type: &str,
    cluster_ids: &[String],
    spore_code_hash: &str,
    spore_hash_type: &str,
    block: &Value,
) -> Result<usize> {
    let transactions = block
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::MessageHandlingError("block has no transactions".to_string()))?;
    let mut burned_count = 0;

    for transaction in transactions {
        let Some(transaction_hash) = transaction.get("hash").and_then(Value::as_str) else {
            continue;
        };
        let input_outpoints = input_outpoints(transaction);
        if input_outpoints.is_empty() {
            continue;
        }
        let live_spores = db
            .find_live_spores_by_outpoints(network_type, cluster_ids, &input_outpoints)
            .await
            .map_err(db_read_error)?;
        if live_spores.is_empty() {
            continue;
        }

        let output_spores = output_spore_type_args(transaction, spore_code_hash, spore_hash_type);
        for spore in live_spores {
            if let Some(output_index) = output_spores.get(&spore.id.to_ascii_lowercase()) {
                db.advance_live_spore_outpoint(
                    network_type,
                    &spore.id,
                    &spore.tx_hash,
                    spore.index,
                    transaction_hash,
                    *output_index,
                )
                .await
                .map_err(db_write_error)?;
                continue;
            }
            if db
                .mark_spore_burned(
                    network_type,
                    &spore.id,
                    &spore.tx_hash,
                    spore.index,
                    transaction_hash,
                )
                .await
                .map_err(db_write_error)?
            {
                burned_count += 1;
                warn!(
                    spore_id = %spore.id,
                    burned_tx_hash = transaction_hash,
                    "Spore was permanently marked as burned"
                );
            }
        }
    }
    Ok(burned_count)
}

fn input_outpoints(transaction: &Value) -> Vec<(String, u32)> {
    transaction
        .get("inputs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|input| {
            let previous_output = input.get("previous_output")?;
            let tx_hash = previous_output.get("tx_hash")?.as_str()?.to_string();
            let index = parse_hex_u32(previous_output.get("index")?.as_str()?)?;
            Some((tx_hash, index))
        })
        .collect()
}

fn output_spore_type_args(
    transaction: &Value,
    spore_code_hash: &str,
    spore_hash_type: &str,
) -> HashMap<String, u32> {
    transaction
        .get("outputs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, output)| Some((index, output.get("type")?)))
        .filter(|(_, type_script)| !type_script.is_null())
        .filter(|(_, type_script)| {
            type_script
                .get("code_hash")
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case(spore_code_hash))
                && type_script
                    .get("hash_type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case(spore_hash_type))
        })
        .filter_map(|(index, type_script)| {
            Some((
                type_script.get("args")?.as_str()?.to_ascii_lowercase(),
                index as u32,
            ))
        })
        .collect()
}

fn parse_hex_u32(value: &str) -> Option<u32> {
    u32::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16).ok()
}

fn db_read_error(error: sqlx::Error) -> AppError {
    AppError::MessageHandlingError(format!("failed to read burn sync data: {}", error))
}

fn db_write_error(error: sqlx::Error) -> AppError {
    AppError::MessageHandlingError(format!("failed to update burn sync data: {}", error))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn extracts_input_outpoints() {
        let tx = json!({"inputs": [{"previous_output": {"tx_hash": "0xabc", "index": "0x2"}}]});
        assert_eq!(input_outpoints(&tx), vec![("0xabc".to_string(), 2)]);
    }

    #[test]
    fn extracts_output_type_args_case_insensitively() {
        let tx = json!({"outputs": [
            {"type": {"code_hash": "0xspore", "hash_type": "data1", "args": "0xABCD"}},
            {"type": null},
            {"type": {"code_hash": "0xother", "hash_type": "data1", "args": "0x1234"}}
        ]});
        let ids = output_spore_type_args(&tx, "0xSPORE", "data1");
        assert_eq!(ids.get("0xabcd"), Some(&0));
        assert!(!ids.contains_key("0x1234"));
    }
}
