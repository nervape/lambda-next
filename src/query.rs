use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::time::Instant;
use tracing::{debug, info, instrument, warn};

use crate::burn::sync_burned_spores;
use crate::ckb::CkbClient;
use crate::config::{ClusterSyncConfig, Config, SporeFilterConfig};
use crate::db::SporeDb;
use crate::spore::process_cells_with_filters;
use crate::utils::{AppError, Result};

#[derive(Debug)]
pub struct QuerySummary {
    pub rpc_url: String,
    pub total_cells: usize,
    pub pages: usize,
    pub ranges: usize,
    pub from_block: u64,
    pub to_block: u64,
}

/// Incrementally query live cells for configured clusters. Every cluster owns
/// an independent checkpoint; clusters at the same height share one scan.
#[instrument(skip(client, config, db))]
pub async fn query_cells_http(
    client: &CkbClient,
    config: &Config,
    db: Option<&Arc<SporeDb>>,
) -> Result<QuerySummary> {
    let db = db.ok_or_else(|| {
        AppError::MessageHandlingError(
            "cluster incremental sync requires database storage".to_string(),
        )
    })?;
    let clusters = normalized_clusters(&config.spore_sync.clusters)?;
    migrate_legacy_checkpoint(config, db, &clusters).await?;

    let mut last_error = None;
    for rpc_url in client.rpc_urls() {
        match query_endpoint(client, rpc_url, config, db, &clusters).await {
            Ok(summary) => return Ok(summary),
            Err(error) => {
                warn!(rpc_url = %rpc_url, error = %error,
                    "CKB RPC endpoint failed; resuming per-cluster checkpoints on the next endpoint");
                last_error = Some(error);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        AppError::MessageHandlingError("no CKB HTTP RPC endpoints are configured".to_string())
    }))
}

async fn migrate_legacy_checkpoint(
    config: &Config,
    db: &Arc<SporeDb>,
    clusters: &[ClusterSyncConfig],
) -> Result<()> {
    let network = config.ckb.network_type.as_str();
    let legacy_key = clusters
        .iter()
        .map(|cluster| cluster.cluster_id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let legacy_state = db
        .get_cluster_sync_state(network, &legacy_key)
        .await
        .map_err(sync_state_read_error)?;
    let Some((last_scanned, last_hash)) = legacy_state else {
        return Ok(());
    };
    if clusters.len() == 1 {
        return Ok(());
    }

    for cluster in clusters {
        let cluster_id = &cluster.cluster_id;
        let state = db
            .get_cluster_sync_state(network, cluster_id)
            .await
            .map_err(sync_state_read_error)?;
        if state.is_none() {
            db.update_cluster_sync_state(network, cluster_id, last_scanned, last_hash.as_deref())
                .await
                .map_err(sync_state_write_error)?;
            info!(
                network,
                cluster_id,
                last_scanned_block = last_scanned,
                "Migrated legacy combined checkpoint to cluster"
            );
        }
    }
    Ok(())
}

async fn query_endpoint(
    client: &CkbClient,
    rpc_url: &str,
    config: &Config,
    db: &Arc<SporeDb>,
    clusters: &[ClusterSyncConfig],
) -> Result<QuerySummary> {
    let query_start_time = Instant::now();
    let tip = client.get_indexer_tip(rpc_url).await?;
    let safe_tip = tip.block_number.saturating_sub(config.ckb.confirmations);
    let network = config.ckb.network_type.as_str();
    let cluster_ids = clusters
        .iter()
        .map(|cluster| cluster.cluster_id.clone())
        .collect::<Vec<_>>();
    sync_burned_spores(
        client,
        rpc_url,
        db,
        network,
        &cluster_ids,
        &config.ckb.type_script_code_hash,
        &config.ckb.type_script_hash_type,
        safe_tip,
    )
    .await?;
    let mut groups: BTreeMap<u64, Vec<String>> = BTreeMap::new();

    for cluster in clusters {
        let cluster_id = &cluster.cluster_id;
        let state = db
            .get_cluster_sync_state(network, cluster_id)
            .await
            .map_err(sync_state_read_error)?;
        let start = state.map_or_else(
            || {
                cluster
                    .sync_start_block
                    .unwrap_or(config.ckb.sync_start_block)
            },
            |(last_scanned, _)| {
                last_scanned
                    .saturating_add(1)
                    .saturating_sub(config.ckb.overlap_blocks)
            },
        );
        if start <= safe_tip {
            groups.entry(start).or_default().push(cluster_id.clone());
        }
    }

    let from_block = groups
        .keys()
        .next()
        .copied()
        .unwrap_or(safe_tip.saturating_add(1));
    let mut total_cells = 0;
    let mut page_count = 0;
    let mut range_count = 0;

    for (start_block, group_cluster_ids) in groups {
        let filters: Vec<_> = group_cluster_ids
            .iter()
            .map(|id| cluster_filter(id))
            .collect();
        let final_end = safe_tip.saturating_add(1);
        let mut range_start = start_block;
        while range_start < final_end {
            let range_end = range_start
                .saturating_add(config.ckb.block_batch_size.max(1))
                .min(final_end);
            let (cells, pages) = query_block_range(
                client,
                rpc_url,
                config,
                &filters,
                db,
                (range_start, range_end),
            )
            .await?;
            total_cells += cells;
            page_count += pages;
            range_count += 1;

            let completed_block = range_end - 1;
            let completed_hash = (completed_block == safe_tip).then_some(tip.block_hash.as_str());
            for cluster_id in &group_cluster_ids {
                db.update_cluster_sync_state(network, cluster_id, completed_block, completed_hash)
                    .await
                    .map_err(sync_state_write_error)?;
            }
            info!(rpc_url = %rpc_url, cluster_count = group_cluster_ids.len(), range_start,
                range_end, completed_block, "Per-cluster block range committed");
            range_start = range_end;
        }
    }

    info!(rpc_url = %rpc_url, cluster_count = clusters.len(), from_block,
        to_block = safe_tip, total_cells, pages = page_count, ranges = range_count,
        total_duration_ms = query_start_time.elapsed().as_millis(),
        "Incremental per-cluster query completed");
    Ok(QuerySummary {
        rpc_url: rpc_url.to_string(),
        total_cells,
        pages: page_count,
        ranges: range_count,
        from_block,
        to_block: safe_tip,
    })
}

async fn query_block_range(
    client: &CkbClient,
    rpc_url: &str,
    config: &Config,
    filters: &[SporeFilterConfig],
    db: &Arc<SporeDb>,
    block_range: (u64, u64),
) -> Result<(usize, usize)> {
    let mut cursor: Option<String> = None;
    let mut total_cells = 0;
    let mut pages = 0;
    loop {
        let page_start = Instant::now();
        let response = client
            .get_cells_by_type_script_code_hash(
                rpc_url,
                &config.ckb.type_script_code_hash,
                &config.ckb.type_script_hash_type,
                config.ckb.query_limit,
                cursor.as_deref(),
                block_range,
            )
            .await?;
        let (cells, next_cursor) = client.parse_cells_response(&response)?;
        let cells_count = cells.len();
        pages += 1;
        total_cells += cells_count;
        debug!(rpc_url = %rpc_url, range_start = block_range.0, range_end = block_range.1,
            page = pages, cells_count, duration_ms = page_start.elapsed().as_millis(),
            "Retrieved incremental HTTP RPC page");

        if !cells.is_empty() {
            process_cells_with_filters(&cells, filters, &config.ckb, Some(db))
                .await
                .map_err(|e| AppError::SporeError(e.to_string()))?;
        }
        cursor = next_cursor;
        if cursor.is_none() || cells_count == 0 {
            break;
        }
    }
    Ok((total_cells, pages))
}

fn normalized_clusters(clusters: &[ClusterSyncConfig]) -> Result<Vec<ClusterSyncConfig>> {
    let mut seen = HashSet::new();
    let mut result: Vec<_> = clusters
        .iter()
        .filter_map(|cluster| {
            let cluster_id = cluster.cluster_id.trim();
            if cluster_id.is_empty() || !seen.insert(cluster_id.to_string()) {
                return None;
            }
            Some(ClusterSyncConfig {
                cluster_id: cluster_id.to_string(),
                sync_start_block: cluster.sync_start_block,
            })
        })
        .collect();
    result.sort_unstable_by(|a, b| a.cluster_id.cmp(&b.cluster_id));
    if result.is_empty() {
        return Err(AppError::MessageHandlingError(
            "spore_sync.clusters must contain at least one cluster ID".to_string(),
        ));
    }
    Ok(result)
}

fn cluster_filter(cluster_id: &str) -> SporeFilterConfig {
    SporeFilterConfig {
        name: cluster_id.to_string(),
        enabled: true,
        cluster_id: cluster_id.to_string(),
    }
}

fn sync_state_read_error(error: sqlx::Error) -> AppError {
    AppError::MessageHandlingError(format!("failed to read sync state: {}", error))
}

fn sync_state_write_error(error: sqlx::Error) -> AppError {
    AppError::MessageHandlingError(format!("failed to update sync state: {}", error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_clusters() {
        let clusters = vec![
            ClusterSyncConfig {
                cluster_id: " 0xb ".to_string(),
                sync_start_block: Some(20),
            },
            ClusterSyncConfig {
                cluster_id: "0xa".to_string(),
                sync_start_block: Some(10),
            },
            ClusterSyncConfig {
                cluster_id: "0xb".to_string(),
                sync_start_block: Some(30),
            },
        ];
        let normalized = normalized_clusters(&clusters).unwrap();
        assert_eq!(normalized[0].cluster_id, "0xa");
        assert_eq!(normalized[0].sync_start_block, Some(10));
        assert_eq!(normalized[1].cluster_id, "0xb");
        assert_eq!(normalized[1].sync_start_block, Some(20));
    }

    #[test]
    fn rejects_empty_cluster_ids() {
        assert!(normalized_clusters(&[ClusterSyncConfig {
            cluster_id: "  ".to_string(),
            sync_start_block: None,
        }])
        .is_err());
    }
}
