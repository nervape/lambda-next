use std::sync::Arc;
use tokio::signal;
use tokio::time::{sleep, Duration, Instant};
use tracing::{error, info, instrument, Level};
use tracing_subscriber::EnvFilter;

mod burn;
mod ckb;
mod config;
mod db;
mod dob_decoder;
mod query;
mod spore;
mod utils;

use ckb::CkbClient;
use config::Config;
use utils::{AppError, Result};

#[tokio::main]
#[instrument]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .with_ansi(true)
        .init();

    let app_start_time = Instant::now();
    info!("Starting HTTP-based CKB indexer");

    let config = Config::load().map_err(|e| {
        error!("Failed to load configuration: {}", e);
        AppError::ConfigError(e)
    })?;

    let ckb_client = CkbClient::new(&config.rpc).map_err(AppError::CkbError)?;
    info!(
        endpoint_count = ckb_client.rpc_urls().len(),
        "CKB HTTP RPC client initialized"
    );

    let db = if let Some(db_config) = &config.database {
        if db_config.enabled {
            let db_config = db::DbConfig {
                url: db_config.url.clone(),
                max_connections: db_config.max_connections,
            };

            // A configured database is required for a useful indexer. Fail fast
            // instead of running successfully while silently dropping all data.
            let db = db::SporeDb::new(&db_config).await.map_err(|e| {
                AppError::MessageHandlingError(format!("failed to initialize database: {}", e))
            })?;
            db.initialize().await.map_err(|e| {
                AppError::MessageHandlingError(format!("failed to migrate database: {}", e))
            })?;
            Some(Arc::new(db))
        } else {
            info!("Database storage is disabled");
            None
        }
    } else {
        info!("No database configuration found");
        None
    };

    let mut shutdown = Box::pin(signal::ctrl_c());
    let mut queries_run = 0_u64;
    let mut total_query_time = Duration::ZERO;

    info!(
        interval_secs = config.ckb.query_interval_secs,
        "Application running; press Ctrl+C to stop"
    );

    loop {
        let query_start = Instant::now();
        match query::query_cells_http(&ckb_client, &config, db.as_ref()).await {
            Ok(summary) => {
                queries_run += 1;
                total_query_time += query_start.elapsed();
                info!(
                    queries_run,
                    rpc_url = %summary.rpc_url,
                    cells = summary.total_cells,
                    pages = summary.pages,
                    ranges = summary.ranges,
                    from_block = summary.from_block,
                    to_block = summary.to_block,
                    avg_duration_secs = total_query_time.as_secs_f64() / queries_run as f64,
                    "Query cycle completed"
                );
            }
            Err(error) => {
                error!(%error, "All CKB HTTP RPC endpoints failed for this query cycle");
            }
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(config.ckb.query_interval_secs)) => {}
            result = &mut shutdown => {
                if let Err(error) = result {
                    error!(%error, "Failed to listen for Ctrl+C");
                }
                info!("Received shutdown signal");
                break;
            }
        }
    }

    let app_runtime = app_start_time.elapsed();
    info!(
        runtime_secs = app_runtime.as_secs_f64(),
        "Application shut down cleanly"
    );
    Ok(())
}
