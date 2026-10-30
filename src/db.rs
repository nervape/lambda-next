use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, QueryBuilder, Row};
use std::error::Error;
use tracing::{debug, info, trace};

/// Database configuration parameters
#[derive(Debug, Clone, Deserialize)]
pub struct DbConfig {
    pub url: String,
    pub max_connections: u32,
}

/// Server-side decode result
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServerDecodeResult {
    pub render_output: String,
    pub dob_content: Value,
}

/// Spore data structure for storing in database
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DbSporeData {
    pub id: String,
    pub cluster_id: String,
    pub content_type: String,
    pub content: String,
    pub tx_hash: String,
    pub index: u32,
    pub owner: String,
    pub capacity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dob_decode_output: Option<ServerDecodeResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSporeOutPoint {
    pub id: String,
    pub tx_hash: String,
    pub index: u32,
}

/// PostgreSQL database interface for Spore data
pub struct SporeDb {
    pool: Pool<Postgres>,
}

impl SporeDb {
    /// Create a new database connection pool
    pub async fn new(config: &DbConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        debug!("Connecting to PostgreSQL at {}", config.url);

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await?;

        info!("Connected to PostgreSQL database");

        // Initialize database schema
        Self::init_schema(&pool).await?;

        Ok(Self { pool })
    }

    /// Initialize database schema if not exists
    async fn init_schema(pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
        info!("Initializing database schema");

        // Execute each statement separately
        // 1. Create the spores table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS spores (
                id TEXT PRIMARY KEY,
                cluster_id TEXT NOT NULL,
                content_type TEXT NOT NULL,
                content TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                index INTEGER NOT NULL,
                owner TEXT NOT NULL,
                capacity TEXT NOT NULL,
                render_output TEXT,
                dob_content JSONB,
                status TEXT NOT NULL DEFAULT 'live'
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            ALTER TABLE spores ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'live'
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cluster_sync_state (
                network_type TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                last_scanned_block BIGINT NOT NULL,
                last_block_hash TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (network_type, scope_key)
            )
            "#,
        )
        .execute(pool)
        .await?;

        // 2. Create index on cluster_id
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_spores_cluster_id ON spores(cluster_id)
            "#,
        )
        .execute(pool)
        .await?;

        // 3. Create index on owner
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_spores_owner ON spores(owner)
            "#,
        )
        .execute(pool)
        .await?;

        // 4. Create index on content_type
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_spores_content_type ON spores(content_type)
            "#,
        )
        .execute(pool)
        .await?;

        info!("Database schema setup complete");
        Ok(())
    }

    /// Insert or update a spore
    pub async fn upsert_spore(&self, spore: &DbSporeData) -> Result<(), sqlx::Error> {
        debug!("Upserting spore with ID: {}", spore.id);

        // Convert the dob_content to a JSON value if it exists
        let (render_output, dob_content) = match &spore.dob_decode_output {
            Some(decode) => {
                trace!("DOB decode output available for ID {}", spore.id);
                (Some(&decode.render_output), Some(&decode.dob_content))
            }
            None => {
                trace!("No DOB decode output for ID {}", spore.id);
                (None, None)
            }
        };

        // Default network to mainnet if not specified
        let network = spore.network_type.as_deref().unwrap_or("mainnet");

        trace!("Database insert/update for ID {}", spore.id);

        sqlx::query(
            r#"
            INSERT INTO spores
            (id, cluster_id, content_type, content, tx_hash, index, owner, capacity, render_output, dob_content, network_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (id) DO UPDATE SET
                cluster_id = EXCLUDED.cluster_id,
                content_type = EXCLUDED.content_type,
                content = EXCLUDED.content,
                tx_hash = EXCLUDED.tx_hash,
                index = EXCLUDED.index,
                owner = EXCLUDED.owner,
                capacity = EXCLUDED.capacity,
                render_output = COALESCE(EXCLUDED.render_output, spores.render_output),
                dob_content = COALESCE(EXCLUDED.dob_content, spores.dob_content),
                network_type = EXCLUDED.network_type
            "#,
        )
        .bind(&spore.id)
        .bind(&spore.cluster_id)
        .bind(&spore.content_type)
        .bind(&spore.content)
        .bind(&spore.tx_hash)
        .bind(spore.index as i32)
        .bind(&spore.owner)
        .bind(&spore.capacity)
        .bind(render_output)
        .bind(dob_content)
        .bind(network)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get spore by ID
    pub async fn get_spore_by_id(&self, id: &str) -> Result<Option<DbSporeData>, sqlx::Error> {
        debug!("Getting spore with ID: {}", id);

        let row: Option<(String, String, String, String, String, i32, String, String, Option<String>, Option<serde_json::Value>, Option<String>)> =
            sqlx::query_as(
                r#"
                SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity, render_output, dob_content, network_type
                FROM spores
                WHERE id = $1
                "#,
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(
            |(
                id,
                cluster_id,
                content_type,
                content,
                tx_hash,
                index,
                owner,
                capacity,
                render_output,
                dob_content,
                network_type,
            )| {
                let dob_decode_output = match (render_output, dob_content) {
                    (Some(render), Some(content)) => Some(ServerDecodeResult {
                        render_output: render,
                        dob_content: content,
                    }),
                    _ => None,
                };

                DbSporeData {
                    id,
                    cluster_id,
                    content_type,
                    content,
                    tx_hash,
                    index: index as u32,
                    owner,
                    capacity,
                    dob_decode_output,
                    network_type,
                }
            },
        ))
    }

    /// Get spores by cluster ID with pagination
    pub async fn get_spores_by_cluster(
        &self,
        cluster_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DbSporeData>, sqlx::Error> {
        debug!("Getting spores with cluster_id: {}", cluster_id);

        let rows: Vec<(String, String, String, String, String, i32, String, String, Option<String>, Option<serde_json::Value>, Option<String>)> =
            sqlx::query_as(
                r#"
                SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity, render_output, dob_content, network_type
                FROM spores
                WHERE cluster_id = $1
                ORDER BY id
                LIMIT $2 OFFSET $3
                "#,
            )
            .bind(cluster_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    cluster_id,
                    content_type,
                    content,
                    tx_hash,
                    index,
                    owner,
                    capacity,
                    render_output,
                    dob_content,
                    network_type,
                )| {
                    let dob_decode_output = match (render_output, dob_content) {
                        (Some(render), Some(content)) => Some(ServerDecodeResult {
                            render_output: render,
                            dob_content: content,
                        }),
                        _ => None,
                    };

                    DbSporeData {
                        id,
                        cluster_id,
                        content_type,
                        content,
                        tx_hash,
                        index: index as u32,
                        owner,
                        capacity,
                        dob_decode_output,
                        network_type,
                    }
                },
            )
            .collect())
    }

    /// Get spores by owner address with pagination
    pub async fn get_spores_by_owner(
        &self,
        owner: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DbSporeData>, sqlx::Error> {
        debug!("Getting spores with owner: {}", owner);

        let rows: Vec<(String, String, String, String, String, i32, String, String, Option<String>, Option<serde_json::Value>, Option<String>)> =
            sqlx::query_as(
                r#"
                SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity, render_output, dob_content, network_type
                FROM spores
                WHERE owner = $1
                ORDER BY id
                LIMIT $2 OFFSET $3
                "#,
            )
            .bind(owner)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    cluster_id,
                    content_type,
                    content,
                    tx_hash,
                    index,
                    owner,
                    capacity,
                    render_output,
                    dob_content,
                    network_type,
                )| {
                    let dob_decode_output = match (render_output, dob_content) {
                        (Some(render), Some(content)) => Some(ServerDecodeResult {
                            render_output: render,
                            dob_content: content,
                        }),
                        _ => None,
                    };

                    DbSporeData {
                        id,
                        cluster_id,
                        content_type,
                        content,
                        tx_hash,
                        index: index as u32,
                        owner,
                        capacity,
                        dob_decode_output,
                        network_type,
                    }
                },
            )
            .collect())
    }

    /// Update only owner and capacity fields for a spore
    /// This is an optimization for spores that are already fully populated
    pub async fn update_spore_owner_capacity(
        &self,
        id: &str,
        owner: &str,
        capacity: &str,
        tx_hash: &str,
        index: u32,
    ) -> Result<(), sqlx::Error> {
        debug!("Partial update (owner/capacity) for spore ID: {}", id);

        sqlx::query(
            r#"
            UPDATE spores
            SET owner = $2, capacity = $3, tx_hash = $4, index = $5
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(owner)
        .bind(capacity)
        .bind(tx_hash)
        .bind(index as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn find_live_spores_by_outpoints(
        &self,
        network_type: &str,
        cluster_ids: &[String],
        outpoints: &[(String, u32)],
    ) -> Result<Vec<LiveSporeOutPoint>, sqlx::Error> {
        if outpoints.is_empty() || cluster_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut builder = QueryBuilder::new(
            "SELECT id, tx_hash, index FROM spores WHERE status = 'live' AND network_type = ",
        );
        builder.push_bind(network_type);
        builder.push(" AND cluster_id = ANY(");
        builder.push_bind(cluster_ids);
        builder.push(") AND (");
        for (position, (tx_hash, index)) in outpoints.iter().enumerate() {
            if position > 0 {
                builder.push(" OR ");
            }
            builder.push("(tx_hash = ");
            builder.push_bind(tx_hash);
            builder.push(" AND index = ");
            builder.push_bind(*index as i32);
            builder.push(")");
        }
        builder.push(")");

        builder
            .build()
            .map(|row: sqlx::postgres::PgRow| LiveSporeOutPoint {
                id: row.get("id"),
                tx_hash: row.get("tx_hash"),
                index: row.get::<i32, _>("index") as u32,
            })
            .fetch_all(&self.pool)
            .await
    }

    pub async fn mark_spore_burned(
        &self,
        network_type: &str,
        id: &str,
        expected_tx_hash: &str,
        expected_index: u32,
        burn_tx_hash: &str,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE spores
            SET status = 'burned', tx_hash = $5
            WHERE network_type = $1 AND id = $2 AND tx_hash = $3 AND index = $4
              AND status = 'live'
            "#,
        )
        .bind(network_type)
        .bind(id)
        .bind(expected_tx_hash)
        .bind(expected_index as i32)
        .bind(burn_tx_hash)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn advance_live_spore_outpoint(
        &self,
        network_type: &str,
        id: &str,
        expected_tx_hash: &str,
        expected_index: u32,
        new_tx_hash: &str,
        new_index: u32,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE spores
            SET tx_hash = $5, index = $6
            WHERE network_type = $1 AND id = $2 AND tx_hash = $3 AND index = $4
              AND status = 'live'
            "#,
        )
        .bind(network_type)
        .bind(id)
        .bind(expected_tx_hash)
        .bind(expected_index as i32)
        .bind(new_tx_hash)
        .bind(new_index as i32)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Check if a spore record is fully populated with DOB data
    pub async fn is_dob_spore_fully_populated(&self, id: &str) -> Result<bool, sqlx::Error> {
        let result: Option<(bool, bool)> = sqlx::query_as(
            r#"
            SELECT
                render_output IS NOT NULL as has_render_output,
                dob_content IS NOT NULL as has_dob_content
            FROM spores
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match result {
            Some((has_render_output, has_dob_content)) => has_render_output && has_dob_content,
            None => false, // If record doesn't exist, it's not fully populated
        })
    }

    /// Get spores by cluster ID with pagination and optional network type
    pub async fn get_spores_by_cluster_with_network(
        &self,
        cluster_id: &str,
        limit: Option<i64>,
        offset: i64,
        network_type: Option<&str>,
    ) -> Result<Vec<DbSporeData>, sqlx::Error> {
        debug!(
            "Getting spores with cluster_id: {} (limit: {:?}, offset: {}, network filter: {})",
            cluster_id,
            limit,
            offset,
            network_type.unwrap_or("none")
        );

        let mut builder = QueryBuilder::new(
            "SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity, render_output, dob_content, network_type \
            FROM spores WHERE cluster_id = "
        );
        builder.push_bind(cluster_id);
        builder.push(" AND status = 'live'");

        if let Some(network) = network_type {
            builder.push(" AND network_type = ");
            builder.push_bind(network);
        }

        builder.push(" ORDER BY id");

        if let Some(limit) = limit {
            builder.push(" LIMIT ");
            builder.push_bind(limit);
        }

        if offset > 0 {
            builder.push(" OFFSET ");
            builder.push_bind(offset);
        }

        let rows: Vec<(
            String,
            String,
            String,
            String,
            String,
            i32,
            String,
            String,
            Option<String>,
            Option<serde_json::Value>,
            Option<String>,
        )> = builder
            .build_query_as::<(
                String,
                String,
                String,
                String,
                String,
                i32,
                String,
                String,
                Option<String>,
                Option<serde_json::Value>,
                Option<String>,
            )>()
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    cluster_id,
                    content_type,
                    content,
                    tx_hash,
                    index,
                    owner,
                    capacity,
                    render_output,
                    dob_content,
                    network_type,
                )| {
                    let dob_decode_output = match (render_output, dob_content) {
                        (Some(render), Some(content)) => Some(ServerDecodeResult {
                            render_output: render,
                            dob_content: content,
                        }),
                        _ => None,
                    };

                    DbSporeData {
                        id,
                        cluster_id,
                        content_type,
                        content,
                        tx_hash,
                        index: index as u32,
                        owner,
                        capacity,
                        dob_decode_output,
                        network_type,
                    }
                },
            )
            .collect())
    }

    /// Get spores by owner address with pagination and optional content type filter
    pub async fn get_spores_by_owner_filtered(
        &self,
        owner: &str,
        limit: i64,
        offset: i64,
        content_type_filter: Option<&str>,
        include_dob_output: bool,
        network_type: Option<&str>,
    ) -> Result<Vec<DbSporeData>, sqlx::Error> {
        debug!(
            "Getting spores with owner: {} (content filter: {}, dob output: {}, network: {})",
            owner,
            content_type_filter.unwrap_or("none"),
            include_dob_output,
            network_type.unwrap_or("none")
        );

        let query_str = if let Some(content_filter) = content_type_filter {
            if let Some(network) = network_type {
                // Filter by content type AND network
                r#"
                SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity,
                    CASE WHEN $4 THEN render_output ELSE NULL END as render_output,
                    CASE WHEN $4 THEN dob_content ELSE NULL END as dob_content,
                    network_type
                FROM spores
                WHERE owner = $1 AND content_type LIKE $5 || '%' AND network_type = $6
                ORDER BY id
                LIMIT $2 OFFSET $3
                "#
            } else {
                // Filter by content type only
                r#"
                SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity,
                    CASE WHEN $4 THEN render_output ELSE NULL END as render_output,
                    CASE WHEN $4 THEN dob_content ELSE NULL END as dob_content,
                    network_type
                FROM spores
                WHERE owner = $1 AND content_type LIKE $5 || '%'
                ORDER BY id
                LIMIT $2 OFFSET $3
                "#
            }
        } else {
            if let Some(network) = network_type {
                // Filter by network only
                r#"
                SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity,
                    CASE WHEN $4 THEN render_output ELSE NULL END as render_output,
                    CASE WHEN $4 THEN dob_content ELSE NULL END as dob_content,
                    network_type
                FROM spores
                WHERE owner = $1 AND network_type = $5
                ORDER BY id
                LIMIT $2 OFFSET $3
                "#
            } else {
                // No filters
                r#"
                SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity,
                    CASE WHEN $4 THEN render_output ELSE NULL END as render_output,
                    CASE WHEN $4 THEN dob_content ELSE NULL END as dob_content,
                    network_type
                FROM spores
                WHERE owner = $1
                ORDER BY id
                LIMIT $2 OFFSET $3
                "#
            }
        };

        let mut query = sqlx::query_as(query_str)
            .bind(owner)
            .bind(limit)
            .bind(offset)
            .bind(include_dob_output);

        // Add content type filter if present
        if let Some(content_filter) = content_type_filter {
            query = query.bind(content_filter);

            // Add network type if present
            if let Some(network) = network_type {
                query = query.bind(network);
            }
        } else if let Some(network) = network_type {
            // Just add network if content filter not present
            query = query.bind(network);
        }

        let rows: Vec<(
            String,
            String,
            String,
            String,
            String,
            i32,
            String,
            String,
            Option<String>,
            Option<serde_json::Value>,
            Option<String>,
        )> = query.fetch_all(&self.pool).await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    cluster_id,
                    content_type,
                    content,
                    tx_hash,
                    index,
                    owner,
                    capacity,
                    render_output,
                    dob_content,
                    network_type,
                )| {
                    let dob_decode_output = match (render_output, dob_content) {
                        (Some(render), Some(content)) => Some(ServerDecodeResult {
                            render_output: render,
                            dob_content: content,
                        }),
                        _ => None,
                    };

                    DbSporeData {
                        id,
                        cluster_id,
                        content_type,
                        content,
                        tx_hash,
                        index: index as u32,
                        owner,
                        capacity,
                        dob_decode_output,
                        network_type,
                    }
                },
            )
            .collect())
    }

    /// Add network_type column to the schema
    async fn update_schema_for_network(&self) -> Result<(), sqlx::Error> {
        // Check if network_type column exists
        let network_column_exists = sqlx::query(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'spores' AND column_name = 'network_type'
            );
            "#,
        )
        .fetch_one(&self.pool)
        .await?
        .get::<bool, _>(0);

        if !network_column_exists {
            info!("Adding network_type column to spores table");
            sqlx::query(
                r#"
                ALTER TABLE spores
                ADD COLUMN network_type VARCHAR(20) DEFAULT 'mainnet';
                "#,
            )
            .execute(&self.pool)
            .await?;

            // Create index on network_type
            sqlx::query(
                r#"
                CREATE INDEX IF NOT EXISTS idx_spores_network_type ON spores(network_type);
                "#,
            )
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    /// Initialize the database
    pub async fn initialize(&self) -> Result<(), sqlx::Error> {
        // Create schema if needed
        Self::init_schema(&self.pool).await?;

        // Update schema for network_type
        self.update_schema_for_network().await?;

        Ok(())
    }

    pub async fn get_cluster_sync_state(
        &self,
        network_type: &str,
        scope_key: &str,
    ) -> Result<Option<(u64, Option<String>)>, sqlx::Error> {
        let row: Option<(i64, Option<String>)> = sqlx::query_as(
            r#"
            SELECT last_scanned_block, last_block_hash
            FROM cluster_sync_state
            WHERE network_type = $1 AND scope_key = $2
            "#,
        )
        .bind(network_type)
        .bind(scope_key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(block, hash)| (block.max(0) as u64, hash)))
    }

    pub async fn update_cluster_sync_state(
        &self,
        network_type: &str,
        scope_key: &str,
        last_scanned_block: u64,
        last_block_hash: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO cluster_sync_state
                (network_type, scope_key, last_scanned_block, last_block_hash)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (network_type, scope_key) DO UPDATE SET
                last_scanned_block = EXCLUDED.last_scanned_block,
                last_block_hash = EXCLUDED.last_block_hash,
                updated_at = NOW()
            "#,
        )
        .bind(network_type)
        .bind(scope_key)
        .bind(last_scanned_block as i64)
        .bind(last_block_hash)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get a spore by ID with optional network filter
    pub async fn get_spore_by_id_with_network(
        &self,
        id: &str,
        network_type: Option<&str>,
    ) -> Result<Vec<DbSporeData>, sqlx::Error> {
        debug!(
            "Getting spore by ID: {} with network: {:?}",
            id, network_type
        );

        let spores = if let Some(network) = network_type {
            trace!("Filtering by network: {}", network);
            sqlx::query(
                r#"
                SELECT
                    s.id, s.cluster_id, s.content_type, s.content, s.tx_hash, s.index,
                    s.owner, s.capacity, s.network_type,
                    (CASE
                        WHEN s.render_output IS NOT NULL AND s.dob_content IS NOT NULL THEN
                            jsonb_build_object('render_output', s.render_output, 'dob_content', s.dob_content)
                        ELSE NULL
                    END) as dob_decode_output
                FROM spores s
                WHERE s.id = $1 AND s.network_type = $2 AND s.status = 'live'
                "#
            )
            .bind(id)
            .bind(network)
            .map(|row: sqlx::postgres::PgRow| {
                let dob_decode_output: Option<serde_json::Value> = row.get("dob_decode_output");

                // Parse the JSON value into ServerDecodeResult if present
                let dob_output = dob_decode_output.and_then(|json| {
                    serde_json::from_value::<ServerDecodeResult>(json).ok()
                });

                DbSporeData {
                    id: row.get("id"),
                    cluster_id: row.get("cluster_id"),
                    content_type: row.get("content_type"),
                    content: row.get("content"),
                    tx_hash: row.get("tx_hash"),
                    index: row.get::<i32, _>("index") as u32,
                    owner: row.get("owner"),
                    capacity: row.get("capacity"),
                    dob_decode_output: dob_output,
                    network_type: row.get("network_type"),
                }
            })
            .fetch_all(&self.pool)
            .await?
        } else {
            trace!("No network filter, returning all networks");
            sqlx::query(
                r#"
                SELECT
                    s.id, s.cluster_id, s.content_type, s.content, s.tx_hash, s.index,
                    s.owner, s.capacity, s.network_type,
                    (CASE
                        WHEN s.render_output IS NOT NULL AND s.dob_content IS NOT NULL THEN
                            jsonb_build_object('render_output', s.render_output, 'dob_content', s.dob_content)
                        ELSE NULL
                    END) as dob_decode_output
                FROM spores s
                WHERE s.id = $1 AND s.status = 'live'
                "#
            )
            .bind(id)
            .map(|row: sqlx::postgres::PgRow| {
                let dob_decode_output: Option<serde_json::Value> = row.get("dob_decode_output");

                // Parse the JSON value into ServerDecodeResult if present
                let dob_output = dob_decode_output.and_then(|json| {
                    serde_json::from_value::<ServerDecodeResult>(json).ok()
                });

                DbSporeData {
                    id: row.get("id"),
                    cluster_id: row.get("cluster_id"),
                    content_type: row.get("content_type"),
                    content: row.get("content"),
                    tx_hash: row.get("tx_hash"),
                    index: row.get::<i32, _>("index") as u32,
                    owner: row.get("owner"),
                    capacity: row.get("capacity"),
                    dob_decode_output: dob_output,
                    network_type: row.get("network_type"),
                }
            })
            .fetch_all(&self.pool)
            .await?
        };

        trace!("Found {} spores for ID: {}", spores.len(), id);

        Ok(spores)
    }

    /// Get spores by owner address with pagination, filtered by cluster IDs
    pub async fn get_spores_by_owner_with_clusters(
        &self,
        owner: &str,
        limit: i64,
        offset: i64,
        content_type_filter: Option<&str>,
        include_dob_output: bool,
        network_type: Option<&str>,
        cluster_ids: &[String],
    ) -> Result<Vec<DbSporeData>, sqlx::Error> {
        debug!("Getting spores with owner: {} (content filter: {}, dob output: {}, network: {}, cluster_ids: {:?})",
            owner, content_type_filter.unwrap_or("none"), include_dob_output, network_type.unwrap_or("none"), cluster_ids);

        // If no cluster IDs are provided, use the regular function
        if cluster_ids.is_empty() {
            return self
                .get_spores_by_owner_filtered(
                    owner,
                    limit,
                    offset,
                    content_type_filter,
                    include_dob_output,
                    network_type,
                )
                .await;
        }

        // Build the SQL query based on filters
        let mut query_str = String::from(
            r#"
            SELECT id, cluster_id, content_type, content, tx_hash, index, owner, capacity,
                CASE WHEN $4 THEN render_output ELSE NULL END as render_output,
                CASE WHEN $4 THEN dob_content ELSE NULL END as dob_content,
                network_type
            FROM spores
            WHERE owner = $1
            "#,
        );

        // Add content type filter if needed
        if let Some(_) = content_type_filter {
            query_str.push_str(" AND content_type LIKE $5 || '%'");
        }

        // Add network type filter if needed
        if let Some(_) = network_type {
            if content_type_filter.is_some() {
                query_str.push_str(" AND network_type = $6");
            } else {
                query_str.push_str(" AND network_type = $5");
            }
        }

        // Add the cluster_ids filter
        query_str.push_str(" AND cluster_id = ANY($7)");

        // Add order, limit and offset
        query_str.push_str(" ORDER BY id LIMIT $2 OFFSET $3");

        // Prepare the query with bindings
        let mut query = sqlx::query_as(&query_str)
            .bind(owner)
            .bind(limit)
            .bind(offset)
            .bind(include_dob_output);

        // Add content type filter if present
        if let Some(content_filter) = content_type_filter {
            query = query.bind(content_filter);

            // Add network type if present
            if let Some(network) = network_type {
                query = query.bind(network);
            }
        } else if let Some(network) = network_type {
            // Just add network if content filter not present
            query = query.bind(network);
        }

        // Add the cluster_ids as an array parameter
        query = query.bind(cluster_ids);

        let rows: Vec<(
            String,
            String,
            String,
            String,
            String,
            i32,
            String,
            String,
            Option<String>,
            Option<serde_json::Value>,
            Option<String>,
        )> = query.fetch_all(&self.pool).await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    cluster_id,
                    content_type,
                    content,
                    tx_hash,
                    index,
                    owner,
                    capacity,
                    render_output,
                    dob_content,
                    network_type,
                )| {
                    let dob_decode_output = match (render_output, dob_content) {
                        (Some(render), Some(content)) => Some(ServerDecodeResult {
                            render_output: render,
                            dob_content: content,
                        }),
                        _ => None,
                    };

                    DbSporeData {
                        id,
                        cluster_id,
                        content_type,
                        content,
                        tx_hash,
                        index: index as u32,
                        owner,
                        capacity,
                        dob_decode_output,
                        network_type,
                    }
                },
            )
            .collect())
    }

    /// Get a spore by ID, checking if it belongs to the provided cluster IDs
    pub async fn get_spore_by_id_with_clusters(
        &self,
        id: &str,
        network_type: Option<&str>,
        cluster_ids: &[String],
    ) -> Result<Vec<DbSporeData>, sqlx::Error> {
        debug!(
            "Getting spore by ID: {} with network: {:?} and cluster_ids: {:?}",
            id, network_type, cluster_ids
        );

        // If no cluster IDs are provided, use the regular function
        if cluster_ids.is_empty() {
            return self.get_spore_by_id_with_network(id, network_type).await;
        }

        let query_str = if let Some(_) = network_type {
            // Query with network filter and cluster filter
            r#"
            SELECT
                s.id, s.cluster_id, s.content_type, s.content, s.tx_hash, s.index,
                s.owner, s.capacity, s.network_type,
                (CASE
                    WHEN s.render_output IS NOT NULL AND s.dob_content IS NOT NULL THEN
                        jsonb_build_object('render_output', s.render_output, 'dob_content', s.dob_content)
                    ELSE NULL
                END) as dob_decode_output
            FROM spores s
            WHERE s.id = $1 AND s.network_type = $2 AND s.cluster_id = ANY($3)
            "#
        } else {
            // Query with just cluster filter
            r#"
            SELECT
                s.id, s.cluster_id, s.content_type, s.content, s.tx_hash, s.index,
                s.owner, s.capacity, s.network_type,
                (CASE
                    WHEN s.render_output IS NOT NULL AND s.dob_content IS NOT NULL THEN
                        jsonb_build_object('render_output', s.render_output, 'dob_content', s.dob_content)
                    ELSE NULL
                END) as dob_decode_output
            FROM spores s
            WHERE s.id = $1 AND s.cluster_id = ANY($2)
            "#
        };

        let query = if let Some(network) = network_type {
            sqlx::query(query_str)
                .bind(id)
                .bind(network)
                .bind(cluster_ids)
        } else {
            sqlx::query(query_str).bind(id).bind(cluster_ids)
        };

        let spores = query
            .map(|row: sqlx::postgres::PgRow| {
                let dob_decode_output: Option<serde_json::Value> = row.get("dob_decode_output");

                // Parse the JSON value into ServerDecodeResult if present
                let dob_output = dob_decode_output
                    .and_then(|json| serde_json::from_value::<ServerDecodeResult>(json).ok());

                DbSporeData {
                    id: row.get("id"),
                    cluster_id: row.get("cluster_id"),
                    content_type: row.get("content_type"),
                    content: row.get("content"),
                    tx_hash: row.get("tx_hash"),
                    index: row.get::<i32, _>("index") as u32,
                    owner: row.get("owner"),
                    capacity: row.get("capacity"),
                    dob_decode_output: dob_output,
                    network_type: row.get("network_type"),
                }
            })
            .fetch_all(&self.pool)
            .await?;

        trace!("Found {} spores for ID: {}", spores.len(), id);

        Ok(spores)
    }
}
