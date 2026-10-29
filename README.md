# lambda-next

`lambda-next` 是一个基于 CKB HTTP JSON-RPC 的 Spore 增量同步服务。它只同步配置的 Cluster，将 Spore 数据保存到 PostgreSQL，并提供独立的 HTTP API 查询服务。

## 功能

- 使用 HTTP JSON-RPC，不依赖 WebSocket 或本地 CKB 节点。
- 支持多个 RPC 地址、请求重试和端点故障切换。
- 只同步 `spore_sync.clusters` 中配置的 Cluster。
- 每个 Cluster 使用独立区块检查点，新增 Cluster 不会影响已有 Cluster。
- 使用 `get_cells` 和 `block_range` 分段获取 live Spore。
- DOB 解码失败时保留基础 Spore 数据，不阻塞同步检查点。
- 使用 `get_block_by_number` 增量识别已销毁的 Spore。
- 同时支持 Mainnet、Testnet 和查询 API。

## 项目结构

```text
src/                    同步服务
  ckb.rs                CKB HTTP JSON-RPC 客户端
  query.rs              Cluster 增量同步
  burn.rs               Spore 销毁检测
  spore.rs              Spore 解析与 DOB 处理
  db.rs                 PostgreSQL 存储
spore-api-server/       查询 API
config/default.toml     Mainnet 配置
config/testnet.toml     Testnet 配置
config/api.toml         API 服务配置
docker-compose.yml      PostgreSQL、同步器和 API 服务
```

## 快速启动

需要 Docker Compose。

启动全部服务：

```bash
docker compose up -d --build
```

也可以只启动需要的网络：

```bash
docker compose up -d --build lambda-mainnet
docker compose up -d --build lambda-testnet
```

查看日志：

```bash
docker compose logs -f lambda-mainnet
docker compose logs -f lambda-testnet
```

服务默认包括：

| 服务 | 说明 | 端口 |
| --- | --- | --- |
| `db` | PostgreSQL 16 | `5432` |
| `lambda-mainnet` | Mainnet Spore 同步 | 无公开端口 |
| `lambda-testnet` | Testnet Spore 同步 | 无公开端口 |
| `spore-api-server` | 查询 API | `3002` |

## Cluster 配置

每个 Cluster 使用一段独立配置：

```toml
[[spore_sync.clusters]]
cluster_id = "0x..."
sync_start_block = 12345678
```

- `cluster_id`：需要同步的 Cluster ID。
- `sync_start_block`：该 Cluster 第一次同步的起始区块。
- 如果数据库已有该 Cluster 的检查点，检查点优先于配置值。
- 后续增加 Cluster 时，新 Cluster 从自己的 `sync_start_block` 开始，已有 Cluster 继续使用原检查点。

## 配置说明

### RPC

```toml
[rpc]
urls = ["https://example-rpc-1/", "https://example-rpc-2/"]
connect_timeout_secs = 5
request_timeout_secs = 30
max_retries = 2
retry_interval_millis = 1000
```

- `urls`：按顺序尝试的 CKB HTTP RPC 地址。
- `connect_timeout_secs`：TCP/TLS 建连超时。
- `request_timeout_secs`：单次 RPC 请求总超时。
- `max_retries`：单个端点的重试次数。
- `retry_interval_millis`：递增重试延迟的基础值。

### CKB 同步

```toml
[ckb]
type_script_code_hash = "0x..."
type_script_hash_type = "data1"
query_limit = 300
query_interval_secs = 60
network_type = "mainnet"
sync_start_block = 0
confirmations = 12
overlap_blocks = 24
block_batch_size = 100000
```

- `type_script_code_hash`、`type_script_hash_type`：当前网络的 Spore Type Script。
- `query_limit`：每页 `get_cells` 最大记录数。
- `query_interval_secs`：同步周期。
- `network_type`：`mainnet`、`testnet` 或 `devnet`。
- `sync_start_block`：Cluster 未单独配置起始区块时的默认值。
- `confirmations`：只处理链头之前已确认的区块。
- `overlap_blocks`：live-cell 同步时向前重叠扫描的区块数。
- `block_batch_size`：一次 `get_cells` 查询的区块范围大小。

### DOB Decoder

```toml
[dob_decoder]
enabled = true
url = "https://decoder.example.com"
batch_size = 50
```

DOB Decoder 是外部 HTTP JSON-RPC 服务。解码失败只会记录警告，基础 Spore 数据仍会写入数据库，后续同步不会卡住。

## 同步机制

### Live Spore

同步器从 `get_indexer_tip` 取得链头，并计算：

```text
safe_tip = indexer_tip - confirmations
```

随后按 Cluster 检查点和 `block_batch_size` 构造 `block_range`，通过 `get_cells` 查询 live Spore。完成一个区块范围并成功写库后，才更新对应 Cluster 的检查点。

检查点存储在 `cluster_sync_state`：

```sql
SELECT network_type, scope_key, last_scanned_block, updated_at
FROM cluster_sync_state
ORDER BY network_type, scope_key;
```

### Burn 检测

Burn 检测使用独立检查点：

```text
scope_key = __spore_burn_scan__
```

它逐块调用 `get_block_by_number`，并检查交易：

1. 用 input 的 `previous_output` 匹配数据库中指定 Cluster 的 live Spore。
2. 如果同一交易的 outputs 中存在相同 Spore Type Args，则视为转移，并推进当前 OutPoint。
3. 如果 outputs 中不存在相同 Spore ID，则永久标记为 `burned`。

字段语义：

- `status = 'live'`：`tx_hash + index` 是当前 Cell OutPoint。
- `status = 'burned'`：`tx_hash` 是销毁交易哈希。

查询已销毁 Spore：

```sql
SELECT id, cluster_id, network_type, status, tx_hash
FROM spores
WHERE status = 'burned';
```

首次启用 Burn 检测时，会在当前安全高度初始化检查点，不回扫全部历史区块。

## 查询 API

API 默认监听 `http://localhost:3002`。

```bash
# 按 Cluster 查询
curl 'http://localhost:3002/cluster/0x.../all?network=mainnet&limit=100&offset=0'

# 按 Spore ID 查询
curl 'http://localhost:3002/spore/0x...?network=mainnet'

# 按 Owner 查询
curl 'http://localhost:3002/address/ckb.../all?network=mainnet&limit=100&offset=0'
```

更多 API 说明见 [`spore-api-server/README.md`](spore-api-server/README.md)。

## 本地开发

```bash
cargo test --workspace
cargo check --workspace --locked
```

直接运行同步器时，可通过环境变量覆盖数据库连接：

```bash
LAMBDA_NEXT__DATABASE__URL=postgres://postgres:postgres@localhost:5432/spore_db \
  cargo run --bin lambda_next
```

启动 API：

```bash
cargo run --bin spore-api-server -- -c config/api.toml
```

## 常用排查

```bash
# 检查容器状态
docker compose ps -a

# 查看同步日志
docker compose logs --tail=200 lambda-mainnet

# 查看检查点
docker compose exec -T db psql -U postgres -d spore_db -c \
  "SELECT network_type, scope_key, last_scanned_block FROM cluster_sync_state ORDER BY network_type, scope_key;"

# 查看已销毁 Spore
docker compose exec -T db psql -U postgres -d spore_db -c \
  "SELECT id, cluster_id, network_type, tx_hash FROM spores WHERE status='burned';"
```

