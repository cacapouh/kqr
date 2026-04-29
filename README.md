# kqr — Kafka topic を SQL で。

```
   ┌─────────┐    rdkafka     ┌────────┐  arrow-json  ┌──────────────┐
   │  Kafka  │ ─────────────▶ │  kqr   │ ───────────▶ │  DataFusion  │ ─▶ stdout
   └─────────┘   bounded read └────────┘  RecordBatch └──────────────┘
                                  ▲
                              parquet
                              cache (--reuse)
```

`kqr` は **Kafka topic に対してアドホックに SQL 集計をかける CLI** です。
ksqlDB ほど重くなく、`kcat | jq` より圧倒的に速い。`bounded` な期間切り抜きに
振り切ることで「Kafka 用の DataFusion CLI」というポジションを取っています。

---

## 30 秒デモ

```console
$ kqr query -t bids --last 10m \
    "select side, count(*) as n, avg(price) as avg \
     from bids group by side order by n desc"
┌──────┬───────┬───────────┐
│ side │   n   │   avg     │
├──────┼───────┼───────────┤
│ buy  │ 12453 │ 102.7     │
│ sell │ 11820 │ 102.9     │
└──────┴───────┴───────────┘
```

```console
$ kqr query -t bids -t wins --last 1h \
    "select b.market, count(*) wins from bids b
     join wins w on b.id = w.bid_id group by b.market"
```

```console
$ kqr repl -t orders --last 30m
[kqr] 1 table(s) ready. \q to exit, \d for tables.
kqr> \d
orders  4 cols
kqr> select count(*) from orders;
...
```

---

## なぜ既存ツールで足りないか

| ツール | 強み | kqr が選ばれる理由 |
|---|---|---|
| `kcat \| jq` | ストリームに最適 | jq は行ごとインタプリタ。集計が桁違いに遅い (10–100×差) |
| ksqlDB | 強力な stream SQL | 永続 stream 前提、CLI 用途には重い |
| AKHQ / Conduktor | UI が良い | 集計 SQL が弱い or 有償 |
| **kqr** | bounded ad-hoc 専用 | 起動 < 1s、集計が SQL でフルに、結果は stdout |

詳細なベンチマーク戦略は [DESIGN.md §B](DESIGN.md) 参照。

---

## インストール

### バイナリビルド

```bash
git clone <repo>
cd kqr
cargo install --path kqr-cli
```

`cargo install kqr-cli` は将来 crates.io に公開後に有効化予定。

### Docker

```bash
docker build -t kqr:dev .
docker run --rm kqr:dev --help
```

ホストの Kafka に直結する場合 (Linux):

```bash
docker run --rm --network host kqr:dev query -t demo --last 1m "select count(*) from demo"
```

macOS / Windows:

```bash
docker run --rm --add-host host.docker.internal:host-gateway kqr:dev \
    query -t demo --brokers host.docker.internal:9092 \
    "select count(*) from demo"
```

`docker compose` で立てた Kafka に対してなら:

```bash
docker compose -f docker/compose.yaml up -d --wait
docker run --rm --network kqr_default kqr:dev \
    query -t demo --brokers kafka:9094 "select count(*) from demo"
```

---

## クイックスタート

開発用 Kafka を立てて、サンプルデータを投入し、SQL を打つまでの最短コース:

```bash
# 1. Kafka を立てる
docker compose -f docker/compose.yaml up -d --wait

# 2. JSON を流す
scripts/seed.sh kqr-demo 1000

# 3. SQL を打つ
kqr --brokers localhost:9092 query -t kqr-demo --last 1h \
    "select side, count(*), avg(price) from kqr_demo group by side"

# 4. 対話モードで触る
kqr --brokers localhost:9092 repl -t kqr-demo --last 1h
```

注: ハイフンを含む topic 名は SQL では `_` に置換 (`kqr-demo` → `kqr_demo`)。
警告が stderr に出ます。

### 主要なコマンド

```
kqr topics                                          # topic 一覧
kqr sample -t <topic> -n 10 [--last 5m]             # 生メッセージを覗く
kqr schema -t <topic> [--last 1h]                   # 推論された JSON スキーマ
kqr query  -t <topic> [time-window] <SQL>           # SQL 一発実行
kqr repl   -t <topic> [time-window]                 # 対話モード
```

### 時間窓フラグ (排他)

| フラグ | 意味 |
|---|---|
| `--last <duration>` | `now - duration` から現在まで (例: `10m`, `2h`, `1d`) |
| `--since <dur_or_time>` | duration なら `--last` と同等。RFC3339 なら絶対時刻起点 |
| `--from <rfc3339> --to <rfc3339>` | 絶対範囲 |
| `--offset earliest\|latest --limit N` | offset ベース |

デフォルトは `--last 10m`。

### 出力フォーマット

`--format` で切替: `table` (default) / `json` / `ndjson` / `csv`。
パイプに渡すと `table` は `csv` に自動フォールバック。

### Parquet キャッシュ

```bash
kqr query -t bids --last 1h --reuse "select count(*) from bids"
# 2回目以降、同じ topic + window なら Kafka を叩かずに ~/.cache/kqr/.../*.parquet を読む
```

TTL はデフォルト 1h、`config.toml` の `[cache] ttl = "30m"` で調整。

---

## 設定ファイル

`~/.config/kqr/config.toml`:

```toml
default_profile = "local"

[profiles.local]
brokers = "localhost:9092"

[profiles.prod]
brokers = "kafka-prod-1:9092,kafka-prod-2:9092"
sasl_mechanism = "PLAIN"
sasl_username = "${KQR_PROD_USER}"
sasl_password = "${KQR_PROD_PASS}"
schema_registry_url = "http://schema-registry:8081"

[cache]
ttl = "1h"
```

`${ENV}` は実行時に展開されます。`--profile prod` で切替、CLI フラグ
(`--brokers` 等) は常に config を上書き。

### Consumer group (opt-in)

デフォルトは consumer group を **使わない** (CLI 用途、副作用なし)。
明示的に group に参加して offset を commit したい場合のみ:

```bash
kqr query -t orders --consumer-group-id my-runner --last 5m "select ..."
# stderr に "[kqr] --consumer-group-id set: offsets will be committed to group 'my-runner'"
```

---

## アーキテクチャ

```
kqr-core/src/
  app/                     # application layer (pure logic)
    decode/                # MessageDecoder (JsonDecoder のみ)
    table.rs               # RecordBatch → MemTable
    query.rs               # SessionContext + execute / explain
    output.rs              # table / json / ndjson / csv
    cache.rs               # Parquet キャッシュ
  infra/                   # external I/O. rdkafka はここだけ
    config.rs              # TOML 読み込み + ${ENV} 展開
    kafka/
      mod.rs               # pub trait KafkaSource
      window.rs            # TimeWindow
      consumer.rs          # rdkafka 実装

kqr-cli/src/
  main.rs                  # tokio::main, dispatch
  cli.rs                   # clap derive
  commands/                # query / repl / schema / sample / topics
  progress.rs              # indicatif スピナー / non-TTY periodic
```

**レイヤー分離ルール**: `rdkafka::*` を `use` してよいのは
[kqr-core/src/infra/kafka/](kqr-core/src/infra/kafka/) 配下のみ。
セキュリティ監査時には `infra/` を読めば「kqr が外部に何をしているか」が
網羅できる、という保証を維持しています。

---

## 開発

### 動作チェック

```bash
scripts/check.sh                 # fmt + build + test + clippy
scripts/check.sh --if-changed    # Rust/Cargo に差分がない時はスキップ
scripts/check.sh --integration   # testcontainers で Kafka を立てて統合テスト
```

`.claude/settings.json` の Stop hook 経由で `--if-changed` 付きが
Claude Code セッション後に自動走行します。

### Kafka 開発環境

```bash
docker compose -f docker/compose.yaml up -d --wait
scripts/seed.sh                   # kqr-demo に 100 メッセージ
docker compose -f docker/compose.yaml down -v
```

### テスト

```bash
cargo test --workspace                       # ユニットテスト (高速)
cargo test --workspace -- --include-ignored  # + testcontainers 統合テスト
```

---

## ロードマップ

- [ ] `cargo install kqr-cli` (crates.io 公開)
- [ ] Avro / Protobuf decoder (`MessageDecoder` trait はすでに用意済み)
- [ ] Schema Registry 統合
- [ ] HTTP API (`kqr-server`)
- [ ] MCP server (`kqr-mcp`)
- [ ] Web UI

---

## ライセンス

Apache-2.0 OR MIT
