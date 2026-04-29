# kqr — Kafka Query Runner

## プロジェクト概要

Kafka topic をアドホックに SQL で集計する CLI ツールを Rust で作る。
`kcat | jq` 比 10x、ksqlDB ほど重くなく、bounded query に割り切ることで
「Kafka 用の DataFusion CLI」というポジションを取る。

最終的に Web UI / HTTP server / MCP server を生やす可能性があるため、
**コアはライブラリ、CLI は薄皮** という構造を最初から守ること。

---

## 非機能要件 (最重要)

- **言語**: Rust (stable, edition 2021 以上)
- **構造**: cargo workspace
  - `kqr-core/` — Kafka 消費 + Arrow 変換 + DataFusion 連携 (lib)
  - `kqr-cli/`  — clap で引数を受けて core を呼ぶだけ (bin)
  - 将来 `kqr-server/`, `kqr-mcp/` を足せる前提
- **依存**: `rdkafka`, `arrow`, `arrow-json`, `datafusion`, `clap` (derive),
  `tokio`, `anyhow`, `thiserror`, `tracing`, `tracing-subscriber`,
  `serde`, `serde_json`, `rustyline`, `comfy-table` (or `tabled`)
- **エラー処理**: lib 側は `thiserror` で型付き、bin 側は `anyhow`
- **ログ**: `tracing`、`-v` / `-vv` で level 切替
- **テスト**: `kqr-core` の各モジュールにユニットテスト。
  Kafka 周りは `testcontainers` で integration test を 1 本用意
- **README**: 後述の構成で書く

---

## 機能要件

### コマンド

```
kqr query   -t <topic> [time-window] <SQL>     # 一発実行
kqr repl    -t <topic> [time-window]            # 対話モード
kqr schema  -t <topic> [time-window]            # 推論スキーマ表示
kqr sample  -t <topic> [-n N]                   # 生メッセージ覗き見
kqr topics                                      # topic 一覧
```

### 時間窓フラグ (共通)

- `--last <duration>`  例: `10m`, `1h`, `2d` (humantime)
- `--from <rfc3339> --to <rfc3339>`  絶対時刻
- `--offset earliest|latest --limit <N>`  件数指定

排他。同時指定はエラー。デフォルトは `--last 10m`。

### テーブル名規約

`-t <topic>` で指定された topic 名がそのまま SQL のテーブル名になる。
`-t bids -t wins` のように複数指定可能で、JOIN 可能にする。
topic 名が SQL 識別子として不正な場合 (ハイフン等) は警告して
`_` に置換した名前を使い、その旨を stderr に出す。

### 出力形式

`--format` で切替: `table` (default), `json`, `ndjson`, `csv`.
`table` は `comfy-table`、TTY じゃなければ `csv` にフォールバック。

### 設定ファイル

`~/.config/kqr/config.toml` にプロファイル定義:

```toml
default_profile = "local"

[profiles.local]
brokers = "localhost:9092"

[profiles.prod]
brokers = "kafka-prod-1:9092,kafka-prod-2:9092"
sasl_mechanism = "PLAIN"
sasl_username = "${KQR_PROD_USER}"   # env 展開
sasl_password = "${KQR_PROD_PASS}"
schema_registry_url = "http://schema-registry:8081"
```

`--profile prod` で切替。CLI フラグは config を上書き。

### Schema 推論

JSON のみまず対応。`arrow-json::ReaderBuilder::infer_schema` を使う。
最初の N (default 1000) 件を読んで schema 確定。
`--schema-sample <N>` で調整可能。
推論失敗時は `kqr schema` の結果と共に分かりやすいエラー。

Avro / Protobuf は将来対応。trait `MessageDecoder` を切って差し替え可能に
しておく (今は `JsonDecoder` のみ実装)。

### Kafka 消費

- consumer group は使わない (CLI 用途のため)
- `assign` + `offsets_for_times` でパーティション直接アサイン
- パーティション並列消費 (各パーティションに `tokio::task`)
- バックプレッシャは `tokio::sync::mpsc` の bounded channel
- 終了条件: 「全パーティションが目的 offset に到達」または `--limit` 到達

### REPL

`rustyline` ベース。以下のメタコマンド:
- `\d` — テーブル一覧
- `\d <table>` — schema 表示
- `\timing on|off` — 実行時間表示
- `\format json|table|csv` — 出力形式
- `\reuse on|off` — 後述の cache を使うか
- `\q` — 終了
- 履歴は `~/.local/state/kqr/history`

### キャッシュ (重要・差別化要素)

`--reuse` 指定時、消費した RecordBatch を Parquet で
`~/.cache/kqr/<broker_hash>/<topic>/<window_hash>.parquet` に保存。
同じ topic + window の query が再実行されたら、Kafka を叩かずに
Parquet を読む。
クエリ試行錯誤のループを爆速化する。`--no-reuse` で無効化。
TTL は config で設定 (default 1h)。

### EXPLAIN

`--explain` で DataFusion の logical / physical plan を出力。

---

## モジュール設計 (kqr-core)

```
kqr-core/src/
  lib.rs
  config.rs        — Config, Profile, env 展開
  kafka/
    mod.rs
    consumer.rs    — partition assignment, offsets_for_times
    window.rs      — TimeWindow enum (Last / Range / Offset)
  decode/
    mod.rs         — trait MessageDecoder
    json.rs        — JsonDecoder + infer_schema
  table.rs         — RecordBatch 蓄積 → MemTable 化
  cache.rs         — Parquet cache layer
  query.rs         — SessionContext 構築 + execute
  output.rs        — RecordBatch → table/json/csv/ndjson
  error.rs
```

`kqr-cli/src/` は `main.rs` + `cli.rs` (clap derive) + `repl.rs` のみ。
ビジネスロジックを cli に書かない。

---

## 実装順 (Claude Code への指示)

以下の順番で **PR 単位で commit** すること。
各ステップで `cargo test`, `cargo clippy -- -D warnings`,
`cargo fmt` が通ることを確認。

1. **workspace スキャフォールド**
   - `Cargo.toml` (workspace)、両クレートの skeleton、CI 用 `rust-toolchain.toml`
2. **config 層**
   - TOML 読み込み、env 展開、`--profile`、ユニットテスト
3. **kafka/window.rs と consumer.rs**
   - `TimeWindow` の解析と `offsets_for_times` 実装
   - `kqr sample` を最初の動作確認用コマンドとして実装
4. **decode/json.rs と table.rs**
   - schema 推論 + RecordBatch 化、`kqr schema` を実装
5. **query.rs と output.rs**
   - `kqr query` 実装、`--format` 切替、`--explain`
6. **cache.rs**
   - Parquet 保存 / 読み出し、`--reuse`、TTL
7. **repl.rs**
   - メタコマンド、履歴
8. **integration test**
   - testcontainers で Kafka 立ち上げ、JSON 投入 → query で集計検証
9. **README / examples**
   - 後述の README 構成

各ステップ完了時、変更点と次ステップの計画を簡潔に報告すること。

---

## README に書く内容 (最後に作成)

1. ロゴ的な ASCII か一言キャッチ ("Kafka topic を SQL で。")
2. **30 秒デモ**: `kqr query -t bids --last 10m "select ..."` の GIF か出力例
3. **なぜ既存ツールで足りないか**:
   - `kcat | jq` — シリアル & 集計弱い
   - ksqlDB — 重い、永続 stream 前提
   - AKHQ / Conduktor — 集計 SQL は弱い or 有償
   - kqr は **bounded ad-hoc** に振り切る
4. インストール (`cargo install kqr-cli`)
5. クイックスタート (5 例ほど)
6. config 例
7. アーキテクチャ図 (Kafka → Arrow → DataFusion → 出力)
8. ロードマップ (Avro/Protobuf, HTTP server, MCP, Web UI)
9. ライセンス: Apache-2.0 OR MIT

---

## やってはいけないこと

- consumer group を使った long-running 消費 (CLI の責務外)
- ビジネスロジックを `kqr-cli` に書く (テスト不能になる)
- topic ごとの schema 戦略を JSON 以外で頑張る (今回は JSON のみ)
- 認証 / multi-tenant の作り込み (CLI なので不要)
- Kafka を毎回フルスキャン (window と limit を必ず尊重)
