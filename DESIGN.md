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
- `rdkafka` を `kqr-core/src/infra/kafka/` の外で `use` する (後述のレイヤー分離違反)

---

## 追加決定事項 (要望リスト 反映)

### A. App 層 / Infra 層 の分離

`kqr-core` 内部を 2 層に分ける。`rdkafka` などの外部 I/O は infra 層に閉じ込める。

```
kqr-core/src/
  lib.rs
  error.rs
  config.rs              # 設定読み込み (env / file)
  app/                   # application layer — pure logic
    mod.rs
    decode/              # trait MessageDecoder + JsonDecoder
    table.rs             # RecordBatch → MemTable
    cache.rs             # Parquet cache 層 (Parquet I/O は ports 経由)
    query.rs             # SessionContext + execute
    output.rs            # 出力フォーマット
  infra/                 # infrastructure layer — external I/O
    mod.rs
    kafka/
      mod.rs             # pub trait KafkaSource (port)
      consumer.rs        # rdkafka 実装 (rdkafka 依存はここだけ)
      window.rs          # TimeWindow enum
    fs/                  # 必要に応じて Parquet/設定ファイル I/O 実装
```

**強制ルール**:

- `rdkafka::*` の `use` を許すのは `kqr-core/src/infra/kafka/` 配下のみ
- application 層は `infra::kafka::KafkaSource` (もしくはそれに類する port trait) 越しにしか Kafka に触らない
- 同様に Parquet 読み書き / 設定ファイル I/O も infra 配置とする

**目的**: セキュリティ監査のとき `infra/` だけ読めば「kqr が Kafka に対して何をしているか」が網羅できる状態を保つ。external I/O が散らばっていない、という保証。

(将来 `cargo deny` または手書きの check で 「app 層から rdkafka を使ったら fail」 を機械的に強制する。)

### B. クエリ形式 — SQL (DataFusion) で確定

集計性能を優先して SQL を採用する (jq 形式は不採用)。理由:

- DataFusion は SQL を Arrow RecordBatch 上のベクトル化オペレータに compile し、列指向 + マルチコア並列で動く。aggregate / group-by / join の典型ワークロードで行指向 jq 比 10–100× のスループット差が出る (Arrow / DataFusion ベンチ、TPC-H 文献)。
- jq は単一スレッドの行ごと JSON インタプリタ。projection / filter は速いが集計が弱く、kqr の主用途 (group by, count, sum, distinct, JOIN across topics) ではボトルネックになる。
- SQL の方言知識を流用できる UX 上の利点も SQL 側にある。

実証は step 8 の integration test 兼ベンチで行う (`docker compose` の Kafka に既知の JSON を投入 → 集計クエリのスループット計測)。

### C. ベンチマーク / 動作チェック基盤

- `docker/compose.yaml` — 開発用 single-broker Kafka (KRaft)。
  - host listener: `localhost:9092`
  - docker network listener: `kafka:9094` (docker run / compose run の kqr 用)
  - `docker compose -f docker/compose.yaml up -d --wait` で起動
- `scripts/seed.sh` — 既知 JSON データセットを topic に投入 (手動確認 / bench 入力用)
- `scripts/check.sh` — `cargo fmt --check && build && test && clippy -D warnings` を順に流す
  - `--if-changed` … HEAD と比較して `*.rs` / `*.toml` / `Cargo.lock` に差分がない時はスキップ
  - `--integration` … docker Kafka を起動 (本物の integration test は step 8 で実装)
- `.claude/settings.json` の Stop hook が毎ターン終了時に `scripts/check.sh --if-changed` を走らせる
- 本格的な criterion ベンチは step 5 完了後に `bench/` を生やして実装する

### D. 追加 CLI フラグ (実体は step 3+)

時間窓フラグの追加と consumer group の opt-in 対応。`kqr query` / `repl` /
`schema` / `sample` の共通フラグとして以下を実装する。

- **`--from <time>`** — 開始時刻 (絶対)。RFC3339 文字列 (`2026-04-29T10:00:00Z`)。
  内部的には `offsets_for_times` で各パーティションの開始 offset に解決する。
- **`--to <time>`** — 終了時刻 (絶対)。同上。
- **`--since <time>`** — 開始時刻を「現在から見て」指定。以下のいずれか:
  - humantime duration (`10m`, `2h`, `1d`) → `now - 10m` 起点
  - RFC3339 absolute → `--from` と等価
  - 終端は今の clock 時刻 (= `--to now`)。`--last` の親戚 alias。
- **`--last <duration>`** — 既存仕様維持。`--since 10m` と semantically 同じ。
- **`--offset earliest|latest --limit <N>`** — 既存。
- **排他**: `{--last | --since [+--to] | --from --to | --offset --limit}` のうち
  どれか 1 グループのみ。同時指定はエラー。デフォルトは `--last 10m`。

- **`--consumer-group-id <id>`** — 指定時のみ consumer group に join し、
  Kafka に offset を commit する。**省略時のデフォルトは従来通り `assign` +
  `offsets_for_times` (group 不使用、副作用なし)**。指定時は副作用 (offset commit) を
  伴うため、stderr に warning を1行出す。
- **`--progress`** — 消費進捗を表示する。
  - TTY 出力: `indicatif` で 1 行スピナー (経過時間 / 受信メッセージ数 / 進捗バー)。
    出力テーブルとは stderr に分離して表示が崩れないようにする。
  - Non-TTY 出力 (パイプ / リダイレクト): デフォルト 5 秒に 1 回 `[progress] ...` 行を
    stderr に出す。`--progress-interval <duration>` で間隔調整可。
  - 過剰出力対策: 1 秒に 1 回より速くは更新しない、結果 stdout と progress stderr を
    必ず分けることで `kqr ... > out.csv` がノイズ無し。

`offsets_for_times` を使う以上、`--from` / `--since` は kafka log retention の範囲内
でしか機能しない。範囲外の指定はエラーではなく、利用可能な最古 offset から開始した
旨を warning に出す。

### E. Dockerfile / `docker run` 配布 (追加要望2)

ルートに **multi-stage `Dockerfile`** を配置。

- Builder: `rust:1.91-slim-bookworm` + librdkafka system deps (cmake, libsasl2-dev,
  libssl-dev, libzstd-dev, zlib1g-dev) を事前 install。step 3 で rdkafka が入っても
  Dockerfile 改修不要にする forward-compat 設計。
- Runtime: `debian:bookworm-slim` + ランタイム so (libsasl2-2, libssl3, libzstd1) +
  非 root user (`kqr` UID 10001)。`ENTRYPOINT ["kqr"]`、`CMD ["--help"]`。
- ビルドキャッシュは `Cargo.lock` 含めた `--locked` で再現性確保。`strip` でサイズ削減。

**使い方**:

```bash
docker build -t kqr:dev .
docker run --rm kqr:dev --help

# Linux: ホストの Kafka に直結
docker run --rm --network host kqr:dev query -t demo --last 1m "select count(*) from demo"

# macOS / Windows: ホストの Kafka に host-gateway 経由
docker run --rm --add-host host.docker.internal:host-gateway kqr:dev \
    query -t demo --brokers host.docker.internal:9092 ...

# docker compose の Kafka に直結 (DOCKER listener: kafka:9094 を使う)
docker compose -f docker/compose.yaml up -d --wait
docker run --rm --network kqr_default kqr:dev \
    query -t demo --brokers kafka:9094 ...
```

`.dockerignore` で `target/` などビルドコンテキストから除外。

### F. やってはいけないこと の訂正

上の §A (やってはいけないこと) の以下の項目は **opt-in に格下げ**:

- ~~consumer group を使った long-running 消費 (CLI の責務外)~~
  → **デフォルトは consumer group を使わない**。`--consumer-group-id` 明示時のみ
  join し、その場合は offset commit が副作用として発生することを stderr に告知する。
