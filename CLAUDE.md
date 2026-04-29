# kqr — Claude Code 向け規約

詳細仕様は [DESIGN.md](DESIGN.md)。ここはセッション開始時に必ず読む短いルール集。

## レイヤー分離 (最優先)

`kqr-core` は app 層 / infra 層を厳密に分ける。

- `rdkafka` の `use` を許すのは `kqr-core/src/infra/kafka/` 配下のみ
- application 層 (`kqr-core/src/app/`) は `infra` が公開する trait 経由でしか Kafka を触らない
- Parquet I/O などの外部 I/O も infra に寄せる

理由: セキュリティ監査時に `infra/` だけ読めば「kqr が外部に何をしているか」が網羅できる状態を保つため。

## クエリエンジン

SQL via DataFusion で確定。jq 形式は採用しない (集計が桁違いに速い)。

## 開発フロー

- 変更後は `scripts/check.sh` を流す (fmt / build / test / clippy)
- Stop hook (`.claude/settings.json`) が `--if-changed` 付きで自動実行する
- Kafka を絡めた手動確認: `docker compose -f docker/compose.yaml up -d --wait` → `scripts/seed.sh`
- 完全な integration check: `scripts/check.sh --integration` (実体は step 8 以降)

## ファイルの扱い

- `要望リスト.md` はユーザーの未整理メモ。**コミット対象外**。セッション開始時に目を通すこと
- DESIGN.md が仕様の正本。仕様変更はコミットメッセージに要約を残す
