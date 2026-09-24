# Changelog

## [Unreleased]

- 2026-09-24 [Fix] Outcome 的 `unknown` 腿在提交满 `UNKNOWN_LEG_TIMEOUT_SECS` 且 `userFills` 历史覆盖完成、没有该 cloid 时收成零成交失败 (`src/exec.rs`, `src/store.rs`)
