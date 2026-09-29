# Changelog

## [Unreleased]

- 2026-09-29 [Fix] 余额不足 NATS 通知设置进程内 1 小时冷却（Outcome 按平台、Polymarket 按账号），Polymarket 余额不足时异步推送后继续切换账号；重启后重置冷却状态 (`src/notify.rs`, `src/exec.rs`, `tests/unit/notify.rs`)
- 2026-09-24 [Fix] Outcome 的 `unknown` 腿在提交满 `UNKNOWN_LEG_TIMEOUT_SECS` 且 `userFills` 历史覆盖完成、没有该 cloid 时收成零成交失败 (`src/exec.rs`, `src/store.rs`)
