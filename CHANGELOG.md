# Changelog

## [Unreleased]

- 2026-09-29 [Feature] 增加 `backfill-arb`，把英超事件 `019f2452-6b31-7c38-8c53-4d5867edd46d` 的历史成交回填为已完成套利订单；该事件不再参与新套利计算，止盈和再平衡仍处理已回填持仓 (`src/backfill.rs`, `src/bin/backfill_arb.rs`, `src/exec.rs`, `src/store.rs`)
- 2026-09-29 [Chore] 按市场、互补结果、买卖方向、5 分钟和整单份数差不超过 1.5 份对 Polymarket/Outcome 历史成交进行确定性推断配对，分别保存已匹配与未匹配记录供人工核对 (`matched-trades-1473-1477.json`, `unmatched-trades-1473-1477.json`)
- 2026-09-29 [Fix] 余额不足 NATS 通知设置进程内 1 小时冷却（Outcome 按平台、Polymarket 按账号），Polymarket 余额不足时异步推送后继续切换账号；重启后重置冷却状态 (`src/notify.rs`, `src/exec.rs`, `tests/unit/notify.rs`)
- 2026-09-24 [Fix] Outcome 的 `unknown` 腿在提交满 `UNKNOWN_LEG_TIMEOUT_SECS` 且 `userFills` 历史覆盖完成、没有该 cloid 时收成零成交失败 (`src/exec.rs`, `src/store.rs`)
