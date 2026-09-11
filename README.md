# market-arb

Polymarket ↔ Outcome（HIP-4 / Hyperliquid）市价套利服务。单进程：发现市场、订单簿、计算、下单、成交回填、对冲。

源码、脚本和 systemd unit 用 **git 同步**。不要在生产机 `cargo build`：Release 编译会打满 CPU/内存，容易把交易服务器卡死。Linux 二进制在开发机交叉编译，只 scp 可执行文件。

本机 SSH 配置了 Host `arb`（`~/.ssh/config`），因此下面一律用 `ssh arb` / `scp ... arb:`，与 `user@ip` 等价。

**在哪执行：**

| 标记       | 含义                                                                                |
| ---------- | ----------------------------------------------------------------------------------- |
| 【本机】   | 在你的 Mac 上执行。`build-linux.sh`、`deploy.sh`、`scp`、`ssh arb ...` 都是本机命令 |
| 【服务器】 | 先 `ssh arb` 登录后再执行，或写在 `ssh arb '...'` 引号里面                          |

`./scripts/deploy.sh` **始终在本机跑**（它自己会 scp 并远程 restart）。不要在服务器上执行 `deploy.sh` 或 `cargo build`。

## 本地运行

【本机】开发调试：

```bash
cp .env.example .env
cp polymarket_funders.json.example polymarket_funders.json
# 填好 Postgres、.env 与 polymarket_funders.json
cargo run --release
```

自动交易由三个互相独立、默认关闭的执行开关控制：`ENABLE_ARB`（新套利）、`ENABLE_REBALANCE`（再平衡）、`ENABLE_TAKE_PROFIT`（止盈）。某项为 `false` 时仍扫描并计算对应机会，但不会 claim 生命周期动作、写入新订单/交易腿或提交真实交易；其他已开启流程不受影响。成交回填、结算处理也不受这些开关影响，显式人工入口 `place-test --confirm` 保持独立。旧 `ENABLE_TRADING`、`ENABLE_BUY`、`TAKE_PROFIT_ENABLED` 不再读取，升级时必须逐项配置。详见 `.env.example`。

启动时会在业务初始化前自动执行嵌入二进制的数据库迁移，迁移失败不会进入业务流程。已应用的迁移文件不可原地修改；`0009_position_status_width.sql` 将 `position_status` 扩为 `VARCHAR(32)`，允许完整保存 `settlement_pending`。加宽 `varchar` 不重写表，但依赖该列的索引会重建、相关 CHECK 约束会重新校验，全程持 `ACCESS EXCLUSIVE` 锁，锁时长随 `arb_orders` 规模增长。部署时应为该锁预留窗口，并避免同时存在长事务。

应用 0009 后，回退构建必须仍支持 pending 并嵌入相同内容的 0009；直接换回缺少该迁移的旧二进制会被 sqlx 版本校验拒绝。不要通过缩回列宽、删除迁移记录或绕过校验来回退。

## 盘口一致性与建单准入

盘口保留整本全量基线，不以逐档 timestamp 合并 REST 和 WebSocket。PM 同一连接内的同毫秒增量按接收顺序应用；较旧增量或无效消息使该 token 等待完整同步。全量快照会移除未包含的档位，但更旧或同毫秒内容冲突的全量不能恢复已删除档位。断线、新连接均推进连接代际，旧请求不能跨连接恢复盘口。

REST 是独立完整快照候选，不作为后续 WS 增量的底本。请求前记录 token 的连接代际和本地修订号，回包后重新核对；请求期间发生盘口、tick、完整性变化或其他 REST 提交时，旧候选被拒绝。新套利和止盈仍必须通过本次 HTTP 二次确认，不会把被拒响应重新包装成新鲜盘口，也不会自动降级为仅凭 WS 下单。持续高频变化可能连续跳过交易；这是保守确认的可用性边界，不保证有限时间内成交。

PM tick 单独保存交易所时间高水位；WS tick、完整快照及单/批 REST 使用同一接纳规则。旧观察不能覆盖新 tick，同时间异值会撤销 tick 可信性，需更晚的明确观察恢复。tick 不刷新深度 TTL，也不恢复 WS 完整性；无时间戳 `/tick-size` 只允许按请求票据初始化缺失值，不能解除时间冲突。自动交易在 tick 缺失或原 cap 与当前 tick 不兼容时跳过/拒绝提交，不借人工入口的无 tick fallback 绕过保护。

新套利 HTTP 确认保持原方向、股数和 cap，但按本次盘口刷新均价、成本、费用、利润及收益率；后续余额检查、建档和通知使用刷新后的估算。这不是资金预留，不能消除确认之后的行情变化或并发余额消耗。

PM 买入签名保持既有 maker 金额两位、taker 股数五位精度，但不再通过向上取分扩大 cap 或截断原股数。金额由原 Decimal 尾数与 scale 精确构造，并以整数交叉乘法验证最终 `makerAmount/takerAmount <= cap`。不可表示时新套利在双腿建档前整笔跳过，不自动缩量或改 cap；再平衡在候选择优前排除该买入，签名末端也执行同一检查。手工买入也遵守原 cap 与 tick 相容、原数量无损的限制；PM 卖出精度不变。例如 `7×0.333` 拒绝，`10×0.333` 可表示。

再平衡买入的规划与执行共用 `最终限价本金 + 当前估计手续费` 的余额需求（余额等于需求时允许），先排除不可执行买入，再比较买卖边际收益；收益及费用估计仍按原盘口均价，不用 cap 本金替换预计成本。费用余量不是所有成交路径的严格上界，也不是资金预留。资金不足或 PM 金额不可表示不会被当作已完成再平衡。

过期 PM 盘口的 REST 补全按 token 轮转，按尝试推进游标；失败、缺返回和版本冲突不会让排序前一批长期占用全部配额。`BOOK_RESYNC_BATCH` 限制每轮尝试量，不承诺每个响应都可接纳。

`MAX_ACTIVE_ORDERS` 的权威检查位于父单与初始腿建档事务内，通过统一 PostgreSQL transaction advisory lock 串行准入；限额 0 仍表示不限，但也遵守相同锁协议。计数保持原有 `pending/actived/completed` 口径（completed 即使已平仓或结算仍计入），不是未平仓仓位数。等待额度锁时确认过期会跳过建单，不能留下 pending 腿。多实例须保持一致限额并全部升级，旧进程或绕过入口的直接 SQL 不受此协议保护。

新套利会跨 PM 小数档累计完整整数份额，成本按实际吃档量计算，cap 按最后吃到的价格确定；Outcome 仍逐档取整。每个物理报价区间使用精确预算及收益门槛，取首个可行区间内的最大合法整数数量，不做跨区间全局最优搜索，也不逐股遍历大单。PM `.30×4.5 + .61×100`、Outcome `.40×200`、零费率、预算100、最低利润1时选择39股（利润1.005）；PM费率0.07、Outcome费率0时选择14股。

**新套利精度口径：** 从已解析 Decimal 的尾数与 scale 精确转换为有理数，在深度乘加之前消除中间舍入；数量搜索、HTTP重新计价、两轮余额检查及ROI方向择优共享精确口径。PM估算仍为 `r*(P-P²/S)`（原均价费用公式的等价式）；Outcome 买入只计实际随订单发送的 builder 比例，未来结算准备单列。预算以即时现金成本判断，利润/APR以净预计兑付减即时成本判断，精确比较且不加epsilon。输入价格须为合法预测价格、预算为正，PM系数支持[0,1]、Outcome协议比例须[0,1)；无效参数跳过，不回退旧固定费率或舍入算法。

数据库及通知的财务字段仍为 Decimal 展示投影：预计费用/准备保守投影，净收入及收益不向乐观方向舍入；利润由展示净收入减展示现金成本派生，均价和收益率为近似值，不以展示值重新作交易门槛判断。资金检查使用精确需求，Outcome按最终cap本金加cap对应builder费用预留，required向上转换，不包含未来结算准备；无法表示的计划保守拒绝。`ArbPlan`精确估值仅保存在内存并绑定身份、股数与cap，`net_shares`仍是股数，`expected_revenue`才是预计收入。实扣成交费与历史账务不因估算更新而重算。

### Outcome 动态费用与估算边界

启动获取 `userFees`（实际账户地址，不是agent）和 `outcomeMeta`，每300秒刷新组合内存快照，900秒过期。交易热路径不请求费用接口；刷新失败保留原快照但不延长时间。没有有效费率只暂停依赖它的新套利/止盈/再平衡，对账、结算查询和状态收尾继续。旧 `OUTCOME_TAKER_FEE_RATE` 已忽略，无固定费率兜底；无需新增env或SQL迁移。

当前估算模型只支持已验证的 `venue=out`、`quoteToken=USDC`、顶层 `feeScale=1` 和合法逐市场 `deployerFeeScale=s`：`r = userSpotCrossRate × (1-activeReferralDiscount) × [s+max(s,1)]`。当前样本 `0.0007×0.96×2=0.001344`，即13.44bps；不额外乘固定2，不另套未经核实的staking/稳定币折扣。这是当前市场的样本支持模型，不是所有Outcome市场的通用官方费率。未知或缺失规则按不可估值处理。

本策略采用正仓位、IOC交易：增加正仓的买入协议费0，减少正仓的卖出协议费为名义额×r。若实际订单带非零builder费，`b=OUTCOME_BUILDER_FEE/100000`，买卖均另预估名义额×b；builder best-effort并不意味着预估可以忽略。卖出仍须通过可用token余额检查，不支持把任意负仓买入直接解释为免费开仓。

严格互补q对的新套利暂以 `q×r` 作为未来结算准备，来源标记 `estimated_from_taker_close`，净预计收入 `q×(1-r)`；这只是所选结算假设下的情景估计，不是实际扣费或严格费用上界。准备影响profit/ROI/APR，不进入买腿req_fee、当前余额或现金budget，也不重复从profit扣减。止盈保持“两腿净卖出收入-q”门槛；再平衡卖超额取净现金，补PM或Outcome缺口均按新增配对净预计兑付减即时买成本。

最终确认绑定计划、费率规则和有效期限，尚未发送就过期/变化时放弃本轮；多腿开始发送后使用冻结依据，不因后台刷新重签重发。估算在父单计划JSON及 `legs.last_order_info.fee_estimate` 留档，不进入实际成交费用。`fills.fee_rate_bps` 对Outcome仍可NULL；实际费用只取有效fill fee（包括0，已含builderFee），缺实扣证据继续等待。

分钟统计：`outcome_fee_refresh_ok/failed` 统计完整刷新结果，`outcome_fee_unavailable` 统计费率不可用跳过。正常细节DEBUG、就绪/失效/恢复INFO、刷新失败WARN；不打印完整费用响应。


## 人工重算历史 unknown 收益

历史订单没有新事件时不会自动恢复估值。部署包含此命令的新版二进制后，建议先停服务，在项目工作目录以服务用户执行一次，再根据汇总排查残留 unknown 后启动服务：

```sh
cd /var/www/arb_market
sudo -u market-arb ./dist/market-arb recompute-actuals --confirm
```

这是独立于日常事件驱动的人工维护入口，不加入正常启动、systemd 或定时扫描。无参数仍启动原服务；`--help` 仅显示帮助；缺确认、未知/重复/多余参数均在配置和 I/O 前拒绝。

命令沿用工作目录 dotenv 配置，仅使用 `APP_POSTGRES_URI`、`HYPERLIQUID_INFO_URL`、`OUTCOME_ACCOUNT_ADDRESS`、`OUTCOME_BUILDER_ADDRESS/FEE`，不加载 funders、不解析私钥、不要求 common；只读费用客户端不创建 signer，不发送交易或连接 WS。不执行迁移，业务库须已完成已有迁移，并预留投影写入和 info 费用查询权限。

一轮固定最大订单 ID，每页20个，仅尝试未 settled 且处于 `watching/settlement_pending` 的 unknown 投影。ID 上界不是一致性快照，可能看到启动前已分配 ID、稍后才提交的行；不追逐上界外新订单。锁内重检后跳过已恢复、已关闭/最终结算或删除的订单。只原子更新收益及投影，不改交易腿、fills、历史 fee_estimate、实扣费用、结算和生命周期。优先冻结快照；只有完全缺失快照且已提交的历史组才使用身份匹配的有效内存费率，损坏/过期/错误钱包证据仍 unknown，无证据不清零原金额。

有候选才查询费用，长批次在页间按300秒间隔刷新；刷新失败计入 errors，但仍尝试冻结证据，不延长旧缓存有效期。账户缺失也不阻止冻结路径。单单失败继续并推进游标，数据库连接或分页失败停止。运行后输出 `upper_id/scanned/repaired/still_unknown/skipped/errors`；存在 still_unknown 或 errors 时非零退出，并发正常跳过不算失败。重跑只尝试剩余 unknown，不覆盖已成功估值；成功仅代表本轮候选，不代表全局风控一定放行。

## 成交确认与恢复

ACK 仅记录已受理及预期撮合量，交易腿保留 `actived`，不生成 `ack:` 伪成交或暂估实际费用。Polymarket 必须取齐订单关联的成交并等待 `CONFIRMED`／`FAILED` 最终状态；兼容 REST 的 `TRADE_STATUS_*` 和既有裸值，只有 Confirmed 部分计入仓位，`MATCHED`、`MATCHED_NOT_BROADCASTED`、`MINED`、`RETRYING` 和未知状态继续等待。订单状态兼容明确的 `ORDER_STATUS_*` 枚举及裸值，未知状态不能靠后缀猜为取消；原始状态保留在证据中。订单查询中的正撮合量不替代成交确认。

PM trades 固定查询首次提交起 300 秒的时间窗口；分页完成后仍待确认时重扫相同窗口，五分钟不是确认时限。order 返回 404／JSON null 时继续查 trades；完整分页扫描没有匹配本单的成交，且与已存成交及已知执行约束不冲突时，将 PM 腿标为 `failed`，实际数量／价格／费用均为零。首次完整空扫描即可收口，不必等满五分钟，也适用于历史未终态腿；平台延迟可见仍可能造成零成交误判，终态腿不再自动回填。非空成交集合须精确匹配本单且全终态、费用证据齐全后才收尾；查询失败或分页不完整不等于零成交。另一平台已有成交仍保留，父单沿用现有汇总和后续持仓管理。曾经查到的关联成交 ID 持续取并集、撮合量持续取最大值作为下界，后续缺单或较旧快照不能抹掉这些约束。即使尚未拿到 fills，已知正成交证据也禁止随意换绑订单 ID。旧 JSON 中仍在的同单证据可自动恢复，已被旧版本完全覆盖的证据无法凭空找回，历史终态不会自动重开。

Outcome 丢失 ACK 时使用已持久化的 cloid 查询真实 oid；数字 oid 以 JSON 整数发送，cloid 以字符串发送。成交历史使用首次提交前 30 秒到本轮固定终点的时间窗口查询，每次最多读一页，已成功页面和下一游标一起落库。同毫秒饱和、历史保留窗口不足或无法证明零成交时继续核对，不把查询不到／空页／超时解释成零成交。

Outcome 历史覆盖采用初始探测、数据分页、最终覆盖验证三个阶段，每次回填最多一个 HTTP 请求。两次探测中已见到的本单真实成交也会持久化；尾页不直接授权零成交终态，而要重新验证当前保留窗口仍覆盖扫描起点。重启不复用旧 `history_complete` 布尔值作为新覆盖证明；旧 v1 游标安全恢复到新阶段。无法证明覆盖的取消单继续等待，但可靠预期数量与真实确认成交精确吻合的既有捷径不受影响。此修复不重开历史已终态交易腿，也不把本地扫描轮次视为交易所原子快照。

真实成交、分页进度、交易腿最终账务及父单完成通过事务落库；重复页和重启重放按真实成交 ID 去重。迟到 ACK／Unknown 不覆盖终态，旧 `ack:` 行保留但不参与新汇总。此次逻辑仅处理开放腿与新订单，历史终态订单不会自动重开或重算。

`UNKNOWN_LEG_TIMEOUT_SECS` 同时覆盖 `unknown` 和确认中的 `actived`，按首次提交时间计时，已有部分成交或持续重试不会推迟告警。超时仅告警、暂停新套利并继续回填，不清零成交。等待 PM 链上确认也会延后父单完成和生命周期 claim 释放。

**费用口径：** 明确提供金额且币种为 USDC（PM 也接受 pUSD）的费用按原值入账，包含显式 0；Outcome `fee` 已含 builderFee，不重复叠加。PM maker 的官方零费规则记录为 `calculated_maker_zero`。PM taker 未返回实扣金额时，优先使用 COMMON 对应事件/index/condition/token 的 `feeSchedule.rate`，`feesEnabled=false` 明确为零；目录或费率缺失才回退 `POLYMARKET_FEE_BPS_PRIOR / 10000`（默认700bps，即系数0.07，并非成交本金统一收7%）。COMMON查询失败、畸形/越界费率、市场身份冲突不静默回退。费率按[官方费用公式](https://docs.polymarket.com/trading/fees) `shares × rate × price × (1-price)` 逐笔计算，不使用指数参数，五位小数四舍五入，不乘1.3安全倍率，费用来源仍记为 `calculated`。新费用快照不再请求 `/clob-markets`；已有 `clob-markets` 快照继续兼容并冻结。该政策假设本次读取的COMMON/default系数适用于待确认成交、pUSD按1美元计价，计算金额不是实扣证明。

PM成交接口的 `fee_rate_bps` 仅留在原始JSON，不作为规范化输入，也不会因其缺失或畸形拒绝整页；实际费用金额、身份、数量和价格仍严格校验。新 `fills.fee_rate_bps` 表示从已选费用快照派生的bps（rate×10000），不声称是平台报告值；无实际费的maker记0。有效实际费用不依赖COMMON补可选费率，无法从已有来源恢复bps时允许NULL。快照保存 `source=common/env`、比例rate、bps、市场身份、选取时点及env回退原因；计算金额保存在 `raw.accounting`，`fills.fee` 没有实扣时仍为NULL。重扫先批量合并已存证据，只有真正缺费用快照才查来源，一页共享一次选择；事务内再次合并冻结快照和bps，不用新费率改旧账。已有实扣但币种不支持仍等待；历史终态不自动重开、重算，旧NULL/0不批量回填。

## 市场与结算口径

common 数据库提供给本服务的统一事件视为已经完成业务筛选的二元市场。Outcome 结算遵循 HIP-4 原始分数兑付：side 0 每股兑付 `settleFraction`，side 1 每股兑付 `1-settleFraction`，两侧合计为 1；`0.5` 时两侧各兑付 0.5。分数必须位于 `[0,1]`，缺字段、不可解析或越界值不会落为已结算。

普通持仓的结算检查使用当前已加载或恢复的事件 `Topic.end_date`（来自 common 的 `events.end_date`）：结束时间未到时跳过两平台结算接口和结算判定，继续既有止盈、再平衡计算及执行开关逻辑；到达结束时间（含相等）或结束时间缺失时照常查询。止盈和再平衡提交前会重新比较当前 UTC 时间，不复用扫描初次的时间判断。已进入 `settlement_pending` 的订单不受此时间门禁影响，仍按独立清扫节奏核实结算。该规则会推迟发现平台提前结算或不可交易的情况，跳过检查不代表已确认市场仍可交易。

时间门禁的排查优先看每分钟 `minute stats`：`settlement_skipped_before_end` 统计未到期跳过次数，`settlement_end_date_missing` 统计时间缺失而继续查询的次数；配合原有 `settlement_scan`、`settlement_pending_scan` 判断处理路径。这些均是检查次数，不是去重订单数或 HTTP 请求数，提交前复查也会计数。需要逐订单排查时，启用 `market_arb::exec` 的 DEBUG 可见 `settlement time gate evaluated`，包含 `order_id`、`end_date`、`checked_at` 和 `decision`（`skip_before_end`、`query_due`、`query_missing_end_date`）。日志过滤器在加载 `.env` 前初始化，应通过进程启动环境或 systemd 覆盖配置设置 `RUST_LOG`，仅修改 `.env` 不会调整日志级别。

任一平台先确认结算时，订单停止止盈、再平衡和所有新交易；两平台 payout 以及 Outcome 实际结算费证据未齐时保持 `settlement_pending`。Outcome 使用官方 `userFillsByTime` 中 `dir=Settlement` 的 `sz/px/fee/feeToken/tid` 核实实扣，并核对钱包/token 的真实买卖成交、转移记录和结算数量。缺费用不是零费用，零兑付也须明确零费证据；已确认无剩余 Outcome 仓位则记 `not_applicable`，不查询无关结算费。不会用单平台结果推算另一侧，也不会自动卖出另一平台持仓。

同钱包/token 的所有相关订单（包括历史 settled）共同参与剩余股数核对，按份额分配真实结算费；确定性尾差保证分配总额等于官方实扣。同组事件和分配独立保存，不改写普通交易 fills。费用核实后 `actual_rev` 为原结算收入减分配费用，`actual_profit` 同额扣减，保留现有成交成本/均价精度口径；证据标记 `settlement_fee_status=verified`、`profit_basis=net_payout_less_trade_costs`。查询窗口不完整、外部持仓或归属不明均继续等待，不使用估算值顶替实扣。未结算 actual 汇总按既有毛锁定兑付减 Outcome 剩余净仓位的卖出费用准备（假设 payout=1，包含 builder），保持 estimated，不是实扣证据。

未结算订单按规范化 wallet/token 分组，优先使用按 submitted_at、leg.id 排序的最新合法冻结 `fee_estimate`；冻结来源没有 TTL，新损坏候选不覆盖旧合法快照。仅当组内相关有成交腿全部已提交、且 `last_order_info` 为 SQL NULL 或合法对象完全缺少 `fee_estimate` 键时，订单真实变化事件才显式使用当前配置账户、严格对应市场的最新有效内存缓存兜底。此处“历史缺失”只是数据形态，不能区分旧数据与新数据意外丢失；JSON null、损坏字段、未提交、钱包/市场不匹配或缓存缺失/过期保持 unknown，不默认零。套利、止盈、再平衡建腿/成交、对账新增或更正执行证据及真实终态转换，在原事务内通过同步缓存 resolver 投影；PM 异步成交费快照与此 resolver 独立，锁内不做网络请求。重复回执、仅诊断/分页进度变化、无状态转换的完成扫描均不重估。

来源只写 `arb_orders.actuals_projection`：各组 `source_kind=frozen/latest_valid_fallback`，冻结保留来源腿，兜底保留原始抓取快照及数值 Unix 秒有效期，不伪造历史腿。顶层标记是否含兜底及最早 `fallback_valid_until`；有效期受双源900秒 TTL 和 monotonic 剩余时间共同限制，选取时仍严格验证缓存 TTL 与身份，原始期限只作为事件估值的审计信息，已落库收益不因当前时间超过该期限自动失效。已取消周期收益扫描及其游标：持仓检查、行情或费率缓存刷新、时间流逝不会改收益及 computed_at；下一次订单真实变化才使用当时合法费率。历史 unknown 无新事件不会自动恢复，不做启动回填。通知只取条件完成事务中的有效已存收益，不独立重算。亏损门禁仍检查投影 status/version/stale，MAX_REALIZED_LOSS 不关闭，真实成交 `fills/actual_fee`、最终结算及零仓关闭不被此估算改写。

止盈 `take_profit` 和再平衡 `rebalance` 成功 claim 时，在父单锁内保存有效的 claim 前整笔收益及估值证据；基线固定，不滚动吸收本 claim 的部分成交。只有本 claim 的正常 `pending/actived` 待确认造成当前投影 unknown、全腿结构化校验通过、claim 外证据完全不变时，亏损汇总临时使用该基线，且该订单不计入 blocking unknown。当前完整估值恢复后立即优先使用当前收益。新套利 unknown、unknown 状态交易腿、其他 claim/异常证据/缺快照/负持仓均不豁免，claim 外相关输入变化永久撤销本 claim 资格。本 claim 新腿还须匹配基线已知的同平台 token/label 和 wallet/funder 身份；此前未出现的新 token 或账户不享受豁免（包括合法的再平衡补仓），操作仍按原规则执行。未提交腿按 created_at 加 `PENDING_LEG_TIMEOUT_SECS`、已提交腿按 submitted_at 加 `UNKNOWN_LEG_TIMEOUT_SECS`，另有 claimed_at 加这两个 timeout 之和的固定上限；查询时到期即撤销，不依赖新事件、轮询或周期任务，新增腿不续期。已选择的历史 fallback 不重新套费用缓存 TTL。风险 SUM 和 unknown 计数使用同一数据库快照与资格结果，只取少量 active claim 候选，不逐行情扫描全库 legs。释放精确 claim、最终结算/零仓关闭清理临时证据；实际投影及通知始终保持当前真实状态，不使用风险基线。此窗口接受短期估值滞后，尤其再平衡 BUY 增加仓位时，新增风险可能尚未反映到亏损汇总；既有 stale-unknown 前置保护、亏损上限、余额及订单互斥保持不变。

历史已最终结算的 unknown/旧版毛收益订单不自动重写。使用 `cargo run --bin settlement-audit -- --order-id ID` 只读对账（`APP_POSTGRES_URI` 由进程环境注入，不加载环境文件、不运行迁移、不初始化签名）。报告包含实费、旧值、新值与 `report_fingerprint`；审阅并明确确认后，才可附加 `--apply --confirm-report FINGERPRINT` 应用。写入前重查证据、参与者与旧金额，指纹变化拒绝应用；保留 `actual_cost` 和原 `settled_at`，记录更正审计。历史证据缺失时不得强制套用当前费率。服务启动迁移会新增结算组/事件/分配表并放宽实际收入、利润列的数值精度；不会自动补扣历史费用。

`settlement_pending` 走独立的扫描游标和批量，默认每 `SETTLEMENT_PENDING_SCAN_INTERVAL_SECS`（60 秒）清扫一次、每次至多 `SETTLEMENT_PENDING_SCAN_BATCH` 条，不再占用 `POSITION_SCAN_BATCH` 给活跃订单的配额，因此 pending 积压不会拉长止盈响应。清扫在同一个循环内串行执行，同一订单不会被两条路径并发处理。pending 没有超时自动最终化，长期缺失对手方 payout 时会持续驻留并每轮重试。

套利、补齐对冲和开放持仓账务中的每对 `$1` 估值，依赖跨平台事件定义与结算规则一致；“二元市场”本身不保证这一点。common 的配对规则及真实市场的分数、平局、取消／退款语义仍需独立核验。最终账务按各平台实际 payout 核算，不代表估值前提已被验证，也不消除跨平台判决分歧风险。

Outcome 兑付落在 `(0,1)` 时会打 `warn` 并计入 `outcome_fractional_settlement`，用于发现上述估值前提失效。该指标按**观测次数**计数而非去重订单数：结算确认后当轮扫描只会计一次，但订单最终化前的后续清扫会重复计数。计数不改变入账口径，也不会拦截交易——分数结算仍按实际 payout 核算。Polymarket 也支持按 price 分数兑付，但不参与这个 Outcome 专用指标。

最终核算返回错误时，`settlement_finalize_fail` 记录该分类失败并输出订单、市场和错误上下文；上层仍计入 `exec_err`，两项不能相加作为失败总数。失败日志不保证事务一定未提交，重试仍依赖既有最终核算幂等性。

### 独立市场证据与最终化

`0011_platform_settlement_results.sql` 新增 `order_platform_settlement_results`，按订单/平台保存版本化市场身份、查询 endpoint、精确 payout 和首次成功观测时间。同身份同 payout 重试幂等，冲突拒绝覆盖。PM 继续使用 CLOB `markets/{condition_id}` 的明确单 winner，严格匹配响应 condition 和唯一 token；恰好一个 winner 仅作为终态门槛，所有 token 均按自身 price 的 Decimal 值兑付（包括 0.5/0.5、0.3/0.7），完整二元向量须在 [0,1] 且精确合计 1，不自动归一化。closed/价格本身不证明结算，无明确单 winner 的退款/平局仍待确认。Outcome 使用 `settledOutcome` 的分数和持久化 side token 映射。市场证据不是账户到账证明，观测时间不是官方结算时间。

PM 新来源为 `clob_market_price`，`evidence_version=1` 仍只表示封装结构；保存、缓存及最终化均重放来源响应，拒绝未知来源/版本。未最终化订单的旧 `clob_market_winner` 缓存视为 miss，继续停止交易并重新取证；合法新响应在父单锁内原子升级，`previous_evidence` 保留旧 source/payouts/evidence/observed_at，新观测时间更新而 pending 首次时间不变。已 settled 历史金额和证据不自动修改，无需新增迁移。部署必须先停止/排空旧结算 worker 再启用新版；不能直接回滚到不识别新语义的旧执行器。

两个查询分支分别提交证据；成功分支不等慢侧网络返回。Outcome 自身结果确认即开始账户事件采集，未核实成交/活跃 claim 不阻止原始采集，但阻止证明、费用封存与最终入账。分页固定窗口终点，未封存的完整窗口（包括已有部分事件）下轮扩大重扫；进度和诊断更新比较旧进度，过期请求不能覆盖新页。sealed 组不重新扫描。

首次结果与 pending 转移同事务，保留活跃 claim；pending 禁止新腿和初次发送，但允许在途成交对账。pending 清扫可释放已收尾 claim，释放不恢复 watching。数据库最终化从已保存的两个平台结果读取 payout，要求所有腿实际数量/价格/费用齐全且无 claim，再在同一事务保存费用应用、整单金额和 `settlement_result.platform_amounts`（平台成本、卖出净回款、剩余毛兑付、实结算费、净收入、利润）。已发往交易所的请求不能撤回，最终化等待其稳定收尾。

验证：`cargo test --lib --quiet`、`cargo test --test settlement_fees --quiet`、`cargo check --bins`。显式 DB 测试用进程 `APP_POSTGRES_URI` 连接本地测试库，运行 `cargo test --test settlement_fees -- --ignored`；仅创建/迁移/清理唯一私有 schema，缺配置会失败而非假通过。`state_machine` 普通 fixture 也使用唯一私有 schema；旧最终化用例已补明确的市场/成交/零实费测试证据。历史审计 apply、部署和业务迁移须另行授权。

## Polymarket 余额缓存

USDC 余额按 funder 使用 10 秒缓存。TTL 从 fetch 完成写入本地缓存时计算，只限制本地复用时间，不保证服务端余额快照年龄或外部转账被发现的端到端时延。缓存不是资金预留，也不保证读余额到下单之间的原子性；本实例 `/order` 请求返回后会主动失效，外部或其他进程的余额变化不会主动触发该失效。

刷新期间遇到下单失效，会丢弃对应 generation 的结果；每次缓存调用最多启动 3 次 fetch，持续冲突后返回错误，不退回已判旧的余额。这不是整条 HTTP、funder 选择流程的次数或总时限保证。`pm_balance_refresh` 按实际 fetch 尝试计数，`pm_balance_refresh_fail` 只在 fetch 返回错误或冲突耗尽时计一次，中间冲突及调用取消不计终止失败。

## Outcome 可用余额

现金与 token 均按 `max(total - hold, 0)` 检查可用余额，不把冻结量当可支配资金。匹配币种的 total/hold 缺失、畸形或为负数时返回错误且不缓存失败；合法余额数组没有该币种时返回零。现金余额只使用 USDC，不回退或合并 USDH；USDC 不存在、合法 total 为零或全部冻结时均返回零，USDH 的金额及格式不影响 USDC 余额结果。缓存机制不变，仍不能代替资金预留。

## 本地下单测试

【本机】只测 Polymarket / Outcome 买卖下单接口：走现有签名和提交路径，不连 Postgres、不订盘口、不轮询成交。默认只签名不发单；加 `--confirm`（或 `PLACE_TEST_CONFIRM=1`）才打到主网。

```bash
# 先 dry-run 看签名是否正常（两平台都行，不加 --confirm 即 dry-run）
cargo run --bin place-test -- --platform polymarket --side buy \
  --token <pm_token_id> --shares 5 --price 0.40

cargo run --bin place-test -- --platform outcome --side buy \
  --token '#5160' --shares 5 --price 0.40

# 真实提交
cargo run --bin place-test -- --platform polymarket --side sell \
  --token <pm_token_id> --shares 5 --price 0.40 --confirm

cargo run --bin place-test -- --platform outcome --side sell \
  --token '#5160' --shares 5 --price 0.40 --confirm

# 两平台各买各卖（4 笔）
cargo run --bin place-test -- --all \
  --pm-token <pm_token_id> --out-token '#5160' \
  --shares 5 --price 0.40 --confirm
```

`--side` 省略时默认买卖都测。读 `.env` 与 `polymarket_funders.json`。`ACK` / `NO_MATCH` 都说明下单链路通；`FAILED` 或签名错误才是接口异常。

## 本机交叉编译

【本机】macOS 编 Linux x86_64：

```bash
brew install zig
cargo install cargo-zigbuild   # 装到 ~/.cargo/bin，不是本项目依赖
./scripts/build-linux.sh
```

产物：`dist/market-arb`（已在 `.gitignore`，不要提交）。服务器是 ARM 时：

```bash
TARGET=aarch64-unknown-linux-gnu ./scripts/build-linux.sh
```

无 zig 时可用 Docker：`cargo install cross`，脚本会自动走 `cross build`。

## 首次部署

服务器只 clone，不在上面编译。

【服务器】clone 仓库：

```bash
git clone <仓库地址> /var/www/arb_market
```

【本机】编译并只上传二进制：

```bash
./scripts/build-linux.sh
ssh arb 'mkdir -p /var/www/arb_market/dist'
scp dist/market-arb arb:/var/www/arb_market/dist/market-arb.new
```

【服务器】安装 systemd 并启动（先 `ssh arb`）：

```bash
cd /var/www/arb_market
sudo ./scripts/install-systemd.sh
sudo nano /var/www/arb_market/.env    # 填密钥；已 gitignore，不要提交
sudo nano /var/www/arb_market/polymarket_funders.json
sudo ./scripts/start.sh
```

工作目录、`.env`、账户 JSON、二进制都在 `/var/www/arb_market`（二进制在 `dist/`，已 gitignore）。`git pull` 不会覆盖它们。

`polymarket_api_creds.json` 和 `polymarket_funder_cursor` 也保存在该工作目录，不需要配置状态目录。安装脚本保留根目录属主，仅将根目录的组设为服务组并授予组读写、执行权限，不递归修改工作树；已有状态文件及实际写入用的临时文件会校正为服务用户所有、权限 `0600`，不会清空内容。异常的符号链接或非普通文件会阻止安装，需要先人工核对。service 使用 `UMask=0077` 保护新建文件。

注意：`.gitignore` 不负责文件系统权限。状态保存采用临时文件加重命名，需要根目录可写；这也意味着服务用户能够删除或重命名根目录中的其他目录项。此部署方式接受这一权限边界。

## 日常更新

普通代码或脚本更新：【本机】编二进制并 scp，【服务器】`git pull` 后重启。若 unit 或安装权限逻辑有改动，还需要按下方步骤重新安装 unit。

【本机】

```bash
./scripts/build-linux.sh
scp dist/market-arb arb:/var/www/arb_market/dist/market-arb.new
```

【服务器】（先 `ssh arb`）

```bash
cd /var/www/arb_market
git pull
sudo ./scripts/restart.sh
```

unit 或安装权限逻辑更新时（包括首次应用根目录写权限修复），在 `git pull` 后用以下步骤替代上面的直接重启。安装前先停服务，避免状态文件写入与权限校正并发；安装脚本会执行 `daemon-reload`：

```bash
sudo ./scripts/stop.sh
sudo ./scripts/install-systemd.sh
sudo ./scripts/start.sh
```

只更新二进制、unit 没变时，不必在服务器 `git pull`。【本机】一条命令即可（会编译、scp、远程 restart）：

```bash
DEPLOY_HOST=arb ./scripts/deploy.sh
```

【本机】已经编过、只上传并重启：

```bash
SKIP_BUILD=1 ./scripts/deploy.sh
```

`SKIP_BUILD=1` 也是在本机执行，只是跳过编译。

`deploy.sh`：本机交叉编译 → `scp` 为 `/var/www/arb_market/dist/market-arb.new` → 远程 `restart.sh` 停服务、装正式二进制、再启动。

【本机】更新生产 `.env` 或账户 JSON 后必须重启：

```bash
scp .env arb:/tmp/.env
scp polymarket_funders.json arb:/tmp/polymarket_funders.json
ssh arb 'sudo install -m 0640 -o market-arb -g market-arb /tmp/.env /var/www/arb_market/.env && sudo install -m 0600 -o market-arb -g market-arb /tmp/polymarket_funders.json /var/www/arb_market/polymarket_funders.json && sudo systemctl restart market-arb'
```

【本机】安装路径或用户不是默认值时：

```bash
DEPLOY_HOST=arb DEPLOY_PATH=/var/www/arb_market/dist SERVICE_USER=market-arb ./scripts/deploy.sh
```

## 服务器命令

先 `ssh arb` 登录后再执行。换二进制用 `restart.sh`（会先停再拷 `market-arb.new`）；已在跑时不要只用 `start.sh`。

| 动作 | 命令                               |
| ---- | ---------------------------------- |
| 启动 | `sudo ./scripts/start.sh`          |
| 重启 | `sudo ./scripts/restart.sh`        |
| 停止 | `sudo ./scripts/stop.sh`           |
| 日志 | `sudo ./scripts/log.sh`            |
| 状态 | `sudo systemctl status market-arb` |

`restart.sh` 先停进程，把 `dist/market-arb.new` 装到 `dist/market-arb`，再启动。没有 `.new` 时沿用已有正式二进制。

## 查看日志

正常套利候选 `arb opportunity`、批量盘口拉取 `polymarket books fetched`、重同步 `polymarket book resync`、持仓盘口拉取 `hedge rest book fetched` 的逐次明细使用 DEBUG；失败、真实交易及重要状态变化日志保持原级别。INFO 下优先查看每分钟 `minute stats`：套利计算与候选沿用 `calc/found/arb_disabled/claimed/orders`，候选次数不代表订单数。

盘口新增统计：`pm_book_resync_batches/requested/returned/applied/skipped/failed`（每个后缀都带 `pm_book_resync_` 前缀），分别记录完成批次数、请求 token 数、返回条目数、应用数、丢弃数和失败批次数；无待刷新 token 的轮次不计批次，成功空响应不计失败。`hedge_pm_book_*`、`hedge_out_book_*` 按平台记录 `requests/accepted/discarded/failed`，一次已完成请求只进入一种结果；丢弃指 HTTP/解析成功但快照未被接受，不等于请求失败。底层 HTTP 不重复累计上层统计。

上述三个前缀均有 `elapsed_ms`（累计）及 `max_ms`（最大单次）字段，含成功和失败耗时。批量 resync 耗时包含请求和应用过程；持仓盘口耗时为单次请求/解析，不包含 tick 初始化及等待同批其他请求。计数与耗时在处理结果时记录，取消未完成的操作不计。复用原子计数，每分钟逐字段读取并清零，因此并发跨分钟时字段可能落入相邻窗口，并非事务一致性快照。临时排查可在进程启动环境设置 `RUST_LOG=info,market_arb::exec=debug,market_arb::platforms::polymarket=debug`，排查结束后恢复 INFO。

【服务器】进程 stdout 进 systemd journal，没有 `/var/www/arb_market/*.log`。`-u market-arb` 按 **unit 名** 过滤（`market-arb.service`），不是按 Linux 用户。别的服务即使也跑在 `market-arb` 用户下，也不会出现在这条命令里。

```bash
sudo journalctl -u market-arb           # 默认用 less 打开；/ 搜索，n 下一个
sudo journalctl -u market-arb -f        # 实时跟踪
sudo journalctl -u market-arb -n 200    # 最近 200 行
sudo journalctl -u market-arb --since today --until "18:00"  # 今天到 18:00
sudo journalctl -u market-arb -g "submit failed"            # 按关键字过滤
sudo journalctl -u market-arb -p err    # 只看 error 及以上
```

磁盘上的 journal 是二进制（`/var/log/journal/<machine-id>/system.journal`），不要用 `less` 直接打开。要当文本文件翻：

```bash
# 今天全量日志
sudo journalctl -u market-arb --since today --no-pager > /tmp/market-arb.log
# 从某次重启起的日志
sudo journalctl -u market-arb --since "2026-09-06 17:36" --no-pager > /tmp/market-arb.log

less /tmp/market-arb.log
```

## 相关文件

| 路径                                                     | 说明                                                   |
| -------------------------------------------------------- | ------------------------------------------------------ |
| `scripts/build-linux.sh`                                 | 【本机】交叉编译                                       |
| `scripts/deploy.sh`                                      | 【本机】编译、scp 为 `market-arb.new`、远程 restart.sh |
| `scripts/start.sh` / `restart.sh` / `stop.sh` / `log.sh` | 【服务器】启停与日志；启停时安装 `.new`                |
| `scripts/install-systemd.sh`                             | 【服务器】首次安装 systemd                             |
| `deploy/market-arb.service`                              | systemd unit                                           |
| `.env.example`                                           | 环境变量模板                                           |
| `polymarket_funders.json.example`                        | Polymarket 多账户 JSON 模板（真实文件已 gitignore）    |
