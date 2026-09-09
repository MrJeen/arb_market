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

**新套利精度口径：** 从已解析 Decimal 的尾数与 scale 精确转换为有理数，在深度乘加之前消除中间舍入；数量搜索、HTTP重新计价、两轮余额检查及ROI方向择优共享精确口径。PM估算仍为 `r*(P-P²/S)`（原均价费用公式的等价式），Outcome估算仍为 `t*O`，预算/利润/APR用精确不等式比较，不加epsilon。输入价格须为合法预测价格、预算为正、两费率支持[0,1]；无效参数跳过，不回退旧舍入算法。费用公式不变，但旧Decimal近门槛的舍入假阳性/假阴性不保证兼容。

数据库及通知的财务字段仍为 Decimal 展示投影：cost/fee按可容纳的最大共同scale采用nearest-even，总成本为展示分项之和，利润由展示总成本派生；均价和收益率也为近似值，不以这些展示值重新作交易门槛判断。资金检查使用精确含费需求，通知中的required向上转换，不能低估；无法表示的计划保守拒绝。`ArbPlan`的精确估值仅保存在内存，并绑定身份、股数与cap；增加私有字段后不再支持外部结构体字面量构造。止盈、再平衡、实扣费用及历史账务口径未改变。

## 成交确认与恢复

ACK 仅记录已受理及预期撮合量，交易腿保留 `actived`，不生成 `ack:` 伪成交或暂估实际费用。Polymarket 必须取齐订单关联的成交并等待 `CONFIRMED`／`FAILED` 最终状态；兼容 REST 的 `TRADE_STATUS_*` 和既有裸值，只有 Confirmed 部分计入仓位，`MATCHED`、`MATCHED_NOT_BROADCASTED`、`MINED`、`RETRYING` 和未知状态继续等待。订单状态兼容明确的 `ORDER_STATUS_*` 枚举及裸值，未知状态不能靠后缀猜为取消；原始状态保留在证据中。订单查询中的正撮合量不替代成交确认。

PM trades 固定查询首次提交起 300 秒的时间窗口；分页完成后仍待确认时重扫相同窗口，五分钟不是确认时限。order 返回 404／JSON null 时继续查 trades，但须完整、非空、精确匹配本单且全终态，费用证据齐全后才能收尾；空集合继续等待。曾经查到的关联成交 ID 持续取并集、撮合量持续取最大值作为下界，后续缺单或较旧快照不能抹掉这些约束。即使尚未拿到 fills，已知正成交证据也禁止随意换绑订单 ID。旧 JSON 中仍在的同单证据可自动恢复，已被旧版本完全覆盖的证据无法凭空找回，历史终态不会自动重开。

Outcome 丢失 ACK 时使用已持久化的 cloid 查询真实 oid；数字 oid 以 JSON 整数发送，cloid 以字符串发送。成交历史使用首次提交前 30 秒到本轮固定终点的时间窗口查询，每次最多读一页，已成功页面和下一游标一起落库。同毫秒饱和、历史保留窗口不足或无法证明零成交时继续核对，不把查询不到／空页／超时解释成零成交。

Outcome 历史覆盖采用初始探测、数据分页、最终覆盖验证三个阶段，每次回填最多一个 HTTP 请求。两次探测中已见到的本单真实成交也会持久化；尾页不直接授权零成交终态，而要重新验证当前保留窗口仍覆盖扫描起点。重启不复用旧 `history_complete` 布尔值作为新覆盖证明；旧 v1 游标安全恢复到新阶段。无法证明覆盖的取消单继续等待，但可靠预期数量与真实确认成交精确吻合的既有捷径不受影响。此修复不重开历史已终态交易腿，也不把本地扫描轮次视为交易所原子快照。

真实成交、分页进度、交易腿最终账务及父单完成通过事务落库；重复页和重启重放按真实成交 ID 去重。迟到 ACK／Unknown 不覆盖终态，旧 `ack:` 行保留但不参与新汇总。此次逻辑仅处理开放腿与新订单，历史终态订单不会自动重开或重算。

`UNKNOWN_LEG_TIMEOUT_SECS` 同时覆盖 `unknown` 和确认中的 `actived`，按首次提交时间计时，已有部分成交或持续重试不会推迟告警。超时仅告警、暂停新套利并继续回填，不清零成交。等待 PM 链上确认也会延后父单完成和生命周期 claim 释放。

**费用口径：** 明确提供金额且币种为 USDC（PM 也接受 pUSD）的费用按原值入账，包含显式 0；Outcome `fee` 已含 builderFee，不重复叠加。PM maker 的官方零费规则记录为 `calculated_maker_zero`。PM taker 未返回实扣金额时，查询 `/clob-markets/{condition_id}` 的 `fd.r`，按[官方费用公式](https://docs.polymarket.com/trading/fees) `shares × rate × price × (1-price)` 逐笔计算，不读取或使用指数参数，五位小数四舍五入，不乘策略 1.3 安全倍率，来源记录为 `calculated`。该政策明确假设所查 schedule 适用于该成交、pUSD 按 1 美元计价；计算金额不是交易所实扣证明。费率、查询时点及币种／舍入政策保存在成交证据中，`fills.fee` 没有实扣值时保持 NULL；重扫先批量合并本页同单已存成交，复用已确定快照，仅为真正缺少证据的成交查询费用接口，不用新费率覆盖旧快照；终态后不自动重新估费。缺少／畸形 schedule、非支持币种等仍保留 `fee_evidence_missing` 或查询错误并告警，不用缺字段推断零费。

## 市场与结算口径

common 数据库提供给本服务的统一事件视为已经完成业务筛选的二元市场。Outcome 结算遵循 HIP-4 原始分数兑付：side 0 每股兑付 `settleFraction`，side 1 每股兑付 `1-settleFraction`，两侧合计为 1；`0.5` 时两侧各兑付 0.5。分数必须位于 `[0,1]`，缺字段、不可解析或越界值不会落为已结算。

普通持仓的结算检查使用当前已加载或恢复的事件 `Topic.end_date`（来自 common 的 `events.end_date`）：结束时间未到时跳过两平台结算接口和结算判定，继续既有止盈、再平衡计算及执行开关逻辑；到达结束时间（含相等）或结束时间缺失时照常查询。止盈和再平衡提交前会重新比较当前 UTC 时间，不复用扫描初次的时间判断。已进入 `settlement_pending` 的订单不受此时间门禁影响，仍按独立清扫节奏核实结算。该规则会推迟发现平台提前结算或不可交易的情况，跳过检查不代表已确认市场仍可交易。

时间门禁的排查优先看每分钟 `minute stats`：`settlement_skipped_before_end` 统计未到期跳过次数，`settlement_end_date_missing` 统计时间缺失而继续查询的次数；配合原有 `settlement_scan`、`settlement_pending_scan` 判断处理路径。这些均是检查次数，不是去重订单数或 HTTP 请求数，提交前复查也会计数。需要逐订单排查时，启用 `market_arb::exec` 的 DEBUG 可见 `settlement time gate evaluated`，包含 `order_id`、`end_date`、`checked_at` 和 `decision`（`skip_before_end`、`query_due`、`query_missing_end_date`）。日志过滤器在加载 `.env` 前初始化，应通过进程启动环境或 systemd 覆盖配置设置 `RUST_LOG`，仅修改 `.env` 不会调整日志级别。

任一平台先确认结算时，订单进入 `settlement_pending`：该订单立即停止止盈、再平衡和所有新交易，只保留两平台结算查询。两平台 payout 都可信后才核算最终 `actual_cost`、`actual_rev`、`actual_profit` 并转为 `settled`；不会用单平台结果推算另一侧，也不会自动卖出另一平台持仓。

`settlement_pending` 走独立的扫描游标和批量，默认每 `SETTLEMENT_PENDING_SCAN_INTERVAL_SECS`（60 秒）清扫一次、每次至多 `SETTLEMENT_PENDING_SCAN_BATCH` 条，不再占用 `POSITION_SCAN_BATCH` 给活跃订单的配额，因此 pending 积压不会拉长止盈响应。清扫在同一个循环内串行执行，同一订单不会被两条路径并发处理。pending 没有超时自动最终化，长期缺失对手方 payout 时会持续驻留并每轮重试。

套利、补齐对冲和开放持仓账务中的每对 `$1` 估值，依赖跨平台事件定义与结算规则一致；“二元市场”本身不保证这一点。common 的配对规则及真实市场的分数、平局、取消／退款语义仍需独立核验。最终账务按各平台实际 payout 核算，不代表估值前提已被验证，也不消除跨平台判决分歧风险。

Outcome 兑付落在 `(0,1)` 时会打 `warn` 并计入 `outcome_fractional_settlement`，用于发现上述估值前提失效。该指标按**观测次数**计数而非去重订单数：结算确认后当轮扫描只会计一次，但订单最终化前的后续清扫会重复计数。计数不改变入账口径，也不会拦截交易——分数结算仍按实际 payout 核算。Polymarket 侧按 winner 构造，恒为 0/1，不参与该判定。

最终核算返回错误时，`settlement_finalize_fail` 记录该分类失败并输出订单、市场和错误上下文；上层仍计入 `exec_err`，两项不能相加作为失败总数。失败日志不保证事务一定未提交，重试仍依赖既有最终核算幂等性。

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

## 日常更新

脚本或 unit 有改动时：【本机】编二进制并 scp，【服务器】`git pull` 后重启。

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
