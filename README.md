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

`ENABLE_BUY=false` 时只计算不下单。配置项见 `.env.example`。

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
scp dist/market-arb arb:/var/www/arb_market/dist/market-arb
```

【服务器】安装 systemd 并启动（先 `ssh arb`）：

```bash
cd /var/www/arb_market
sudo ./scripts/install-systemd.sh
sudo nano /var/www/arb_market/.env    # 填密钥；已 gitignore，不要提交
sudo nano /var/www/arb_market/polymarket_funders.json
sudo systemctl start market-arb
```

工作目录、`.env`、账户 JSON、二进制都在 `/var/www/arb_market`（二进制在 `dist/`，已 gitignore）。`git pull` 不会覆盖它们。

## 日常更新

脚本或 unit 有改动时：【本机】编二进制并 scp，【服务器】`git pull` 后重启。

【本机】

```bash
./scripts/build-linux.sh
scp dist/market-arb arb:/var/www/arb_market/dist/market-arb
```

【服务器】（先 `ssh arb`）

```bash
cd /var/www/arb_market
git pull
sudo ./scripts/install-systemd.sh
sudo systemctl restart market-arb
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

`deploy.sh`：本机交叉编译 → 本机 `scp` 到 `/tmp` → 远程安装到 `/var/www/arb_market/dist/market-arb` → 远程 `systemctl restart`。

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

先 `ssh arb` 登录后再执行。代码更新用 `restart`，不要只用 `start`（进程已在跑时 `start` 不会换成新二进制）。

| 动作 | 命令                                |
| ---- | ----------------------------------- |
| 启动 | `sudo systemctl start market-arb`   |
| 重启 | `sudo systemctl restart market-arb` |
| 停止 | `sudo systemctl stop market-arb`    |
| 状态 | `sudo systemctl status market-arb`  |
| 日志 | `sudo journalctl -u market-arb -f`  |

`restart` 先发 SIGTERM，进程退出后再拉起新二进制。

## 查看日志

【服务器】进程 stdout 进 systemd journal，没有 `/var/www/arb_market/*.log`。`-u market-arb` 按 **unit 名** 过滤（`market-arb.service`），不是按 Linux 用户。别的服务即使也跑在 `market-arb` 用户下，也不会出现在这条命令里。

```bash
sudo journalctl -u market-arb           # 默认用 less 打开；/ 搜索，n 下一个
sudo journalctl -u market-arb -f        # 实时跟踪
sudo journalctl -u market-arb -n 200    # 最近 200 行
sudo journalctl -u market-arb --since today --until "18:00"
sudo journalctl -u market-arb -g "submit failed"
sudo journalctl -u market-arb -p err    # 只看 error 及以上
```

磁盘上的 journal 是二进制（`/var/log/journal/<machine-id>/system.journal`），不要用 `less` 直接打开。要当文本文件翻：

```bash
sudo journalctl -u market-arb --since today --no-pager > /tmp/market-arb.log
less /tmp/market-arb.log
```

## 相关文件

| 路径                              | 说明                                                |
| --------------------------------- | --------------------------------------------------- |
| `scripts/build-linux.sh`          | 【本机】交叉编译                                    |
| `scripts/deploy.sh`               | 【本机】编译、scp、远程重启                         |
| `scripts/install-systemd.sh`      | 【服务器】首次安装 systemd                          |
| `deploy/market-arb.service`       | systemd unit                                        |
| `.env.example`                    | 环境变量模板                                        |
| `polymarket_funders.json.example` | Polymarket 多账户 JSON 模板（真实文件已 gitignore） |
