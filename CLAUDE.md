# 项目工作规则

## Rust 格式规范

- Rust 代码统一遵循 rustfmt 默认格式，不手动维护另一套风格；无明确需求不新增 `rustfmt.toml` 自定义配置。
- 修改 Rust 代码后运行 `cargo fmt --all`，并以 `cargo fmt --all -- --check` 通过作为交付条件。
- 格式化后检查 Git 差异，确认纯格式变化没有改变业务逻辑，并保留用户已有改动。
- 若格式化涉及历史未格式化文件，应明确区分纯格式变化与业务改动，不混淆两者。
- 对 rustfmt 无法充分整理的宏内部代码（例如 `tokio::select!`），手动展开为可读结构，不改变条件、执行顺序、错误处理或并发行为。

# 测试约定

- 所有测试实现统一放在 `tests/` 目录，禁止在 `src/` 中直接编写 `#[test]` 或 `#[tokio::test]` 测试函数。
- 需要访问模块私有实现的单元测试，放在 `tests/unit/` 下与 `src/` 对应的镜像路径中，并由源模块通过 `#[cfg(test)]`、`#[path = "..."]` 和 `mod tests;` 引用。
- 跨模块集成测试继续作为 `tests/` 下的独立测试目标维护。

## 测试数据库连接

```
COMMON_POSTGRES_URI=postgres://root:root@localhost:5432/aimarket
APP_POSTGRES_URI=postgres://root:root@localhost:5432/arb_crypto
```
