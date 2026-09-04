#[tokio::main]
async fn main() -> anyhow::Result<()> {
    market_arb::run().await
}
