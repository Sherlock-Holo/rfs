use anyhow::Result;
use rfs_server::run;

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
