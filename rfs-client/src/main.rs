use anyhow::Result;

use rfs_client::run;

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
