#![type_length_limit = "2072953"]

use anyhow::Result;

use rfs_server::run;

#[async_std::main]
async fn main() -> Result<()> {
    run().await
}
