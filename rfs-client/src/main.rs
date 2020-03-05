use anyhow::Result;

use rfs_client::{enter_tokio, run};

#[async_std::main]
async fn main() -> Result<()> {
    enter_tokio(run()).await
}
