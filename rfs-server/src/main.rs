use anyhow::Result;

use rfs_server::{enter_tokio, run};

#[async_std::main]
async fn main() -> Result<()> {
    enter_tokio(Box::pin(run())).await
}
