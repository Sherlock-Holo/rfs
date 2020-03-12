use anyhow::Result;

use rfs_server::{enter_tokio, get_tokio_handle, run};

#[async_std::main]
async fn main() -> Result<()> {
    enter_tokio(Box::pin(run(get_tokio_handle()))).await
}
