#![type_length_limit = "111913108"]

use anyhow::Result;

use rfs_server::{enter_tokio, run};

#[async_std::main]
async fn main() -> Result<()> {
    enter_tokio(run()).await
}
