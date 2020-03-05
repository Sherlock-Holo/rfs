#![type_length_limit = "112013908"]

use anyhow::Result;

use rfs_server::{enter_tokio, run};

#[async_std::main]
async fn main() -> Result<()> {
    enter_tokio(run()).await
}
