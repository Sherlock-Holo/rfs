use anyhow::Result;
use smol::Task;

use rfs::init_smol_runtime;
use rfs_server::run;

fn main() -> Result<()> {
    init_smol_runtime();

    smol::block_on(Task::spawn(run()))
}
