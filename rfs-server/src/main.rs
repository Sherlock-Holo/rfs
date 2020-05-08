use anyhow::Result;
use smol::Task;

use rfs_server::run;

fn main() -> Result<()> {
    for _ in 0..num_cpus::get().max(1) {
        std::thread::spawn(|| smol::run(futures_util::future::pending::<()>()));
    }

    smol::block_on(Task::spawn(run()))
}
