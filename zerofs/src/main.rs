use anyhow::{Context, Result};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_eager_driver_handoff()
        .build()
        .context("Failed to build Tokio runtime")?;

    #[cfg(feature = "hotpath-profile")]
    let _hotpath_guard = hotpath::HotpathGuardBuilder::new("zerofs::main")
        .sections(vec![
            hotpath::Section::FunctionsTiming,
            hotpath::Section::Futures,
            hotpath::Section::Threads,
        ])
        .build();
    #[cfg(feature = "hotpath-profile")]
    hotpath::tokio_runtime!(runtime.handle());

    runtime.block_on(zerofs::run_cli())
}
