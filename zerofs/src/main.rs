use anyhow::Context;
use std::process::ExitCode;
use zerofs::operator_required::CliFailure;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// The one place the process decides its exit status from an error. `zerofs
/// run` exits with `OPERATOR_REQUIRED_EXIT_STATUS` when its error is marked as
/// needing an operator; every other failure exits with 1, as a `Result` from
/// `main` did.
fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_eager_driver_handoff()
        .build()
        .context("Failed to build Tokio runtime")
    {
        Ok(runtime) => runtime,
        Err(error) => return CliFailure::from(error).report(),
    };

    #[cfg(feature = "hotpath-profile")]
    let _hotpath_guard = hotpath::HotpathGuardBuilder::new("zerofs::main")
        .sections(vec![
            hotpath::Section::FunctionsTiming,
            hotpath::Section::Futures,
            hotpath::Section::Io,
            hotpath::Section::Threads,
        ])
        .build();
    #[cfg(feature = "hotpath-profile")]
    hotpath::tokio_runtime!(runtime.handle());

    match runtime.block_on(zerofs::run_cli()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => failure.report(),
    }
}
