#[cfg(feature = "hotpath-profile")]
fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build Hotpath lifecycle runtime");
    let _hotpath_guard = hotpath::HotpathGuardBuilder::new("zerofs::hotpath_lifecycle")
        .sections(vec![
            hotpath::Section::FunctionsTiming,
            hotpath::Section::Futures,
            hotpath::Section::Threads,
        ])
        .build();
    hotpath::tokio_runtime!(runtime.handle());

    runtime.block_on(async {
        hotpath::future!(async {}, label = "zerofs.hotpath.lifecycle.completed").await;
        drop(hotpath::future!(
            std::future::pending::<()>(),
            label = "zerofs.hotpath.lifecycle.cancelled"
        ));
        let hold_ms = std::env::var("ZEROFS_HOTPATH_LIFECYCLE_HOLD_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1_000);
        tokio::time::sleep(std::time::Duration::from_millis(hold_ms)).await;
    });
}

#[cfg(not(feature = "hotpath-profile"))]
fn main() {
    panic!("hotpath_lifecycle requires the hotpath-profile feature");
}
