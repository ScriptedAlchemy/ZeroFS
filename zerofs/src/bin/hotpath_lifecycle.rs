#[cfg(feature = "hotpath-profile")]
fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build Hotpath lifecycle runtime");
    let _hotpath_guard = hotpath::HotpathGuardBuilder::new("zerofs::hotpath_lifecycle")
        .sections(vec![hotpath::Section::Futures, hotpath::Section::Threads])
        .build();
    hotpath::tokio_runtime!(runtime.handle());

    runtime.block_on(async {
        hotpath::future!(async {}, label = "zerofs.hotpath.lifecycle.completed").await;
        drop(hotpath::future!(
            std::future::pending::<()>(),
            label = "zerofs.hotpath.lifecycle.cancelled"
        ));
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    });
}

#[cfg(not(feature = "hotpath-profile"))]
fn main() {
    panic!("hotpath_lifecycle requires the hotpath-profile feature");
}
