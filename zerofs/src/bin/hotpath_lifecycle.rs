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
            hotpath::Section::Io,
            hotpath::Section::Threads,
        ])
        .build();
    hotpath::tokio_runtime!(runtime.handle());

    runtime.block_on(async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        hotpath::future!(async {}, label = "zerofs.hotpath.lifecycle.completed").await;
        drop(hotpath::future!(
            std::future::pending::<()>(),
            label = "zerofs.hotpath.lifecycle.cancelled"
        ));

        let (client, mut server) = tokio::io::duplex(64);
        let mut client = hotpath::io!(client, label = "zerofs.hotpath.lifecycle.loopback");
        let server = tokio::spawn(async move {
            let mut request = [0; 5];
            server
                .read_exact(&mut request)
                .await
                .expect("read lifecycle I/O request");
            assert_eq!(&request, b"hello");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            server
                .write_all(b"goodbye")
                .await
                .expect("write lifecycle I/O response");
        });
        client
            .write_all(b"hello")
            .await
            .expect("write lifecycle I/O request");
        let mut response = [0; 7];
        client
            .read_exact(&mut response)
            .await
            .expect("read lifecycle I/O response");
        assert_eq!(&response, b"goodbye");
        server.await.expect("join lifecycle I/O server");

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
