//! Test-only networking helpers shared by the extractor unit tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;

/// An address that actively REFUSES connections at the time of return.
///
/// The naive idiom — bind an ephemeral listener, note its port, drop it, then
/// dial the freed port expecting refusal — is racy: under parallel test runs
/// on a busy host the OS can recycle the port to another listener between the
/// drop and the request, turning a connection-refused expectation into a live
/// connection (observed once in a full-workspace coverage run executing
/// alongside the e2e stack). This helper verifies the refusal with a probe
/// after the drop and retries with a fresh port until it holds.
pub(crate) async fn refused_addr() -> SocketAddr {
    for _ in 0..8 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral listener");
        let addr = listener.local_addr().expect("listener local addr");
        drop(listener);
        if tokio::net::TcpStream::connect(addr).await.is_err() {
            return addr;
        }
    }
    panic!("could not obtain a connection-refusing port after 8 attempts");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refused_addr_actually_refuses() {
        let addr = refused_addr().await;
        assert!(
            tokio::net::TcpStream::connect(addr).await.is_err(),
            "address {addr} accepted a connection"
        );
    }
}
