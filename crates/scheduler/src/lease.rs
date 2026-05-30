//! [`Lease`]: an RAII guard holding one concurrency slot on a backend
//! (plan §6.1–§6.2).

use tokio::sync::OwnedSemaphorePermit;

/// An RAII handle to a single acquired slot on a backend.
///
/// Holding a `Lease` keeps one [`OwnedSemaphorePermit`] checked out of the
/// backend's semaphore; dropping the lease returns the permit, freeing exactly
/// one slot (plan §6.2). Carry it for the lifetime of an in-flight request and
/// drop it — explicitly or by leaving scope — when the call completes or fails
/// over to another backend.
#[derive(Debug)]
pub struct Lease {
    /// Id of the backend this slot belongs to.
    pub backend_id: String,
    /// Base URL the request should be sent to (resolved at acquire time).
    pub base_url: String,
    // The held permit; its `Drop` frees the slot. Never read directly — the
    // leading underscore documents that and exempts it from the dead-code lint.
    _permit: OwnedSemaphorePermit,
}

impl Lease {
    /// Wrap an acquired permit together with the backend it was taken from.
    pub fn new(
        backend_id: impl Into<String>,
        base_url: impl Into<String>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            backend_id: backend_id.into(),
            base_url: base_url.into(),
            _permit: permit,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn lease_holds_a_slot_then_frees_it_on_drop() {
        let sem = Arc::new(Semaphore::new(2));
        let permit = sem.clone().try_acquire_owned().unwrap();
        let lease = Lease::new("b1", "http://x", permit);

        assert_eq!(lease.backend_id, "b1");
        assert_eq!(lease.base_url, "http://x");
        assert_eq!(sem.available_permits(), 1, "permit is held while leased");

        drop(lease);
        assert_eq!(sem.available_permits(), 2, "slot returns on drop");
    }
}
