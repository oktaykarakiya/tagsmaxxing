//! [`Lease`]: an RAII guard holding acquired capacity on a backend
//! (plan §6.1–§6.2, §26.2).

use crate::capacity::CapacityPermit;

/// An RAII handle to acquired capacity on a backend.
///
/// Holding a `Lease` keeps a [`CapacityPermit`] checked out of the backend's
/// capacity guard; dropping the lease releases the permit, freeing the slot
/// (and refunding token-bucket credits for `Rated` capacity). Carry it for the
/// lifetime of an in-flight request and drop it — explicitly or by leaving
/// scope — when the call completes or fails over to another backend.
#[derive(Debug)]
pub struct Lease {
    /// Id of the backend this capacity was taken from.
    pub backend_id: String,
    /// Base URL the request should be sent to (resolved at acquire time).
    pub base_url: String,
    // The held capacity guard; its `Drop` frees the slot and refunds
    // token-bucket credits. Never read directly — the leading underscore
    // documents that and exempts it from the dead-code lint.
    _guard: CapacityPermit,
}

impl Lease {
    /// Wrap an acquired capacity permit together with the backend it was taken from.
    pub fn new(
        backend_id: impl Into<String>,
        base_url: impl Into<String>,
        guard: CapacityPermit,
    ) -> Self {
        Self {
            backend_id: backend_id.into(),
            base_url: base_url.into(),
            _guard: guard,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    use crate::capacity::Capacity;

    #[test]
    fn lease_holds_a_slot_from_slots_capacity_then_frees_on_drop() {
        let cap = Capacity::new_slots(2);
        assert_eq!(cap.free(), 2);

        let guard = cap.try_acquire().unwrap();
        let lease = Lease::new("b1", "http://x", guard);

        assert_eq!(lease.backend_id, "b1");
        assert_eq!(lease.base_url, "http://x");
        assert_eq!(cap.free(), 1, "slot is held while leased");

        drop(lease);
        assert_eq!(cap.free(), 2, "slot returns on drop");
    }
}
