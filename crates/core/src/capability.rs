// SPDX-License-Identifier: AGPL-3.0-or-later

//! Capability set — which roles a backend can serve (plan §26.3).
//!
//! A [`CapSet`] is a compact bitset of [`Role`]s stored in a `u8`. It replaces
//! the old `Vec<Role>` on [`Backend`](kb_scheduler::Backend) so that capability
//! checks are O(1) bitwise operations.

use crate::role::Role;

// Bit constants — one per role.
const TEXT: u8 = 0b00001;
const VISION: u8 = 0b00010;
const CODE: u8 = 0b00100;
const EMBED: u8 = 0b01000;
const RERANK: u8 = 0b10000;

/// A compact set of model capabilities (plan §26.3).
///
/// Backed by a `u8`; bitwise operations make [`supports`](Self::supports)
/// O(1). Construct via [`From<Role>`], [`From<Vec<Role>>`], or the builder
/// methods ([`insert`](Self::insert), [`empty`](Self::empty)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapSet(u8);

impl CapSet {
    /// An empty capability set (serves no role).
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Add `role` to the set.
    pub fn insert(&mut self, role: Role) {
        let bit = role_to_bit(role);
        self.0 |= bit;
    }

    /// Whether this set covers `role`.
    pub fn supports(&self, role: Role) -> bool {
        let bit = role_to_bit(role);
        self.0 & bit != 0
    }

    /// Whether no roles are in the set.
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Number of roles in the set.
    pub fn len(&self) -> u32 {
        self.0.count_ones()
    }

    /// The raw bits (for serialization, if needed).
    pub fn bits(&self) -> u8 {
        self.0
    }
}

impl Default for CapSet {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Role> for CapSet {
    fn from(role: Role) -> Self {
        let mut set = Self::empty();
        set.insert(role);
        set
    }
}

impl From<&[Role]> for CapSet {
    fn from(roles: &[Role]) -> Self {
        let mut set = Self::empty();
        for &role in roles {
            set.insert(role);
        }
        set
    }
}

impl From<Vec<Role>> for CapSet {
    fn from(roles: Vec<Role>) -> Self {
        Self::from(roles.as_slice())
    }
}

/// Map a [`Role`] to its bit-flag.
const fn role_to_bit(role: Role) -> u8 {
    match role {
        Role::Text => TEXT,
        Role::Vision => VISION,
        Role::Code => CODE,
        Role::Embed => EMBED,
        Role::Rerank => RERANK,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn empty_supports_nothing() {
        let set = CapSet::empty();
        for role in Role::all() {
            assert!(
                !set.supports(*role),
                "{role:?} must not be supported by empty set"
            );
        }
    }

    #[test]
    fn single_role_insert_and_check() {
        let mut set = CapSet::empty();
        set.insert(Role::Text);
        assert!(set.supports(Role::Text));
        assert!(!set.supports(Role::Embed));
        assert!(!set.supports(Role::Vision));
        assert!(!set.supports(Role::Code));
        assert!(!set.supports(Role::Rerank));
    }

    #[test]
    fn multi_role_set() {
        let mut set = CapSet::empty();
        set.insert(Role::Text);
        set.insert(Role::Embed);
        assert!(set.supports(Role::Text));
        assert!(set.supports(Role::Embed));
        assert!(!set.supports(Role::Vision));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn from_single_role() {
        let set = CapSet::from(Role::Rerank);
        assert!(set.supports(Role::Rerank));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn from_role_slice() {
        let set = CapSet::from(&[Role::Text, Role::Code, Role::Embed][..]);
        assert!(set.supports(Role::Text));
        assert!(set.supports(Role::Code));
        assert!(set.supports(Role::Embed));
        assert!(!set.supports(Role::Vision));
        assert!(!set.supports(Role::Rerank));
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn from_role_vec() {
        let set = CapSet::from(vec![Role::Text, Role::Embed]);
        assert!(set.supports(Role::Text));
        assert!(set.supports(Role::Embed));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn all_five_roles() {
        let set = CapSet::from(Role::all());
        for role in Role::all() {
            assert!(set.supports(*role), "{role:?} must be supported");
        }
        assert_eq!(set.len(), 5);
    }

    #[test]
    fn is_empty_for_empty_set() {
        assert!(CapSet::empty().is_empty());
    }

    #[test]
    fn is_empty_for_non_empty_set() {
        let set = CapSet::from(Role::Text);
        assert!(!set.is_empty());
    }

    #[test]
    fn default_is_empty() {
        assert!(CapSet::default().is_empty());
    }

    #[test]
    fn bits_are_distinct() {
        // Each role maps to a unique bit.
        let mut seen = 0u8;
        for role in Role::all() {
            let bit = role_to_bit(*role);
            assert_ne!(bit, 0, "{role:?} bit must be non-zero");
            assert_eq!(
                seen & bit,
                0,
                "{role:?} bit {bit:#b} collides with {seen:#b}"
            );
            seen |= bit;
        }
    }

    #[test]
    fn capset_is_copy_and_eq() {
        let a = CapSet::from(Role::Text);
        let b = a;
        assert_eq!(a, b);
        let mut c = a;
        c.insert(Role::Embed);
        assert_ne!(a, c);
    }
}
