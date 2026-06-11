// SPDX-License-Identifier: AGPL-3.0-or-later

//! Internal helper macro for the crate's "string enum" pattern.

/// Define a `Copy` enum whose variants map 1:1 to canonical lowercase strings (the values
/// stored in Postgres `TEXT` columns and exchanged over the JSON API).
///
/// One string literal per variant drives `as_str`, `Display`, `FromStr`, and the serde
/// rename simultaneously, so those representations cannot drift apart. Unknown strings parse
/// to [`CoreError::UnknownVariant`](crate::error::CoreError::UnknownVariant).
macro_rules! str_enum {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$var_meta:meta])* $variant:ident = $str:literal ),+ $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash,
            ::serde::Serialize, ::serde::Deserialize
        )]
        $vis enum $name {
            $( $(#[$var_meta])* #[serde(rename = $str)] $variant ),+
        }

        impl $name {
            /// The canonical lowercase string form (matches the database `TEXT` value).
            #[must_use]
            $vis const fn as_str(&self) -> &'static str {
                match self { $( Self::$variant => $str ),+ }
            }

            /// Every variant, in declaration order.
            #[must_use]
            $vis fn all() -> &'static [Self] {
                &[ $( Self::$variant ),+ ]
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::core::str::FromStr for $name {
            type Err = $crate::error::CoreError;
            fn from_str(s: &str) -> ::core::result::Result<Self, Self::Err> {
                match s {
                    $( $str => ::core::result::Result::Ok(Self::$variant), )+
                    other => ::core::result::Result::Err(
                        $crate::error::CoreError::UnknownVariant {
                            kind: stringify!($name),
                            value: other.to_owned(),
                        }
                    ),
                }
            }
        }
    };
}

pub(crate) use str_enum;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    str_enum! {
        /// A throwaway enum exercising the macro's generated code.
        enum Sample {
            /// First.
            Alpha = "alpha",
            /// Second, multi-word to check rename.
            BetaTwo = "beta_two",
        }
    }

    #[test]
    fn as_str_matches_declared_literal() {
        assert_eq!(Sample::Alpha.as_str(), "alpha");
        assert_eq!(Sample::BetaTwo.as_str(), "beta_two");
    }

    #[test]
    fn display_uses_as_str() {
        assert_eq!(Sample::BetaTwo.to_string(), "beta_two");
    }

    #[test]
    fn from_str_round_trips_every_variant() {
        for v in Sample::all() {
            assert_eq!(Sample::from_str(v.as_str()).unwrap(), *v);
        }
    }

    #[test]
    fn from_str_rejects_unknown() {
        let err = Sample::from_str("nope").unwrap_err();
        assert!(matches!(
            err,
            crate::error::CoreError::UnknownVariant { kind: "Sample", .. }
        ));
    }

    #[test]
    fn serde_string_equals_as_str() {
        for v in Sample::all() {
            let json = serde_json::to_string(v).unwrap();
            assert_eq!(json, format!("\"{}\"", v.as_str()));
            let back: Sample = serde_json::from_str(&json).unwrap();
            assert_eq!(back, *v);
        }
    }
}
