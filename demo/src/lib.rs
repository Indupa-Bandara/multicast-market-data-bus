//! Shared helpers for the demo binaries: tiny `--key value` argument parsing and the
//! deterministic payload both sides agree on.
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;

use wire::{Action, L2Update, Side};

/// `--key value` command-line arguments.
pub struct Args(HashMap<String, String>);

impl Args {
    /// Parse `std::env::args()`; exits with status 2 on a dangling flag.
    pub fn parse() -> Args {
        let mut m = HashMap::new();
        let mut it = std::env::args().skip(1);
        while let Some(k) = it.next() {
            let k = k.trim_start_matches('-').to_string();
            match it.next() {
                Some(v) => {
                    m.insert(k, v);
                }
                None => {
                    eprintln!("missing value for --{k}");
                    std::process::exit(2);
                }
            }
        }
        Args(m)
    }

    /// Value of `--key`, or `default` if absent. Exits with status 2 if it does not parse.
    pub fn get<T: FromStr>(&self, key: &str, default: T) -> T
    where
        T::Err: Debug,
    {
        match self.0.get(key) {
            Some(v) => v.parse().unwrap_or_else(|e| {
                eprintln!("bad value for --{key}: {e:?}");
                std::process::exit(2)
            }),
            None => default,
        }
    }
}

/// The payload of the message with sequence number `i` (sequence numbers start at 1).
///
/// The receiver recomputes this to check every delivered message end to end, which catches
/// corruption and misordering that sequence numbers alone would not.
pub fn mk(i: u64) -> L2Update {
    L2Update {
        instrument_id: (i % 100) as u32,
        side: if i.is_multiple_of(2) {
            Side::Bid
        } else {
            Side::Ask
        },
        action: Action::Set,
        price: 10_000 + (i % 1_000) as i64,
        qty: i,
    }
}
