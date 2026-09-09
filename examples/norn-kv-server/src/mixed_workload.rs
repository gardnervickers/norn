//! Versioned inputs and deterministic data for the mixed network benchmark.

use serde::{Deserialize, Serialize};

/// Traffic profile. All rates refer to total requests across all connections.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    /// Mixed requests with uniform read-key selection.
    #[default]
    Balanced,
    /// Small probe connections compete with large reads routed to one shard.
    Interference,
    /// Balanced traffic followed by a higher rate and recovery.
    Overload,
}

/// One of the four request classes, in histogram/deadline-array order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// A 256-byte immutable read.
    SmallRead,
    /// A 4 KiB immutable read.
    MediumRead,
    /// A 32 KiB immutable read.
    LargeRead,
    /// A 4 KiB overwrite in a connection-owned namespace.
    Overwrite,
}

impl Class {
    /// Stable index for counters and deadline arrays.
    pub fn index(self) -> usize {
        match self {
            Self::SmallRead => 0,
            Self::MediumRead => 1,
            Self::LargeRead => 2,
            Self::Overwrite => 3,
        }
    }

    /// Value length, excluding protocol framing.
    pub fn value_len(self) -> usize {
        [256, 4096, 32768, 4096][self.index()]
    }

    /// Acknowledged binary-protocol command.
    pub fn opcode(self) -> u8 {
        if self == Self::Overwrite {
            crate::protocol::OP_SET
        } else {
            crate::protocol::OP_GET
        }
    }

    /// Deterministic 70/20/5/5 probability mix, independent of completion timing.
    pub fn for_sequence(seed: u64, sequence: u64) -> Self {
        match sample(seed, sequence) % 100 {
            0..=69 => Self::SmallRead,
            70..=89 => Self::MediumRead,
            90..=94 => Self::LargeRead,
            _ => Self::Overwrite,
        }
    }

    /// Immutable classes that must be populated before a trial.
    pub fn read_classes() -> [Self; 3] {
        [Self::SmallRead, Self::MediumRead, Self::LargeRead]
    }
}

/// A measurement phase; warmup is configured separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase {
    /// Label attached to the scheduled-arrival cohort.
    pub name: String,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Total intended requests per second.
    pub rate: u64,
}

/// Serializable workload contract. Unknown fields are rejected to catch typos.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Manifest {
    /// Contract version. Only version one is supported.
    pub version: u32,
    /// Deterministic request-selection seed.
    pub seed: u64,
    /// Client runtime threads.
    pub threads: usize,
    /// Total persistent client connections.
    pub connections: usize,
    /// Maximum outstanding requests per connection.
    pub pipeline: usize,
    /// Maximum reserved request plus expected-response bytes per connection.
    pub max_in_flight_bytes_per_connection: usize,
    /// Number of small immutable keys.
    pub small_keys: usize,
    /// Number of medium immutable keys.
    pub medium_keys: usize,
    /// Number of large immutable keys.
    pub large_keys: usize,
    /// Fixed overwrite slots owned by each connection.
    pub overwrite_slots: usize,
    /// Number of Norn server workers, used for interference-key ownership.
    pub server_workers: usize,
    /// Owner of large-read keys in the interference profile.
    pub hot_shard: usize,
    /// Warmup duration before measured phases.
    pub warmup_ms: u64,
    /// Initial steady interval.
    pub steady_ms: u64,
    /// Elevated-rate interval in the overload profile.
    pub overload_ms: u64,
    /// Original-rate interval following overload.
    pub recovery_ms: u64,
    /// Maximum time allowed to drain outstanding requests.
    pub drain_ms: u64,
    /// Total steady intended requests per second.
    pub rate: u64,
    /// Total intended requests per second during overload.
    pub overload_rate: u64,
    /// Arrivals later than this tolerance are classified as generator misses.
    pub max_schedule_lag_us: u64,
    /// Per-class observation deadlines; these never abort server operations.
    pub deadlines_us: [u64; 4],
    /// Highest histogram value. Larger observations are reported as overflow.
    pub histogram_max_us: u64,
    /// Traffic scenario.
    pub profile: Profile,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: 1,
            seed: 1,
            threads: 8,
            connections: 128,
            pipeline: 32,
            max_in_flight_bytes_per_connection: 1024 * 1024,
            small_keys: 32768,
            medium_keys: 4096,
            large_keys: 1024,
            overwrite_slots: 8,
            server_workers: 1,
            hot_shard: 0,
            warmup_ms: 10000,
            steady_ms: 20000,
            overload_ms: 15000,
            recovery_ms: 20000,
            drain_ms: 10000,
            rate: 20000,
            overload_rate: 40000,
            max_schedule_lag_us: 2000,
            deadlines_us: [2000, 5000, 20000, 5000],
            histogram_max_us: 60_000_000,
            profile: Profile::Balanced,
        }
    }
}

impl Manifest {
    /// Check bounds before connecting or allocating workload storage.
    ///
    /// # Errors
    /// Returns a description of an unsupported or inconsistent input.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 {
            return Err("unsupported workload version".into());
        }
        if self.threads == 0 || self.threads > self.connections || self.connections > 4096 {
            return Err("require 1 <= threads <= connections <= 4096".into());
        }
        if self.pipeline == 0 || self.pipeline > 65536 {
            return Err("pipeline must be in 1..=65536".into());
        }
        if self.max_in_flight_bytes_per_connection < 32768 + 512 {
            return Err("byte budget must accommodate one large request/response".into());
        }
        if [
            self.small_keys,
            self.medium_keys,
            self.large_keys,
            self.overwrite_slots,
        ]
        .iter()
        .any(|&n| n == 0 || n > 1_000_000)
        {
            return Err("key and overwrite-slot counts must be in 1..=1000000".into());
        }
        if self.server_workers == 0
            || self.server_workers > 256
            || self.hot_shard >= self.server_workers
        {
            return Err("require 1 <= server_workers <= 256 and hot_shard < server_workers".into());
        }
        if self.profile == Profile::Interference && self.connections < 4 {
            return Err("interference requires at least four connections".into());
        }
        if self.rate == 0
            || self.overload_rate == 0
            || self.rate > 100_000_000
            || self.overload_rate > 100_000_000
        {
            return Err("rates must be in 1..=100000000 requests/s".into());
        }
        if self.steady_ms == 0
            || self.drain_ms == 0
            || self.deadlines_us.contains(&0)
            || self.histogram_max_us == 0
        {
            return Err(
                "steady/drain durations, deadlines, and histogram range must be positive".into(),
            );
        }
        if self.histogram_max_us > 3_600_000_000
            || self.max_schedule_lag_us == 0
            || self.max_schedule_lag_us > 3_600_000_000
            || self.deadlines_us.iter().any(|&us| us > 3_600_000_000)
        {
            return Err("histogram, lag, and deadline bounds must fit within one hour".into());
        }
        if [
            self.warmup_ms,
            self.steady_ms,
            self.overload_ms,
            self.recovery_ms,
            self.drain_ms,
        ]
        .iter()
        .any(|&n| n > 3_600_000)
        {
            return Err("each duration must be at most one hour".into());
        }
        if self.profile == Profile::Overload
            && (self.overload_ms == 0 || self.recovery_ms == 0 || self.overload_rate <= self.rate)
        {
            return Err(
                "overload requires positive overload/recovery intervals and overload_rate > rate"
                    .into(),
            );
        }
        Ok(())
    }

    /// Key count for an immutable class (overwrite returns its per-client slots).
    pub fn key_count(&self, class: Class) -> usize {
        [
            self.small_keys,
            self.medium_keys,
            self.large_keys,
            self.overwrite_slots,
        ][class.index()]
    }

    /// Measured intervals; excludes population, warmup, and drain.
    pub fn phases(&self) -> Vec<Phase> {
        let mut phases = vec![Phase {
            name: "steady".into(),
            duration_ms: self.steady_ms,
            rate: self.rate,
        }];
        if self.profile == Profile::Overload {
            phases.push(Phase {
                name: "overload".into(),
                duration_ms: self.overload_ms,
                rate: self.overload_rate,
            });
            phases.push(Phase {
                name: "recovery".into(),
                duration_ms: self.recovery_ms,
                rate: self.rate,
            });
        }
        phases
    }

    /// Resolve a read key using the profile's deterministic ownership policy.
    pub fn read_key(&self, class: Class, index: usize) -> Vec<u8> {
        if self.profile == Profile::Interference && class == Class::LargeRead {
            hot_read_key(class, index, self.server_workers, self.hot_shard)
        } else {
            read_key(class, index)
        }
    }
}

/// Deterministic pseudo-random sample (`SplitMix64` finalizer).
pub fn sample(seed: u64, sequence: u64) -> u64 {
    let mut z = seed.wrapping_add(sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Immutable read key with a stable class namespace.
pub fn read_key(class: Class, index: usize) -> Vec<u8> {
    format!("mixed:r:{}:{index:08x}", class.index()).into_bytes()
}

/// Key owned by one logical writer and one fixed overwrite slot.
pub fn write_key(connection: usize, slot: usize) -> Vec<u8> {
    format!("mixed:w:{connection:08x}:{slot:08x}").into_bytes()
}

/// Stable owner hash matching the server's FNV-1a routing contract.
pub fn key_owner(key: &[u8], workers: usize) -> usize {
    let hash = key.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (hash % workers as u64) as usize
}

/// Deterministically choose a distinct key for the requested owner.
///
/// `workers` must be nonzero and `owner` less than `workers`.
pub fn hot_read_key(class: Class, index: usize, workers: usize, owner: usize) -> Vec<u8> {
    assert!(workers > 0 && owner < workers);
    for salt in 0_u64.. {
        let key = format!("mixed:h:{}:{index:08x}:{salt:x}", class.index()).into_bytes();
        if key_owner(&key, workers) == owner {
            return key;
        }
    }
    unreachable!("exhausted key space")
}

/// Deterministic full value, including generation-sensitive bytes.
pub fn payload(key: &[u8], len: usize, generation: u64) -> Vec<u8> {
    let seed = key
        .iter()
        .fold(generation, |acc, byte| sample(acc, u64::from(*byte)));
    let mut result = Vec::with_capacity(len);
    for index in 0..len.div_ceil(8) {
        result.extend_from_slice(&sample(seed, index as u64).to_le_bytes());
    }
    result.truncate(len);
    result
}

/// Intended arrival offset for a global request index, rounded down to ns.
///
/// `rate` must be positive. Saturates instead of wrapping at extreme indices.
pub fn scheduled_offset_ns(index: u64, rate: u64) -> u64 {
    ((u128::from(index) * 1_000_000_000) / u128::from(rate)).min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_rejects_typos_and_invalid_limits() {
        assert!(serde_json::from_str::<Manifest>(r#"{"ratte":10}"#).is_err());
        let mut manifest = Manifest::default();
        assert!(manifest.validate().is_ok());
        manifest.pipeline = 0;
        assert!(manifest.validate().is_err());
        manifest.pipeline = 32;
        manifest.profile = Profile::Overload;
        manifest.overload_rate = manifest.rate;
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn namespaces_payloads_and_owner_mapping_are_stable() {
        assert_ne!(read_key(Class::SmallRead, 1), write_key(0, 1));
        let key = read_key(Class::MediumRead, 5);
        assert_eq!(payload(&key, 4096, 0), payload(&key, 4096, 0));
        assert_ne!(payload(&key, 4096, 0), payload(&key, 4096, 1));
        for owner in 0..4 {
            let key = hot_read_key(Class::LargeRead, 1, 4, owner);
            assert_eq!(key_owner(&key, 4), owner);
        }
        assert_eq!(key_owner(b"a", 2), 0);
        assert_eq!(key_owner(b"b", 2), 1);
    }

    #[test]
    fn schedule_retains_phase_offsets_without_rate_division_loss() {
        assert_eq!(scheduled_offset_ns(1, 3), 333_333_333);
        assert_eq!(scheduled_offset_ns(3, 3), 1_000_000_000);
        let mut merged = (0..4)
            .flat_map(|connection| {
                (0..25).map(move |n| scheduled_offset_ns(connection + n * 4, 100))
            })
            .collect::<Vec<_>>();
        merged.sort_unstable();
        assert_eq!(merged, (0..100).map(|n| n * 10_000_000).collect::<Vec<_>>());
    }
}
