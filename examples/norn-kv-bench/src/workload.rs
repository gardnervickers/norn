//! Deterministic key, value, and request generation.
//!
//! Every value is a slice of one shared random pool chosen by a 64-bit tag,
//! so the client can validate a response with a single `memcmp` instead of
//! regenerating payload bytes on the hot path.

use crate::profile::{Op, Profile, MAX_VALUE_LEN};

/// SplitMix64 finalizer.
pub fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(mix(seed))
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.0)
    }

    /// Uniform in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1_u64 << 53) as f64)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Exponentially distributed with the given mean.
    pub fn exp(&mut self, mean: f64) -> f64 {
        -(1.0 - self.next_f64()).ln() * mean
    }
}

const POOL_LEN: usize = 4 << 20;

/// Shared random bytes from which all values are sliced.
pub struct ValuePool {
    bytes: Vec<u8>,
}

impl ValuePool {
    pub fn new(seed: u64) -> Self {
        let mut rng = Rng::new(seed ^ 0x706f_6f6c);
        let mut bytes = Vec::with_capacity(POOL_LEN + MAX_VALUE_LEN);
        while bytes.len() < POOL_LEN + MAX_VALUE_LEN {
            bytes.extend_from_slice(&rng.next_u64().to_le_bytes());
        }
        Self { bytes }
    }

    pub fn value(&self, tag: u64, len: usize) -> &[u8] {
        let offset = (tag % POOL_LEN as u64) as usize;
        &self.bytes[offset..offset + len]
    }
}

/// Popularity sampler and value layout for one immutable keyspace.
pub struct Keyspace {
    pub index: u8,
    pub keys: u64,
    value_min: usize,
    value_span: u64,
    seed: u64,
    /// Cumulative popularity by rank; empty for uniform popularity.
    cdf: Vec<f64>,
    /// Rank-to-id permutation `id = (rank * stride + offset) % keys` with
    /// `stride` coprime to `keys`, so hot ranks are spread over the id space
    /// (and therefore over server shards).
    stride: u64,
    offset: u64,
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

impl Keyspace {
    pub fn new(
        index: u8,
        keys: u64,
        value_min: usize,
        value_max: usize,
        zipf: f64,
        seed: u64,
    ) -> Self {
        let cdf = if zipf > 0.0 {
            let mut cdf = Vec::with_capacity(keys as usize);
            let mut total = 0.0;
            for rank in 0..keys {
                total += 1.0 / ((rank + 1) as f64).powf(zipf);
                cdf.push(total);
            }
            for p in &mut cdf {
                *p /= total;
            }
            cdf
        } else {
            Vec::new()
        };
        let mut stride = ((keys as f64 * 0.618_033_988_7) as u64).max(1);
        while gcd(stride, keys) != 1 {
            stride += 1;
        }
        let seed = mix(seed ^ (u64::from(index) << 56));
        Self {
            index,
            keys,
            value_min,
            value_span: (value_max - value_min + 1) as u64,
            seed,
            cdf,
            stride,
            offset: mix(seed) % keys,
        }
    }

    pub fn sample(&self, rng: &mut Rng) -> u64 {
        if self.cdf.is_empty() {
            return rng.below(self.keys);
        }
        let u = rng.next_f64();
        let rank = self.cdf.partition_point(|&p| p < u) as u64;
        ((u128::from(rank.min(self.keys - 1)) * u128::from(self.stride) + u128::from(self.offset))
            % u128::from(self.keys)) as u64
    }

    pub fn tag(&self, id: u64) -> u64 {
        mix(self.seed ^ id)
    }

    pub fn value_len(&self, id: u64) -> usize {
        self.value_min + (mix(self.tag(id)) % self.value_span) as usize
    }

    pub fn key(&self, id: u64, out: &mut [u8; KEY_LEN]) {
        encode_key(out, b'k', u64::from(self.index), id);
    }
}

pub const KEY_LEN: usize = 18;

fn encode_key(out: &mut [u8; KEY_LEN], kind: u8, a: u64, b: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out[0] = kind;
    out[1] = b':';
    for i in 0..4 {
        out[2 + i] = HEX[((a >> (12 - 4 * i)) & 0xf) as usize];
    }
    for i in 0..12 {
        out[6 + i] = HEX[((b >> (44 - 4 * i)) & 0xf) as usize];
    }
}

/// Values and keys of connection-owned writable slots.
pub struct Writable {
    pub slots: u32,
    value_min: usize,
    value_span: u64,
    seed: u64,
}

impl Writable {
    pub fn tag(&self, conn: u32, slot: u32, version: u32) -> u64 {
        mix(mix(self.seed ^ u64::from(conn)) ^ (u64::from(slot) << 32 | u64::from(version)))
    }

    pub fn value_len(&self, tag: u64) -> usize {
        self.value_min + (mix(tag) % self.value_span) as usize
    }

    pub fn key(&self, conn: u32, slot: u32, out: &mut [u8; KEY_LEN]) {
        encode_key(out, b'w', u64::from(slot & 0xffff), u64::from(conn));
    }
}

/// Keys of GET-miss requests: never populated.
pub fn miss_key(id: u64, out: &mut [u8; KEY_LEN]) {
    encode_key(out, b'm', 0, id & 0xffff_ffff_ffff);
}

#[derive(Debug, Clone, Copy)]
pub struct Class {
    pub op: Op,
    pub keyspace: u8,
}

/// Everything shared read-only by client threads.
pub struct Workload {
    pub pool: ValuePool,
    pub keyspaces: Vec<Keyspace>,
    pub writable: Writable,
    pub classes: Vec<Class>,
    class_cdf: Vec<u32>,
    burst_sizes: Vec<usize>,
    burst_cdf: Vec<u32>,
    pub churn_keyspace: u8,
    pub churn_class: u8,
}

fn cdf(weights: impl Iterator<Item = u32>) -> Vec<u32> {
    let mut total = 0;
    weights
        .map(|w| {
            total += w;
            total
        })
        .collect()
}

fn pick(cdf: &[u32], rng: &mut Rng) -> usize {
    let x = rng.below(u64::from(*cdf.last().unwrap())) as u32;
    cdf.partition_point(|&c| c <= x)
}

impl Workload {
    pub fn new(profile: &Profile) -> Self {
        let keyspaces = profile
            .keyspaces
            .iter()
            .enumerate()
            .map(|(i, ks)| {
                Keyspace::new(
                    i as u8,
                    ks.keys,
                    ks.value_min,
                    ks.value_max,
                    ks.zipf,
                    profile.seed,
                )
            })
            .collect();
        let classes = profile
            .mix
            .iter()
            .map(|c| Class {
                op: c.op,
                keyspace: c
                    .keyspace
                    .as_deref()
                    .and_then(|name| profile.keyspace_index(name))
                    .unwrap_or(0) as u8,
            })
            .collect();
        Self {
            pool: ValuePool::new(profile.seed),
            keyspaces,
            writable: Writable {
                slots: profile.writable.slots_per_connection,
                value_min: profile.writable.value_min,
                value_span: (profile.writable.value_max - profile.writable.value_min + 1) as u64,
                seed: mix(profile.seed ^ 0x7772_6974),
            },
            classes,
            class_cdf: cdf(profile.mix.iter().map(|c| c.weight)),
            burst_sizes: profile.bursts.iter().map(|b| b.size).collect(),
            burst_cdf: cdf(profile.bursts.iter().map(|b| b.weight)),
            churn_keyspace: profile.keyspace_index(&profile.churn.keyspace).unwrap_or(0) as u8,
            churn_class: profile.mix.len() as u8,
        }
    }

    pub fn pick_class(&self, rng: &mut Rng) -> u8 {
        pick(&self.class_cdf, rng) as u8
    }

    pub fn pick_burst(&self, rng: &mut Rng) -> usize {
        self.burst_sizes[pick(&self.burst_cdf, rng)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_fixed_width_and_distinct() {
        let ks = Keyspace::new(1, 1000, 10, 20, 0.0, 1);
        let mut a = [0; KEY_LEN];
        let mut b = [0; KEY_LEN];
        ks.key(1, &mut a);
        ks.key(2, &mut b);
        assert_ne!(a, b);
        assert_eq!(&a[..6], b"k:0001");
        let mut w = [0; KEY_LEN];
        miss_key(1, &mut w);
        assert_ne!(a, w);
    }

    #[test]
    fn zipf_sampler_is_skewed_and_in_range() {
        let ks = Keyspace::new(0, 10_000, 1, 1, 0.99, 7);
        let mut rng = Rng::new(3);
        let mut counts = vec![0_u32; 10_000];
        for _ in 0..200_000 {
            counts[ks.sample(&mut rng) as usize] += 1;
        }
        let mut sorted = counts.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        // The hottest 1% of keys should carry well over 1% of requests.
        let top: u32 = sorted[..100].iter().sum();
        assert!(top > 200_000 / 4, "top 1% carried {top}");
        // The hottest rank is not pinned to id zero.
        let hottest = counts.iter().position(|&c| c == sorted[0]).unwrap() as u64;
        assert_eq!(hottest, ks.offset);
        assert_ne!(hottest, 0);
    }

    #[test]
    fn value_lengths_stay_in_range() {
        let ks = Keyspace::new(0, 100, 64, 512, 0.0, 1);
        for id in 0..100 {
            let len = ks.value_len(id);
            assert!((64..=512).contains(&len));
        }
    }

    #[test]
    fn pick_respects_weights() {
        let cdf = cdf([1, 0, 3].into_iter());
        let mut rng = Rng::new(1);
        let mut counts = [0; 3];
        for _ in 0..4000 {
            counts[pick(&cdf, &mut rng)] += 1;
        }
        assert_eq!(counts[1], 0);
        assert!(counts[2] > counts[0] * 2);
    }
}
