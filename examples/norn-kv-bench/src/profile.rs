//! Versioned workload profiles.
//!
//! A profile fixes everything that shapes the traffic: key populations,
//! request mix, burst sizes, connection churn, client concurrency, phase
//! lengths, and the absolute offered rates of the latency segments. Keep a
//! profile unchanged when comparing server builds.

use serde::{Deserialize, Serialize};

pub const PROFILE_VERSION: u32 = 2;
/// Upper bound on distinct request classes (including the churn class).
pub const MAX_CLASSES: usize = 16;
/// Largest value the client will generate. `norn-kv-server` accepts request
/// bodies up to 64 KiB by default.
pub const MAX_VALUE_LEN: usize = 60 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub version: u32,
    pub name: String,
    pub seed: u64,
    /// Number of server workers requested from `norn-kv-server`.
    pub server_workers: usize,
    pub client: ClientConfig,
    /// Immutable populations read by GET classes.
    pub keyspaces: Vec<KeyspaceConfig>,
    /// Connection-owned keys used by SET and read-your-write GETs.
    pub writable: WritableConfig,
    pub mix: Vec<ClassConfig>,
    /// Number of requests written back to back for one arrival.
    pub bursts: Vec<BurstConfig>,
    pub churn: ChurnConfig,
    pub phases: PhaseConfig,
    /// Absolute offered request rates (requests/s) of the open-loop segments.
    pub rates: Vec<u64>,
    #[serde(default)]
    pub qualification: Qualification,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub threads: usize,
    /// Persistent connections, striped across client threads.
    pub connections: usize,
    /// Outstanding requests per connection in the closed-loop segments.
    pub closed_loop_depth: usize,
    /// Open-loop arrivals beyond this many outstanding requests on one
    /// connection are dropped and reported; this bounds client memory when
    /// the offered rate exceeds capacity.
    pub max_outstanding_per_connection: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyspaceConfig {
    pub name: String,
    pub keys: u64,
    pub value_min: usize,
    pub value_max: usize,
    /// Zipf exponent for key popularity; `0` selects uniform popularity.
    pub zipf: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritableConfig {
    pub slots_per_connection: u32,
    pub value_min: usize,
    pub value_max: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    /// GET of an immutable key from `keyspace`.
    Get,
    /// GET of a key that is never written.
    GetMiss,
    /// SET of a connection-owned slot with a new version.
    Set,
    /// GET of a connection-owned slot; validates the latest issued version.
    GetOwn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassConfig {
    pub class: String,
    pub op: Op,
    #[serde(default)]
    pub keyspace: Option<String>,
    pub weight: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BurstConfig {
    pub size: usize,
    pub weight: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChurnConfig {
    /// Short-lived connections opened per second across all client threads.
    pub connections_per_sec: u64,
    /// Small-keyspace GETs pipelined on each short-lived connection.
    pub requests_per_connection: usize,
    /// Keyspace read by churn connections.
    pub keyspace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseConfig {
    pub warmup_ms: u64,
    pub capacity_ms: u64,
    /// Discarded lead-in for every measured segment.
    pub settle_ms: u64,
    /// Measured duration of every open-loop segment.
    pub segment_ms: u64,
    pub drain_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Qualification {
    /// Maximum p99 arrival lag in an open-loop segment before the client is
    /// considered to have distorted the schedule.
    pub max_lag_p99_us: f64,
    /// Minimum server CPU utilization in the capacity segment before the
    /// result is considered server-bound.
    pub min_capacity_utilization: f64,
    /// Maximum involuntary context switches per second of the server or of
    /// the client threads before the host is considered disturbed.
    #[serde(default = "default_preemptions")]
    pub max_preemptions_per_s: f64,
}

fn default_preemptions() -> f64 {
    20.0
}

impl Default for Qualification {
    fn default() -> Self {
        Self {
            max_lag_p99_us: 50.0,
            min_capacity_utilization: 0.9,
            max_preemptions_per_s: default_preemptions(),
        }
    }
}

impl Profile {
    pub fn load(path: &str) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path).map_err(|e| format!("read profile {path}: {e}"))?;
        let profile: Profile =
            serde_json::from_str(&raw).map_err(|e| format!("parse profile {path}: {e}"))?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn validate(&self) -> Result<(), String> {
        let fail = |msg: String| Err(format!("profile {}: {msg}", self.name));
        if self.version != PROFILE_VERSION {
            return fail(format!("version {} != {PROFILE_VERSION}", self.version));
        }
        let c = &self.client;
        if c.threads == 0 || c.connections < c.threads {
            return fail("need threads >= 1 and connections >= threads".into());
        }
        if c.closed_loop_depth == 0 || c.max_outstanding_per_connection < c.closed_loop_depth {
            return fail("need closed_loop_depth >= 1 and max_outstanding >= depth".into());
        }
        if self.server_workers == 0 {
            return fail("server_workers must be >= 1".into());
        }
        if self.keyspaces.is_empty() {
            return fail("need at least one keyspace".into());
        }
        for ks in &self.keyspaces {
            if ks.keys == 0 || ks.value_min == 0 || ks.value_min > ks.value_max {
                return fail(format!(
                    "keyspace {} has an empty key or value range",
                    ks.name
                ));
            }
            if ks.value_max > MAX_VALUE_LEN {
                return fail(format!(
                    "keyspace {} value_max exceeds {MAX_VALUE_LEN}",
                    ks.name
                ));
            }
            if !(ks.zipf >= 0.0 && ks.zipf.is_finite()) {
                return fail(format!("keyspace {} zipf must be finite and >= 0", ks.name));
            }
        }
        let w = &self.writable;
        if w.slots_per_connection == 0 || w.value_min == 0 || w.value_min > w.value_max {
            return fail("writable needs slots and a value range".into());
        }
        if w.value_max > MAX_VALUE_LEN {
            return fail(format!("writable value_max exceeds {MAX_VALUE_LEN}"));
        }
        if self.mix.is_empty() || self.mix.len() + 1 > MAX_CLASSES {
            return fail(format!("mix needs 1..{} classes", MAX_CLASSES - 1));
        }
        for class in &self.mix {
            if class.weight == 0 {
                return fail(format!("class {} has zero weight", class.class));
            }
            match (class.op, &class.keyspace) {
                (Op::Get, Some(name)) => {
                    if self.keyspace_index(name).is_none() {
                        return fail(format!(
                            "class {} names unknown keyspace {name}",
                            class.class
                        ));
                    }
                }
                (Op::Get, None) => {
                    return fail(format!("GET class {} needs a keyspace", class.class))
                }
                (_, Some(_)) => {
                    return fail(format!(
                        "only GET classes take a keyspace ({})",
                        class.class
                    ))
                }
                (_, None) => {}
            }
            if class.class == "churn" {
                return fail("class name `churn` is reserved".into());
            }
        }
        if self.bursts.is_empty() || self.bursts.iter().any(|b| b.size == 0 || b.weight == 0) {
            return fail("bursts need nonzero sizes and weights".into());
        }
        if self
            .bursts
            .iter()
            .any(|b| b.size > c.max_outstanding_per_connection)
        {
            return fail("burst size exceeds max_outstanding_per_connection".into());
        }
        if self.churn.connections_per_sec > 0 {
            if self.churn.requests_per_connection == 0 {
                return fail("churn requests_per_connection must be >= 1".into());
            }
            if self.keyspace_index(&self.churn.keyspace).is_none() {
                return fail(format!(
                    "churn names unknown keyspace {}",
                    self.churn.keyspace
                ));
            }
        }
        let p = &self.phases;
        if p.capacity_ms <= p.settle_ms || p.segment_ms == 0 || p.drain_timeout_ms == 0 {
            return fail("capacity_ms must exceed settle_ms; segment and drain must be > 0".into());
        }
        if self.rates.contains(&0) {
            return fail("rates must be positive".into());
        }
        Ok(())
    }

    pub fn keyspace_index(&self, name: &str) -> Option<usize> {
        self.keyspaces.iter().position(|ks| ks.name == name)
    }

    /// Class names in report order; the churn session class is last.
    pub fn class_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.mix.iter().map(|c| c.class.clone()).collect();
        names.push("churn".into());
        names
    }

    pub fn mean_burst(&self) -> f64 {
        let total: u64 = self.bursts.iter().map(|b| u64::from(b.weight)).sum();
        self.bursts
            .iter()
            .map(|b| b.size as f64 * f64::from(b.weight))
            .sum::<f64>()
            / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_profiles_validate() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/profiles");
        let mut seen = 0;
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "json") {
                Profile::load(path.to_str().unwrap()).unwrap();
                seen += 1;
            }
        }
        assert!(seen >= 3);
    }
}
