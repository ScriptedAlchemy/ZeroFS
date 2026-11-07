use serde::{Deserialize, Serialize};

/// Persistent observations belong to a device identity, never a reusable node name.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SmartSample {
    pub device: String,
    pub reallocated: Option<u64>,
    pub pending: Option<u64>,
    pub uncorrectable: Option<u64>,
    pub crc: Option<u64>,
    pub failed: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemberHealth {
    pub quarantine: Option<String>,
    pub monitoring_error: Option<String>,
    pub kernel_coverage: Option<String>,
    pub evacuation_error: Option<String>,
    pub kernel_cursor: Option<(String, u64)>,
    pub since_ms: Option<i64>,
    pub last_sample: Option<SmartSample>,
    pub crc_increases: u32,
    pub warning: Option<String>,
    pub observed_ms: Option<i64>,
}

impl MemberHealth {
    pub(super) fn quarantine(&mut self, reason: String, now: i64) {
        if self.quarantine.is_none() {
            self.quarantine = Some(reason);
            self.since_ms = Some(now);
        }
    }

    pub(super) fn observe(&mut self, mut sample: SmartSample, now: i64) {
        if let Some(old) = &self.last_sample {
            if old.device != sample.device {
                self.quarantine("device identity changed".into(), now);
                return;
            }
            if sample
                .reallocated
                .zip(old.reallocated)
                .is_some_and(|(n, p)| n > p)
            {
                self.quarantine("reallocated sector growth".into(), now);
            }
            if sample
                .crc
                .zip(self.last_sample.as_ref().and_then(|s| s.crc))
                .is_some_and(|(n, p)| n > p)
            {
                self.crc_increases = self.crc_increases.saturating_add(1);
                self.warning = Some("UDMA CRC growth".into());
                if self.crc_increases >= 2 {
                    self.quarantine("repeated UDMA CRC growth".into(), now);
                }
            }
        }
        if sample.failed
            || sample.pending.is_some_and(|n| n > 0)
            || sample.uncorrectable.is_some_and(|n| n > 0)
        {
            self.quarantine("SMART media failure".into(), now);
        }
        if self.last_sample.is_none() && sample.reallocated.is_some_and(|n| n > 0) {
            self.warning = Some("historical reallocated sectors; baseline established".into());
        }
        if let Some(previous) = &self.last_sample {
            sample.reallocated = sample.reallocated.or(previous.reallocated);
            sample.pending = sample.pending.or(previous.pending);
            sample.uncorrectable = sample.uncorrectable.or(previous.uncorrectable);
            sample.crc = sample.crc.or(previous.crc);
        }
        self.last_sample = Some(sample);
        self.observed_ms = Some(now);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentFault {
    pub location: String,
    pub physical_key: String,
    pub member_id: String,
    pub reason: String,
    pub observed_ms: i64,
    pub attempts: u32,
    pub next_retry_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_health_crc_repeat_is_sticky_across_stable_samples() {
        let mut h = MemberHealth::default();
        for crc in [10, 11, 11, 11] {
            h.observe(
                SmartSample {
                    device: "disk".into(),
                    crc: Some(crc),
                    ..Default::default()
                },
                1,
            );
            assert!(h.quarantine.is_none());
        }
        h.observe(
            SmartSample {
                device: "disk".into(),
                crc: Some(12),
                ..Default::default()
            },
            2,
        );
        assert_eq!(h.crc_increases, 2);
        assert!(h.quarantine.is_some());
        h.observe(
            SmartSample {
                device: "disk".into(),
                crc: Some(12),
                ..Default::default()
            },
            3,
        );
        assert_eq!(h.crc_increases, 2);
        assert_eq!(h.since_ms, Some(2));
    }

    #[test]
    fn disk_health_pending_and_reallocation_growth() {
        let mut h = MemberHealth::default();
        h.observe(
            SmartSample {
                device: "disk".into(),
                reallocated: Some(4),
                ..Default::default()
            },
            1,
        );
        assert!(h.quarantine.is_none());
        h.observe(
            SmartSample {
                device: "disk".into(),
                reallocated: Some(5),
                ..Default::default()
            },
            2,
        );
        assert!(h.quarantine.is_some());
        let mut h = MemberHealth::default();
        h.observe(
            SmartSample {
                pending: Some(1),
                ..Default::default()
            },
            1,
        );
        assert!(h.quarantine.is_some());
    }
}
