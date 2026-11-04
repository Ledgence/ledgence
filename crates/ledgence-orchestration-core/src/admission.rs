//! Integer admission accounting over transaction-loaded policy and reservations.
//! The adapter supplies fresh database time after locking the resource authority.
//! These transitions do not allocate offers or authorize external execution.
use ledgence_orchestration_api::{ContractError, ResourcePolicy, ResourceRate, Result, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionBucket {
    pub credit: u64,
    pub high_water_at: Timestamp,
}
impl AdmissionBucket {
    /// Initial creation may start at burst. Policy edits use change_admission_rate.
    pub fn full(rate: &ResourceRate, now: Timestamp) -> Result<Self> {
        rate.validate()?;
        time(now)?;
        Ok(Self {
            credit: rate.capacity(),
            high_water_at: now,
        })
    }
    pub fn validate(&self, rate: &ResourceRate) -> Result<()> {
        rate.validate()?;
        time(self.high_water_at)?;
        if self.credit > rate.capacity() {
            return Err(invalid("admission credit exceeds burst"));
        }
        Ok(())
    }
}
pub fn refill_admission_bucket(
    bucket: &AdmissionBucket,
    rate: &ResourceRate,
    now: Timestamp,
) -> Result<AdmissionBucket> {
    bucket.validate(rate)?;
    time(now)?;
    let high_water_at = now.max(bucket.high_water_at);
    let elapsed = high_water_at - bucket.high_water_at;
    let headroom = rate.capacity() - bucket.credit;
    // Saturate before multiplication; neither huge elapsed time nor clock jumps
    // can overflow or accumulate more than the configured burst.
    let fill_time = headroom.div_ceil(u64::from(rate.numerator));
    let refill = if elapsed >= fill_time {
        headroom
    } else {
        elapsed
            .checked_mul(u64::from(rate.numerator))
            .ok_or_else(|| invalid("admission refill overflow"))?
    };
    Ok(AdmissionBucket {
        credit: bucket.credit + refill,
        high_water_at,
    })
}
pub fn change_admission_rate(
    bucket: Option<&AdmissionBucket>,
    old_rate: Option<&ResourceRate>,
    new_rate: Option<&ResourceRate>,
    now: Timestamp,
) -> Result<Option<AdmissionBucket>> {
    time(now)?;
    let old = match (bucket, old_rate) {
        (Some(b), Some(r)) => Some((refill_admission_bucket(b, r, now)?, r)),
        (None, None) => None,
        _ => return Err(invalid("resource rate and bucket disagree")),
    };
    let Some(rate) = new_rate else {
        return Ok(None);
    };
    rate.validate()?;
    match old {
        None => Ok(Some(AdmissionBucket {
            credit: 0,
            high_water_at: now,
        })),
        Some((old, old_rate)) => {
            let credit = (u128::from(old.credit) * u128::from(rate.period_ms))
                / u128::from(old_rate.period_ms);
            let credit = u64::try_from(credit.min(u128::from(rate.capacity())))
                .map_err(|_| invalid("admission conversion overflow"))?;
            Ok(Some(AdmissionBucket {
                credit,
                high_water_at: old.high_water_at,
            }))
        }
    }
}
/// `occupied` includes current-epoch unstarted offers. Old offers must be
/// invalidated atomically when a policy revision changes the accounting epoch.
pub fn admission_offer_capacity(
    policy: &ResourcePolicy,
    bucket: Option<&AdmissionBucket>,
    offered: u64,
    occupied: u64,
) -> Result<u32> {
    policy.validate()?;
    if offered > occupied {
        return Err(invalid("offered occupancy exceeds total"));
    }
    let rate_capacity = match (&policy.rate, bucket) {
        (Some(rate), Some(bucket)) => {
            bucket.validate(rate)?;
            let tokens = bucket.credit / u64::from(rate.period_ms);
            if offered > tokens {
                return Err(invalid("unstarted offers exceed shadow credit"));
            }
            tokens - offered
        }
        (None, None) => u64::MAX,
        _ => return Err(invalid("resource rate and bucket disagree")),
    };
    if !policy.enabled {
        return Ok(0);
    }
    let available = u64::from(policy.max_occupancy)
        .saturating_sub(occupied)
        .min(rate_capacity);
    u32::try_from(available).map_err(|_| invalid("admission capacity overflow"))
}
pub fn spend_admission_token(
    bucket: &AdmissionBucket,
    rate: &ResourceRate,
) -> Result<AdmissionBucket> {
    bucket.validate(rate)?;
    let credit = bucket
        .credit
        .checked_sub(u64::from(rate.period_ms))
        .ok_or_else(|| invalid("admission start lacks token"))?;
    Ok(AdmissionBucket {
        credit,
        high_water_at: bucket.high_water_at,
    })
}
/// Only schedule a time wake when elapsed time can make one more offer possible.
/// Occupancy release, cancellation and offer expiry supply their own durable work.
pub fn next_admission_refill(
    policy: &ResourcePolicy,
    bucket: Option<&AdmissionBucket>,
    offered: u64,
    occupied: u64,
    now: Timestamp,
) -> Result<Option<Timestamp>> {
    policy.validate()?;
    time(now)?;
    if offered > occupied {
        return Err(invalid("offered occupancy exceeds total"));
    }
    let (rate, bucket) = match (&policy.rate, bucket) {
        (Some(rate), Some(bucket)) => (rate, refill_admission_bucket(bucket, rate, now)?),
        (None, None) => return Ok(None),
        _ => return Err(invalid("resource rate and bucket disagree")),
    };
    // Check the shadow invariant even when a pause or capacity debt blocks work.
    admission_offer_capacity(policy, Some(&bucket), offered, occupied)?;
    if !policy.enabled
        || occupied >= u64::from(policy.max_occupancy)
        || offered >= u64::from(rate.burst)
    {
        return Ok(None);
    }
    let required = (offered + 1) * u64::from(rate.period_ms);
    if bucket.credit >= required {
        return Ok(Some(now));
    }
    let delay = (required - bucket.credit).div_ceil(u64::from(rate.numerator));
    let due = bucket
        .high_water_at
        .checked_add(delay)
        .ok_or_else(|| invalid("admission due time overflow"))?;
    time(due)?;
    Ok(Some(due))
}
fn invalid(s: &str) -> ContractError {
    ContractError::InvalidInput(s.into())
}
fn time(t: Timestamp) -> Result<()> {
    if t > 253_402_300_799_999 {
        Err(invalid("admission timestamp exceeds supported range"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rate() -> ResourceRate {
        ResourceRate {
            numerator: 1,
            period_ms: 1000,
            burst: 2,
        }
    }
    fn policy() -> ResourcePolicy {
        ResourcePolicy {
            fair_sharing: None,
            enabled: true,
            max_occupancy: 5,
            rate: Some(rate()),
            offer_timeout_ms: 1000,
            start_timeout_ms: 1000,
        }
    }
    #[test]
    fn full_bucket_advances_watermark_and_rollback_never_mints_credit() {
        let rate = rate();
        let full = AdmissionBucket::full(&rate, 0).unwrap();
        let idle = refill_admission_bucket(&full, &rate, 100_000).unwrap();
        assert_eq!(idle.high_water_at, 100_000);
        let spent = spend_admission_token(&idle, &rate).unwrap();
        assert_eq!(
            refill_admission_bucket(&spent, &rate, 100_000)
                .unwrap()
                .credit,
            1000
        );
        let rollback = refill_admission_bucket(&spent, &rate, 500).unwrap();
        assert_eq!(rollback, spent);
        assert_eq!(
            refill_admission_bucket(&rollback, &rate, 100_500)
                .unwrap()
                .credit,
            1500
        );
    }
    #[test]
    fn offers_reserve_shadow_credit_but_only_start_debits() {
        let p = policy();
        let b = AdmissionBucket::full(p.rate.as_ref().unwrap(), 0).unwrap();
        assert_eq!(admission_offer_capacity(&p, Some(&b), 0, 0).unwrap(), 2);
        assert_eq!(admission_offer_capacity(&p, Some(&b), 1, 1).unwrap(), 1);
        assert_eq!(admission_offer_capacity(&p, Some(&b), 2, 2).unwrap(), 0);
        assert_eq!(
            next_admission_refill(&p, Some(&b), 2, 2, 1000).unwrap(),
            None
        );
        let spent = spend_admission_token(&b, p.rate.as_ref().unwrap()).unwrap();
        assert_eq!(admission_offer_capacity(&p, Some(&spent), 1, 2).unwrap(), 0);
        assert_eq!(
            next_admission_refill(&p, Some(&spent), 1, 2, 0).unwrap(),
            Some(1000)
        );
        // Cancel the remaining unused offer: its reserved credit becomes available.
        assert_eq!(admission_offer_capacity(&p, Some(&spent), 0, 1).unwrap(), 1);
        // Finish a started source: occupancy changes, never the bucket itself.
        assert_eq!(admission_offer_capacity(&p, Some(&spent), 0, 0).unwrap(), 1);
        assert_eq!(spent.credit, 1000);
    }
    #[test]
    fn policy_change_preserves_debt_and_never_refills_new_capacity() {
        let old = rate();
        let spent = spend_admission_token(&AdmissionBucket::full(&old, 0).unwrap(), &old).unwrap();
        let new = ResourceRate {
            numerator: 2,
            period_ms: 3000,
            burst: 5,
        };
        let changed = change_admission_rate(Some(&spent), Some(&old), Some(&new), 0)
            .unwrap()
            .unwrap();
        assert_eq!(changed.credit, 3000);
        let mut p = policy();
        p.max_occupancy = 1;
        p.rate = Some(new);
        assert_eq!(
            admission_offer_capacity(&p, Some(&changed), 0, 3).unwrap(),
            0
        );
        assert_eq!(
            next_admission_refill(&p, Some(&changed), 0, 3, 0).unwrap(),
            None
        );
        assert_eq!(
            change_admission_rate(None, None, Some(&new), 100)
                .unwrap()
                .unwrap()
                .credit,
            0
        );
        assert!(change_admission_rate(None, Some(&old), Some(&new), 0).is_err());
    }
    #[test]
    fn seeded_transitions_preserve_credit_and_shadow_invariants() {
        let mut seed = 20261103u64;
        let mut next = || {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            seed
        };
        for _ in 0..100 {
            let mut p = policy();
            let mut b = AdmissionBucket::full(p.rate.as_ref().unwrap(), 0).unwrap();
            let (mut offers, mut started) = (0u64, 0u64);
            for _ in 0..1000 {
                let now = b
                    .high_water_at
                    .saturating_add(next() % 2000)
                    .saturating_sub(next() % 2000);
                b = refill_admission_bucket(&b, p.rate.as_ref().unwrap(), now).unwrap();
                match next() % 5 {
                    0 if admission_offer_capacity(&p, Some(&b), offers, offers + started)
                        .unwrap()
                        > 0 =>
                    {
                        offers += 1
                    }
                    1 if offers > 0 => {
                        let total = offers + started;
                        b = spend_admission_token(&b, p.rate.as_ref().unwrap()).unwrap();
                        offers -= 1;
                        started += 1;
                        assert_eq!(offers + started, total);
                    }
                    2 if offers > 0 => offers -= 1,
                    3 if started > 0 => started -= 1,
                    4 => {
                        let old = p.rate.unwrap();
                        let new = ResourceRate {
                            numerator: (next() % 100 + 1) as u32,
                            period_ms: (next() % 10000 + 1) as u32,
                            burst: (next() % 10 + 1) as u32,
                        };
                        let before = u128::from(b.credit) * u128::from(new.period_ms);
                        b = change_admission_rate(Some(&b), Some(&old), Some(&new), now)
                            .unwrap()
                            .unwrap();
                        assert!(u128::from(b.credit) * u128::from(old.period_ms) <= before);
                        p.rate = Some(new);
                        p.max_occupancy = (next() % 20 + 1) as u32;
                        offers = 0;
                    }
                    _ => {}
                }
                let r = p.rate.as_ref().unwrap();
                assert!(b.credit <= r.capacity());
                assert!(offers * u64::from(r.period_ms) <= b.credit);
                assert!(offers == 0 || offers + started <= u64::from(p.max_occupancy));
            }
        }
    }
    #[test]
    fn large_elapsed_and_invalid_buckets_are_bounded() {
        let r = ResourceRate {
            numerator: 1_000_000,
            period_ms: 86_400_000,
            burst: 1_000_000,
        };
        let b = AdmissionBucket {
            credit: 0,
            high_water_at: 0,
        };
        assert_eq!(
            refill_admission_bucket(&b, &r, 253_402_300_799_999)
                .unwrap()
                .credit,
            r.capacity()
        );
        assert!(
            AdmissionBucket {
                credit: r.capacity() + 1,
                high_water_at: 0
            }
            .validate(&r)
            .is_err()
        );
        assert!(spend_admission_token(&b, &r).is_err());
    }
}
