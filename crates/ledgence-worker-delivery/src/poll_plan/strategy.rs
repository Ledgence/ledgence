use std::{collections::hash_map::RandomState, hash::BuildHasher};

use super::PollStrategy;

pub(super) enum Selector {
    Stride(Stride),
    Lottery(Lottery),
}

impl Selector {
    pub(super) fn new(strategy: PollStrategy, weights: &[u32]) -> Self {
        match strategy {
            PollStrategy::Stride => {
                let mut stride = Stride::new(weights.len());
                stride.configure(weights);
                Self::Stride(stride)
            }
            PollStrategy::Lottery => Self::Lottery(Lottery(
                RandomState::new().hash_one("Ledgence poll lottery"),
            )),
            PollStrategy::SeededLottery(seed) => Self::Lottery(Lottery(seed)),
        }
    }

    pub(super) fn configure(&mut self, weights: &[u32]) {
        if let Self::Stride(stride) = self {
            stride.configure(weights);
        }
    }

    pub(super) fn issue(&mut self, weights: &[u32], total: u64) -> usize {
        match self {
            Self::Stride(stride) => stride.issue(weights),
            Self::Lottery(lottery) => lottery.issue(weights, total),
        }
    }
}

// Virtual pass for route i is numerator[i] / nominal_weight[i]. All static
// strides are exactly QUANTUM / weight; cross products compare them without
// floating point, a shared denominator, or constraints on combinations of weights.
const QUANTUM: u64 = 1 << 32;

pub(super) struct Stride {
    numerator: Vec<u64>,
    nominal: Vec<u32>,
    last_active: Vec<u32>,
}

impl Stride {
    fn new(count: usize) -> Self {
        Self {
            numerator: vec![0; count],
            nominal: vec![1; count],
            last_active: vec![0; count],
        }
    }

    fn lowest(&self, weights: &[u32]) -> Option<usize> {
        (0..weights.len())
            .filter(|&i| weights[i] != 0)
            .min_by(|&a, &b| {
                let lhs = u128::from(self.numerator[a]) * u128::from(self.nominal[b]);
                let rhs = u128::from(self.numerator[b]) * u128::from(self.nominal[a]);
                lhs.cmp(&rhs)
            })
    }

    fn configure(&mut self, weights: &[u32]) {
        if weights.iter().all(|w| *w == 0) || weights == self.last_active {
            // No service elapses while paused. Resuming equivalent weights keeps
            // exact phase even if the common virtual frontier is fractional.
            return;
        }
        if let Some(first) = self.lowest(&self.last_active) {
            let (frontier_n, frontier_d) = (self.numerator[first], u64::from(self.nominal[first]));
            for (n, &w) in self.numerator.iter_mut().zip(&self.nominal) {
                // Retain lead measured in fractions of a single old stride.
                // Removing floor(frontier * weight) rounds the remaining lead
                // up by < 1 / QUANTUM of a poll, only on weight changes.
                let elapsed = u128::from(frontier_n) * u128::from(w) / u128::from(frontier_d);
                *n = n.saturating_sub(u64::try_from(elapsed).unwrap_or(u64::MAX));
            }
        }
        for (nominal, &updated) in self.nominal.iter_mut().zip(weights) {
            if updated != 0 {
                *nominal = updated;
            }
        }
        self.last_active.copy_from_slice(weights);
    }

    fn issue(&mut self, weights: &[u32]) -> usize {
        let selected = self.lowest(weights).expect("non-idle weights");
        self.numerator[selected] += QUANTUM;
        // Slide all pass values by the same whole virtual tick, preserving
        // exact order. The lowest active pass after this update is < 1
        // tick, so numerators stay bounded independently of process uptime.
        let ticks = (0..weights.len())
            .filter(|&i| weights[i] != 0)
            .map(|i| self.numerator[i] / u64::from(self.nominal[i]))
            .min()
            .expect("non-idle weights");
        for (n, &weight) in self.numerator.iter_mut().zip(&self.nominal) {
            *n = n.saturating_sub(ticks.saturating_mul(u64::from(weight)));
        }
        selected
    }
}

pub(super) struct Lottery(u64);

impl Lottery {
    // SplitMix64 is a reproducible scheduling stream, not a credential generator.
    fn word(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut word = self.0;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d049bb133111eb);
        word ^ (word >> 31)
    }

    fn issue(&mut self, weights: &[u32], total: u64) -> usize {
        // Reject incomplete residue classes before reducing a uniform word.
        let cutoff = total.wrapping_neg() % total;
        let mut ticket = loop {
            let word = self.word();
            if word >= cutoff {
                break word % total;
            }
        };
        for (i, &weight) in weights.iter().enumerate() {
            if ticket < u64::from(weight) {
                return i;
            }
            ticket -= u64::from(weight);
        }
        unreachable!("ticket below the total eligible weight")
    }
}
