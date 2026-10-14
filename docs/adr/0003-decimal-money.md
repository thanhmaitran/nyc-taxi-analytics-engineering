# ADR 0003 – Exact decimals for money and distance

**Status:** accepted

**Context.** The first benchmark on fixtures showed incremental and full-refresh marts differing by ~1e-14 in
`sum(total_amount)`: floating-point addition is not associative, and the two paths aggregate rows in different
order. Counts and fact rows matched; only float sums differed.

**Decision.** Cast `fare_amount`, `tip_amount`, `total_amount`, `airport_fee`, `congestion_surcharge`,
`cbd_congestion_fee` and `trip_distance` to `decimal(18,2)` in staging.

**Consequences.** Sums are exact and order-independent, so equivalence checks can use strict equality. Source
values with more than two decimals are rounded to two; values outside the decimal range would become NULL
(very large `total_amount` outliers) and should be checked in the audit on real data.
