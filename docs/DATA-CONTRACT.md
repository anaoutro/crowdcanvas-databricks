# Shared fictional festival contract

No live integration is included. EncoreOps resolves individual anonymous bookings; CrowdCanvas analyzes synthetic aggregate observations. Their shared event is the cancellation of the Main set.

## EncoreOps records

Session_Key identifies an alternative session. Capacity and remaining seats are integers. Pass_Id identifies an anonymous booking and is unique across recovery requests. Party size is 1–8; original total is synthetic BRL. Requests progress through New, Held, Confirmed, Expired, CreditApproved or RefundApproved. Approved compensation is not a payment.

## CrowdCanvas events

| Field | Meaning |
|---|---|
| event_id | Immutable unique snapshot identity |
| zone | MAIN, HARBOR, GROVE or FOOD |
| event_time | Observation timestamp, UTC and minute-aligned |
| arrival_time | Delivery timestamp, UTC and not before observation |
| occupancy | Nonnegative integer not exceeding the zone's synthetic nominal capacity |
| queue_wait_minutes | Finite nonnegative value ≤120 with at most two decimal places |

The source expects one snapshot per zone-minute. MAIN/HARBOR/GROVE/FOOD nominal capacities are 1000/500/700/300. These are telemetry scenario assumptions, separate from EncoreOps alternative-session booking capacity. Six observations are deliberately missing. The source spans 18:00–19:14 UTC; cancellation is at 18:30. There are thirty minutes before and forty-five after; comparisons use means rather than raw totals.

## Export boundary

The seed and telemetry are independent synthetic fixtures. A future ticket connector would publish cancellation and confirmed reassignment events; a separate anonymous sensor source would publish zone observations. No attendee-level tracking or causal outcome claim is implied by the current constructed pattern.
