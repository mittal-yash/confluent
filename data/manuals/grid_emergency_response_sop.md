# Grid Emergency Response Standard Operating Procedure (SOP)

Applicable to: all assets. This SOP defines the allowed autonomous actions and
when each is appropriate. The agent mesh MUST choose only from these actions.

## Allowed actions
- monitor: keep watching; no intervention. Use for warning-level anomalies that
  are stable and not trending toward critical.
- throttle_load: reduce load on the asset to a safe level. Use for thermal
  warnings where a tie route is not available.
- reroute_load: shift load to a tie asset. Preferred first response for thermal
  excursions on transformers and lines when a healthy tie exists.
- dispatch_crew: send a qualified field crew. Required for mechanical faults
  (vibration, leaks) and to follow up any P1 isolation.
- isolate: open the breaker / take the asset offline. Reserved for critical
  excursions that do not respond to load reduction, or imminent equipment
  failure (e.g. vibration above critical).
- create_work_order: always raised alongside any physical action so the work is
  tracked to completion.

## Severity -> priority mapping
- critical severity -> P1 work order, SLA 2 hours.
- warning severity  -> P2 work order, SLA 8 hours.
- info severity     -> P3 work order, SLA 24 hours.

## Decision principles
1. Prefer the least disruptive action that removes the hazard (reroute/throttle
   before isolate).
2. Protect critical assets and human safety first.
3. Always ground the decision in the asset's spec sheet thresholds and the
   relevant equipment guide; never act on an unverified assumption.
4. After acting, verify telemetry returns within the normal envelope before
   closing the incident.

## Crew selection
- Match crew skills to asset_type (transformer -> `transformer`, line ->
  `line`, water_pump -> `pump`; `general` can assist any).
- Prefer an available crew in the same region, nearest by distance.
