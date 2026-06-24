# Water Pump Vibration & Cavitation Maintenance

Applicable to: motor-driven water pumps (asset_type: water_pump).
Reference standards: ISO 10816 vibration severity.

## Vibration thresholds (RMS velocity, mm/s)
- Good: below 2.8 mm/s.
- Warning: 4.5-7.1 mm/s indicates developing bearing wear or misalignment.
- Critical: above 7.1 mm/s; bearing failure is imminent and unplanned outage
  risk is high.

## Oil / discharge pressure
- Nominal discharge pressure is asset-specific (see spec sheet).
- A pressure drop of more than 30% below nominal indicates cavitation, a closed
  valve, or a suction-side leak.

## Recommended response
1. Rising vibration above the warning threshold with no pressure loss: schedule
   a P2 work order and dispatch a crew skilled in `pump` to inspect bearings and
   alignment (dispatch_crew). The unit may keep running under watch.
2. Vibration above critical OR pressure drop above 30%: isolate the pump to
   avoid catastrophic bearing failure (isolate), raise a P1 work order.
3. Verify vibration returns below 2.8 mm/s and pressure to nominal after repair.

## Common root causes
- Worn or under-lubricated bearings.
- Shaft misalignment or impeller imbalance.
- Cavitation from suction restriction or air ingress.
