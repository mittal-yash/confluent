# Power Transformer Thermal Loading & Oil Temperature Guide

Applicable to: oil-immersed power transformers (asset_type: transformer).
Reference standards: IEEE C57.91 loading guide, IEC 60076-7.

## Top-oil temperature thresholds
- Normal top-oil temperature under rated load is approximately 55-65 C above
  no-load conditions.
- Warning threshold: 85 C. Sustained operation above 85 C accelerates paper
  insulation ageing (the Arrhenius rule: ageing roughly doubles per +6 C).
- Critical threshold: 95 C top-oil. Above this, gas bubbling risk and rapid
  insulation degradation occur; load must be reduced immediately.

## Overload behaviour
- Short-time emergency loading above 100% of nameplate MVA is permissible only
  for limited duration and only when top-oil temperature stays below the
  critical limit.
- A combination of high ambient temperature (heatwave) plus load above 110% is
  the most common driver of thermal excursions on this fleet.

## Recommended response when top-oil exceeds 95 C
1. Reduce loading immediately by rerouting load to a tie transformer or feeder
   (throttle_load or reroute_load action).
2. If temperature does not fall within 5 minutes after load reduction, isolate
   the transformer to prevent insulation failure (isolate action).
3. Create a P1 work order and dispatch a crew skilled in `transformer` to
   inspect cooling (fans, pumps, oil level) and gas levels.
4. Verify recovery: top-oil should return below 85 C before returning the unit
   to normal service.

## Common root causes
- Blocked or failed cooling fans / oil pumps.
- Low oil level reducing heat transfer.
- Sustained overload during peak demand or heatwave.
- High harmonic loading raising eddy-current losses.
