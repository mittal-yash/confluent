# Transmission Line Conductor Temperature & Dynamic Line Rating

Applicable to: overhead transmission lines (asset_type: transmission_line).
Reference standards: IEEE 738 conductor thermal rating.

## Conductor temperature thresholds
- Normal conductor temperature: 40-50 C under typical load and ambient.
- Warning threshold: 75 C. Above this, conductor sag increases and approaches
  statutory ground-clearance limits.
- Critical threshold: 90 C. Annealing of aluminium conductor begins; permanent
  loss of strength can occur.

## Drivers
- High current (load) combined with high ambient temperature and low wind speed
  reduces convective cooling and raises conductor temperature.
- During a heatwave with still air, dynamic line rating falls and the same load
  produces a higher conductor temperature.

## Recommended response when conductor exceeds 90 C
1. Reroute load onto the parallel tie line to reduce current (reroute_load).
2. If no tie capacity is available, throttle load on the affected corridor
   (throttle_load) and notify the system operator.
3. Dispatch a crew skilled in `line` only if physical damage or clearance
   infringement is suspected.
4. Verify the conductor temperature returns below 75 C.

## Notes
- Rerouting raises load on the tie line; confirm the tie line stays within its
  own thermal envelope after transfer.
