"""Build per-asset spec-sheet documents.

The spec sheet is both structured reference data (joined by Flink) and a
narrative chunk in the RAG corpus, so the diagnosis agent can retrieve the
exact, asset-specific thresholds it must reason over.
"""
from __future__ import annotations

from common.assets import (
    ASSET_LINE,
    ASSET_PUMP,
    ASSET_SUBSTATION,
    ASSET_TRANSFORMER,
    Asset,
)

_GUIDE = {
    ASSET_TRANSFORMER: "Transformer Oil Thermal Loading Guide (IEEE C57.91)",
    ASSET_LINE: "Transmission Line Dynamic Rating Guide (IEEE 738)",
    ASSET_PUMP: "Pump Vibration & Cavitation Maintenance Guide (ISO 10816)",
    ASSET_SUBSTATION: "Substation Operations Guide",
}


def build_spec_doc(a: Asset) -> dict:
    return {
        "spec_sheet_id": a.spec_sheet_id,
        "asset_id": a.asset_id,
        "asset_type": a.asset_type,
        "name": a.name,
        "voltage_kv": a.voltage_kv,
        "rated_load_mw": a.rated_load_mw,
        "nominal_temp_c": a.nominal_temp_c,
        "warning_temp_c": a.warning_temp_c,
        "critical_temp_c": a.critical_temp_c,
        "warning_vibration_mm_s": a.warning_vibration_mm_s,
        "nominal_vibration_mm_s": a.nominal_vibration_mm_s,
        "nominal_oil_pressure_kpa": a.nominal_oil_pressure_kpa,
        "install_year": a.install_year,
        "criticality": a.criticality,
        "tie_assets": a.tie_assets,
        "reference_guide": _GUIDE.get(a.asset_type, "Asset Operations Guide"),
        "spec_text": _narrative(a),
    }


def _narrative(a: Asset) -> str:
    lines = [
        f"Spec sheet for {a.asset_id} ({a.name}), a {a.asset_type} in the "
        f"{a.region} region, substation {a.substation}. Voltage class "
        f"{a.voltage_kv} kV, criticality {a.criticality}, installed {a.install_year}.",
    ]
    if a.rated_load_mw:
        lines.append(f"Rated load: {a.rated_load_mw} MW. Sustained operation above "
                     f"100% rated load is an overload condition.")
    if a.warning_temp_c:
        lines.append(
            f"Temperature thresholds: nominal {a.nominal_temp_c} C, warning "
            f"{a.warning_temp_c} C, critical {a.critical_temp_c} C. Above the "
            f"critical limit, reduce load immediately and isolate if it does not "
            f"recover."
        )
    if a.warning_vibration_mm_s:
        lines.append(
            f"Vibration thresholds (RMS velocity): nominal "
            f"{a.nominal_vibration_mm_s} mm/s, warning {a.warning_vibration_mm_s} "
            f"mm/s; above warning indicates bearing wear and needs a crew "
            f"inspection."
        )
    if a.nominal_oil_pressure_kpa:
        lines.append(
            f"Nominal pressure {a.nominal_oil_pressure_kpa} kPa; a drop greater "
            f"than 30% indicates a leak or cavitation."
        )
    if a.tie_assets:
        lines.append(f"Tie routes available for load transfer: "
                     f"{', '.join(a.tie_assets)}.")
    lines.append(f"Refer to the {_GUIDE.get(a.asset_type, 'Asset Operations Guide')} "
                 f"for the full response procedure.")
    return " ".join(lines)
