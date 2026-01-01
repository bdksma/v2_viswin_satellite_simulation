# rf_channel.py
# =====================================================
# RF CHANNEL SIMULATION (uplink + downlink, viswin-aware)
# =====================================================
# Meniru konsep viswin_doppler:
# - Saat elev rendah: loss lebih besar, BER lebih tinggi.
# - Saat elev tinggi: link lebih bagus.
# - Ada propagation delay (tetap).
#
# API:
#   propagate(packet: dict, elev_deg: float, direction: str) -> dict | None
# =====================================================

from __future__ import annotations

import random
import time
from typing import Optional, Dict, Any

# -----------------------
# Base channel parameters
# -----------------------
PROPAGATION_DELAY_S = 0.25

# Base (worst-ish) probabilities. Akan dimodulasi oleh elevasi.
BASE_PACKET_LOSS = 0.08
BASE_BIT_ERROR = 0.02
BASE_DUPLICATE = 0.002

# Extra "deep fade" burst loss (rf_like feeling)
BURST_FADE_START_PROB = 0.0015
BURST_FADE_LENGTH_PKTS = 25

# Internal state
_in_fade = False
_fade_remaining = 0

def _link_quality_from_elev(elev_deg: float, elev_mask: float = 10.0) -> float:
    """
    Map elev to quality [0..1].
    - <= mask : 0
    - 90 deg  : 1
    """
    if elev_deg <= elev_mask:
        return 0.0
    q = (elev_deg - elev_mask) / (90.0 - elev_mask)
    if q < 0.0:
        q = 0.0
    if q > 1.0:
        q = 1.0
    return q

def propagate(packet: Dict[str, Any], elev_deg: float, direction: str = "downlink") -> Optional[Dict[str, Any]]:
    """
    Simulasi RF:
    - fixed propagation delay
    - burst fade (loss beberapa paket berturut)
    - packet loss random
    - bit error (mark as corrupted)
    - duplicate (optional)
    """
    global _in_fade, _fade_remaining

    # propagation delay
    time.sleep(PROPAGATION_DELAY_S)

    q = _link_quality_from_elev(float(elev_deg))

    # burst fade: lebih sering ketika q kecil
    fade_start = BURST_FADE_START_PROB * (1.0 + (1.0 - q) * 3.0)
    if _in_fade:
        _fade_remaining -= 1
        if _fade_remaining <= 0:
            _in_fade = False
        return None

    if random.random() < fade_start:
        _in_fade = True
        _fade_remaining = BURST_FADE_LENGTH_PKTS
        return None

    # packet loss: turun saat q naik
    loss_p = BASE_PACKET_LOSS * (1.0 - q) ** 1.6
    # uplink biasanya lebih rapuh (optional)
    if direction.lower() == "uplink":
        loss_p *= 1.15

    if random.random() < loss_p:
        return None

    out = dict(packet)

    # bit error: turun saat q naik
    ber_p = BASE_BIT_ERROR * (1.0 - q) ** 2.0
    if direction.lower() == "uplink":
        ber_p *= 1.10

    if random.random() < ber_p:
        out["corrupted"] = True
        out["rf_note"] = "bit_error"
    else:
        out["corrupted"] = bool(out.get("corrupted", False))

    # duplicate: kecil, naik sedikit saat q kecil
    dup_p = BASE_DUPLICATE * (1.0 + (1.0 - q))
    if random.random() < dup_p:
        out["duplicated"] = True  # marker saja, pengirim yang duplikasi real
    else:
        out["duplicated"] = False

    return out

if __name__ == "__main__":
    # quick debug
    pkt = {"seq": 1, "corrupted": False}
    for e in [5, 10, 15, 30, 60, 80]:
        ok = 0
        lost = 0
        corr = 0
        for _ in range(200):
            r = propagate(pkt, e)
            if r is None:
                lost += 1
            else:
                ok += 1
                if r.get("corrupted"):
                    corr += 1
        print(e, "lost", lost, "ok", ok, "corr", corr)
