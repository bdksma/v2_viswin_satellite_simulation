# common/rf_channel_leo.py
from __future__ import annotations

import random
import time
from typing import Optional, Dict, Any

PROPAGATION_DELAY_S = 0.25

BASE_PACKET_LOSS = 0.08
BASE_BIT_ERROR = 0.02
BASE_DUPLICATE = 0.002

BURST_FADE_START_PROB = 0.0015
BURST_FADE_LENGTH_PKTS = 25

_in_fade = False
_fade_remaining = 0

def _link_quality_from_elev(elev_deg: float, elev_mask: float = 10.0) -> float:
    if elev_deg <= elev_mask:
        return 0.0
    q = (elev_deg - elev_mask) / (90.0 - elev_mask)
    return max(0.0, min(1.0, q))

def _corrupt_base64_payload(packet: Dict[str, Any], severity: float) -> None:
    """
    Corrupt base64 text by flipping random chars.
    This simulates symbol errors after demod.
    severity in [0..1] controls how many chars flipped.
    """
    s = packet.get("payload_b64")
    if not isinstance(s, str) or not s:
        return
    # flip a small fraction of chars
    n = len(s)
    flips = max(1, int(n * 0.002 * severity))  # ~0.2% * severity
    s_list = list(s)
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    for _ in range(flips):
        i = random.randrange(n)
        s_list[i] = random.choice(alphabet)
    packet["payload_b64"] = "".join(s_list)

def propagate(packet: Dict[str, Any], elev_deg: float, direction: str = "downlink") -> Optional[Dict[str, Any]]:
    global _in_fade, _fade_remaining

    # delay: for IMG we keep delay small so frames can complete
    pkt_type = str(packet.get("type", "")).upper()
    delay = PROPAGATION_DELAY_S
    if pkt_type == "IMG":
        delay = 0.002  # 2ms per packet
    if delay > 0:
        time.sleep(delay)

    q = _link_quality_from_elev(float(elev_deg))

    # burst fade
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

    # packet loss
    loss_p = BASE_PACKET_LOSS * (1.0 - q) ** 1.6
    if direction.lower() == "uplink":
        loss_p *= 1.15
    if random.random() < loss_p:
        return None

    out = dict(packet)

    # bit error probability
    ber_p = BASE_BIT_ERROR * (1.0 - q) ** 2.0
    if direction.lower() == "uplink":
        ber_p *= 1.10

    corrupted = (random.random() < ber_p)
    out["corrupted"] = bool(corrupted)

    # if corrupted and it's IMG, actually corrupt payload
    # severity higher when q smaller
    if corrupted and pkt_type == "IMG":
        severity = 0.3 + (1.0 - q) * 0.7  # 0.3..1.0
        _corrupt_base64_payload(out, severity)

    # duplicate
    dup_p = BASE_DUPLICATE * (1.0 + (1.0 - q))
    out["duplicated"] = (random.random() < dup_p)

    return out