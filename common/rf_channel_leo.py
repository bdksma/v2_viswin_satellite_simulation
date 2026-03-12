from __future__ import annotations
import random, time
from typing import Optional, Dict, Any

# base delay for TM
PROPAGATION_DELAY_S = 0.25

# Base channel params
BASE_PACKET_LOSS = 0.08
BASE_BIT_ERROR   = 0.02
BASE_DUPLICATE   = 0.002

BURST_FADE_START_PROB  = 0.0015
BURST_FADE_LENGTH_PKTS = 25

_in_fade = False
_fade_remaining = 0

def _link_quality_from_elev(elev_deg: float, elev_mask: float = 10.0) -> float:
    if elev_deg <= elev_mask:
        return 0.0
    q = (elev_deg - elev_mask) / (90.0 - elev_mask)
    return max(0.0, min(1.0, q))

def _corrupt_base64_payload(packet: Dict[str, Any], severity: float) -> None:
    s = packet.get("payload_b64")
    if not isinstance(s, str) or not s:
        return
    n = len(s)
    flips = max(1, int(n * 0.002 * severity))  # ~0.2% chars
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    arr = list(s)
    for _ in range(flips):
        i = random.randrange(n)
        arr[i] = random.choice(alphabet)
    packet["payload_b64"] = "".join(arr)

def propagate(packet: Dict[str, Any], elev_deg: float, direction: str = "downlink") -> Optional[Dict[str, Any]]:
    global _in_fade, _fade_remaining

    pkt_type = str(packet.get("type", "")).upper()

    # Delay: small for image packets so frame can complete
    delay = PROPAGATION_DELAY_S
    if pkt_type in ("IMG", "IMG2"):
        delay = 0.002  # 2ms per packet
    if delay > 0:
        time.sleep(delay)

    q = _link_quality_from_elev(float(elev_deg))

    # Burst fade
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

    # --- Amplifier+Filter effect for IMG2 ---
    # IMG   = noisy link (worse)
    # IMG2  = after filter+amplifier (better)
    loss_p = BASE_PACKET_LOSS * (1.0 - q) ** 1.6
    ber_p  = BASE_BIT_ERROR   * (1.0 - q) ** 2.0

    if pkt_type == "IMG":
        loss_p *= 1.30
        ber_p  *= 1.20
    elif pkt_type == "IMG2":
        loss_p *= 0.25   # amplifier improves link
        ber_p  *= 0.35

    if direction.lower() == "uplink":
        loss_p *= 1.15
        ber_p  *= 1.10

    if random.random() < loss_p:
        return None

    out = dict(packet)

    corrupted = (random.random() < ber_p)
    out["corrupted"] = bool(corrupted)

    # If corrupted and payload is base64, actually corrupt payload
    if corrupted and pkt_type in ("IMG", "IMG2"):
        severity = 0.3 + (1.0 - q) * 0.7
        _corrupt_base64_payload(out, severity)

    dup_p = BASE_DUPLICATE * (1.0 + (1.0 - q))
    out["duplicated"] = (random.random() < dup_p)

    return out