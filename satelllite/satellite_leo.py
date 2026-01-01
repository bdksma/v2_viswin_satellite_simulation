# satellite_node.py
# ==========================================
# SIMULASI SATELLITE NODE (LEO, viswin-aware)
# - Visibility window (elev > mask) dari orbit.py
# - Doppler dari orbit.py
# - Downlink TM sebagai "burst packets" mengikuti kapasitas rate_dl_mbps
# - Uplink TC diterima kapan saja, tapi dieksekusi hanya saat visible
# - RF channel effects dari rf_channel.py
# ==========================================

from __future__ import annotations

import socket
import time
import threading
import json
from typing import List, Dict, Any

from orbit_leo import DEFAULT_ORBIT
from rf_channel_leo import propagate

# ================================
# NETWORK CONFIG
# ================================
SAT_IP = "127.0.0.1"
SAT_TC_PORT = 5002          # receive TC from BBU (UDP)

BBU_IP = "127.0.0.1"
BBU_TM_PORT = 6001          # send TM to BBU (UDP)

# ================================
# TM PACKET / BURST CONFIG
# ================================
TIME_STEP_S = 1.0          # sampling per detik (mirip viswin TIME_STEP)
PAYLOAD_BYTES = 256        # ukuran payload TM (untuk hitung kapasitas)
HEADER_BYTES = 32          # estimasi header overhead JSON/metadata
BITS_PER_PACKET = (PAYLOAD_BYTES + HEADER_BYTES) * 8

MAX_PKTS_PER_STEP = 2000   # safety cap supaya tidak "meledak" kalau rate besar

running = True

# ================================
# ONBOARD TC QUEUE
# ================================
_tc_queue: List[str] = []
_tc_lock = threading.Lock()

def _enqueue_tc(cmd: str):
    with _tc_lock:
        _tc_queue.append(cmd)

def _dequeue_tc() -> str | None:
    with _tc_lock:
        if not _tc_queue:
            return None
        return _tc_queue.pop(0)

# ================================
# THREAD: SEND TELEMETRY (DOWNLINK)
# ================================
def telemetry_sender():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    seq = 0

    while running:
        st = DEFAULT_ORBIT.get_state()
        visible = bool(st["visible"])
        elev = float(st["elev_deg"])
        doppler = float(st["doppler_hz"])
        rate_dl_mbps = float(st["rate_dl_mbps"])

        if not visible or rate_dl_mbps <= 0.0:
            # tetap "generate" (untuk log/debug), tapi tidak transmit
            print(f"[SAT] TM gen (NOT visible) elev={elev:.1f}deg doppler={doppler:.0f}Hz")
            time.sleep(TIME_STEP_S)
            continue

        # kapasitas bit per TIME_STEP
        bits_step = rate_dl_mbps * 1e6 * TIME_STEP_S
        max_pkts = int(bits_step // BITS_PER_PACKET)
        if max_pkts <= 0:
            time.sleep(TIME_STEP_S)
            continue
        if max_pkts > MAX_PKTS_PER_STEP:
            max_pkts = MAX_PKTS_PER_STEP

        for _ in range(max_pkts):
            tm_packet: Dict[str, Any] = {
                "type": "TM",
                "seq": seq,
                "ts": st["ts"],
                "elev_deg": elev,
                "doppler_hz": doppler,
                "visible": True,
                "corrupted": False,
                "payload_len": PAYLOAD_BYTES,
            }

            # RF channel
            tm_out = propagate(tm_packet, elev_deg=elev, direction="downlink")
            if tm_out is None:
                # lost
                pass
            else:
                sock.sendto(json.dumps(tm_out).encode("utf-8"), (BBU_IP, BBU_TM_PORT))
            seq = (seq + 1) & 0xFFFFFFFF

        print(f"[SAT] DOWNLINK: elev={elev:.1f}deg rate={rate_dl_mbps*1e3:.1f}kbps sent_pkts={max_pkts}")
        time.sleep(TIME_STEP_S)

# ================================
# THREAD: RECEIVE TELECOMMAND (UPLINK RX)
# ================================
def telecommand_receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SAT_IP, SAT_TC_PORT))
    print("[SAT] Telecommand receiver listening (UDP)")

    while running:
        data, addr = sock.recvfrom(4096)
        cmd = data.decode("utf-8", errors="replace").strip()
        _enqueue_tc(cmd)
        print(f"[SAT] TC RECEIVED (queued): {cmd}")

# ================================
# THREAD: EXECUTE TC WHEN VISIBLE
# ================================
def telecommand_executor():
    while running:
        st = DEFAULT_ORBIT.get_state()
        if not st["visible"]:
            time.sleep(0.5)
            continue

        cmd = _dequeue_tc()
        if cmd is None:
            time.sleep(0.2)
            continue

        # Apply RF to represent uplink corruption/loss at reception time (optional)
        pkt = {"type": "TC", "cmd": cmd, "ts": time.time(), "corrupted": False}
        pkt2 = propagate(pkt, elev_deg=float(st["elev_deg"]), direction="uplink")
        if pkt2 is None:
            print(f"[SAT] TC LOST (RF): {cmd}")
            continue

        if pkt2.get("corrupted"):
            print(f"[SAT] TC CORRUPTED -> ignored: {cmd}")
            continue

        print(f"[SAT] TC EXECUTED: {cmd}")

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    print("=== SATELLITE NODE STARTED (LEO viswin) ===")

    threads = [
        threading.Thread(target=telemetry_sender, daemon=True),
        threading.Thread(target=telecommand_receiver, daemon=True),
        threading.Thread(target=telecommand_executor, daemon=True),
    ]
    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        running = False
        print("\n[SAT] Shutting down")
