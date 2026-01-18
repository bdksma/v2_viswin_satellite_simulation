# satellite_node.py
# ==========================================
# SIMULASI SATELLITE NODE (LEO, viswin-aware) + IMAGE RAW TX
# - Visibility window (elev > mask) dari orbit.py
# - Doppler dari orbit.py
# - Downlink TM sebagai "burst packets" mengikuti kapasitas rate_dl_mbps
# - Downlink IMG (raw) sebagai chunk packets saat visible
# - Uplink TC diterima kapan saja, tapi dieksekusi hanya saat visible
# - RF channel effects dari rf_channel.py (loss/ber/fade)
# ==========================================

from __future__ import annotations

import base64
import os
import socket
import time
import threading
import json
from typing import List, Dict, Any, Optional

from common.orbit_leo import DEFAULT_ORBIT
from common.rf_channel_leo import propagate

# ================================
# NETWORK CONFIG
# ================================
SAT_IP = "127.0.0.1"
SAT_TC_PORT = 5002          # receive TC from BBU (UDP)

BBU_IP = "127.0.0.1"
BBU_TM_PORT = 6001          # send TM/IMG to BBU (UDP)

# ================================
# TM PACKET / BURST CONFIG
# ================================
TIME_STEP_S = 1.0
PAYLOAD_BYTES = 256
HEADER_BYTES = 32
BITS_PER_PACKET = (PAYLOAD_BYTES + HEADER_BYTES) * 8
MAX_PKTS_PER_STEP = 2000

# ================================
# IMAGE RAW CONFIG
# ================================
RAW_FILE_PATH = "raw_224mb.bin"

# 2048x2048 12-bit packed => 6,291,456 bytes per frame (IF truly packed-only)
FRAME_BYTES = (2048 * 2048 * 12) // 8

IMG_CHUNK_BYTES = 1200

# Testing: kirim hanya frame pertama
SEND_ONLY_FIRST_FRAME = True

# Throttle per chunk
IMG_CHUNK_DELAY_S = 0.0015

# IMPORTANT: keep RF/noise scheme BUT make IMG survive loss:
# retransmit each chunk a few times (still passes through propagate)
IMG_REPEAT = 3

running = True

# ================================
# ONBOARD TC QUEUE
# ================================
_tc_queue: List[str] = []
_tc_lock = threading.Lock()

def _enqueue_tc(cmd: str):
    with _tc_lock:
        _tc_queue.append(cmd)

def _dequeue_tc() -> Optional[str]:
    with _tc_lock:
        if not _tc_queue:
            return None
        return _tc_queue.pop(0)

# ================================
# IMAGE TX STATE
# ================================
_img_sent_once = False

def _iter_frames_from_raw_file(path: str):
    """
    Yield (frame_id, frame_bytes) reading fixed FRAME_BYTES.
    NOTE: if your raw has per-line overhead, this may not match real frame boundaries.
    But pipeline + decoder will still try header mode first.
    """
    with open(path, "rb") as f:
        frame_id = 0
        while True:
            buf = f.read(FRAME_BYTES)
            if not buf:
                break
            if len(buf) < FRAME_BYTES:
                break
            yield frame_id, buf
            frame_id += 1

def send_image_if_needed(sock: socket.socket, visible: bool, rate_dl_mbps: float, elev: float):
    """
    Kirim IMG raw sebagai paket JSON:
      {
        "type":"IMG",
        "frame_id": int,
        "chunk_idx": int,
        "last": bool,
        "payload_b64": str
      }
    Still uses propagate() to keep noise/loss/fade,
    but each chunk is repeated IMG_REPEAT times to increase completion chance.
    """
    global _img_sent_once

    if not visible or rate_dl_mbps <= 0.0:
        return

    if not os.path.exists(RAW_FILE_PATH):
        print(f"[SAT] RAW file not found: {RAW_FILE_PATH}")
        return

    if SEND_ONLY_FIRST_FRAME and _img_sent_once:
        return

    for frame_id, frame in _iter_frames_from_raw_file(RAW_FILE_PATH):
        if SEND_ONLY_FIRST_FRAME and frame_id > 0:
            break

        chunks = [frame[i:i + IMG_CHUNK_BYTES] for i in range(0, len(frame), IMG_CHUNK_BYTES)]
        total = len(chunks)

        print(f"[SAT] IMG TX start frame={frame_id} chunks={total} (visible={visible}, elev={elev:.1f}deg, rate={rate_dl_mbps*1e3:.1f}kbps)")

        for idx, ch in enumerate(chunks):
            pkt = {
                "type": "IMG",
                "frame_id": frame_id,
                "chunk_idx": idx,
                "last": (idx == total - 1),
                "payload_b64": base64.b64encode(ch).decode("ascii"),
                "corrupted": False,
            }

            # repeat sending each chunk to survive RF loss
            for _r in range(IMG_REPEAT):
                out = propagate(pkt, elev_deg=elev, direction="downlink")
                if out is not None:
                    sock.sendto(json.dumps(out).encode("utf-8"), (BBU_IP, BBU_TM_PORT))

            if IMG_CHUNK_DELAY_S > 0:
                time.sleep(IMG_CHUNK_DELAY_S)

        print(f"[SAT] IMG TX done frame={frame_id}")

        if SEND_ONLY_FIRST_FRAME:
            _img_sent_once = True
            break

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

        # 1) kirim image raw (hanya saat visible)
        send_image_if_needed(sock, visible=visible, rate_dl_mbps=rate_dl_mbps, elev=elev)

        # 2) TM biasa
        if not visible or rate_dl_mbps <= 0.0:
            print(f"[SAT] TM gen (NOT visible) elev={elev:.1f}deg doppler={doppler:.0f}Hz")
            time.sleep(TIME_STEP_S)
            continue

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
                "duplicated": False,
            }

            tm_out = propagate(tm_packet, elev_deg=elev, direction="downlink")
            if tm_out is not None:
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
        data, _ = sock.recvfrom(4096)
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
    print("=== SATELLITE NODE STARTED (LEO viswin + IMG) ===")
    print(f"[SAT] RAW_FILE_PATH={RAW_FILE_PATH} exists={os.path.exists(RAW_FILE_PATH)} size={os.path.getsize(RAW_FILE_PATH) if os.path.exists(RAW_FILE_PATH) else 0}")
    print(f"[SAT] SEND_ONLY_FIRST_FRAME={SEND_ONLY_FIRST_FRAME}, IMG_CHUNK_BYTES={IMG_CHUNK_BYTES}, IMG_REPEAT={IMG_REPEAT}")

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
