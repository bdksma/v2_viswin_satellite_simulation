# satellite_node.py (FINAL - IMG noisy+redundant)

from __future__ import annotations
import base64, os, socket, time, threading, json
from typing import List, Dict, Any, Optional

from common.orbit_leo import DEFAULT_ORBIT
from common.rf_channel_leo import propagate

SAT_IP = "127.0.0.1"
SAT_TC_PORT = 5002
BBU_IP = "127.0.0.1"
BBU_TM_PORT = 6001

TIME_STEP_S = 1.0
PAYLOAD_BYTES = 256
HEADER_BYTES = 32
BITS_PER_PACKET = (PAYLOAD_BYTES + HEADER_BYTES) * 8
MAX_PKTS_PER_STEP = 2000

RAW_FILE_PATH = "raw_224mb.bin"
FRAME_BYTES = (2048 * 2048 * 12) // 8

IMG_CHUNK_BYTES = 6000
SEND_ONLY_FIRST_FRAME = True
IMG_CHUNK_DELAY_S = 0.0005

# redundancy copies per chunk (for "filter+amplifier" at receiver)
IMG_REP_COPIES = 5

running = True
_tc_queue: List[str] = []
_tc_lock = threading.Lock()
_img_sent_once = False

def _enqueue_tc(cmd: str):
    with _tc_lock:
        _tc_queue.append(cmd)

def _dequeue_tc() -> Optional[str]:
    with _tc_lock:
        return _tc_queue.pop(0) if _tc_queue else None

def _iter_frames_from_raw_file(path: str):
    with open(path, "rb") as f:
        frame_id = 0
        while True:
            buf = f.read(FRAME_BYTES)
            if not buf or len(buf) < FRAME_BYTES:
                break
            yield frame_id, buf
            frame_id += 1

def send_image_if_needed(sock: socket.socket, visible: bool, rate_dl_mbps: float, elev: float):
    global _img_sent_once
    if not visible or rate_dl_mbps <= 0.0:
        return
    if not os.path.exists(RAW_FILE_PATH):
        print(f"[SAT] RAW not found: {RAW_FILE_PATH}")
        return
    if SEND_ONLY_FIRST_FRAME and _img_sent_once:
        return

    for frame_id, frame in _iter_frames_from_raw_file(RAW_FILE_PATH):
        if SEND_ONLY_FIRST_FRAME and frame_id > 0:
            break

        chunks = [frame[i:i + IMG_CHUNK_BYTES] for i in range(0, len(frame), IMG_CHUNK_BYTES)]
        total = len(chunks)
        print(f"[SAT] IMG TX start frame={frame_id} chunks={total} rep={IMG_REP_COPIES} elev={elev:.1f}")

        for idx, ch in enumerate(chunks):
            payload_b64 = base64.b64encode(ch).decode("ascii")

            for rep in range(IMG_REP_COPIES):
                pkt = {
                    "type": "IMG",
                    "frame_id": frame_id,
                    "chunk_idx": idx,
                    "last": (idx == total - 1),
                    "rep": rep,                 # <-- NEW
                    "payload_b64": payload_b64,
                }
                out = propagate(pkt, elev_deg=elev, direction="downlink")
                if out is not None:
                    sock.sendto(json.dumps(out).encode("utf-8"), (BBU_IP, BBU_TM_PORT))

            if IMG_CHUNK_DELAY_S > 0:
                time.sleep(IMG_CHUNK_DELAY_S)

        print(f"[SAT] IMG TX done frame={frame_id}")
        if SEND_ONLY_FIRST_FRAME:
            _img_sent_once = True
            break

def telemetry_sender():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    seq = 0

    while running:
        st = DEFAULT_ORBIT.get_state()
        visible = bool(st["visible"])
        elev = float(st["elev_deg"])
        doppler = float(st["doppler_hz"])
        rate_dl_mbps = float(st["rate_dl_mbps"])

        send_image_if_needed(sock, visible, rate_dl_mbps, elev)

        if not visible or rate_dl_mbps <= 0.0:
            time.sleep(TIME_STEP_S)
            continue

        bits_step = rate_dl_mbps * 1e6 * TIME_STEP_S
        max_pkts = int(bits_step // BITS_PER_PACKET)
        max_pkts = max(0, min(max_pkts, MAX_PKTS_PER_STEP))

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

        time.sleep(TIME_STEP_S)

def telecommand_receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SAT_IP, SAT_TC_PORT))
    print("[SAT] Telecommand receiver listening (UDP)")
    while running:
        data, _ = sock.recvfrom(4096)
        _enqueue_tc(data.decode("utf-8", errors="replace").strip())

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
        if pkt2 is None or pkt2.get("corrupted"):
            continue
        print(f"[SAT] TC EXECUTED: {cmd}")

if __name__ == "__main__":
    print("=== SATELLITE NODE STARTED (LEO viswin + IMG redundant) ===")
    print(f"[SAT] RAW_FILE_PATH={RAW_FILE_PATH} exists={os.path.exists(RAW_FILE_PATH)} size={os.path.getsize(RAW_FILE_PATH) if os.path.exists(RAW_FILE_PATH) else 0}")
    print(f"[SAT] IMG_CHUNK_BYTES={IMG_CHUNK_BYTES} IMG_REP_COPIES={IMG_REP_COPIES}")

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