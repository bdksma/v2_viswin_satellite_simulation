# bbu_node.py
# ==========================================
# SIMULASI BBU NODE (LEO viswin-aware) + IMAGE REASSEMBLY
# - Receive TM/IMG from satellite (UDP)
# - Stream to Web (TCP) newline framed
# - Reassemble IMG -> decode -> save PNG + TIFF
# ==========================================

from __future__ import annotations

import base64
import json
import os
import socket
import threading
import time
from typing import Dict, List, Optional, Tuple

from common.orbit_leo import DEFAULT_ORBIT
from common.rf_channel_leo import propagate  # keep for TC uplink simulation
from common.raw_to_image import decode_one_frame, save_png_tiff

BBU_IP = "127.0.0.1"

BBU_TM_PORT = 6001
SAT_IP = "127.0.0.1"
SAT_TC_PORT = 5002

BBU_TC_PORT = 7001
BBU_TM_PORT_WEB = 7002

IMG_OUT_DIR = "output_images"
os.makedirs(IMG_OUT_DIR, exist_ok=True)

running = True
telemetry_live: List[str] = []
telemetry_history: List[str] = []
telecommand_queue: List[str] = []

web_tm_conn: Optional[socket.socket] = None
web_tm_lock = threading.Lock()

class ImageReassembler:
    def __init__(self):
        self.frames: Dict[int, Dict] = {}
        self.lock = threading.Lock()

    def push(self, pkt: dict) -> Optional[Tuple[int, bytes]]:
        try:
            frame_id = int(pkt["frame_id"])
            idx = int(pkt["chunk_idx"])
            last = bool(pkt.get("last", False))
            payload_b64 = pkt["payload_b64"]
            data = base64.b64decode(payload_b64)
        except Exception:
            return None

        with self.lock:
            st = self.frames.setdefault(frame_id, {"chunks": {}, "last_idx": None, "t0": time.time()})
            # dedup: if repeated chunk arrives, overwrite same idx (OK)
            st["chunks"][idx] = data

            if idx % 200 == 0:
                print(f"[BBU] IMG RX frame={frame_id} got={len(st['chunks'])} last={st['last_idx']}")

            if last:
                st["last_idx"] = idx
                print(f"[BBU] IMG last chunk received frame={frame_id} last_idx={idx} got={len(st['chunks'])}")

            if st["last_idx"] is None:
                return None

            last_idx = st["last_idx"]
            if all(i in st["chunks"] for i in range(last_idx + 1)):
                raw = b"".join(st["chunks"][i] for i in range(last_idx + 1))
                del self.frames[frame_id]
                return frame_id, raw

        return None

    def cleanup(self, max_age_s: float = 180.0):
        now = time.time()
        with self.lock:
            drop = [fid for fid, st in self.frames.items() if (now - st.get("t0", now)) > max_age_s]
            for fid in drop:
                print(f"[BBU] IMG drop incomplete frame={fid} (timeout)")
                del self.frames[fid]

img_reasm = ImageReassembler()
latest_image = {"frame_id": None, "png": None, "tif": None}

def _enqueue_web(msg: str):
    telemetry_history.append(msg)
    if len(telemetry_history) > 5000:
        telemetry_history[:] = telemetry_history[-5000:]

def tm_receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((BBU_IP, BBU_TM_PORT))
    print("[BBU] Listening TM/IMG from satellite (UDP)")

    while running:
        data, _ = sock.recvfrom(65535)

        try:
            raw_text = data.decode("utf-8", errors="replace").strip()
            pkt = json.loads(raw_text)
            pkt_type = pkt.get("type")
        except Exception:
            raw_text = data.decode("utf-8", errors="replace").strip()
            pkt = None
            pkt_type = None

        # IMG
        if pkt_type == "IMG":
            res = img_reasm.push(pkt)
            if res is not None:
                frame_id, raw_frame = res
                print(f"[BBU] IMG frame complete: {frame_id}, bytes={len(raw_frame)}")

                try:
                    img16 = decode_one_frame(raw_frame, 2048, 2048)
                    out_png = os.path.join(IMG_OUT_DIR, f"frame_{frame_id:05d}.png")
                    out_tif = os.path.join(IMG_OUT_DIR, f"frame_{frame_id:05d}.tif")
                    save_png_tiff(img16, out_png, out_tif)

                    latest_image["frame_id"] = frame_id
                    latest_image["png"] = out_png
                    latest_image["tif"] = out_tif

                    print(f"[BBU] IMG saved: {out_png} + {out_tif}")

                    _enqueue_web(f"IMG|{frame_id}|{out_png}|{out_tif}")

                except Exception as e:
                    print(f"[BBU] IMG decode failed frame {frame_id}: {e}")
            else:
                img_reasm.cleanup(max_age_s=180.0)
            continue

        # TM biasa
        _enqueue_web(raw_text)

        if DEFAULT_ORBIT.is_visible():
            telemetry_live.append(raw_text)
            if len(telemetry_live) > 2000:
                telemetry_live[:] = telemetry_live[-2000:]

def tm_server_for_web():
    global web_tm_conn
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((BBU_IP, BBU_TM_PORT_WEB))
    server.listen(1)
    print("[BBU] TM server for Web listening on 7002")

    while running:
        conn, _ = server.accept()
        with web_tm_lock:
            web_tm_conn = conn
        print("[BBU] Web connected for TM")

        try:
            conn.settimeout(1.0)
            while running:
                if DEFAULT_ORBIT.is_visible() and telemetry_live:
                    tm = telemetry_live.pop(0)
                    msg = f"LIVE|{tm}"
                elif telemetry_history:
                    tm = telemetry_history[-1]
                    msg = f"HIST|{tm}"
                else:
                    time.sleep(0.3)
                    continue

                try:
                    conn.sendall((msg + "\n").encode("utf-8"))
                except Exception:
                    break

                time.sleep(0.15)
        finally:
            print("[BBU] Web disconnected")
            try:
                conn.close()
            except Exception:
                pass
            with web_tm_lock:
                web_tm_conn = None

def tc_receiver_from_web():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((BBU_IP, BBU_TC_PORT))
    sock.listen(5)
    print("[BBU] Waiting TC from Web (TCP)")

    while running:
        conn, _ = sock.accept()
        try:
            tc = conn.recv(4096).decode("utf-8", errors="replace").strip()
            if tc:
                telecommand_queue.append(tc)
                print(f"[BBU] TC queued from Web: {tc}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

def tc_sender():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print("[BBU] TC sender started (UDP->SAT)")

    while running:
        if not telecommand_queue:
            time.sleep(0.2)
            continue

        st = DEFAULT_ORBIT.get_state()
        if not st["visible"]:
            print("[BBU] TC queued, waiting visibility")
            time.sleep(0.8)
            continue

        tc = telecommand_queue.pop(0)

        pkt = {"type": "TC", "cmd": tc, "ts": time.time(), "corrupted": False}
        pkt2 = propagate(pkt, elev_deg=float(st["elev_deg"]), direction="uplink")
        if pkt2 is None:
            print(f"[BBU] TC DROP (RF): {tc}")
            continue
        if pkt2.get("corrupted"):
            print(f"[BBU] TC CORRUPTED -> still sent: {tc}")

        sock.sendto(tc.encode("utf-8"), (SAT_IP, SAT_TC_PORT))
        print(f"[BBU] TC SENT to satellite: {tc}")
        time.sleep(0.2)

def status_printer():
    while running:
        st = DEFAULT_ORBIT.get_state()
        print(
            f"[BBU] Visible={st['visible']} elev={st['elev_deg']:.1f}deg "
            f"DL={st['rate_dl_mbps']*1e3:.1f}kbps UL={st['rate_ul_mbps']*1e3:.1f}kbps | "
            f"LIVE={len(telemetry_live)} HIST={len(telemetry_history)} TCQ={len(telecommand_queue)} | "
            f"IMG={latest_image.get('frame_id')}"
        )
        time.sleep(3)

if __name__ == "__main__":
    print("=== BBU NODE STARTED (LEO viswin + IMG) ===")

    threads = [
        threading.Thread(target=tm_receiver, daemon=True),
        threading.Thread(target=tm_server_for_web, daemon=True),
        threading.Thread(target=tc_receiver_from_web, daemon=True),
        threading.Thread(target=tc_sender, daemon=True),
        threading.Thread(target=status_printer, daemon=True),
    ]
    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        running = False
