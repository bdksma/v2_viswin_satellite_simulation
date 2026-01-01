# bbu_node.py
# ==========================================
# SIMULASI BBU NODE (LEO viswin-aware)
# - Receive TM from satellite (UDP)
# - Stream TM to Web (TCP)
# - Receive TC from Web (TCP)
# - Send TC to satellite (UDP) hanya saat visible
# ==========================================

from __future__ import annotations

import socket
import threading
import time
import json
from typing import List, Optional

from orbit_leo import DEFAULT_ORBIT
from rf_channel_leo import propagate

BBU_IP = "127.0.0.1"

# Satellite links
BBU_TM_PORT = 6001     # listen TM from satellite (UDP)
SAT_IP = "127.0.0.1"
SAT_TC_PORT = 5002     # send TC to satellite (UDP)

# Web links
BBU_TC_PORT = 7001      # Web -> BBU (TCP, short connection)
BBU_TM_PORT_WEB = 7002  # BBU -> Web (TCP stream)

running = True
telemetry_live: List[str] = []        # packets during current visibility
telemetry_history: List[str] = []     # last-known / all packets
telecommand_queue: List[str] = []

web_tm_conn: Optional[socket.socket] = None
web_tm_lock = threading.Lock()

def tm_receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((BBU_IP, BBU_TM_PORT))
    print("[BBU] Listening TM from satellite (UDP)")

    while running:
        data, _ = sock.recvfrom(65535)
        raw = data.decode("utf-8", errors="replace")

        telemetry_history.append(raw)
        if len(telemetry_history) > 5000:
            telemetry_history[:] = telemetry_history[-5000:]

        if DEFAULT_ORBIT.is_visible():
            telemetry_live.append(raw)
            if len(telemetry_live) > 2000:
                telemetry_live[:] = telemetry_live[-2000:]
            print("[BBU] TM RX (LIVE)")
        else:
            print("[BBU] TM RX (HIST)")

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
                # pilih apa yang dikirim
                if DEFAULT_ORBIT.is_visible() and telemetry_live:
                    tm = telemetry_live.pop(0)
                    msg = f"LIVE|{tm}"
                elif telemetry_history:
                    tm = telemetry_history[-1]
                    msg = f"HIST|{tm}"
                else:
                    time.sleep(0.5)
                    continue

                try:
                    conn.sendall(msg.encode("utf-8"))
                except Exception:
                    break

                time.sleep(0.2)
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

        # Apply RF model for uplink before sending (optional simulation)
        pkt = {"type": "TC", "cmd": tc, "ts": time.time(), "corrupted": False}
        pkt2 = propagate(pkt, elev_deg=float(st["elev_deg"]), direction="uplink")
        if pkt2 is None:
            print(f"[BBU] TC DROP (RF): {tc}")
            continue
        if pkt2.get("corrupted"):
            print(f"[BBU] TC CORRUPTED -> still sent (channel effect): {tc}")

        sock.sendto(tc.encode("utf-8"), (SAT_IP, SAT_TC_PORT))
        print(f"[BBU] TC SENT to satellite: {tc}")
        time.sleep(0.2)

def status_printer():
    while running:
        st = DEFAULT_ORBIT.get_state()
        print(
            f"[BBU] Visible={st['visible']} elev={st['elev_deg']:.1f}deg "
            f"DL={st['rate_dl_mbps']*1e3:.1f}kbps UL={st['rate_ul_mbps']*1e3:.1f}kbps | "
            f"LIVE={len(telemetry_live)} HIST={len(telemetry_history)} TCQ={len(telecommand_queue)}"
        )
        time.sleep(3)

if __name__ == "__main__":
    print("=== BBU NODE STARTED (LEO viswin) ===")

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
