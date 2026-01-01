# ==========================================
# SIMULASI BBU NODE (FINAL STABLE)
# ==========================================

import socket
import threading
import time
import random

from common.orbit import is_visible

BBU_IP = "127.0.0.1"

# Satellite
BBU_TM_PORT = 6001
SAT_IP = "127.0.0.1"
SAT_TC_PORT = 5002

# Web
BBU_TC_PORT = 7001   # Web -> BBU (TC)
BBU_TM_PORT_WEB = 7002  # BBU -> Web (TM)


PACKET_LOSS_PROB = 0.1
PROP_DELAY = 0.2

telemetry_live = []        # current pass
telemetry_history = []    # past passes
telecommand_queue = []
visible = False
running = True
web_tm_conn = None

def visibility_manager():
    global visible
    while running:
        visible = is_visible()
        time.sleep(1)

# ==========================================
def tm_receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((BBU_IP, BBU_TM_PORT))
    print("[BBU] Listening TM from satellite")

    while running:
        data, _ = sock.recvfrom(1024)
        tm = data.decode()

        telemetry_history.append(tm)

        if visible:
            telemetry_live.append(tm)
            print(f"[BBU] TM RX (LIVE): {tm}")
        else:
            print(f"[BBU] TM RX (HIST): {tm}")


# ==========================================
def tm_server_for_web():
    global web_tm_conn
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((BBU_IP, BBU_TM_PORT_WEB))
    server.listen(1)

    print("[BBU] TM server for Web listening on 7002")
    web_tm_conn, _ = server.accept()
    print("[BBU] Web connected for TM")

    while running:
        if visible and telemetry_live:
            tm = telemetry_live.pop(0)
            msg = f"LIVE|{tm}"
        elif telemetry_history:
            tm = telemetry_history[-1]   # last known
            msg = f"HIST|{tm}"
        else:
            time.sleep(1)
            continue

        try:
            web_tm_conn.sendall(msg.encode())
            print(f"[BBU] TM sent to Web: {msg}")
        except:
            print("[BBU] Web disconnected")
            break

        time.sleep(1)

# ==========================================
def tc_sender():
    global visible
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    print("[BBU] TC sender started")

    while running:
        if not telecommand_queue:
            time.sleep(0.5)
            continue

        if not visible:
            print("[BBU] TC queued, waiting visibility")
            time.sleep(1)
            continue

        # === VISIBILITY TRUE ===
        tc = telecommand_queue.pop(0)
        sock.sendto(tc.encode(), (SAT_IP, SAT_TC_PORT))
        print(f"[BBU] TC SENT to satellite: {tc}")

        time.sleep(1)

# ==========================================
def tc_receiver_from_web():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((BBU_IP, BBU_TC_PORT))
    sock.listen(1)
    print("[BBU] Waiting TC from Web")

    while running:
        conn, _ = sock.accept()
        tc = conn.recv(1024).decode()
        telecommand_queue.append(tc)
        print(f"[BBU] TC queued from Web: {tc}")
        conn.close()

# ==========================================
if __name__ == "__main__":
    print("=== BBU NODE STARTED ===")

    threads = [
        threading.Thread(target=visibility_manager, daemon=True),
        threading.Thread(target=tm_receiver, daemon=True),
        threading.Thread(target=tm_server_for_web, daemon=True),
        threading.Thread(target=tc_sender, daemon=True),
        threading.Thread(target=tc_receiver_from_web, daemon=True),
    ]

    for t in threads:
        t.start()

    try:
        while True:
            print(
                    f"[BBU] Visible={is_visible()} | "
                    f"LIVE={len(telemetry_live)} | "
                    f"HIST={len(telemetry_history)} | "
                    f"TC={len(telecommand_queue)}"
                )
            time.sleep(3)
    except KeyboardInterrupt:
        running = False
