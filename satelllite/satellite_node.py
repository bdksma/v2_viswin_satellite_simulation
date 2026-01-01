# satellite_node.py
# ==========================================
# SIMULASI SATELLITE NODE (LEO)
# - Generate Telemetry (TM)
# - Apply Doppler (orbit model)
# - Apply RF channel effects
# - Send TM to BBU (UDP)
# - Receive TC from BBU (UDP)
# =========================================

import socket
import time
import math
import threading

from common.orbit import doppler_shift, is_visible
from common.rf_channel import propagate

# ================================
# KONFIGURASI JARINGAN
# ================================
# SAT_IP = "127.0.0.1"

# SAT_TC_PORT = 5002      # TC masuk dari BBU
# BBU_IP = "127.0.0.1"
# BBU_TM_PORT = 6001      # Port TM listener di BBU

# ==========================================
# NETWORK CONFIG
# ==========================================
SAT_IP = "127.0.0.1"
SAT_TC_PORT = 5002          # receive TC from BBU
#SAT_TM_PORT = 5001          # TM keluar ke BBU

BBU_IP = "127.0.0.1"
BBU_TM_PORT = 6001          # send TM to BBU

TM_INTERVAL = 1.0           # seconds
running = True

# ================================
# PARAMETER ORBIT (SIMPLIFIED)
# ================================
# ORBIT_PERIOD = 5400     # detik (90 menit)
# MAX_DOPPLER = 5000      # Hz (contoh LEO)
# TM_INTERVAL = 1.0       # detik

# seq_tm = 0
# running = True

# ================================
# FUNGSI DOPPLER SIMPLIFIED
# ================================
# def compute_doppler(t):
#    """
#    Doppler sinusoidal sederhana
#    Positif saat mendekat, negatif saat menjauh
#    """
#    phase = 2 * math.pi * (t % ORBIT_PERIOD) / ORBIT_PERIOD
#    return int(MAX_DOPPLER * math.sin(phase))

# ================================
# THREAD: KIRIM TELEMETRY
# ================================
def telemetry_sender():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    seq = 0

    while running:
        visible = is_visible()

        tm_packet = {
            "seq": seq,
            "doppler": doppler_shift(),
            "visible": visible,
            "corrupted": False
        }

        if visible:
            tm_packet = propagate(tm_packet)
            if tm_packet:
                sock.sendto(str(tm_packet).encode(), (BBU_IP, BBU_TM_PORT))
                print(f"[SAT] TM SENT: {tm_packet}")
            else:
                print("[SAT] TM LOST (RF)")
        else:
            print("[SAT] TM Generated (NOT Visible)")

        seq += 1
        time.sleep(TM_INTERVAL)

# ================================
# THREAD: TERIMA TELECOMMAND
# ================================
def telecommand_receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SAT_IP, SAT_TC_PORT))

    print("[SAT] Telecommand receiver listening")

    while running:
        data, addr = sock.recvfrom(1024)
        tc = data.decode()

        if is_visible():
            print(f"[SAT] TC EXECUTED: {tc}")
        else:
            print(f"[SAT] TC RECEIVED but NOT VISIBLE: {tc}")

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    print("=== SATELLITE NODE STARTED ===")

    t_tm = threading.Thread(target=telemetry_sender, daemon=True)
    t_tc = threading.Thread(target=telecommand_receiver, daemon=True)

    t_tm.start()
    t_tc.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        running = False
        print("\n[SAT] Shutting down")