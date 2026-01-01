# web_app.py
# ==================================================
# WEB MONITORING (Streamlit client) - unchanged API
# ==================================================
# Jalankan:
#   pip install streamlit
#   streamlit run web_app.py
# ==================================================

import socket
import streamlit as st
import time

BBU_IP = "127.0.0.1"
BBU_TM_PORT = 7002   # BBU -> Web TM (TCP stream)
BBU_TC_PORT = 7001   # Web -> BBU TC (TCP short connection)

st.set_page_config(page_title="Satellite Web Monitor")
st.title("🛰️ Satellite Web Monitoring (LEO viswin)")

# ================= SESSION STATE =================
if "tm_buffer" not in st.session_state:
    st.session_state.tm_buffer = []

if "tm_socket" not in st.session_state:
    st.session_state.tm_socket = None
    st.session_state.connected = False

# ================= CONNECT TO BBU =================
if not st.session_state.connected:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((BBU_IP, BBU_TM_PORT))
        s.setblocking(False)

        st.session_state.tm_socket = s
        st.session_state.connected = True
        st.success("Connected to BBU TM stream")
    except Exception:
        st.warning("Waiting for BBU TM server...")
        time.sleep(1)
        st.rerun()

# ================= RECEIVE TM =================
if st.session_state.connected:
    try:
        data = st.session_state.tm_socket.recv(65535)
        if data:
            msg = data.decode(errors="replace")
            if "|" in msg:
                mode, tm = msg.split("|", 1)
            else:
                mode, tm = "UNK", msg

            st.session_state.tm_buffer.append((mode, tm))
            st.session_state.tm_buffer = st.session_state.tm_buffer[-200:]
    except BlockingIOError:
        pass
    except Exception:
        st.session_state.connected = False
        st.session_state.tm_socket = None

# ================= SEND TC =================
st.subheader("📡 Telecommand (uplink)")

tc = st.text_input("Command", "PING")
if st.button("Send TC"):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((BBU_IP, BBU_TC_PORT))
        s.sendall(tc.encode())
        s.close()
        st.success("TC sent to BBU (queued)")
    except Exception as e:
        st.error(f"TC failed: {e}")

# ================= DISPLAY TM =================
st.subheader("📈 Telemetry (downlink)")

if st.session_state.tm_buffer:
    # show last 20
    for mode, tm in reversed(st.session_state.tm_buffer[-20:]):
        if mode == "LIVE":
            st.success(f"🟢 LIVE  {tm[:300]}")
        elif mode == "HIST":
            st.info(f"🕒 HIST  {tm[:300]}")
        else:
            st.write(tm[:300])
else:
    st.info("Waiting telemetry...")

time.sleep(0.5)
st.rerun()
