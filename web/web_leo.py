# web_app.py (show two images noisy vs fixed)
import os, socket, time
import streamlit as st

BBU_IP = "127.0.0.1"
BBU_TM_PORT = 7002
BBU_TC_PORT = 7001

st.set_page_config(page_title="Satellite Web Monitor", layout="wide")
st.title("🛰️ LEO Satellite Web Monitoring")

if "tm_buffer" not in st.session_state:
    st.session_state.tm_buffer = []
if "tm_socket" not in st.session_state:
    st.session_state.tm_socket = None
    st.session_state.connected = False
if "rx_buf" not in st.session_state:
    st.session_state.rx_buf = ""

if "latest" not in st.session_state:
    st.session_state.latest = {
        "frame_id": None,
        "noisy_png": None,
        "fixed_png": None,
        "noisy_tif": None,
        "fixed_tif": None,
    }

if not st.session_state.connected:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((BBU_IP, BBU_TM_PORT))
        s.setblocking(False)
        st.session_state.tm_socket = s
        st.session_state.connected = True
        st.success("Connected to BBU TM stream")
    except Exception:
        st.warning("Waiting for BBU TM server...")
        time.sleep(1)
        st.rerun()

def handle_line(line: str):
    line = line.strip()
    if not line:
        return
    if "|" in line:
        mode, payload = line.split("|", 1)
    else:
        mode, payload = "UNK", line

    # IMG2 message
    if payload.startswith("IMG2|"):
        parts = payload.split("|")
        # IMG2|frame_id|noisy_png|fixed_png|noisy_tif|fixed_tif
        if len(parts) >= 6:
            st.session_state.latest = {
                "frame_id": int(parts[1]),
                "noisy_png": parts[2],
                "fixed_png": parts[3],
                "noisy_tif": parts[4],
                "fixed_tif": parts[5],
            }
            st.session_state.tm_buffer.append(("IMG2", payload))
            st.session_state.tm_buffer = st.session_state.tm_buffer[-200:]
        return

    st.session_state.tm_buffer.append((mode, payload))
    st.session_state.tm_buffer = st.session_state.tm_buffer[-200:]

# receive stream
try:
    data = st.session_state.tm_socket.recv(65535)
    if data:
        st.session_state.rx_buf += data.decode(errors="replace")
        while "\n" in st.session_state.rx_buf:
            line, st.session_state.rx_buf = st.session_state.rx_buf.split("\n", 1)
            handle_line(line)
except BlockingIOError:
    pass
except Exception:
    st.session_state.connected = False
    st.session_state.tm_socket = None

col_left, col_right = st.columns([1, 2])

with col_left:
    st.subheader("📡 Telecommand (uplink)")
    tc = st.text_input("Command", "PING")
    if st.button("Send TC"):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((BBU_IP, BBU_TC_PORT))
            s.sendall(tc.encode("utf-8"))
            s.close()
            st.success("TC sent to BBU (queued)")
        except Exception as e:
            st.error(f"TC failed: {e}")

with col_right:
    st.subheader("🖼 Latest Image (Noisy vs Fixed)")
    li = st.session_state.latest
    if li["noisy_png"] and os.path.exists(li["noisy_png"]) and li["fixed_png"] and os.path.exists(li["fixed_png"]):
        st.write(f"Frame ID: **{li['frame_id']}**")
        c1, c2 = st.columns(2)
        with c1:
            st.caption("Noisy (before filter/amplifier)")
            st.image(li["noisy_png"], use_container_width=True)
        with c2:
            st.caption("Fixed (after filter/amplifier)")
            st.image(li["fixed_png"], use_container_width=True)
    else:
        st.info("No decoded image yet. Waiting for IMG2 frame...")

st.subheader("📈 Telemetry (downlink)")
if st.session_state.tm_buffer:
    for mode, payload in reversed(st.session_state.tm_buffer[-20:]):
        if mode == "LIVE":
            st.success(f"🟢 LIVE  {payload[:250]}")
        elif mode == "HIST":
            st.info(f"🕒 HIST  {payload[:250]}")
        elif mode == "IMG2":
            st.warning(f"🖼 IMG2  {payload[:250]}")
        else:
            st.write(payload[:250])
else:
    st.info("Waiting telemetry...")

time.sleep(0.3)
st.rerun()