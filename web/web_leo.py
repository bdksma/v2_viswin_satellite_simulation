# web_app.py
import os
import socket
import time
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
if "latest_image" not in st.session_state:
    st.session_state.latest_image = {"frame_id": None, "png": None, "tif": None}

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

    if payload.startswith("IMG|"):
        parts = payload.split("|")
        if len(parts) >= 4:
            try:
                frame_id = int(parts[1])
            except Exception:
                frame_id = None
            png_path = parts[2]
            tif_path = parts[3]
            st.session_state.latest_image = {"frame_id": frame_id, "png": png_path, "tif": tif_path}
            st.session_state.tm_buffer.append(("IMG", payload))
            st.session_state.tm_buffer = st.session_state.tm_buffer[-200:]
        return

    st.session_state.tm_buffer.append((mode, payload))
    st.session_state.tm_buffer = st.session_state.tm_buffer[-200:]

if st.session_state.connected:
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

col_left, col_right = st.columns([1, 1])

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
    st.subheader("🖼 Latest Image (decoded)")
    li = st.session_state.latest_image
    if li["png"]:
        if os.path.exists(li["png"]):
            st.write(f"Frame ID: **{li['frame_id']}**")
            st.image(li["png"], caption=os.path.basename(li["png"]), use_container_width=True)
            st.caption(f"TIFF saved at: {li['tif']}")
        else:
            st.warning(f"PNG path not found yet: {li['png']}")
    else:
        st.info("No decoded image yet. Waiting for IMG frame...")

st.subheader("📈 Telemetry (downlink)")
if st.session_state.tm_buffer:
    for mode, payload in reversed(st.session_state.tm_buffer[-30:]):
        preview = payload[:350]
        if mode == "LIVE":
            st.success(f"🟢 LIVE  {preview}")
        elif mode == "HIST":
            st.info(f"🕒 HIST  {preview}")
        elif mode == "IMG":
            st.warning(f"🖼 IMG  {preview}")
        else:
            st.write(preview)
else:
    st.info("Waiting telemetry...")

time.sleep(0.3)
st.rerun()
