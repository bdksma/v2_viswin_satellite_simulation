# web_app.py
from __future__ import annotations

import os
import socket
import time
from typing import Optional, Tuple

import streamlit as st

BBU_IP = "127.0.0.1"
BBU_TM_PORT = 7002
BBU_TC_PORT = 7001

MAX_TM_BUFFER = 300
SOCKET_CONNECT_TIMEOUT = 2.0
UI_REFRESH_S = 0.3

st.set_page_config(page_title="Satellite Web Monitor", layout="wide")
st.title("🛰️ LEO Test Satellite Web Monitoring")


# ---------------------------
# Session state init
# ---------------------------
def init_state() -> None:
    if "tm_buffer" not in st.session_state:
        st.session_state.tm_buffer = []

    if "tm_socket" not in st.session_state:
        st.session_state.tm_socket = None

    if "connected" not in st.session_state:
        st.session_state.connected = False

    if "rx_buf" not in st.session_state:
        st.session_state.rx_buf = ""

    if "last_rx_time" not in st.session_state:
        st.session_state.last_rx_time = None

    if "latest" not in st.session_state:
        st.session_state.latest = {
            "frame_id": None,
            "noisy_png": None,
            "fixed_png": None,
            "noisy_tif": None,
            "fixed_tif": None,
        }


init_state()


# ---------------------------
# Socket helpers
# ---------------------------
def close_tm_socket() -> None:
    sock = st.session_state.tm_socket
    if sock is not None:
        try:
            sock.close()
        except Exception:
            pass
    st.session_state.tm_socket = None
    st.session_state.connected = False


def connect_tm_socket() -> bool:
    close_tm_socket()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(SOCKET_CONNECT_TIMEOUT)
        s.connect((BBU_IP, BBU_TM_PORT))
        s.setblocking(False)

        st.session_state.tm_socket = s
        st.session_state.connected = True
        return True
    except Exception:
        close_tm_socket()
        return False


# ---------------------------
# Buffer helpers
# ---------------------------
def push_tm(mode: str, payload: str) -> None:
    st.session_state.tm_buffer.append(
        (mode, payload, time.strftime("%H:%M:%S")))
    if len(st.session_state.tm_buffer) > MAX_TM_BUFFER:
        st.session_state.tm_buffer = st.session_state.tm_buffer[
            -MAX_TM_BUFFER:]


def parse_stream_line(line: str) -> Tuple[str, str]:
    line = line.strip()
    if not line:
        return "EMPTY", ""
    if "|" in line:
        mode, payload = line.split("|", 1)
        return mode.strip().upper(), payload
    return "UNK", line


def handle_line(line: str) -> None:
    mode, payload = parse_stream_line(line)

    if not payload:
        return

    # Format dari BBU:
    # HIST|IMG2|frame_id|noisy_png|fixed_png|noisy_tif|fixed_tif
    if payload.startswith("IMG2|"):
        parts = payload.split("|")
        if len(parts) >= 6:
            try:
                st.session_state.latest = {
                    "frame_id": int(parts[1]),
                    "noisy_png": parts[2],
                    "fixed_png": parts[3],
                    "noisy_tif": parts[4],
                    "fixed_tif": parts[5],
                }
                push_tm("IMG2", payload)
            except Exception as e:
                push_tm(
                    "ERR",
                    f"Failed to parse IMG2 payload: {e} | {payload[:200]}")
        else:
            push_tm("ERR", f"Malformed IMG2 payload: {payload[:200]}")
        return

    push_tm(mode, payload)


def read_available_stream() -> None:
    sock = st.session_state.tm_socket
    if sock is None:
        return

    while True:
        try:
            data = sock.recv(65535)
            if not data:
                raise ConnectionError("BBU closed connection")

            st.session_state.last_rx_time = time.time()
            st.session_state.rx_buf += data.decode("utf-8", errors="replace")

            while "\n" in st.session_state.rx_buf:
                line, st.session_state.rx_buf = st.session_state.rx_buf.split(
                    "\n", 1)
                handle_line(line)

        except BlockingIOError:
            break
        except Exception:
            close_tm_socket()
            break


# ---------------------------
# Initial / reconnect logic
# ---------------------------
if not st.session_state.connected:
    ok = connect_tm_socket()
    if ok:
        st.success("Connected to BBU TM stream")
    else:
        st.warning("Waiting for BBU TM server...")
        time.sleep(1.0)
        st.rerun()

read_available_stream()

# ---------------------------
# Status bar
# ---------------------------
status_col1, status_col2, status_col3, status_col4 = st.columns(4)

with status_col1:
    st.metric("TM Connection",
              "Connected" if st.session_state.connected else "Disconnected")

with status_col2:
    st.metric("Buffered Messages", len(st.session_state.tm_buffer))

with status_col3:
    latest_frame = st.session_state.latest.get("frame_id")
    st.metric("Latest Frame",
              "-" if latest_frame is None else str(latest_frame))

with status_col4:
    if st.session_state.last_rx_time is None:
        last_rx = "-"
    else:
        last_rx = time.strftime("%H:%M:%S",
                                time.localtime(st.session_state.last_rx_time))
    st.metric("Last RX", last_rx)

# ---------------------------
# Main layout
# ---------------------------
col_left, col_right = st.columns([1, 2])

with col_left:
    st.subheader("📡 Telecommand (uplink)")

    tc = st.text_input("Command", "PING")
    tc_clean = tc.strip()

    btn_col1, btn_col2 = st.columns(2)

    with btn_col1:
        if st.button("Send TC", use_container_width=True):
            if not tc_clean:
                st.error("Command kosong.")
            elif len(tc_clean) > 1024:
                st.error("Command terlalu panjang.")
            else:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2.0)
                    s.connect((BBU_IP, BBU_TC_PORT))
                    s.sendall(tc_clean.encode("utf-8"))
                    s.close()
                    st.success("TC sent to BBU queue")
                except Exception as e:
                    st.error(f"TC failed: {e}")

    with btn_col2:
        if st.button("Reconnect TM", use_container_width=True):
            ok = connect_tm_socket()
            if ok:
                st.success("Reconnected to BBU TM stream")
            else:
                st.error("Reconnect failed")

    st.markdown("---")
    st.subheader("ℹ️ Link Status")
    if st.session_state.connected:
        st.success("TM stream aktif")
    else:
        st.error("TM stream putus")

    li = st.session_state.latest
    st.write(f"Frame terakhir: **{li.get('frame_id')}**")

with col_right:
    st.subheader("🖼 Latest Image (Noisy vs Fixed)")
    li = st.session_state.latest

    noisy_png = li.get("noisy_png")
    fixed_png = li.get("fixed_png")

    noisy_ok = bool(noisy_png and os.path.exists(noisy_png))
    fixed_ok = bool(fixed_png and os.path.exists(fixed_png))

    if noisy_ok and fixed_ok:
        st.write(f"Frame ID: **{li['frame_id']}**")
        c1, c2 = st.columns(2)

        with c1:
            st.caption("Noisy (before correction)")
            st.image(noisy_png, use_container_width=True)

        with c2:
            st.caption("Fixed (after correction / ECC path)")
            st.image(fixed_png, use_container_width=True)

        dl1, dl2 = st.columns(2)
        with dl1:
            if li.get("noisy_tif") and os.path.exists(li["noisy_tif"]):
                with open(li["noisy_tif"], "rb") as f:
                    st.download_button(
                        "Download Noisy TIF",
                        data=f,
                        file_name=os.path.basename(li["noisy_tif"]),
                        use_container_width=True,
                    )

        with dl2:
            if li.get("fixed_tif") and os.path.exists(li["fixed_tif"]):
                with open(li["fixed_tif"], "rb") as f:
                    st.download_button(
                        "Download Fixed TIF",
                        data=f,
                        file_name=os.path.basename(li["fixed_tif"]),
                        use_container_width=True,
                    )
    else:
        st.info("No decoded image yet. Waiting for IMG/IMG2 frame...")

# ---------------------------
# Telemetry viewer
# ---------------------------
st.subheader("📈 Telemetry (downlink)")

if st.session_state.tm_buffer:
    for mode, payload, ts in reversed(st.session_state.tm_buffer[-25:]):
        text = f"[{ts}] {payload[:300]}"

        if mode == "LIVE":
            st.success(f"🟢 LIVE  {text}")
        elif mode == "HIST":
            st.info(f"🕒 HIST  {text}")
        elif mode == "IMG2":
            st.warning(f"🖼 IMG2  {text}")
        elif mode == "ERR":
            st.error(text)
        else:
            st.write(text)
else:
    st.info("Waiting telemetry...")

time.sleep(UI_REFRESH_S)
st.rerun()
