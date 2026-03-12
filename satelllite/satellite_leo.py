from __future__ import annotations

import base64
import json
import os
import socket
import threading
import time
from typing import Any, Dict, List, Optional, Iterator, Tuple

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
FRAME_BYTES = (2048 * 2048 * 12) // 8  # 12-bit packed, 2048x2048

# Chunking
IMG_CHUNK_BYTES = 6000
SEND_ONLY_FIRST_FRAME = True

# Repetition copies (for robustness)
IMG_REP_COPIES_NOISY = 2  # for IMG
IMG_REP_COPIES_FIXED = 3  # for IMG2

# ECC: XOR parity per group of K chunks
ECC_K = 16  # 16 data chunks + 1 parity
ECC_PARITY_REPS = 2

IMG_CHUNK_DELAY_S = 0.0005
IMG_REP_DELAY_S = 0.0002

_running = True
_tc_queue: List[str] = []
_tc_lock = threading.Lock()
_img_lock = threading.Lock()
_img_sent_once = False


def is_running() -> bool:
    return _running


def stop_running() -> None:
    global _running
    _running = False


def _enqueue_tc(cmd: str) -> None:
    with _tc_lock:
        _tc_queue.append(cmd)


def _dequeue_tc() -> Optional[str]:
    with _tc_lock:
        return _tc_queue.pop(0) if _tc_queue else None


def _iter_frames_from_raw_file(path: str) -> Iterator[Tuple[int, bytes]]:
    with open(path, "rb") as f:
        frame_id = 0
        while True:
            buf = f.read(FRAME_BYTES)
            if not buf:
                break
            if len(buf) < FRAME_BYTES:
                print(
                    f"[SAT] Incomplete trailing frame ignored: "
                    f"frame_id={frame_id} len={len(buf)} expected={FRAME_BYTES}"
                )
                break
            yield frame_id, buf
            frame_id += 1


def _pad_to(data: bytes, n: int) -> bytes:
    if len(data) > n:
        raise ValueError(f"_pad_to: len(data)={len(data)} > target={n}")
    if len(data) == n:
        return data
    return data + (b"\x00" * (n - len(data)))


def _xor_bytes_same_len(a: bytes, b: bytes) -> bytes:
    if len(a) != len(b):
        raise ValueError(
            f"_xor_bytes_same_len: length mismatch {len(a)} != {len(b)}")
    return bytes(x ^ y for x, y in zip(a, b))


def _safe_send_udp(sock: socket.socket, packet: Dict[str, Any]) -> bool:
    try:
        raw = json.dumps(packet, separators=(",", ":")).encode("utf-8")
        sock.sendto(raw, (BBU_IP, BBU_TM_PORT))
        return True
    except Exception as e:
        print(f"[SAT] UDP send failed: {e}")
        return False


def _should_send_image_once() -> bool:
    with _img_lock:
        return not _img_sent_once


def _mark_image_sent_once() -> None:
    global _img_sent_once
    with _img_lock:
        _img_sent_once = True


def send_image_if_needed(
    sock: socket.socket,
    visible: bool,
    rate_dl_mbps: float,
    elev: float,
) -> None:
    if not visible or rate_dl_mbps <= 0.0:
        return

    if not os.path.exists(RAW_FILE_PATH):
        print(f"[SAT] RAW not found: {RAW_FILE_PATH}")
        return

    if SEND_ONLY_FIRST_FRAME and not _should_send_image_once():
        return

    for frame_id, frame in _iter_frames_from_raw_file(RAW_FILE_PATH):
        if not is_running():
            return

        if SEND_ONLY_FIRST_FRAME and frame_id > 0:
            break

        frame_bytes = len(frame)
        chunks = [
            frame[i:i + IMG_CHUNK_BYTES]
            for i in range(0, frame_bytes, IMG_CHUNK_BYTES)
        ]
        total = len(chunks)

        if total == 0:
            print(f"[SAT] Empty frame skipped: frame_id={frame_id}")
            continue

        print(f"[SAT] IMG/IMG2 TX start "
              f"frame={frame_id} chunks={total} elev={elev:.1f}")

        # -------------------------------------------------
        # 1) NOISY stream (IMG)
        # -------------------------------------------------
        for idx, ch in enumerate(chunks):
            if not is_running():
                return

            payload_b64 = base64.b64encode(ch).decode("ascii")
            pkt = {
                "type": "IMG",
                "frame_id": frame_id,
                "chunk_idx": idx,
                "total_chunks": total,
                "frame_bytes": frame_bytes,
                "chunk_len": len(ch),
                "payload_b64": payload_b64,
                "noisy": True,
            }

            if idx == 0 or (idx + 1) % 200 == 0 or (idx + 1) == total:
                print(f"[SAT] IMG progress frame={frame_id} chunk={idx + 1}/{total}")

            for _ in range(IMG_REP_COPIES_NOISY):
                if not is_running():
                    return
                out = propagate(pkt, elev_deg=elev, direction="downlink")
                if out is not None:
                    _safe_send_udp(sock, out)
                time.sleep(IMG_REP_DELAY_S)

            time.sleep(IMG_CHUNK_DELAY_S)

        # -------------------------------------------------
        # 2) FIXED stream (IMG2) + XOR parity ECC
        # -------------------------------------------------
        group_id = 0
        for gstart in range(0, total, ECC_K):
            if not is_running():
                return

            group = chunks[gstart:gstart + ECC_K]
            max_len = max(len(x) for x in group)
            parity = b"\x00" * max_len

            for ch in group:
                parity = _xor_bytes_same_len(parity, _pad_to(ch, max_len))

            # Send data chunks in this ECC group
            for j, ch in enumerate(group):
                if not is_running():
                    return

                idx = gstart + j
                payload_b64 = base64.b64encode(ch).decode("ascii")

                pkt2 = {
                    "type": "IMG2",
                    "frame_id": frame_id,
                    "chunk_idx": idx,
                    "total_chunks": total,
                    "frame_bytes": frame_bytes,
                    "chunk_len": len(ch),
                    "payload_b64": payload_b64,
                    "is_parity": False,
                    "ecc_group": group_id,
                    "ecc_k": ECC_K,
                    "ecc": {
                        "scheme": "xor_parity",
                        "k": ECC_K,
                        "group": group_id,
                    },
                }

                for _ in range(IMG_REP_COPIES_FIXED):
                    if not is_running():
                        return
                    out = propagate(pkt2, elev_deg=elev, direction="downlink")
                    if out is not None:
                        _safe_send_udp(sock, out)
                    time.sleep(IMG_REP_DELAY_S)

                time.sleep(IMG_CHUNK_DELAY_S)

            # Send parity chunk
            parity_b64 = base64.b64encode(parity).decode("ascii")
            pktp = {
                "type": "IMG2",
                "frame_id": frame_id,
                "chunk_idx": -1,  # parity marker
                "total_chunks": total,
                "frame_bytes": frame_bytes,
                "chunk_len": len(parity),
                "payload_b64": parity_b64,
                "is_parity": True,
                "ecc_group": group_id,
                "ecc_k": ECC_K,
                "parity_len": len(parity),
                "ecc": {
                    "scheme": "xor_parity",
                    "k": ECC_K,
                    "group": group_id,
                },
            }

            for _ in range(ECC_PARITY_REPS):
                if not is_running():
                    return
                outp = propagate(pktp, elev_deg=elev, direction="downlink")
                if outp is not None:
                    _safe_send_udp(sock, outp)
                time.sleep(IMG_REP_DELAY_S)

            group_id += 1

        print(f"[SAT] IMG/IMG2 TX done frame={frame_id}")

        if SEND_ONLY_FIRST_FRAME:
            _mark_image_sent_once()
            break


def telemetry_sender() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    seq = 0

    try:
        while is_running():
            try:
                st = DEFAULT_ORBIT.get_state()
                visible = bool(st["visible"])
                elev = float(st["elev_deg"])
                doppler = float(st["doppler_hz"])
                rate_dl_mbps = float(st["rate_dl_mbps"])
            except Exception as e:
                print(f"[SAT] DEFAULT_ORBIT.get_state() failed: {e}")
                time.sleep(TIME_STEP_S)
                continue

            try:
                send_image_if_needed(sock, visible, rate_dl_mbps, elev)
            except Exception as e:
                print(f"[SAT] send_image_if_needed failed: {e}")

            if not visible or rate_dl_mbps <= 0.0:
                time.sleep(TIME_STEP_S)
                continue

            bits_step = rate_dl_mbps * 1e6 * TIME_STEP_S
            max_pkts = int(bits_step // BITS_PER_PACKET)
            max_pkts = max(0, min(max_pkts, MAX_PKTS_PER_STEP))

            for _ in range(max_pkts):
                if not is_running():
                    break

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

                try:
                    tm_out = propagate(
                        tm_packet,
                        elev_deg=elev,
                        direction="downlink",
                    )
                    if tm_out is not None:
                        _safe_send_udp(sock, tm_out)
                except Exception as e:
                    print(f"[SAT] TM propagate/send failed: {e}")

                seq = (seq + 1) & 0xFFFFFFFF

            time.sleep(TIME_STEP_S)

    finally:
        sock.close()
        print("[SAT] telemetry_sender stopped")


def telecommand_receiver() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SAT_IP, SAT_TC_PORT))
    sock.settimeout(1.0)

    print(
        f"[SAT] Telecommand receiver listening UDP on {SAT_IP}:{SAT_TC_PORT}")

    try:
        while is_running():
            try:
                data, _ = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except Exception as e:
                if is_running():
                    print(f"[SAT] TC recv failed: {e}")
                continue

            try:
                cmd = data.decode("utf-8", errors="replace").strip()
                if cmd:
                    _enqueue_tc(cmd)
            except Exception as e:
                print(f"[SAT] TC decode/enqueue failed: {e}")

    finally:
        sock.close()
        print("[SAT] telecommand_receiver stopped")


def telecommand_executor() -> None:
    while is_running():
        try:
            st = DEFAULT_ORBIT.get_state()
            visible = bool(st["visible"])
            elev = float(st["elev_deg"])
        except Exception as e:
            print(f"[SAT] TC executor get_state failed: {e}")
            time.sleep(0.5)
            continue

        if not visible:
            time.sleep(0.5)
            continue

        cmd = _dequeue_tc()
        if cmd is None:
            time.sleep(0.2)
            continue

        pkt = {
            "type": "TC",
            "cmd": cmd,
            "ts": time.time(),
            "corrupted": False,
        }

        try:
            pkt2 = propagate(pkt, elev_deg=elev, direction="uplink")
        except Exception as e:
            print(f"[SAT] TC propagate failed: {e}")
            continue

        if pkt2 is None:
            print(f"[SAT] TC dropped by channel model: {cmd}")
            continue

        if pkt2.get("corrupted"):
            print(f"[SAT] TC corrupted and ignored: {cmd}")
            continue

        print(f"[SAT] TC EXECUTED: {cmd}")

    print("[SAT] telecommand_executor stopped")


def ensure_raw_file() -> bool:
    if os.path.exists(RAW_FILE_PATH) and os.path.getsize(RAW_FILE_PATH) > 1024:
        return True

    file_id = "1QCCcpoYiz-r5q99flKM4Dc5qn3gjia1G"
    url = f"https://drive.google.com/uc?id={file_id}"

    print(f"[SAT] RAW missing. Downloading from Drive -> {RAW_FILE_PATH} ...")

    try:
        import gdown  # type: ignore
    except ImportError:
        print("[SAT] gdown is not installed. Run: pip install gdown")
        return False

    try:
        gdown.download(url, RAW_FILE_PATH, quiet=False, fuzzy=True)
    except Exception as e:
        print(f"[SAT] Download failed: {e}")
        return False

    size = os.path.getsize(RAW_FILE_PATH) if os.path.exists(
        RAW_FILE_PATH) else 0
    ok = os.path.exists(RAW_FILE_PATH) and size > 1024 * 1024

    print(f"[SAT] RAW download ok={ok} size={size}")
    return ok


if __name__ == "__main__":
    print("=== SATELLITE NODE STARTED (LEO viswin + IMG noisy + IMG2 ECC) ===")

    ensure_raw_file()

    size = os.path.getsize(RAW_FILE_PATH) if os.path.exists(
        RAW_FILE_PATH) else 0
    print(
        f"[SAT] RAW_FILE_PATH={RAW_FILE_PATH} exists={os.path.exists(RAW_FILE_PATH)} size={size}"
    )
    print(f"[SAT] IMG_CHUNK_BYTES={IMG_CHUNK_BYTES} ECC_K={ECC_K}")

    threads = [
        threading.Thread(target=telemetry_sender,
                         daemon=True,
                         name="telemetry_sender"),
        threading.Thread(target=telecommand_receiver,
                         daemon=True,
                         name="telecommand_receiver"),
        threading.Thread(target=telecommand_executor,
                         daemon=True,
                         name="telecommand_executor"),
    ]

    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[SAT] KeyboardInterrupt received, shutting down...")
        stop_running()
        time.sleep(1.5)
        print("[SAT] Shutdown complete")
