from __future__ import annotations

import base64
import json
import os
import socket
import threading
import time
from typing import Dict, List, Optional, Tuple, Any

from common.orbit_leo import DEFAULT_ORBIT
from common.rf_channel_leo import propagate
from common.raw_to_image import decode_one_frame, save_png_tiff

BBU_IP = "127.0.0.1"
BBU_TM_PORT = 6001
SAT_IP = "127.0.0.1"
SAT_TC_PORT = 5002
BBU_TC_PORT = 7001
BBU_TM_PORT_WEB = 7002

IMG_OUT_DIR = "output_images"
os.makedirs(IMG_OUT_DIR, exist_ok=True)

_running = True

telemetry_live: List[str] = []
telemetry_history: List[str] = []
telecommand_queue: List[str] = []

telemetry_lock = threading.Lock()
history_lock = threading.Lock()
tc_lock = threading.Lock()
latest_lock = threading.Lock()


def is_running() -> bool:
    return _running


def stop_running() -> None:
    global _running
    _running = False


def _xor_bytes_same_len(a: bytes, b: bytes) -> bytes:
    if len(a) != len(b):
        raise ValueError(f"XOR length mismatch: {len(a)} != {len(b)}")
    return bytes(x ^ y for x, y in zip(a, b))


def _pad_to(data: bytes, n: int) -> bytes:
    if len(data) > n:
        raise ValueError(f"_pad_to: len(data)={len(data)} > target={n}")
    if len(data) == n:
        return data
    return data + (b"\x00" * (n - len(data)))


def majority_vote_bytes(copies: List[bytes]) -> bytes:
    if not copies:
        return b""
    if len(copies) == 1:
        return copies[0]

    m = min(len(c) for c in copies)
    trimmed = [c[:m] for c in copies]

    out = bytearray(m)
    for i in range(m):
        freq: Dict[int, int] = {}
        for c in trimmed:
            b = c[i]
            freq[b] = freq.get(b, 0) + 1
        out[i] = max(freq.items(), key=lambda x: x[1])[0]
    return bytes(out)


def _enqueue_history(msg: str) -> None:
    with history_lock:
        telemetry_history.append(msg)
        if len(telemetry_history) > 5000:
            telemetry_history[:] = telemetry_history[-5000:]


def _enqueue_live(msg: str) -> None:
    with telemetry_lock:
        telemetry_live.append(msg)
        if len(telemetry_live) > 2000:
            telemetry_live[:] = telemetry_live[-2000:]


def _dequeue_live() -> Optional[str]:
    with telemetry_lock:
        if telemetry_live:
            return telemetry_live.pop(0)
        return None


def _get_last_history() -> Optional[str]:
    with history_lock:
        if telemetry_history:
            return telemetry_history[-1]
        return None


def _enqueue_tc(tc: str) -> None:
    with tc_lock:
        telecommand_queue.append(tc)


def _dequeue_tc() -> Optional[str]:
    with tc_lock:
        if telecommand_queue:
            return telecommand_queue.pop(0)
        return None


# -------------------- Reassembler for NOISY IMG --------------------
class ImgNoisyReasm:
    """
    Sinkron dengan SAT versi sekarang:
    - tidak pakai 'last'
    - selesai jika jumlah chunk == total_chunks
    - kalau ada chunk hilang dan timeout, boleh finalize dengan zero-fill
    """

    def __init__(self):
        self.frames: Dict[int, Dict[str, Any]] = {}
        self.lock = threading.Lock()

    def push(self, pkt: dict) -> Optional[Tuple[int, bytes]]:
        try:
            frame_id = int(pkt["frame_id"])
            idx = int(pkt["chunk_idx"])
            total_chunks = int(pkt["total_chunks"])
            frame_bytes = int(pkt["frame_bytes"])
            chunk_len = int(pkt.get("chunk_len", 0))
            data = base64.b64decode(pkt["payload_b64"], validate=False)
        except Exception:
            return None

        with self.lock:
            st = self.frames.setdefault(
                frame_id,
                {
                    "chunks": {},  # idx -> bytes
                    "total_chunks": total_chunks,
                    "frame_bytes": frame_bytes,
                    "chunk_len_hint": chunk_len,
                    "t0": time.time(),
                    "last_update": time.time(),
                },
            )

            st["last_update"] = time.time()
            st["total_chunks"] = total_chunks
            st["frame_bytes"] = frame_bytes
            if chunk_len > 0:
                st["chunk_len_hint"] = chunk_len

            # noisy stream: keep first chunk only
            if idx not in st["chunks"]:
                st["chunks"][idx] = data

            if len(st["chunks"]) >= st["total_chunks"]:
                raw = self._build_frame(st, allow_missing=False)
                del self.frames[frame_id]
                return frame_id, raw

        return None

    def _build_frame(self, st: dict, allow_missing: bool) -> bytes:
        total_chunks = int(st["total_chunks"])
        frame_bytes = int(st["frame_bytes"])
        chunk_len_hint = int(st.get("chunk_len_hint", 0))

        if chunk_len_hint <= 0:
            any_chunk = next(iter(st["chunks"].values()), b"")
            chunk_len_hint = len(any_chunk) if any_chunk else 1

        parts: List[bytes] = []
        for i in range(total_chunks):
            if i in st["chunks"]:
                parts.append(st["chunks"][i])
            else:
                if not allow_missing:
                    raise ValueError("Missing chunk in strict build")
                parts.append(b"\x00" * chunk_len_hint)

        return b"".join(parts)[:frame_bytes]

    def cleanup(self, max_age_s: float = 120.0) -> List[Tuple[int, bytes]]:
        """
        Untuk noisy IMG:
        kalau frame terlalu lama tapi belum lengkap, finalize pakai zero-fill
        """
        now = time.time()
        completed: List[Tuple[int, bytes]] = []

        with self.lock:
            drop_ids = []
            for frame_id, st in self.frames.items():
                age = now - st.get("last_update", now)
                if age > max_age_s:
                    try:
                        raw = self._build_frame(st, allow_missing=True)
                        completed.append((frame_id, raw))
                    except Exception as e:
                        print(
                            f"[BBU] IMG cleanup build failed frame={frame_id}: {e}"
                        )
                    drop_ids.append(frame_id)

            for frame_id in drop_ids:
                del self.frames[frame_id]

        return completed


# -------------------- Reassembler for FIXED IMG2 + ECC --------------------
class ImgFixedReasmECC:
    """
    Sinkron dengan SAT versi sekarang:
    - data chunk tidak punya rep index
    - duplicate chunk: simpan beberapa copy per idx
    - parity chunk: simpan beberapa copy per group
    - complete jika semua chunk 0..total_chunks-1 tersedia
    - jika tepat 1 chunk hilang per group dan parity ada -> recover
    """

    def __init__(self):
        self.frames: Dict[int, Dict[str, Any]] = {}
        self.lock = threading.Lock()

    def push(self, pkt: dict) -> Optional[Tuple[int, bytes]]:
        try:
            frame_id = int(pkt["frame_id"])
            idx = int(pkt["chunk_idx"])
            total_chunks = int(pkt["total_chunks"])
            frame_bytes = int(pkt["frame_bytes"])
            is_parity = bool(pkt.get("is_parity", False))
            group_id = int(pkt.get("ecc_group", -1))
            ecc_k = int(pkt.get("ecc_k", pkt.get("ecc", {}).get("k", 16)))
            chunk_len = int(pkt.get("chunk_len", pkt.get("parity_len", 0)))
            data = base64.b64decode(pkt["payload_b64"], validate=False)
        except Exception:
            return None

        with self.lock:
            st = self.frames.setdefault(
                frame_id,
                {
                    "chunks": {},  # idx -> list[bytes]
                    "parity": {},  # group_id -> list[bytes]
                    "total_chunks": total_chunks,
                    "frame_bytes": frame_bytes,
                    "ecc_k": ecc_k,
                    "chunk_len_hint": chunk_len,
                    "t0": time.time(),
                    "last_update": time.time(),
                },
            )

            st["last_update"] = time.time()
            st["total_chunks"] = total_chunks
            st["frame_bytes"] = frame_bytes
            st["ecc_k"] = ecc_k
            if chunk_len > 0:
                st["chunk_len_hint"] = chunk_len

            if is_parity:
                st["parity"].setdefault(group_id, []).append(data)
                if len(st["parity"][group_id]) > 4:
                    st["parity"][group_id] = st["parity"][group_id][-4:]
            else:
                st["chunks"].setdefault(idx, []).append(data)
                if len(st["chunks"][idx]) > 4:
                    st["chunks"][idx] = st["chunks"][idx][-4:]

            self._attempt_ecc_recover(st)

            if self._is_complete(st):
                raw = self._build_frame(st)
                del self.frames[frame_id]
                return frame_id, raw

        return None

    def _is_complete(self, st: dict) -> bool:
        total_chunks = int(st["total_chunks"])
        for i in range(total_chunks):
            if i not in st["chunks"] or not st["chunks"][i]:
                return False
        return True

    def _get_chunk_majority(self, copies: List[bytes]) -> bytes:
        return majority_vote_bytes(copies)

    def _attempt_ecc_recover(self, st: dict) -> None:
        total_chunks = int(st["total_chunks"])
        k = int(st.get("ecc_k", 16))

        for group_id, gstart in enumerate(range(0, total_chunks, k)):
            gend = min(gstart + k, total_chunks)
            idxs = list(range(gstart, gend))
            missing = [
                i for i in idxs if i not in st["chunks"] or not st["chunks"][i]
            ]

            if len(missing) != 1:
                continue

            parity_copies = st["parity"].get(group_id)
            if not parity_copies:
                continue

            parity = self._get_chunk_majority(parity_copies)

            known_datas: List[bytes] = []
            max_len = len(parity)

            for i in idxs:
                if i == missing[0]:
                    continue
                data_i = self._get_chunk_majority(st["chunks"][i])
                known_datas.append(data_i)
                if len(data_i) > max_len:
                    max_len = len(data_i)

            try:
                acc = _pad_to(parity, max_len)
                for data_i in known_datas:
                    acc = _xor_bytes_same_len(acc, _pad_to(data_i, max_len))
                recovered = acc
                st["chunks"][missing[0]] = [recovered]
                print(
                    f"[BBU] ECC recovered missing chunk idx={missing[0]} group={group_id}"
                )
            except Exception as e:
                print(f"[BBU] ECC recovery failed group={group_id}: {e}")

    def _build_frame(self, st: dict) -> bytes:
        total_chunks = int(st["total_chunks"])
        frame_bytes = int(st["frame_bytes"])

        parts: List[bytes] = []
        for i in range(total_chunks):
            data_i = self._get_chunk_majority(st["chunks"][i])
            parts.append(data_i)

        return b"".join(parts)[:frame_bytes]

    def cleanup(self, max_age_s: float = 900.0) -> None:
        now = time.time()
        with self.lock:
            drop = [
                fid for fid, st in self.frames.items()
                if (now - st.get("last_update", now)) > max_age_s
            ]
            for fid in drop:
                print(f"[BBU] IMG2 drop incomplete frame={fid} (timeout)")
                del self.frames[fid]


noisy_reasm = ImgNoisyReasm()
fixed_reasm = ImgFixedReasmECC()

latest = {
    "frame_id": None,
    "noisy_png": None,
    "fixed_png": None,
    "noisy_tif": None,
    "fixed_tif": None,
}


def _update_latest(**kwargs) -> None:
    with latest_lock:
        latest.update(kwargs)


def _get_latest_snapshot() -> dict:
    with latest_lock:
        return dict(latest)


def _handle_noisy_complete(frame_id: int, raw_noisy: bytes) -> None:
    try:
        img_noisy = decode_one_frame(raw_noisy, 2048, 2048)
        noisy_png = os.path.join(IMG_OUT_DIR,
                                 f"frame_{frame_id:05d}_noisy.png")
        noisy_tif = os.path.join(IMG_OUT_DIR,
                                 f"frame_{frame_id:05d}_noisy.tif")
        save_png_tiff(img_noisy, noisy_png, noisy_tif)

        _update_latest(
            frame_id=frame_id,
            noisy_png=noisy_png,
            noisy_tif=noisy_tif,
        )
        print(f"[BBU] NOISY saved: {noisy_png}")

    except Exception as e:
        print(f"[BBU] NOISY decode failed frame={frame_id}: {e}")


def _handle_fixed_complete(frame_id: int, raw_fixed: bytes) -> None:
    try:
        img_fixed = decode_one_frame(raw_fixed, 2048, 2048)
        fixed_png = os.path.join(IMG_OUT_DIR,
                                 f"frame_{frame_id:05d}_fixed.png")
        fixed_tif = os.path.join(IMG_OUT_DIR,
                                 f"frame_{frame_id:05d}_fixed.tif")
        save_png_tiff(img_fixed, fixed_png, fixed_tif)

        _update_latest(
            frame_id=frame_id,
            fixed_png=fixed_png,
            fixed_tif=fixed_tif,
        )

        print(f"[BBU] FIXED saved: {fixed_png}")

        snap = _get_latest_snapshot()
        if (snap.get("noisy_png") and snap.get("fixed_png")
                and snap.get("frame_id") == frame_id):
            _enqueue_history(
                f"IMG2|{frame_id}|{snap['noisy_png']}|{snap['fixed_png']}|"
                f"{snap['noisy_tif']}|{snap['fixed_tif']}")

    except Exception as e:
        print(f"[BBU] FIXED decode failed frame={frame_id}: {e}")


def tm_receiver() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((BBU_IP, BBU_TM_PORT))
    sock.settimeout(1.0)

    print(
        f"[BBU] Listening TM/IMG/IMG2 from satellite UDP on {BBU_IP}:{BBU_TM_PORT}"
    )

    try:
        while is_running():
            try:
                data, _ = sock.recvfrom(65535)
            except socket.timeout:
                # cleanup berkala
                for frame_id, raw_noisy in noisy_reasm.cleanup(120.0):
                    _handle_noisy_complete(frame_id, raw_noisy)
                fixed_reasm.cleanup(900.0)
                continue
            except Exception as e:
                if is_running():
                    print(f"[BBU] UDP recv failed: {e}")
                continue

            raw_text = data.decode("utf-8", errors="replace").strip()

            try:
                pkt = json.loads(raw_text)
                pkt_type = str(pkt.get("type", "")).upper()
            except Exception:
                pkt = None
                pkt_type = ""

            if pkt_type == "IMG" and pkt is not None:
                res = noisy_reasm.push(pkt)
                if res is not None:
                    frame_id, raw_noisy = res
                    _handle_noisy_complete(frame_id, raw_noisy)
                continue

            if pkt_type == "IMG2" and pkt is not None:
                res2 = fixed_reasm.push(pkt)
                if res2 is not None:
                    frame_id, raw_fixed = res2
                    _handle_fixed_complete(frame_id, raw_fixed)
                else:
                    fixed_reasm.cleanup(900.0)
                continue

            _enqueue_history(raw_text)
            if DEFAULT_ORBIT.is_visible():
                _enqueue_live(raw_text)

    finally:
        sock.close()
        print("[BBU] tm_receiver stopped")


def tm_server_for_web() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((BBU_IP, BBU_TM_PORT_WEB))
    server.listen(5)
    server.settimeout(1.0)

    print(f"[BBU] TM server for Web listening on {BBU_IP}:{BBU_TM_PORT_WEB}")

    try:
        while is_running():
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            except Exception as e:
                if is_running():
                    print(f"[BBU] Web accept failed: {e}")
                continue

            print("[BBU] Web connected for TM")

            try:
                conn.settimeout(1.0)
                while is_running():
                    msg: Optional[str] = None

                    if DEFAULT_ORBIT.is_visible():
                        tm = _dequeue_live()
                        if tm is not None:
                            msg = f"LIVE|{tm}"

                    if msg is None:
                        hist = _get_last_history()
                        if hist is not None:
                            msg = f"HIST|{hist}"

                    if msg is None:
                        time.sleep(0.2)
                        continue

                    conn.sendall((msg + "\n").encode("utf-8"))
                    time.sleep(0.05)

            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
                print("[BBU] Web disconnected")

    finally:
        server.close()
        print("[BBU] tm_server_for_web stopped")


def tc_receiver_from_web() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((BBU_IP, BBU_TC_PORT))
    sock.listen(5)
    sock.settimeout(1.0)

    print(f"[BBU] Waiting TC from Web TCP on {BBU_IP}:{BBU_TC_PORT}")

    try:
        while is_running():
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                continue
            except Exception as e:
                if is_running():
                    print(f"[BBU] TC accept failed: {e}")
                continue

            try:
                conn.settimeout(2.0)
                tc = conn.recv(4096).decode("utf-8", errors="replace").strip()
                if tc:
                    _enqueue_tc(tc)
            except Exception as e:
                print(f"[BBU] TC receive from web failed: {e}")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    finally:
        sock.close()
        print("[BBU] tc_receiver_from_web stopped")


def tc_sender() -> None:
    """
    Sinkron dengan SAT:
    - SAT telecommand_receiver menerima raw UTF-8 via UDP
    - propagate() di BBU dipakai hanya untuk memutuskan apakah uplink lolos / drop
    - yang dikirim ke SAT tetap string command mentah
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print("[BBU] TC sender started (UDP -> SAT)")

    try:
        while is_running():
            tc = _dequeue_tc()
            if tc is None:
                time.sleep(0.2)
                continue

            st = DEFAULT_ORBIT.get_state()
            if not st["visible"]:
                # masukkan lagi ke queue depan itu ribet tanpa deque.
                # jadi di sini lebih aman append kembali ke belakang.
                _enqueue_tc(tc)
                time.sleep(0.8)
                continue

            pkt = {
                "type": "TC",
                "cmd": tc,
                "ts": time.time(),
                "corrupted": False,
            }

            try:
                pkt2 = propagate(pkt,
                                 elev_deg=float(st["elev_deg"]),
                                 direction="uplink")
            except Exception as e:
                print(f"[BBU] TC propagate failed: {e}")
                _enqueue_tc(tc)
                time.sleep(0.5)
                continue

            if pkt2 is None:
                print(f"[BBU] TC dropped by uplink channel: {tc}")
                continue

            if pkt2.get("corrupted"):
                print(f"[BBU] TC corrupted on uplink: {tc}")
                continue

            try:
                sock.sendto(tc.encode("utf-8"), (SAT_IP, SAT_TC_PORT))
                print(f"[BBU] TC sent to SAT: {tc}")
            except Exception as e:
                print(f"[BBU] UDP send TC failed: {e}")
                _enqueue_tc(tc)

    finally:
        sock.close()
        print("[BBU] tc_sender stopped")


def status_printer() -> None:
    while is_running():
        try:
            st = DEFAULT_ORBIT.get_state()
            snap = _get_latest_snapshot()
            print(f"[BBU] Visible={st['visible']} "
                  f"elev={st['elev_deg']:.1f}deg "
                  f"DL={st['rate_dl_mbps'] * 1e3:.1f}kbps "
                  f"| latest={snap.get('frame_id')}")
        except Exception as e:
            print(f"[BBU] status_printer failed: {e}")
        time.sleep(3)


if __name__ == "__main__":
    print("=== BBU NODE STARTED (LEO viswin + IMG noisy + IMG2 ECC fixed) ===")

    threads = [
        threading.Thread(target=tm_receiver, daemon=True, name="tm_receiver"),
        threading.Thread(target=tm_server_for_web,
                         daemon=True,
                         name="tm_server_for_web"),
        threading.Thread(target=tc_receiver_from_web,
                         daemon=True,
                         name="tc_receiver_from_web"),
        threading.Thread(target=tc_sender, daemon=True, name="tc_sender"),
        threading.Thread(target=status_printer,
                         daemon=True,
                         name="status_printer"),
    ]

    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[BBU] KeyboardInterrupt received, shutting down...")
        stop_running()
        time.sleep(1.5)
        print("[BBU] Shutdown complete")
