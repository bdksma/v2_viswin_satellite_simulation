# common/raw_to_image.py
from __future__ import annotations
import os
import numpy as np

HEADER = bytes.fromhex("F030F080")  # 4 bytes marker

def unpack_12bit_packed_2048(data3072: bytes) -> np.ndarray:
    """
    3072 bytes -> 2048 pixels (12-bit) packed:
      [b0,b1,b2] -> p0 = b0 + ((b1 & 0x0F)<<8), p1 = (b2<<4) + (b1>>4)
    Return: uint16 array length 2048
    """
    if len(data3072) != 3072:
        raise ValueError(f"Expected 3072 bytes, got {len(data3072)}")

    b = np.frombuffer(data3072, dtype=np.uint8).reshape(-1, 3)
    b0 = b[:, 0].astype(np.uint16)
    b1 = b[:, 1].astype(np.uint16)
    b2 = b[:, 2].astype(np.uint16)

    p0 = b0 | ((b1 & 0x0F) << 8)
    p1 = (b2 << 4) | (b1 >> 4)

    out = np.empty(2048, dtype=np.uint16)
    out[0::2] = p0
    out[1::2] = p1
    return out

def find_lines_by_header(raw: bytes, max_lines: int = 2048) -> list[bytes]:
    """
    Cari semua header F030F080. Untuk tiap header, ambil 3072 bytes setelah header.
    Return list of data blocks (3072 bytes each).
    """
    lines: list[bytes] = []
    start = 0
    while True:
        idx = raw.find(HEADER, start)
        if idx < 0:
            break
        data_start = idx + len(HEADER)
        data_end = data_start + 3072
        if data_end <= len(raw):
            lines.append(raw[data_start:data_end])
            if len(lines) >= max_lines:
                break
        start = idx + 1
    return lines

def frame_from_headered_raw(raw: bytes, width=2048, height=2048) -> np.ndarray | None:
    """
    Decode 1 frame dari raw yang berisi banyak header per baris.
    Return 2D uint16 image (2048x2048) atau None jika header tidak cukup.
    """
    blocks = find_lines_by_header(raw, max_lines=height)
    if len(blocks) < height:
        return None

    img = np.zeros((height, width), dtype=np.uint16)
    for i in range(height):
        img[i, :] = unpack_12bit_packed_2048(blocks[i])
    return img

def frame_from_continuous_packed(raw: bytes, width=2048, height=2048) -> np.ndarray:
    """
    Fallback: anggap raw berisi pixel 12-bit packed TANPA header.
    2048*2048 pixel -> 2048*2048*12/8 = 6,291,456 bytes
    """
    need = (width * height * 12) // 8
    if len(raw) < need:
        raise ValueError(f"Not enough bytes for one frame: need {need}, got {len(raw)}")

    # Unpack per 3 bytes -> 2 pixels, total pixels = width*height
    data = raw[:need]
    b = np.frombuffer(data, dtype=np.uint8).reshape(-1, 3)
    b0 = b[:, 0].astype(np.uint16)
    b1 = b[:, 1].astype(np.uint16)
    b2 = b[:, 2].astype(np.uint16)

    p0 = b0 | ((b1 & 0x0F) << 8)
    p1 = (b2 << 4) | (b1 >> 4)

    px = np.empty(width * height, dtype=np.uint16)
    px[0::2] = p0
    px[1::2] = p1
    return px.reshape((height, width))

def normalize_to_u8(img16: np.ndarray, clip_percent: float = 0.5) -> np.ndarray:
    """
    Normalize uint16 -> uint8 dengan clipping persentil (biar kontras lebih enak)
    """
    x = img16.astype(np.float32)
    lo = np.percentile(x, clip_percent)
    hi = np.percentile(x, 100.0 - clip_percent)
    if hi <= lo:
        lo, hi = float(x.min()), float(x.max() if x.max() > x.min() else x.min() + 1.0)
    x = np.clip((x - lo) / (hi - lo), 0.0, 1.0)
    return (x * 255.0).astype(np.uint8)

def save_png_tiff(img16: np.ndarray, out_png: str, out_tif: str):
    """
    Save:
      - PNG: 8-bit normalized
      - TIFF: 16-bit (as-is)
    """
    from PIL import Image

    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(out_tif) or ".", exist_ok=True)

    img8 = normalize_to_u8(img16)

    Image.fromarray(img8, mode="L").save(out_png)
    # 16-bit TIFF
    Image.fromarray(img16, mode="I;16").save(out_tif)

def decode_one_frame(raw: bytes, width=2048, height=2048) -> np.ndarray:
    """
    Robust:
      1) coba decode berbasis header per baris
      2) kalau gagal, fallback continuous packed
    """
    img = frame_from_headered_raw(raw, width, height)
    if img is not None:
        return img
    return frame_from_continuous_packed(raw, width, height)
