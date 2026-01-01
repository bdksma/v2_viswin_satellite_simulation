# orbit.py
# =====================================================
# ORBIT & VISIBILITY MODEL (LEO, VISWIN+DOPPLER schema)
# =====================================================
# Tujuan:
# - Menyamakan "visibility window" + "doppler" + "rate vs elevasi"
#   seperti skema di viswin_doppler.py (Skyfield-based),
#   tetapi tetap punya fallback kalau skyfield belum terpasang.
#
# API yang dipakai oleh node:
#   - get_state(now=None) -> dict {visible,elev_deg,doppler_hz,rate_dl_mbps,rate_ul_mbps,ts}
#   - is_visible(now=None) -> bool
#   - doppler_shift(now=None) -> int (Hz)
#   - elevation_deg(now=None) -> float (deg)
#   - data_rate_mbps(elev_deg, max_rate_mbps, alpha, elev_mask_deg) -> float
#
# Catatan:
# - Untuk hasil paling "real" gunakan Skyfield:
#     pip install skyfield
# - Kalau belum ada skyfield, fallback akan memakai model sinusoidal sederhana
#   (cukup untuk testing end-to-end uplink/downlink).
# =====================================================

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Optional, Tuple, Dict

# ----------------------------
# Default "VISWIN+Doppler" cfg
# ----------------------------
DEFAULT_TLE_NAME = "LEO-A (DUMMY)"
DEFAULT_TLE_L1 = "1 51000U 22001A   24120.25000000  .00001234  00000+0  20000-3 0  9991"
DEFAULT_TLE_L2 = "2 51000  97.5000  25.3000 0001000  45.0000 315.0000 15.20000000    16"

DEFAULT_GS_LAT = -6.2000
DEFAULT_GS_LON = 106.8166
DEFAULT_GS_ALT_M = 50.0

DEFAULT_ELEV_MASK_DEG = 10.0

# Doppler calc needs carrier
C = 299_792_458.0
DEFAULT_CARRIER_HZ = 2.2e9

# Rate model (seperti di viswin_doppler.py)
DEFAULT_DL_MAX_RATE_Mbps = 0.258   # ~258 kbps
DEFAULT_UL_MAX_RATE_Mbps = 0.050   # contoh uplink lebih kecil
DEFAULT_RATE_ALPHA = 1.5

# Fallback "simplified orbit" (kalau skyfield tidak ada)
# NOTE: sebelumnya file kamu salah tulis: ORBIT_PERIOD=5*60 (300s) tapi comment 90 menit.
FALLBACK_ORBIT_PERIOD_S = 90 * 60      # 90 menit
FALLBACK_PASS_FRACTION = 10 / 90       # ~10 menit visible dari 90 menit
FALLBACK_MAX_DOPPLER_HZ = 5000         # contoh LEO

@dataclass
class OrbitConfig:
    tle_name: str = DEFAULT_TLE_NAME
    tle_l1: str = DEFAULT_TLE_L1
    tle_l2: str = DEFAULT_TLE_L2
    gs_lat: float = DEFAULT_GS_LAT
    gs_lon: float = DEFAULT_GS_LON
    gs_alt_m: float = DEFAULT_GS_ALT_M
    elev_mask_deg: float = DEFAULT_ELEV_MASK_DEG
    carrier_hz: float = DEFAULT_CARRIER_HZ
    dl_max_rate_mbps: float = DEFAULT_DL_MAX_RATE_Mbps
    ul_max_rate_mbps: float = DEFAULT_UL_MAX_RATE_Mbps
    rate_alpha: float = DEFAULT_RATE_ALPHA

def _try_import_skyfield():
    try:
        from skyfield.api import load, EarthSatellite, wgs84  # type: ignore
        return load, EarthSatellite, wgs84
    except Exception:
        return None, None, None

def data_rate_mbps(
    elev_deg: float,
    max_rate_mbps: float,
    alpha: float,
    elev_mask_deg: float,
) -> float:
    """
    Sama seperti viswin:
      rate = MAX_RATE * (sin(elev)^alpha) jika elev > mask else 0
    """
    if elev_deg <= elev_mask_deg:
        return 0.0
    e_rad = math.radians(float(elev_deg))
    norm = math.sin(e_rad)
    if norm < 0.0:
        norm = 0.0
    if norm > 1.0:
        norm = 1.0
    return float(max_rate_mbps) * (norm ** float(alpha))

class OrbitModel:
    """
    Orbit model:
    - Kalau skyfield tersedia: elev & doppler dihitung dari TLE + GS.
    - Kalau tidak: pakai sinusoidal proxy.
    """
    def __init__(self, cfg: OrbitConfig = OrbitConfig()):
        self.cfg = cfg
        self._load, self._EarthSatellite, self._wgs84 = _try_import_skyfield()
        self._sf_ready = (self._load is not None)

        # Skyfield objects created lazily
        self._ts = None
        self._sat = None
        self._gs = None

    def _sf_init(self):
        if not self._sf_ready:
            return
        if self._ts is not None:
            return
        load, EarthSatellite, wgs84 = self._load, self._EarthSatellite, self._wgs84
        self._ts = load.timescale()
        self._sat = EarthSatellite(self.cfg.tle_l1, self.cfg.tle_l2, self.cfg.tle_name, self._ts)
        self._gs = wgs84.latlon(self.cfg.gs_lat, self.cfg.gs_lon, elevation_m=self.cfg.gs_alt_m)

    def _state_skyfield(self, now: float) -> Tuple[float, float]:
        """
        Return (elev_deg, doppler_hz) at time 'now' (unix seconds UTC).
        Doppler dihitung dari radial range-rate.
        """
        self._sf_init()
        assert self._ts is not None and self._sat is not None and self._gs is not None

        # convert unix seconds -> UTC components
        # Skyfield butuh UTC date-time; kita pakai time.gmtime.
        g = time.gmtime(now)
        frac = (now - int(now))
        t = self._ts.utc(g.tm_year, g.tm_mon, g.tm_mday, g.tm_hour, g.tm_min, g.tm_sec + frac)

        diff = self._sat - self._gs
        topo = diff.at(t)

        # elevation
        alt, az, dist = topo.altaz()
        elev_deg = float(alt.degrees)

        # range-rate (km/s) -> m/s
        try:
            # skyfield >= 1.45 biasanya punya range_velocity()
            rr_km_s = topo.range_velocity().km_per_s  # type: ignore
            rr_m_s = float(rr_km_s) * 1000.0
        except Exception:
            # fallback: estimasi dari vektor velocity magnitude (lebih kasar)
            v = diff.at(t).velocity.km_per_s
            rr_m_s = float((v[0]**2 + v[1]**2 + v[2]**2) ** 0.5) * 1000.0

        doppler_hz = (rr_m_s / C) * float(self.cfg.carrier_hz)
        return elev_deg, doppler_hz

    def _state_fallback(self, now: float) -> Tuple[float, float]:
        """
        Sinusoidal proxy:
        - elev dibuat naik-turun per orbit.
        - visible hanya pada sebagian awal orbit.
        - doppler sinusoidal.
        """
        phase = (now % FALLBACK_ORBIT_PERIOD_S) / FALLBACK_ORBIT_PERIOD_S  # 0..1
        # elev proxy: 0..90..0 (sin)
        elev = 90.0 * max(0.0, math.sin(math.pi * phase))
        doppler = FALLBACK_MAX_DOPPLER_HZ * math.sin(2 * math.pi * phase)
        return elev, doppler

    def get_state(self, now: Optional[float] = None) -> Dict[str, float | bool]:
        if now is None:
            now = time.time()

        if self._sf_ready:
            elev_deg, doppler_hz = self._state_skyfield(now)
        else:
            elev_deg, doppler_hz = self._state_fallback(now)

        visible = elev_deg > float(self.cfg.elev_mask_deg)
        rate_dl = data_rate_mbps(elev_deg, self.cfg.dl_max_rate_mbps, self.cfg.rate_alpha, self.cfg.elev_mask_deg)
        rate_ul = data_rate_mbps(elev_deg, self.cfg.ul_max_rate_mbps, self.cfg.rate_alpha, self.cfg.elev_mask_deg)

        return {
            "ts": float(now),
            "elev_deg": float(elev_deg),
            "doppler_hz": float(doppler_hz),
            "visible": bool(visible),
            "rate_dl_mbps": float(rate_dl),
            "rate_ul_mbps": float(rate_ul),
        }

    def is_visible(self, now: Optional[float] = None) -> bool:
        return bool(self.get_state(now)["visible"])

    def doppler_shift(self, now: Optional[float] = None) -> int:
        return int(round(float(self.get_state(now)["doppler_hz"])))

    def elevation_deg(self, now: Optional[float] = None) -> float:
        return float(self.get_state(now)["elev_deg"])

# Singleton default (biar sat/bbu pakai model yang sama)
DEFAULT_ORBIT = OrbitModel()

# Backward-compatible helpers (mirip file orbit.py lama)
def is_visible(t: Optional[float] = None) -> bool:
    return DEFAULT_ORBIT.is_visible(t)

def doppler_shift(t: Optional[float] = None) -> int:
    return DEFAULT_ORBIT.doppler_shift(t)

def elevation_deg(t: Optional[float] = None) -> float:
    return DEFAULT_ORBIT.elevation_deg(t)

if __name__ == "__main__":
    # quick debug loop
    print("Skyfield ready:", DEFAULT_ORBIT._sf_ready)
    while True:
        st = DEFAULT_ORBIT.get_state()
        print(f"Visible={st['visible']} elev={st['elev_deg']:.2f}deg doppler={st['doppler_hz']:.1f}Hz "
              f"DL={st['rate_dl_mbps']*1e3:.1f}kbps UL={st['rate_ul_mbps']*1e3:.1f}kbps")
        time.sleep(1)
