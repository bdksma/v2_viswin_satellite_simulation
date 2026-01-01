# orbit.py
# =====================================================
# ORBIT & VISIBILITY MODEL (SIMPLIFIED LEO)
# =====================================================

import math
import time

# =====================================================
# ORBIT CONFIG
# =====================================================
ORBIT_PERIOD = 5 * 60        # 90 menit
PASS_DURATION = 2 * 60       # 10 menit visible
MAX_DOPPLER = 5000            # Hz

# =====================================================
# VISIBILITY WINDOW
# =====================================================
def is_visible(t=None):
    """
    Return True jika satelit visible ke ground
    """
    if t is None:
        t = time.time()

    phase = t % ORBIT_PERIOD
    return phase < PASS_DURATION

# =====================================================
# DOPPLER SHIFT
# =====================================================
def doppler_shift(t=None):
    """
    Doppler sinusoidal (mendekat / menjauh)
    """
    if t is None:
        t = time.time()

    phase = (t % ORBIT_PERIOD) / ORBIT_PERIOD
    return int(MAX_DOPPLER * math.sin(2 * math.pi * phase))

# =====================================================
# DEBUG
# =====================================================
if __name__ == "__main__":
    while True:
        print("Visible:", is_visible(), "Doppler:", doppler_shift())
        time.sleep(1)
