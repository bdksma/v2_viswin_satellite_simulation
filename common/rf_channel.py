# rf_channel.py
# =====================================================
# RF CHANNEL SIMULATION
# =====================================================

import random
import time

# =====================================================
# CHANNEL CONFIG
# =====================================================
PACKET_LOSS_PROB = 0.05
PROPAGATION_DELAY = 0.25
BIT_ERROR_PROB = 0.02

# =====================================================
# RF PROPAGATION
# =====================================================
def propagate(packet: dict):
    """
    Simulasi efek RF:
    - delay
    - packet loss
    - corruption
    """
    time.sleep(PROPAGATION_DELAY)

    if random.random() < PACKET_LOSS_PROB:
        return None

    if random.random() < BIT_ERROR_PROB:
        packet = packet.copy()
        packet["corrupted"] = True

    return packet

# =====================================================
# DEBUG
# =====================================================
if __name__ == "__main__":
    for i in range(10):
        pkt = {"seq": i, "data": "TM", "corrupted": False}
        print(propagate(pkt))
