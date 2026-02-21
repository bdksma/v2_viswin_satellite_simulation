# LEO Satellite Communication Simulator

## Overview
A Python-based simulation of a LEO (Low Earth Orbit) satellite communication system with:
- **Satellite node** (`satelllite/`): Simulates satellite telemetry and telecommand
- **BBU node** (`bbu/`): Base Band Unit that processes signals, reassembles images
- **Common modules** (`common/`): Orbit models, RF channel simulation, image processing
- **Web monitor** (`web/`): Streamlit-based web dashboard for monitoring telemetry

## Project Architecture
- `web/web_leo.py` - Main Streamlit web app (frontend on port 5000)
- `web/web_app.py` - Simpler version of the web monitor
- `bbu/bbu_leo.py` - BBU LEO node
- `bbu/bbu_node.py` - BBU node with image reassembly
- `satelllite/satellite_leo.py` - Satellite LEO node
- `satelllite/satellite_node.py` - Satellite node
- `common/orbit.py` - Simple orbit model
- `common/orbit_leo.py` - LEO orbit model with Skyfield fallback
- `common/rf_channel.py` - RF channel simulation
- `common/rf_channel_leo.py` - LEO RF channel with fading/corruption
- `common/raw_to_image.py` - Raw data to image decoder (12-bit packed)

## Tech Stack
- Python 3.11
- Streamlit (web frontend)
- NumPy (numerical processing)
- Pillow (image handling)

## Running
The Streamlit web app runs on port 5000. The satellite and BBU nodes are separate processes that communicate via sockets.
