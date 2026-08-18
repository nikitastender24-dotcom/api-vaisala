#!/usr/bin/env python3

import json
import time
import math
import threading
from datetime import datetime
from collections import defaultdict
import os
import signal
import sys

try:
    import requests
except ImportError:
    os.system("pip install requests")
    import requests

GRID_SIZE_KM = 4
EARTH_RADIUS_KM = 6371.0
MAX_AGE_MINUTES = 20
CLEANUP_INTERVAL = 60

lightning_data = {}
data_lock = threading.Lock()

stats = {"total_received": 0, "active": 0, "vaisala": 0, "nowcast": 0}

def lat_lon_to_grid(lat, lon):
    lat_rad = math.radians(lat)
    km_per_deg_lat = 111.0
    km_per_deg_lon = 111.0 * math.cos(lat_rad)
    
    grid_lat = round(lat / (GRID_SIZE_KM / km_per_deg_lat), 6)
    grid_lon = round(lon / (GRID_SIZE_KM / km_per_deg_lon), 6)
    
    return f"{grid_lat:.6f},{grid_lon:.6f}"

def grid_to_center(grid_key):
    lat_str, lon_str = grid_key.split(',')
    return float(lat_str), float(lon_str)

def get_grid_bounds(grid_key):
    center_lat, center_lon = grid_to_center(grid_key)
    lat_rad = math.radians(center_lat)
    km_per_deg_lat = 111.0
    km_per_deg_lon = 111.0 * math.cos(lat_rad)
    
    half_size = GRID_SIZE_KM / 2
    lat_offset = half_size / km_per_deg_lat
    lon_offset = half_size / km_per_deg_lon
    
    return {
        'min_lat': center_lat - lat_offset,
        'max_lat': center_lat + lat_offset,
        'min_lon': center_lon - lon_offset,
        'max_lon': center_lon + lon_offset
    }

def process_lightning_batch(lightning_list, source_type, timestamp_ms):
    global stats
    
    with data_lock:
        for item in lightning_list:
            if len(item) >= 2:
                lat, lon = item[0], item[1]
                delay = item[2] if len(item) > 2 else 0
                
                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    continue
                
                grid_key = lat_lon_to_grid(lat, lon)
                current_time = time.time()
                
                if grid_key not in lightning_data:
                    lightning_data[grid_key] = {
                        'lat': lat,
                        'lon': lon,
                        'grid_lat': float(grid_key.split(',')[0]),
                        'grid_lon': float(grid_key.split(',')[1]),
                        'count': 0,
                        'first_seen': current_time,
                        'last_seen': current_time,
                        'delays': [],
                        'source': source_type,
                        'raw_coords': []
                    }
                
                entry = lightning_data[grid_key]
                entry['count'] += 1
                entry['last_seen'] = current_time
                if delay > 0:
                    entry['delays'].append(delay)
                if len(entry['raw_coords']) < 10:
                    entry['raw_coords'].append([lat, lon])
                else:
                    entry['raw_coords'].pop(0)
                    entry['raw_coords'].append([lat, lon])
                
                stats['total_received'] += 1
                if source_type == 'vaisala':
                    stats['vaisala'] += 1
                else:
                    stats['nowcast'] += 1

def cleanup_old_data():
    global stats
    
    while True:
        time.sleep(CLEANUP_INTERVAL)
        current_time = time.time()
        cutoff_time = current_time - (MAX_AGE_MINUTES * 60)
        
        with data_lock:
            to_remove = []
            for key, data in lightning_data.items():
                if data['last_seen'] < cutoff_time:
                    to_remove.append(key)
            
            for key in to_remove:
                del lightning_data[key]
            
            stats['active'] = len(lightning_data)

def save_geojson(filename="lightning.geojson"):
    with data_lock:
        features = []
        for key, data in lightning_data.items():
            lat, lon = grid_to_center(key)
            
            properties = {
                'count': data['count'],
                'age_sec': int(time.time() - data['first_seen']),
                'source': data['source']
            }
            
            if data['delays']:
                properties['avg_delay'] = int(sum(data['delays']) / len(data['delays']))
            
            if data['raw_coords'] and len(data['raw_coords']) > 1:
                properties['raw_count'] = len(data['raw_coords'])
            
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lon, lat]
                },
                'properties': properties
            }
            features.append(feature)
        
        geojson = {
            'type': 'FeatureCollection',
            'features': features,
            'metadata': {
                'generated': datetime.utcnow().isoformat() + 'Z',
                'grid_size_km': GRID_SIZE_KM,
                'max_age_minutes': MAX_AGE_MINUTES,
                'total_cells': len(features),
                'total_lightnings': stats['total_received']
            }
        }
    
    with open(filename, 'w') as f:
        json.dump(geojson, f, separators=(',', ':'))

def save_compact_grid(filename="lightning_grid.csv"):
    with data_lock:
        lines = ["grid_lat,grid_lon,count,age_sec,source"]
        for key, data in lightning_data.items():
            lat, lon = grid_to_center(key)
            age = int(time.time() - data['first_seen'])
            lines.append(f"{lat:.6f},{lon:.6f},{data['count']},{age},{data['source']}")
    
    with open(filename, 'w') as f:
        f.write('\n'.join(lines))

def sse_listener(url, headers=None):
    if headers is None:
        headers = {
            'Accept': 'text/event-stream',
            'Cache-Control': 'no-cache',
        }
    
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=None)
        response.raise_for_status()
        
        buffer = ""
        event_type = None
        
        for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
            if chunk:
                buffer += chunk
                lines = buffer.split('\n')
                buffer = lines[-1]
                
                for line in lines[:-1]:
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    if line.startswith('event:'):
                        event_type = line.split(':', 1)[1].strip()
                    elif line.startswith('data:'):
                        data_str = line.split(':', 1)[1].strip()
                        if data_str:
                            try:
                                data = json.loads(data_str)
                                if event_type in ['lightning-vaisala', 'lightning-nowcast']:
                                    process_lightning_batch(
                                        data.get('lightning', []),
                                        event_type.replace('lightning-', ''),
                                        data.get('time_lightning', time.time() * 1000)
                                    )
                                    if stats['total_received'] % 100 == 0:
                                        save_geojson()
                                event_type = None
                            except json.JSONDecodeError:
                                pass
                    elif line.startswith(':'):
                        pass
    
    except requests.exceptions.RequestException:
        return False
    except KeyboardInterrupt:
        return False
    
    return True

def signal_handler(sig, frame):
    save_geojson()
    save_compact_grid()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    
    cleanup_thread = threading.Thread(target=cleanup_old_data, daemon=True)
    cleanup_thread.start()
    
    def auto_save():
        while True:
            time.sleep(10)
            if stats['total_received'] > 0:
                save_geojson()
                save_compact_grid()
    
    save_thread = threading.Thread(target=auto_save, daemon=True)
    save_thread.start()
    
    SSE_URL = "https://www.pogodairadar.com.ua/api/lightning-stream"
    
    while True:
        try:
            success = sse_listener(SSE_URL)
            if not success:
                time.sleep(5)
        except Exception:
            time.sleep(5)

if __name__ == "__main__":
    main()
