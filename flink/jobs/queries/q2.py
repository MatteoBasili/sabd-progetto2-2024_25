import json
import base64
import io
import numpy as np
from PIL import Image
import logging
from pyflink.common import Row, Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

### For metrics evaluation ###
#import time
##############################

from config import (EMPTY_THRESHOLD, SATURATION_THRESHOLD,
                    DISTANCE_FACTOR, OUTLIER_THRESHOLD)

from functools import lru_cache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q2")


def compute_all_points_and_outliers(image3d,
                                    empty_threshold = EMPTY_THRESHOLD,
                                    saturation_threshold = SATURATION_THRESHOLD,
                                    distance_threshold = DISTANCE_FACTOR,
                                    outlier_threshold = OUTLIER_THRESHOLD):
    image3d = image3d.astype(np.float64)
    depth, width, height = image3d.shape

    def get_padded(image, d, x, y, pad=0.0):
        if d < 0 or d >= image.shape[0]:
            return pad
        if x < 0 or x >= image.shape[1]:
            return pad
        if y < 0 or y >= image.shape[2]:
            return pad
        return image[d, x, y]

    all_points = []
    outliers = []
    # For each point
    for y in range(height):
        for x in range(width):
            if image3d[-1, x, y] <= empty_threshold or image3d[-1, x, y] >= saturation_threshold:
                continue
            # Close neighbours
            cn_sum = 0
            cn_count = 0
            for j in range(-distance_threshold, distance_threshold + 1):
                for i in range(-distance_threshold, distance_threshold + 1):
                    for d in range(depth):
                        # Manhattan distance
                        distance = abs(i) + abs(j) + abs(depth - 1 - d)
                        if distance <= distance_threshold:
                            cn_sum += get_padded(image3d, d, x+i, y+j)
                            cn_count += 1
            # Outer neighbours
            on_sum = 0
            on_count = 0
            for j in range(-2 * distance_threshold, 2 * distance_threshold + 1):
                for i in range(-2 * distance_threshold, 2 * distance_threshold + 1):
                    for d in range(depth):
                        distance = abs(i) + abs(j) + abs(depth - 1 - d)
                        if distance > distance_threshold and distance <= 2*distance_threshold:
                            on_sum += get_padded(image3d, d, x+i, y+j)
                            on_count += 1
            # Compare the mean
            close_mean = cn_sum / cn_count
            outer_mean = on_sum / on_count

            dev = abs(close_mean - outer_mean)
            
            # Append outliers
            all_points.append((x, y, dev))
            if dev > outlier_threshold:
                outliers.append((x, y, dev))

    return all_points, outliers

class SlidingWindowOutlierProcessFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        list_state_desc = ListStateDescriptor(
            "window_state",
            Types.PICKLED_BYTE_ARRAY()) 
        self.window_state = runtime_context.get_list_state(list_state_desc)

    def process_element(self, value, ctx):
        # value is a dict with keys batch_id, print_id, tile_id, saturated, arr (numpy array)
        try:
            ### For metrics evaluation ###
            #start = time.perf_counter()
            ##############################
        
            window = list(self.window_state.get())  # retrieve state

            arr = value['arr']
            if len(window) == 3:
                window.pop(0)
            window.append(arr)

            self.window_state.update(window)

            if len(window) < 3:
                # placeholder: top‑5 absent
                top5_flat = ["NA", "0"] * 5

                row = Row(
                    value['batch_id'],
                    value['print_id'],
                    value['tile_id'],
                    *top5_flat
                )

                q3_dict = {
                    "batch_id": str(value['batch_id']),
                    "print_id": value['print_id'],
                    "tile_id": value['tile_id'],
                    "saturated": value['saturated'],
                    "outliers": []          # no outliers as long as the window is incomplete
                }

                yield (row, json.dumps(q3_dict))
                
                ### For metrics evaluation ###
                #latency_ms = (time.perf_counter() - start) * 1_000
                #logger.info(
                 #   f"METRICS|Q2|batch={value['batch_id']}|latency_ms={latency_ms:.2f}"
                #)
                ##############################
                
                return

            image3d = np.stack(window, axis=0)
            all_points, outliers = compute_all_points_and_outliers(image3d)

            top5 = sorted(all_points, key=lambda x: -x[2])[:5]
            top5_flat = []
            for p in top5:
                top5_flat.append(f"({p[0]},{p[1]})")
                top5_flat.append(f"{p[2]}")
            while len(top5_flat) < 10:
                top5_flat.append("NA")
                top5_flat.append("0")

            row = Row(
                value['batch_id'],
                value['print_id'],
                value['tile_id'],
                *top5_flat
            )

            q3_dict = {
                "batch_id": str(value['batch_id']),
                "print_id": value['print_id'],
                "tile_id": value['tile_id'],
                "saturated": value['saturated'],
                "outliers": [{"x": int(x), "y": int(y), "dev": float(dev)} for x, y, dev in outliers]
            }

            # Output tuples (CSV Row, JSON string for Q3)
            yield (row, json.dumps(q3_dict))
            
            ### For metrics evaluation ###
            #latency_ms = (time.perf_counter() - start) * 1_000
            #logger.info(
             #   f"METRICS|Q2|batch={value['batch_id']}|latency_ms={latency_ms:.2f}"
            #)
            ##############################

        except Exception as e:
            logger.error(f"[Q2][process_element] Error: {e}")


def decode_and_prepare(raw):
    try:
        ### For metrics evaluation ###
        #start = time.perf_counter()
        ##############################
    
        data = json.loads(raw)
        required = ["batch_id", "print_id", "tile_id", "tif", "saturated"]
        if not all(k in data for k in required):
            logger.warning(f"Incomplete input: {data}")
            return None

        batch_id = int(data["batch_id"])
        print_id = data["print_id"]
        tile_id = int(data["tile_id"])
        saturated = int(data["saturated"])

        tif_bytes = base64.b64decode(data["tif"])
        img = Image.open(io.BytesIO(tif_bytes))
        arr = np.array(img)

        result = {
            "batch_id": batch_id,
            "print_id": print_id,
            "tile_id": tile_id,
            "saturated": saturated,
            "arr": arr
        }
        
        ### For metrics evaluation ###
        #latency_ms = (time.perf_counter() - start) * 1_000
        #logger.info(f"METRICS|Q2|batch={batch_id}|latency_ms={latency_ms:.2f}")
        ##############################

        return result
        
    except Exception as e:
        logger.error(f"[Q2] [decode_and_prepare] Error: {e}")
        return None
