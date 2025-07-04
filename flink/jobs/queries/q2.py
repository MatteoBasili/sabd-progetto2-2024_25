import json
import base64
import io
import numpy as np
from PIL import Image
import logging
from pyflink.common import Row, Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q2")

# Parametri
DISTANCE_THRESHOLD = 2
OUTLIER_THRESHOLD = 6000


def get_padded(image, d, x, y, pad=0.0):
    if d < 0 or d >= image.shape[0] or x < 0 or x >= image.shape[1] or y < 0 or y >= image.shape[2]:
        return pad
    return image[d, x, y]

def compute_all_points_and_outliers(image3d, outlier_threshold=OUTLIER_THRESHOLD):
    depth, width, height = image3d.shape
    outliers = []
    all_points = []

    for y in range(height):
        for x in range(width):
            pixel_val = image3d[-1, x, y]
            if pixel_val == 0:
                continue  # punto vuoto

            cn_sum, cn_count = 0.0, 0
            on_sum, on_count = 0.0, 0

            for j in range(-DISTANCE_THRESHOLD, DISTANCE_THRESHOLD + 1):
                for i in range(-DISTANCE_THRESHOLD, DISTANCE_THRESHOLD + 1):
                    for d in range(depth):
                        dist = abs(i) + abs(j) + abs(depth - 1 - d)
                        if dist <= DISTANCE_THRESHOLD:
                            val = get_padded(image3d, d, x + i, y + j)
                            if val > 0:
                                cn_sum += val
                                cn_count += 1

            for j in range(-2 * DISTANCE_THRESHOLD, 2 * DISTANCE_THRESHOLD + 1):
                for i in range(-2 * DISTANCE_THRESHOLD, 2 * DISTANCE_THRESHOLD + 1):
                    for d in range(depth):
                        dist = abs(i) + abs(j) + abs(depth - 1 - d)
                        if DISTANCE_THRESHOLD < dist <= 2 * DISTANCE_THRESHOLD:
                            val = get_padded(image3d, d, x + i, y + j)
                            if val > 0:
                                on_sum += val
                                on_count += 1

            if cn_count == 0 or on_count == 0:
                continue

            cn_mean = cn_sum / cn_count
            on_mean = on_sum / on_count
            dev = abs(cn_mean - on_mean)

            all_points.append((x, y, dev))
            if dev > outlier_threshold:
                outliers.append((x, y, dev))

    return all_points, outliers

class SlidingWindowOutlierProcessFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        list_state_desc = ListStateDescriptor(
            "window_state",
            Types.PICKLED_BYTE_ARRAY())  # user pickle for numpy arrays
        self.window_state = runtime_context.get_list_state(list_state_desc)

    def process_element(self, value, ctx):
        # value è un dict con chiavi batch_id, print_id, tile_id, saturated, arr (numpy array)
        try:
            window = list(self.window_state.get())  # recupera stato

            arr = value['arr']
            if len(window) == 3:
                window.pop(0)
            window.append(arr)

            self.window_state.update(window)

            if len(window) < 3:
                return  # non abbastanza layer ancora

            image3d = np.stack(window, axis=0)
            all_points, outliers = compute_all_points_and_outliers(image3d)

            top5 = sorted(all_points, key=lambda x: -x[2])[:5]
            top5_flat = []
            for p in top5:
                top5_flat.append(f"({p[0]},{p[1]})")
                top5_flat.append(f"{round(p[2], 6)}")
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

            # Emissione tuple (CSV Row, JSON string per Q3)
            yield (row, json.dumps(q3_dict))

        except Exception as e:
            logger.error(f"[Q2][process_element] Error: {e}")


def decode_and_prepare(raw):
    try:
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
        img = Image.open(io.BytesIO(tif_bytes)).convert("I;16")
        arr = np.array(img)

        return {
            "batch_id": batch_id,
            "print_id": print_id,
            "tile_id": tile_id,
            "saturated": saturated,
            "arr": arr
        }
    except Exception as e:
        logger.error(f"[Q2][decode_and_prepare] Error: {e}")
        return None

