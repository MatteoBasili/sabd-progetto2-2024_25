import json
import base64
import io
import numpy as np
from PIL import Image
import logging
from pyflink.common import Row, Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor

from functools import lru_cache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q2")

# Params
DISTANCE_THRESHOLD = 2
OUTER_THRESHOLD = 2 * DISTANCE_THRESHOLD
OUTLIER_THRESHOLD = 6000


def _shift2d(mat: np.ndarray, dx: int, dy: int) -> np.ndarray:
    """Shifts the 2-D matrix by dx rows and dy columns with padding to 0 (no wrap-around)."""
    h, w = mat.shape
    pad_x = (max(dx, 0), max(-dx, 0))
    pad_y = (max(dy, 0), max(-dy, 0))
    tmp = np.pad(mat, (pad_x, pad_y), constant_values=0)
    return tmp[pad_x[1]:pad_x[1]+h, pad_y[1]:pad_y[1]+w]

@lru_cache(maxsize=None)
def _generate_offsets(depth: int):
    """Returns two lists of (d, dx, dy) for the inner and outer ring."""
    inner, outer = [], []
    max_d = depth - 1                               # reference layer
    for d in range(depth):
        for dx in range(-OUTER_THRESHOLD, OUTER_THRESHOLD + 1):
            for dy in range(-OUTER_THRESHOLD, OUTER_THRESHOLD + 1):
                dist = abs(dx) + abs(dy) + abs(max_d - d)
                if dist <= DISTANCE_THRESHOLD:
                    inner.append((d, dx, dy))
                elif dist <= OUTER_THRESHOLD:
                    outer.append((d, dx, dy))
    return inner, outer

def compute_all_points_and_outliers(image3d: np.ndarray,
                                         outlier_threshold: float = OUTLIER_THRESHOLD):
    """
    Vectorized version - no loop on (x, y).
    image3d shape: (depth, width, height)
    """
    depth, width, height = image3d.shape
    inner_off, outer_off = _generate_offsets(depth)

    # 2-D accumulators
    cn_sum   = np.zeros((width, height), dtype=np.float64)
    cn_count = np.zeros_like(cn_sum,        dtype=np.int32)
    on_sum   = cn_sum.copy()
    on_count = cn_count.copy()

    # inner ring ----------------------------------------------------------
    for d, dx, dy in inner_off:
        slice2d = image3d[d]
        shifted = _shift2d(slice2d, dx, dy)
        mask = shifted > 0
        cn_sum   += shifted * mask
        cn_count += mask

    # outer ring ----------------------------------------------------------
    for d, dx, dy in outer_off:
        slice2d = image3d[d]
        shifted = _shift2d(slice2d, dx, dy)
        mask = shifted > 0
        on_sum   += shifted * mask
        on_count += mask

    # deviation --------------------------------------------------------------
    valid   = (cn_count > 0) & (on_count > 0) & (image3d[-1] > 0)
    cn_mean = np.divide(cn_sum, cn_count, where=cn_count > 0)
    on_mean = np.divide(on_sum, on_count, where=on_count > 0)
    dev     = np.abs(cn_mean - on_mean)

    xs, ys        = np.where(valid)
    all_points    = list(zip(xs, ys, dev[valid]))
    outlier_mask  = valid & (dev > outlier_threshold)
    xo, yo        = np.where(outlier_mask)
    outliers      = list(zip(xo, yo, dev[outlier_mask]))

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
            window = list(self.window_state.get())  # retrieve state

            arr = value['arr']
            if len(window) == 3:
                window.pop(0)
            window.append(arr)

            self.window_state.update(window)

            if len(window) < 3:
                return  # not enough layers yet

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

            # Output tuples (CSV Row, JSON string for Q3)
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
