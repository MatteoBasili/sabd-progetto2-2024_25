import json
import base64
import io
import numpy as np
from PIL import Image
import logging
from pyflink.common import Row

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q1")

LOW_THRESH = 5000
HIGH_THRESH = 65000

def process_image_array(arr, low_thresh=LOW_THRESH, high_thresh=HIGH_THRESH):
    saturated_count = int(np.sum(arr > high_thresh))
    mask = (arr >= low_thresh) & (arr <= high_thresh)
    filtered_arr = np.where(mask, arr, 0)
    return saturated_count, filtered_arr

def process_batch(raw):
    try:
        data = json.loads(raw)

        required = ["batch_id", "print_id", "tile_id", "layer", "tif"]
        if not all(k in data for k in required):
            logger.warning(f"Incomplete input: {data}")
            return None

        batch_id = int(data["batch_id"])
        tile_id = int(data["tile_id"])
        tif_bytes = base64.b64decode(data["tif"])
        img = Image.open(io.BytesIO(tif_bytes)).convert("I;16")
        arr = np.array(img)

        saturated, filtered_arr = process_image_array(arr)

        filtered_img = Image.fromarray(filtered_arr)
        with io.BytesIO() as output:
            filtered_img.save(output, format="TIFF")
            filtered_bytes = output.getvalue()

        filtered_b64 = base64.b64encode(filtered_bytes).decode("utf-8")

        metrics = Row(
            batch_id=batch_id,
            print_id=str(data["print_id"]),
            tile_id=tile_id,
            saturated=saturated
        )

        filtered_dict = {
            "batch_id": str(batch_id),
            "print_id": str(data["print_id"]),
            "tile_id": str(tile_id),
            "layer": str(data["layer"]),
            "tif": filtered_b64,
            "saturated": str(saturated) 
        }

        return metrics, filtered_dict

    except Exception as e:
        logger.error(f"[process_batch] Error: {e}")
        return None

