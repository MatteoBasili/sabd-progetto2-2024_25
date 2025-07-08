import json
import base64
import io
import numpy as np
from PIL import Image
import logging
from pyflink.common import Row

### For metrics evaluation ###
#import time
##############################

from config import SATURATION_THRESHOLD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q1")

def count_saturated(arr,
                    saturation_thresh = SATURATION_THRESHOLD):
    saturated_count = int(np.sum(arr > saturation_thresh))
    return saturated_count

def process_batch(raw):
    try:
        ### For metrics evaluation ###
        #start = time.perf_counter()
        ##############################
        
        data = json.loads(raw)

        required = ["batch_id", "print_id", "tile_id", "layer", "tif"]
        if not all(k in data for k in required):
            logger.warning(f"Incomplete input: {data}")
            return None

        batch_id = int(data["batch_id"])
        tile_id = int(data["tile_id"])
        tif_bytes = base64.b64decode(data["tif"])
        img = Image.open(io.BytesIO(tif_bytes))
        arr = np.array(img)

        saturated = count_saturated(arr)

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
            "tif": data["tif"],
            "saturated": str(saturated) 
        }
        
        ### For metrics evaluation ###
        #latency_ms = (time.perf_counter() - start) * 1_000
        #logger.info(f"METRICS|Q1|batch={batch_id}|latency_ms={latency_ms:.2f}")
        ##############################

        return metrics, filtered_dict

    except Exception as e:
        logger.error(f"[Q1] [process_batch] Error: {e}")
        return None

