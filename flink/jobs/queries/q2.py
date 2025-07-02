import json
import base64
import io
import numpy as np
from PIL import Image
import logging

from pyflink.common import Types, Row
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, ListStateDescriptor
from pyflink.datastream import OutputTag

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q2")

THRESHOLD = 6000
WINDOW_SIZE = 3

def get_neighbors_coords():
    coords = []
    for dx in range(-4, 5):
        for dy in range(-4, 5):
            dist = abs(dx) + abs(dy)
            if 0 <= dist <= 4:
                coords.append((dx, dy))
    return coords

NEIGHBORS_0_2 = [p for p in get_neighbors_coords() if 0 <= abs(p[0]) + abs(p[1]) <= 2]
NEIGHBORS_3_4 = [p for p in get_neighbors_coords() if 2 < abs(p[0]) + abs(p[1]) <= 4]

q2_output_tag = OutputTag('q2-output', Types.ROW([
    Types.INT(), Types.STRING(), Types.INT(),
    Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT(),
    Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT(),
    Types.STRING(), Types.FLOAT()
]))

outliers_output_tag = OutputTag('outliers-output', Types.STRING())

class SlidingWindowProcessFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        descriptor = ListStateDescriptor("layers_state", Types.PICKLED_BYTE_ARRAY())
        self.layers_state = runtime_context.get_list_state(descriptor)

    def deserialize_layer(self, b64tif):
        tif_bytes = base64.b64decode(b64tif)
        img = Image.open(io.BytesIO(tif_bytes)).convert("I;16")
        return np.array(img)

    def compute_deviation_per_point(self, layers):
        h, w = layers[-1].shape
        arr_3d = np.stack(layers, axis=0)
        deviations = np.zeros((h, w), dtype=np.float32)

        for x in range(h):
            for y in range(w):
                internal_vals = [arr_3d[i, x + dx, y + dy]
                                 for dx, dy in NEIGHBORS_0_2
                                 for i in range(WINDOW_SIZE)
                                 if 0 <= x + dx < h and 0 <= y + dy < w and arr_3d[i, x + dx, y + dy] != 0]
                external_vals = [arr_3d[i, x + dx, y + dy]
                                 for dx, dy in NEIGHBORS_3_4
                                 for i in range(WINDOW_SIZE)
                                 if 0 <= x + dx < h and 0 <= y + dy < w and arr_3d[i, x + dx, y + dy] != 0]

                if internal_vals and external_vals:
                    deviations[x, y] = abs(np.mean(internal_vals) - np.mean(external_vals))
        return deviations

    def process_element(self, value, ctx):
        try:
            data = json.loads(value)
            batch_id = int(data["batch_id"])
            print_id = data["print_id"]
            tile_id = int(data["tile_id"])
            layer = int(data["layer"])

            arr = self.deserialize_layer(data["tif"])

            current_layers = list(self.layers_state.get())
            if len(current_layers) >= WINDOW_SIZE:
                current_layers.pop(0)
            current_layers.append(arr)

            self.layers_state.clear()
            for l in current_layers:
                self.layers_state.add(l)

            if len(current_layers) == WINDOW_SIZE:
                deviations = self.compute_deviation_per_point(current_layers)
                outliers = [((x, y), float(deviations[x, y]))
                            for x in range(deviations.shape[0])
                            for y in range(deviations.shape[1])
                            if deviations[x, y] > THRESHOLD]
                outliers.sort(key=lambda tup: tup[1], reverse=True)
                top5 = outliers[:5]

                row_values = [batch_id, print_id, tile_id]
                for (x, y), dev in top5:
                    row_values.extend([f"{x}_{y}", dev])
                for _ in range(5 - len(top5)):
                    row_values.extend(["", 0.0])

                ctx.output(q2_output_tag, Row(*row_values))

                outliers_output = {
                    "batch_id": batch_id,
                    "print_id": print_id,
                    "tile_id": tile_id,
                    "outliers": [{"x": x, "y": y, "deviation": d} for (x, y), d in outliers]
                }
                ctx.output(outliers_output_tag, json.dumps(outliers_output))

        except Exception as e:
            logger.error(f"Error in Q2 processing: {e}")

