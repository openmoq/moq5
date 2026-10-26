"""The frame-index marker reader, over a luma capture of the whole displayed
frame. Coordinates are FRACTIONS of the capture, derived from the recipe's
source geometry, so a capture of any size or aspect maps onto the same cells.

Outcomes, in the order they are decided:
  capture-empty / capture-null / capture-size  the capture itself is unusable
  unreadable          some cell is within the margin of the threshold
  blank               every cell reads, all one polarity: no marker present
  sync-invalid        mixed polarity, but a sync pair is not as declared
  complement-invalid  sync is right, but a data cell equals its complement
  ok                  the frame number
"""
import json
from collections import namedtuple
from pathlib import Path

RECIPE = json.loads((Path(__file__).resolve().parent / "marker_recipe.json").read_text())
_M = RECIPE["marker"]
_SRC_W, _SRC_H = RECIPE["source"]["width"], RECIPE["source"]["height"]
_CELL, _BITS = _M["cell"], _M["bit_cells"]
_THRESHOLD, _MARGIN = _M["threshold"], _M["margin"]
_ROWS = [(r["y"], {int(c): colour == "white" for c, colour in r["sync"].items()}) for r in _M["rows"]]
_NCELLS = len(_BITS) + len(_ROWS[0][1])

Reading = namedtuple("Reading", "outcome index")


def _centre(cell, row_y, w, h):
    fx = (cell * _CELL + _CELL / 2) / _SRC_W
    fy = (row_y + _CELL / 2) / _SRC_H
    return min(w - 1, int(fx * w)), min(h - 1, int(fy * h))


def classify(luma, width, height):
    if width <= 0 or height <= 0:
        return Reading("capture-empty", None)
    if luma is None:
        return Reading("capture-null", None)
    if len(luma) != width * height:
        return Reading("capture-size", None)
    rows = []
    for row_y, _sync in _ROWS:
        values = []
        for cell in range(_NCELLS):
            x, y = _centre(cell, row_y, width, height)
            values.append(luma[y * width + x])
        rows.append(values)
    flat = [v for r in rows for v in r]
    if any(abs(v - _THRESHOLD) < _MARGIN for v in flat):
        return Reading("unreadable", None)
    bits = [[v > _THRESHOLD for v in r] for r in rows]
    if all(b for r in bits for b in r) or not any(b for r in bits for b in r):
        return Reading("blank", None)
    for (_row_y, sync), row_bits in zip(_ROWS, bits):
        if any(row_bits[cell] != white for cell, white in sync.items()):
            return Reading("sync-invalid", None)
    data, complement = bits[0], bits[1]
    if any(data[k] == complement[k] for k in _BITS):
        return Reading("complement-invalid", None)
    return Reading("ok", sum(1 << k for k in _BITS if data[k]))
