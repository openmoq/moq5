"""Writes marker_vectors.json: synthetic luma captures painted from the
recipe's geometry, each with an expected outcome DECLARED here in the case
table from that geometry -- never computed by a reader. The Python reader
(media_marker.py) and the player's Kotlin reader both consume the same file.

    python -I build_vectors.py            write marker_vectors.json
    python -I build_vectors.py --check    exit 1 unless the file on disk is exactly what this writes
"""
import base64
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RECIPE = json.loads((HERE / "marker_recipe.json").read_text())
SRC_W, SRC_H = RECIPE["source"]["width"], RECIPE["source"]["height"]
CELL = RECIPE["marker"]["cell"]
ROW_Y = [r["y"] for r in RECIPE["marker"]["rows"]]
BLACK, WHITE, BACKGROUND = 16, 235, 128


def cells_for(index):
    """The declared cell polarities for a frame number: {(row, cell): 'white'|'black'}."""
    out = {}
    for k in RECIPE["marker"]["bit_cells"]:
        bit = (index >> k) & 1
        out[(0, k)] = "white" if bit else "black"
        out[(1, k)] = "black" if bit else "white"
    for row, spec in enumerate(RECIPE["marker"]["rows"]):
        for cell, colour in spec["sync"].items():
            out[(row, int(cell))] = colour
    return out


def paint(w, h, cells, fill=BACKGROUND, dx=0, dy=0, levels=None):
    """A w x h luma capture of the whole source frame: background everywhere,
    each declared cell painted as a rectangle scaled from source pixels.
    dx/dy shift the painted marker by that many SOURCE pixels. `levels`
    overrides the value painted for chosen cells."""
    img = bytearray([fill]) * (w * h)
    for (row, cell), colour in cells.items():
        value = (levels or {}).get((row, cell), WHITE if colour == "white" else BLACK)
        x0, x1 = cell * CELL + dx, (cell + 1) * CELL + dx
        y0, y1 = ROW_Y[row] + dy, ROW_Y[row] + CELL + dy
        for y in range(max(0, round(y0 * h / SRC_H)), min(h, round(y1 * h / SRC_H))):
            for x in range(max(0, round(x0 * w / SRC_W)), min(w, round(x1 * w / SRC_W))):
                img[y * w + x] = value
    return bytes(img)


def uniform(w, h, value):
    return bytes([value]) * (w * h)


def cases():
    W, H = 64, 36
    c59 = cells_for(59)
    flipped = dict(cells_for(5)); flipped[(1, 2)] = flipped[(0, 2)]      # complement equals its data bit
    sync_a = dict(cells_for(9)); sync_a[(0, 6)], sync_a[(0, 7)] = "black", "white"
    sync_b = dict(cells_for(9)); sync_b[(1, 6)], sync_b[(1, 7)] = "white", "black"
    t = RECIPE["marker"]["threshold"]; m = RECIPE["marker"]["margin"]
    at_margin_hi = {(0, 0): t + m}          # data bit 0 painted exactly at the margin (reads as white)
    under_margin_hi = {(0, 0): t + m - 1}
    at_margin_lo = {(0, 1): t - m}          # data bit 1 painted exactly at the margin (reads as black)
    under_margin_lo = {(0, 1): t - m + 1}
    return [
        # (name, width, height, pixels, declared outcome, declared index)
        ("index_0", W, H, paint(W, H, cells_for(0)), "ok", 0),
        ("index_1_one_bit", W, H, paint(W, H, cells_for(1)), "ok", 1),
        ("index_32_high_bit", W, H, paint(W, H, cells_for(32)), "ok", 32),
        ("index_59_final", W, H, paint(W, H, c59), "ok", 59),
        ("index_63_all_bits", W, H, paint(W, H, cells_for(63)), "ok", 63),
        ("index_59_at_96x54", 96, 54, paint(96, 54, c59), "ok", 59),
        ("index_59_at_4_by_3", 64, 48, paint(64, 48, c59), "ok", 59),
        ("blank_black", W, H, uniform(W, H, BLACK), "blank", None),
        ("blank_black_zero", W, H, uniform(W, H, 0), "blank", None),
        ("blank_white", W, H, uniform(W, H, WHITE), "blank", None),
        ("grey_unreadable", W, H, uniform(W, H, BACKGROUND), "unreadable", None),
        ("sync_row_data_swapped", W, H, paint(W, H, sync_a), "sync-invalid", None),
        ("sync_row_complement_swapped", W, H, paint(W, H, sync_b), "sync-invalid", None),
        ("complement_bit2_equals_data", W, H, paint(W, H, flipped), "complement-invalid", None),
        # shifted right by one cell over a black background: sampled cell 0 reads background black,
        # row-A cell 6 reads painted cell 5 (bit 5 of 59 is 1: white) and cell 7 reads painted sync
        # cell 6 (white) -> cell 7 is not black
        ("shifted_one_cell_right", W, H, paint(W, H, c59, fill=BLACK, dx=CELL), "sync-invalid", None),
        # shifted down by a row and a half: both sampled rows fall on background grey
        ("shifted_down_off_rows", W, H, paint(W, H, c59, dy=3 * CELL // 2 + 40), "unreadable", None),
        ("margin_exact_high", W, H, paint(W, H, cells_for(1), levels=at_margin_hi), "ok", 1),
        ("margin_short_high", W, H, paint(W, H, cells_for(1), levels=under_margin_hi), "unreadable", None),
        ("margin_exact_low", W, H, paint(W, H, cells_for(1), levels=at_margin_lo), "ok", 1),
        ("margin_short_low", W, H, paint(W, H, cells_for(1), levels=under_margin_lo), "unreadable", None),
    ]


def document():
    return {"schema": "moq5-android-media-marker-vectors-v1",
            "recipe": "marker_recipe.json",
            "note": "expected outcome and index are declared by build_vectors.py from the recipe geometry",
            "vectors": [{"name": n, "width": w, "height": h, "luma_b64": base64.b64encode(px).decode(),
                         "expected": {"outcome": o, "index": i}} for n, w, h, px, o, i in cases()]}


def main(argv):
    text = json.dumps(document(), indent=1) + "\n"
    target = HERE / "marker_vectors.json"
    if argv[1:2] == ["--check"]:
        return 0 if target.exists() and target.read_text() == text else 1
    target.write_text(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
