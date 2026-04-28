"""
Build a master parcels CSV from the Israel Mapping Center shapefiles.

Sources (download manually from data.gov.il — large files, may need browser):
  - gushim.zip   — https://data.gov.il/he/datasets/israel_mapping_center/subgushallshape  (~64 MB)
  - helkot.zip   — https://data.gov.il/he/datasets/israel_mapping_center/shape           (~668 MB)

Output CSV columns:
  gush, chelka, locality, area_sqm, centroid_lat, centroid_lon

Usage:
  python catalog/parcels_shapefile.py gushim.zip helkot.zip parcels.csv
  python catalog/parcels_shapefile.py --inspect gushim.zip          # print fields, exit
  python catalog/parcels_shapefile.py --inspect helkot.zip

The Israel Mapping Center publishes geometries in ITM (EPSG:2039). We reproject
centroids to WGS84 (EPSG:4326) so they're usable as lat/lon directly.
"""
import argparse
import csv
import logging
import os
import sys
import tempfile
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Field names vary across releases. We probe candidates and pick the first hit.
GUSH_FIELD_CANDIDATES = ["GUSH_NUM", "gush_num", "GUSH", "gush", "GUSHNUM"]
CHELKA_FIELD_CANDIDATES = ["PARCEL", "parcel", "CHELKA", "chelka", "PARCEL_NUM", "PARCELNUM"]
LOCALITY_FIELD_CANDIDATES = [
    "LOCALITY_N", "LOCALITY", "SETL_NAME", "settlement", "MUNI_HEB", "MUNICIPAL",
]
AREA_FIELD_CANDIDATES = ["AREA", "area", "Shape_Area", "SHAPE_AREA", "gush_area", "PARCEL_ARE"]


def _open_shp(zip_path: str):
    """Open a shapefile from inside a zip; return (Reader, tmpdir_to_cleanup)."""
    import shapefile
    tmp = tempfile.mkdtemp(prefix="shp_")
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(tmp)
    # Find the .shp file
    shp = None
    for p in Path(tmp).rglob("*.shp"):
        shp = str(p)
        break
    if not shp:
        raise FileNotFoundError(f"No .shp file inside {zip_path}")
    reader = shapefile.Reader(shp, encoding="cp1255")
    return reader, tmp


def _pick_field(field_names: list[str], candidates: list[str]) -> str | None:
    lower = {f.lower(): f for f in field_names}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def _polygon_centroid(parts_points: list[list[tuple[float, float]]]) -> tuple[float, float]:
    """Area-weighted centroid of a (possibly multi-part) polygon in (x, y).
    Uses the shoelace centroid formula on the largest ring as a robust default.
    """
    largest = max(parts_points, key=lambda ring: abs(_signed_area(ring)))
    return _ring_centroid(largest)


def _signed_area(ring: list[tuple[float, float]]) -> float:
    a = 0.0
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        a += x1 * y2 - x2 * y1
    return a / 2.0


def _ring_centroid(ring: list[tuple[float, float]]) -> tuple[float, float]:
    a = _signed_area(ring)
    if a == 0:
        # Degenerate ring — fall back to mean of vertices.
        xs = [p[0] for p in ring]
        ys = [p[1] for p in ring]
        return (sum(xs) / len(xs), sum(ys) / len(ys))
    cx = cy = 0.0
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        f = x1 * y2 - x2 * y1
        cx += (x1 + x2) * f
        cy += (y1 + y2) * f
    return (cx / (6 * a), cy / (6 * a))


def _build_transformer(prj_path: str | None):
    """Build an ITM→WGS84 transformer. If we can read the .prj and it's already
    WGS84 we return identity; otherwise default to EPSG:2039→4326."""
    from pyproj import Transformer
    src_epsg = 2039  # Israel TM Grid (default for IMC publications)
    if prj_path and os.path.exists(prj_path):
        try:
            with open(prj_path) as f:
                wkt = f.read()
            if "WGS_1984" in wkt or "WGS 84" in wkt or "4326" in wkt:
                return None  # already lat/lon
        except OSError:
            pass
    return Transformer.from_crs(f"EPSG:{src_epsg}", "EPSG:4326", always_xy=True)


def inspect(zip_path: str):
    reader, tmp = _open_shp(zip_path)
    try:
        print(f"=== {zip_path} ===")
        print(f"shape type: {reader.shapeTypeName}")
        print(f"records:    {len(reader)}")
        print(f"fields:")
        for f in reader.fields[1:]:  # skip DeletionFlag
            print(f"  {f[0]:20s} type={f[1]} len={f[2]} dec={f[3]}")
        rec = reader.record(0)
        print(f"sample record:")
        for f, v in zip([f[0] for f in reader.fields[1:]], rec):
            print(f"  {f:20s} = {v!r}")
    finally:
        try: reader.close()
        except Exception: pass


def build(gushim_zip: str, helkot_zip: str, out_csv: str):
    """Read both shapefiles, join chelka→gush by gush number, write parcels.csv."""
    g_reader, g_tmp = _open_shp(gushim_zip)
    try:
        g_fields = [f[0] for f in g_reader.fields[1:]]
        g_gush_f = _pick_field(g_fields, GUSH_FIELD_CANDIDATES)
        g_loc_f = _pick_field(g_fields, LOCALITY_FIELD_CANDIDATES)
        if not g_gush_f:
            raise RuntimeError(
                f"Could not find gush field in {gushim_zip}. "
                f"Available: {g_fields}. "
                "Run with --inspect to see actual field names and update GUSH_FIELD_CANDIDATES."
            )
        logger.info("Gushim: gush=%s locality=%s (%d records)",
                    g_gush_f, g_loc_f, len(g_reader))
        gush_to_locality: dict[str, str] = {}
        for rec in g_reader.records():
            gush = str(rec[g_fields.index(g_gush_f)])
            loc = str(rec[g_fields.index(g_loc_f)]) if g_loc_f else ""
            gush_to_locality[gush] = loc
    finally:
        try: g_reader.close()
        except Exception: pass

    # Now stream helkot
    h_reader, h_tmp = _open_shp(helkot_zip)
    try:
        h_fields = [f[0] for f in h_reader.fields[1:]]
        h_gush_f = _pick_field(h_fields, GUSH_FIELD_CANDIDATES)
        h_chelka_f = _pick_field(h_fields, CHELKA_FIELD_CANDIDATES)
        h_area_f = _pick_field(h_fields, AREA_FIELD_CANDIDATES)
        if not h_gush_f or not h_chelka_f:
            raise RuntimeError(
                f"Could not find gush/chelka fields in {helkot_zip}. "
                f"Available: {h_fields}. "
                "Run with --inspect to see actual field names."
            )
        logger.info("Helkot: gush=%s chelka=%s area=%s (%d records)",
                    h_gush_f, h_chelka_f, h_area_f, len(h_reader))

        # Detect CRS from .prj sibling
        prj_candidates = list(Path(h_tmp).rglob("*.prj"))
        prj_path = str(prj_candidates[0]) if prj_candidates else None
        transformer = _build_transformer(prj_path)
        logger.info("CRS transform: %s", "identity (already WGS84)"
                    if transformer is None else "EPSG:2039 → EPSG:4326")

        with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["gush", "chelka", "locality", "area_sqm",
                        "centroid_lat", "centroid_lon"])

            n = 0
            for shape, rec in zip(h_reader.iterShapes(), h_reader.iterRecords()):
                gush = str(rec[h_fields.index(h_gush_f)])
                chelka = str(rec[h_fields.index(h_chelka_f)])
                area = rec[h_fields.index(h_area_f)] if h_area_f else ""
                locality = gush_to_locality.get(gush, "")

                # Build per-part rings from shape.points + shape.parts
                points = list(shape.points)
                parts = list(shape.parts) + [len(points)]
                rings = [points[parts[i]:parts[i + 1]] for i in range(len(parts) - 1)]
                lat = lon = ""
                if rings and rings[0]:
                    try:
                        cx, cy = _polygon_centroid(rings)
                        if transformer is not None:
                            # always_xy=True → (lon, lat) order
                            lon, lat = transformer.transform(cx, cy)
                        else:
                            lon, lat = cx, cy
                    except Exception as e:
                        logger.warning("centroid failed for %s-%s: %s", gush, chelka, e)

                w.writerow([gush, chelka, locality, area, lat, lon])

                n += 1
                if n % 50000 == 0:
                    logger.info("processed %d parcels", n)

            logger.info("done — %d parcels written to %s", n, out_csv)
    finally:
        try: h_reader.close()
        except Exception: pass


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--inspect", action="store_true",
                    help="Print field names + sample record from a single ZIP and exit")
    ap.add_argument("paths", nargs="+", help="gushim.zip helkot.zip out.csv  OR  --inspect file.zip")
    args = ap.parse_args()

    if args.inspect:
        for p in args.paths:
            inspect(p)
        return

    if len(args.paths) != 3:
        ap.error("Build mode requires three positional args: gushim.zip helkot.zip out.csv")
    build(args.paths[0], args.paths[1], args.paths[2])


if __name__ == "__main__":
    main()
