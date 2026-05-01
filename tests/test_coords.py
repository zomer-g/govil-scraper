"""Round-trip tests for ITM <-> WGS84 transforms.

Reference points:
- Tel Aviv City Hall:   WGS84 (34.7806, 32.0853)  ~ ITM (178686, 663551)
- Jerusalem Old City:   WGS84 (35.2345, 31.7767)  ~ ITM (222290, 631840)
- Eilat Airport:        WGS84 (34.9571, 29.5526)  ~ ITM (191600, 384700)

Tolerances are generous: a few metres for ITM, a few arc-seconds for WGS84.
"""

import math
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from coords import (
    itm_to_wgs84, wgs84_to_itm,
    itm_to_wgs84_bbox, wgs84_to_itm_bbox,
    itm_geom_to_wgs84, esri_to_geojson, geom_to_wkt,
)


def _approx(a, b, tol):
    return abs(a - b) < tol


def test_round_trip_tel_aviv():
    # ITM (178686, 663551) is around Allenby St, central Tel Aviv.
    lon, lat = itm_to_wgs84(178686, 663551)
    assert _approx(lon, 34.78, 0.02), f"lon was {lon}"
    assert _approx(lat, 32.06, 0.02), f"lat was {lat}"

    x, y = wgs84_to_itm(lon, lat)
    assert _approx(x, 178686, 5), f"x was {x}"
    assert _approx(y, 663551, 5), f"y was {y}"


def test_round_trip_jerusalem():
    lon, lat = itm_to_wgs84(222290, 631840)
    assert _approx(lon, 35.2345, 0.01), f"lon was {lon}"
    assert _approx(lat, 31.7767, 0.01), f"lat was {lat}"


def test_bbox_round_trip():
    itm = (178000, 663000, 180000, 665000)
    wgs = itm_to_wgs84_bbox(itm)
    back = wgs84_to_itm_bbox(wgs)
    for a, b in zip(itm, back):
        assert _approx(a, b, 1.0)


def test_esri_point_to_geojson():
    g = esri_to_geojson({"x": 178686.0, "y": 663551.0}, "esriGeometryPoint")
    assert g == {"type": "Point", "coordinates": [178686.0, 663551.0]}


def test_esri_polygon_to_geojson():
    rings = [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
    g = esri_to_geojson({"rings": rings}, "esriGeometryPolygon")
    assert g["type"] == "Polygon"
    assert g["coordinates"] == [rings[0]]


def test_geom_transform_recursive():
    poly_itm = {
        "type": "Polygon",
        "coordinates": [[[178000, 663000], [180000, 663000],
                         [180000, 665000], [178000, 665000],
                         [178000, 663000]]],
    }
    poly_wgs = itm_geom_to_wgs84(poly_itm)
    assert poly_wgs["type"] == "Polygon"
    # Each coord is now (lon, lat) somewhere around (34.78, 32.08)
    for ring in poly_wgs["coordinates"]:
        for x, y in ring:
            assert 34.0 < x < 36.0
            assert 31.0 < y < 33.0


def test_wkt_point():
    assert geom_to_wkt({"type": "Point", "coordinates": [1, 2]}) == "POINT(1 2)"


def test_wkt_polygon():
    g = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
    assert geom_to_wkt(g) == "POLYGON((0 0, 1 0, 1 1, 0 0))"


if __name__ == "__main__":
    # Run without pytest
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"OK   {name}")
            except AssertionError as e:
                print(f"FAIL {name}: {e}")
