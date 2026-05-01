# GovMap API — Reverse-Engineered Notes

This file documents the actual GovMap REST endpoints we discovered by
reverse-engineering the GovMap web app's JS bundle (April 2026 build,
`/assets/index-6a9ac5f8.js`). All findings here are **verified against
the live server** unless explicitly marked otherwise.

## TL;DR — the endpoints we use

| Purpose | URL | Method | Auth |
|---|---|---|---|
| Layer field metadata | `/api/spatial-analysis/layer/{layerId}/metadata-fields?layerName={name}` | GET | none |
| **Feature catalog (all layers)** | `/api/geoserver/wfs?service=WFS&request=GetCapabilities` | GET | none |
| **Layer feature schema** | `/api/geoserver/wfs?...&request=DescribeFeatureType&typeName={typeName}` | GET | none |
| **Feature data** | `/api/geoserver/wfs?...&request=GetFeature&typeNames={typeName}&outputFormat=application/json` | GET | none |
| Feature count (cheap) | as above with `resultType=hits` | GET | none |

**Headers required:** none (other than browser-like User-Agent for cloudscraper).
**Origin/Referer:** not strictly required for the WFS endpoints.

The WFS endpoint is a public **OGC WFS 2.0** GeoServer instance.
No auth is required for any of the layers we tested.

## Layer naming

GovMap layers are addressed several ways:

- **Numeric ID** in the URL: `?lay=220826`
- **WFS type name**: `govmap:layer_220826` or `govmap:layer_<semantic>`
  (e.g. `govmap:layer_fire_areas`, `govmap:layer_district_border`)
- **Internal layer name**: `LAYER_220826` (used by `/spatial-analysis/`
  endpoints; rarely needed for our purposes)

Our `layer_catalog.py` resolves all three: `Layer.id` is an
ASCII-stable internal id, `Layer.type_name` is the WFS type name.

## Quirks & pitfalls (the hard-won kind)

### 1. `startIndex` is rejected on this WFS instance

WFS 2.0 paging via `startIndex` returns **HTTP 400 "Unknown error"** on
GovMap, even with `startIndex=0`. The parameter must be **omitted entirely**.

**Solution**: For layers with N ≤ 5000 features, request all in one shot.
For larger layers, paginate via `CQL_FILTER=objectid > {last_id} ORDER BY objectid`.
Implementation lives in `wfs_client.py::iter_features`.

### 2. `outputFormat=application/json` returns GeoJSON in EPSG:3857 by default

WFS GeoJSON convention is WGS84, but GovMap defaults to Web Mercator.
Pass `srsName=EPSG:4326` explicitly to get proper WGS84 output and skip
client-side reprojection.

### 3. The `/api/layers-catalog/...` endpoints are gated

`POST /api/layers-catalog/api/auth` requires a valid third-party API
token (issued via the developer programme at api.govmap.gov.il). Without
it, every other catalog endpoint returns `{"error":"access denied"}`.

We don't need it: WFS GetCapabilities provides the full layer list, and
WFS GetFeature serves the data.

### 4. The `/api/spatial-analysis/layer-data` endpoint exists but is
unreliable for raw scraping

It accepts a `shapeFilterGeoJson` polygon and a `layer.layerName` and is
the same endpoint used by GovMap's "spatial analysis" UI. In our tests
it returned `[]` for layers it should have populated, suggesting it's
configured per-layer for spatial-join use cases only. **Use WFS
GetFeature instead.**

### 5. The catalog of `~52,000` layers is huge

`GetCapabilities` returns ~25 MB of XML on GovMap. Cache it locally if
you need to enumerate layers. For day-to-day use, just hard-code the
specific `type_name`s you care about into `layers.seed.json`.

### 6. Layer 220826 vs `govmap:layer_220826` vs `LAYER_220826`

These all refer to the same layer (fire training zone orders). The
fields on it are identical. Different endpoints accept different
representations:

- WFS: `govmap:layer_220826`
- `/api/spatial-analysis/layer/{id}/metadata-fields`: accepts `220826`,
  responds with `layerName: "LAYER_220826"`

## Verified layers (in `layers.seed.json`)

| ID | type_name | Geometry | Features | Description |
|---|---|---|---|---|
| `FIRE_AREAS_ORDERS` | `govmap:layer_220826` | MultiPolygon | **199** | צווי שטחי אש (IDF firing zone orders by Chief of Staff) |
| `FIRE_AREAS` | `govmap:layer_fire_areas` | Polygon | TBD | שטחי אש (semantic alias, present in WFS catalog) |
| `FIRE_STATIONS` | `govmap:layer_fire_stations` | Point | TBD | תחנות כיבוי אש |
| `FIRE_ADMIN` | `govmap:layer_fire_admin` | Polygon | TBD | אזורי שירותי כבאות |
| `FIRE_FREQUENCY` | `govmap:layer_sviva_fire_frequency` | Polygon | TBD | תדירות שריפות |
| `DISTRICT_BORDERS` | `govmap:layer_district_border` | Polygon | TBD | גבולות מחוז |
| `NATURE_RESERVE_ORDERS` | `govmap:layer_nature_reserves_orders` | Polygon | TBD | צווי שמורות טבע |
| `DEAD_SEA_BORDERS` | `govmap:layer_dead_sea_borders` | Polygon | TBD | גבולות ים המלח |

## Runbook: adding a new layer

1. **Find the layer's URL on GovMap**.
   Browse to `https://www.govmap.gov.il/`, enable a layer through the
   sidebar, then look at the URL — it'll have `?...&lay=<numeric_id>`.
2. **Confirm it exists in WFS**. Run:
   ```python
   from scraper_engine import GovMapSession
   from wfs_client import WFSClient
   s = GovMapSession(); s.warm()
   c = WFSClient(s)
   print(c.hits('govmap:layer_<id>'))   # should return >0
   ```
3. **Get the field schema**:
   ```python
   meta = c.describe_feature_type('govmap:layer_<id>')
   print(meta.field_names)
   ```
4. **Add an entry to `layers.seed.json`**:
   ```json
   {
     "id": "MY_LAYER",
     "label_he": "שם בעברית",
     "label_en": "English name",
     "type_name": "govmap:layer_<id>",
     "endpoint_kind": "wfs",
     "geometry_type": "Polygon|Point|LineString|MultiPolygon",
     "out_fields": ["*"],
     "notes": "..."
   }
   ```
5. **Smoke-test end-to-end**:
   ```python
   from scraper_engine import GovMapSession, GovMapScraper
   s = GovMapSession(); s.warm()
   r = GovMapScraper(s).scrape('MY_LAYER')
   print(r.total_count, r.geometry_type)
   ```

## Endpoint inventory (everything we found in the JS bundle)

These are reachable but **not all of them respond without auth**.
Anything marked "access denied" is gated behind the third-party API
token.

```
PUBLIC (no auth):
  GET  /api/spatial-analysis/layer/{id}/metadata-fields?layerName=...
  GET  /api/geoserver/wfs    (full OGC WFS 2.0)
  GET  /api/geoserver/ows    (alias for /wfs)
  POST /api/spatial-analysis/layer-data       (returns [] in practice)
  POST /api/spatial-analysis/select-feature-on-map  (same)

GATED ("access denied" — needs third-party API token):
  POST /api/layers-catalog/api/auth          (issues a session)
  GET  /api/layers-catalog/catalog
  GET  /api/layers-catalog/layer/{id}
  POST /api/layers-catalog/entitiesByPoint
  POST /api/layers-catalog/entitiesByField
  POST /api/layers-catalog/layer/{id}/filter/count
  POST /api/layers-catalog/layers/extent
  GET  /api/layers-catalog/layer-sample/{id}
  POST /api/layers-catalog/baseLayers
  ... (full enumeration in JS bundle, ~30 endpoints)
```

## OVER integration

Workers package each scrape into a ZIP that includes:
- `<collector_name>.csv` — flat attribute table (utf-8-sig, opens in
  Excel with Hebrew intact)
- `<collector_name>.xlsx` — same data with RTL formatting
- `<collector_name>.geojson` — canonical spatial artifact (WGS84)
- `manifest.json` — sidecar metadata (layer id, bbox, geometry_type,
  feature_count, srs)

The server's `/api/collections/upload` endpoint reads `manifest.json`
to populate the geo columns of the `collections` table. From there,
`over_worker.py` (unchanged from GOV scraper) pushes the CSV +
attachments — including the `.geojson` file — to OVER.
