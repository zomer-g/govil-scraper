/* GovMap tab — Leaflet rectangle-draw + layer picker.
 *
 * On submit, builds a synthetic govmap.gov.il URL of the form:
 *   https://www.govmap.gov.il/?lay=<id>&bbox=<xmin,ymin,xmax,ymax>  (ITM)
 * and POSTs it to the existing /api/scrape (or /api/tasks) endpoint.
 * The host's existing scraper_engine.parse_gov_url dispatches it to
 * govmap_engine.scrape_govmap. No new scrape paths needed.
 */

window.GovMapUI = (function () {
    "use strict";

    let map = null, drawnItems = null, currentLayer = null;
    let currentBboxWGS84 = null;     // [w, s, e, n]
    let layers = [];
    let activated = false;

    function activate() {
        if (activated) {
            // Leaflet re-renders cleanly only if the container had a real size
            // when first initialised; force a refresh in case the tab was
            // hidden during init.
            setTimeout(() => map && map.invalidateSize(), 50);
            return;
        }
        activated = true;
        initMap();
        loadLayers();
        bindUI();
    }

    function initMap() {
        // Centered on Israel
        map = L.map("govmapMap").setView([31.5, 35.0], 8);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
            maxZoom: 19,
            attribution: "© OpenStreetMap",
        }).addTo(map);

        drawnItems = new L.FeatureGroup();
        map.addLayer(drawnItems);

        const drawCtl = new L.Control.Draw({
            draw: {
                polyline: false, polygon: false, circle: false,
                marker: false, circlemarker: false,
                rectangle: { shapeOptions: { color: "#1a56db", weight: 2 } },
            },
            edit: { featureGroup: drawnItems, remove: true },
        });
        map.addControl(drawCtl);

        map.on(L.Draw.Event.CREATED, (e) => {
            drawnItems.clearLayers();
            drawnItems.addLayer(e.layer);
            const b = e.layer.getBounds();
            currentBboxWGS84 = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
            updateBboxDisplay();
            updateSubmitState();
        });
        map.on(L.Draw.Event.DELETED, () => {
            currentBboxWGS84 = null;
            updateBboxDisplay();
            updateSubmitState();
        });
        map.on(L.Draw.Event.EDITED, (e) => {
            e.layers.eachLayer((layer) => {
                const b = layer.getBounds();
                currentBboxWGS84 = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
            });
            updateBboxDisplay();
        });

        // Force a redraw once the tab becomes visible
        setTimeout(() => map.invalidateSize(), 100);
    }

    async function loadLayers() {
        try {
            const r = await fetch("/api/govmap/layers");
            const data = await r.json();
            layers = data.layers || [];
            const sel = document.getElementById("govmapLayerSelect");
            sel.innerHTML = '<option value="">— בחרו שכבה —</option>';
            layers.forEach((l) => {
                const opt = document.createElement("option");
                opt.value = l.id;
                opt.textContent = `${l.label_he} (${l.geometry_type})`;
                sel.appendChild(opt);
            });
        } catch (e) {
            console.error("loadLayers failed", e);
        }
    }

    function bindUI() {
        document.getElementById("govmapLayerSelect").addEventListener("change", onLayerChange);
        document.getElementById("govmapClearBbox").addEventListener("click", () => {
            drawnItems.clearLayers();
            currentBboxWGS84 = null;
            updateBboxDisplay();
            updateSubmitState();
        });
        document.getElementById("govmapPreviewCount").addEventListener("click", previewCount);
        document.getElementById("govmapSubmit").addEventListener("click", submit);
    }

    function onLayerChange(e) {
        currentLayer = layers.find((l) => l.id === e.target.value) || null;
        const meta = document.getElementById("govmapLayerMeta");
        if (!currentLayer) {
            meta.textContent = "";
        } else {
            meta.innerHTML = `<strong>${currentLayer.label_he}</strong>` +
                ` <span class="badge text-bg-light">${currentLayer.geometry_type}</span>` +
                (currentLayer.notes ? `<br><small>${escapeHtml(currentLayer.notes)}</small>` : "");
        }
        updateSubmitState();
    }

    function updateBboxDisplay() {
        const el = document.getElementById("govmapBboxDisplay");
        if (!currentBboxWGS84) {
            el.textContent = "— לא נבחרה תיבה (יסרוק את כל השכבה) —";
            return;
        }
        const [w, s, e, n] = currentBboxWGS84;
        el.innerHTML = "WGS84: " + [w, s, e, n].map(x => x.toFixed(5)).join(", ");
    }

    function updateSubmitState() {
        document.getElementById("govmapSubmit").disabled = !currentLayer;
    }

    async function previewCount() {
        if (!currentLayer) return;
        const el = document.getElementById("govmapPreviewResult");
        el.innerHTML = '<i class="bi bi-hourglass"></i> בודק...';
        const body = { layer: currentLayer.id };
        if (currentBboxWGS84) {
            body.bbox = currentBboxWGS84;
            body.srs = "WGS84";
        } else {
            // Country-wide BBOX in WGS84
            body.bbox = [33.5, 29.0, 36.5, 33.5];
            body.srs = "WGS84";
        }
        try {
            const r = await fetch("/api/govmap/preview-bbox", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });
            const data = await r.json();
            if (!r.ok) {
                el.innerHTML = `<span class="text-danger">${data.error || r.status}</span>`;
                return;
            }
            const cls = data.count > 50000 ? "text-danger" :
                        data.count > 10000 ? "text-warning" : "text-success";
            el.innerHTML = `<span class="${cls}"><strong>${data.count.toLocaleString()}</strong> פיצ'רים</span>`;
        } catch (e) {
            el.innerHTML = `<span class="text-danger">שגיאת רשת</span>`;
        }
    }

    async function submit() {
        if (!currentLayer) return;

        // Build a synthetic govmap.gov.il URL. The standard /api/scrape
        // endpoint dispatches through govmap_engine.scrape_govmap based
        // on the host. The `lay` and `bbox_wgs84` params are parsed by
        // govmap_engine.parse_bbox_from_url server-side.
        const numericId = currentLayer.type_name.replace("govmap:layer_", "");
        let url = "https://www.govmap.gov.il/?lay=" + encodeURIComponent(numericId);
        if (currentBboxWGS84) {
            url += "&bbox_wgs84=" + currentBboxWGS84.join(",");
        }

        const mode = document.getElementById("govmapMode").value;
        const status = document.getElementById("govmapStatus");
        status.innerHTML = '<i class="bi bi-hourglass"></i> פותח משימה...';

        try {
            const endpoint = mode === "server" ? "/api/scrape" : "/api/tasks";
            const body = mode === "server"
                ? { url }
                : { url, mode };
            const r = await fetch(endpoint, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });
            const data = await r.json();
            if (!r.ok) {
                status.innerHTML = '<span class="text-danger">' +
                    escapeHtml(data.error || String(r.status)) + '</span>';
                return;
            }
            const id = data.job_id || data.task_id;
            status.innerHTML =
                '<span class="text-success">משימה החלה: <code>' +
                escapeHtml(id) + '</code>.</span><br>' +
                'עברו ללשונית <strong>סריקה</strong> או <strong>אוספים</strong> ' +
                'כדי לעקוב אחר ההתקדמות.';
        } catch (e) {
            status.innerHTML = '<span class="text-danger">שגיאת רשת: ' +
                escapeHtml(e.message) + '</span>';
        }
    }

    function escapeHtml(s) {
        return String(s || "").replace(/[&<>"']/g, (c) => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
        }[c]));
    }

    return { activate };
})();
