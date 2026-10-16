"use strict";

// The map's state comes from two places: /api/live for everything once (and every
// half minute for the windowed numbers), /api/stream for vehicles as they move.
// Without a server (the copy on GitHub Pages) it plays back data/replay.json instead:
// a recorded hour that went through the same Kafka pipeline in CI.

const STALE_S = 180;
const HISTORY_MS = 2 * 60 * 60 * 1000;
const QUIET_MS = 15 * 60 * 1000; // a line with no window for this long is not running now
const ZAGREB = [45.807, 15.975];
const CELL_M = 250;

const decimal = new Intl.NumberFormat("hr-HR", { maximumFractionDigits: 1, minimumFractionDigits: 1 });
const whole = new Intl.NumberFormat("hr-HR", { maximumFractionDigits: 0 });
const hhmm = new Intl.DateTimeFormat("hr-HR", { timeZone: "Europe/Zagreb", hour: "2-digit", minute: "2-digit" });
const day = new Intl.DateTimeFormat("hr-HR", { timeZone: "Europe/Zagreb", day: "numeric", month: "numeric", year: "numeric" });

const state = {
  clock: 0,
  version: 0,
  vehicles: new Map(), // id -> vehicle
  markers: new Map(), // id -> Leaflet marker
  lines: {},
  alerts: [],
  hotspots: [],
  selected: null,
  showBuses: false,
  showSpots: true,
};

const $ = (id) => document.getElementById(id);

// ---------------------------------------------------------------- the map

const map = L.map("map", { zoomControl: false, attributionControl: true, minZoom: 11, maxZoom: 18 }).setView(ZAGREB, 13);
L.control.zoom({ position: "topleft" }).addTo(map);
map.attributionControl.setPrefix('<a href="https://leafletjs.com">Leaflet</a>');

// OpenStreetMap's own tiles: no key needed. They are full colour, so style.css mutes them
// (and turns them dark in dark mode) to keep the vehicles the brightest thing on the map.
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
}).addTo(map);

const dark = window.matchMedia("(prefers-color-scheme: dark)");
dark.addEventListener("change", () => drawSpots());

const spotLayer = L.layerGroup().addTo(map);
const vehicleLayer = L.layerGroup().addTo(map);

// vehicles glide to new positions, but not while the map zooms
const mapEl = $("map");
const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
map.on("zoomstart", () => mapEl.classList.add("still"));
map.on("zoomend", () => setTimeout(() => mapEl.classList.remove("still"), 60));
if (!reduceMotion) mapEl.classList.add("glide");

// the sheet covers the right side of the map (or its bottom on a phone): [right, bottom] in pixels
function covered() {
  const sheet = document.querySelector(".sheet").getBoundingClientRect();
  const box = mapEl.getBoundingClientRect();
  if (sheet.width < box.width * 0.9) return [box.right - sheet.left, 0];
  return [0, box.bottom - sheet.top];
}

// fly to a place so that it ends up in the part of the map that can be seen
function focus(bounds, maxZoom) {
  map.flyToBounds(bounds, { maxZoom, paddingBottomRight: covered(), duration: reduceMotion ? 0 : 0.8 });
}

// centre the city in the visible part, not under the sheet
map.panBy(covered().map((px) => px / 2), { animate: false });

function css(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// ---------------------------------------------------------------- helpers

function isTramLine(route) {
  const n = Number(route);
  return Number.isInteger(n) && ((n >= 1 && n <= 17) || (n >= 31 && n <= 34));
}

function speedClass(v) {
  if (v.stopped || v.speed_kmh < 3) return "s0";
  if (v.speed_kmh < 12) return "s1";
  if (v.speed_kmh < 25) return "s2";
  return "s3";
}

function plural(n, one, many) {
  const last = n % 10;
  const lastTwo = n % 100;
  return last === 1 && lastTwo !== 11 ? one : many;
}

function ago(ts) {
  const s = Math.max(0, state.clock - ts);
  if (s < 60) return `prije ${s} s`;
  return `prije ${Math.round(s / 60)} min`;
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === "class") node.className = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  for (const child of children) if (child != null) node.append(child);
  return node;
}

function badge(route) {
  return el("span", { class: "badge" }, route);
}

// ---------------------------------------------------------------- vehicles

function icon(v) {
  if (!v.tram) {
    return L.divIcon({ className: "vehicle", html: '<div class="bus"></div>', iconSize: [9, 9], iconAnchor: [4.5, 4.5] });
  }
  const moving = v.heading != null && !v.stopped;
  const nose = moving ? `<i style="transform:rotate(${Math.round(v.heading)}deg)"></i>` : "";
  return L.divIcon({
    className: "vehicle",
    html: `<div class="tram ${speedClass(v)}">${nose}${v.route_id}</div>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12],
  });
}

function iconKey(v) {
  return `${v.tram}|${v.route_id}|${speedClass(v)}|${v.stopped ? "" : Math.round((v.heading ?? -1) / 10)}`;
}

function tooltip(v) {
  const kind = v.tram ? "Tramvaj" : "Autobus";
  const speed = v.stopped || v.speed_kmh < 3 ? "stoji" : `${whole.format(v.speed_kmh)} km/h`;
  return `<b>${kind} ${v.route_id}</b> · vozilo ${v.vehicle_id}<br><span class="figure">${speed}</span> · ${ago(v.ts)}`;
}

function visible(v) {
  return v.tram || state.showBuses;
}

function placeVehicle(v) {
  let marker = state.markers.get(v.vehicle_id);
  if (!visible(v)) {
    if (marker) {
      marker.remove();
      state.markers.delete(v.vehicle_id);
    }
    return;
  }
  if (!marker) {
    marker = L.marker([v.lat, v.lon], { icon: icon(v), keyboard: false, zIndexOffset: v.tram ? 1000 : 0 });
    marker._key = iconKey(v);
    marker.bindTooltip(tooltip(v), { className: "tip", direction: "top", offset: [0, v.tram ? -14 : -6] });
    marker.on("click", () => select(v.route_id));
    marker.addTo(vehicleLayer);
    state.markers.set(v.vehicle_id, marker);
  } else {
    marker.setLatLng([v.lat, v.lon]);
    const key = iconKey(v);
    if (key !== marker._key) {
      marker.setIcon(icon(v));
      marker._key = key;
    }
    marker.setTooltipContent(tooltip(v));
  }
  const element = marker.getElement();
  if (element) element.classList.toggle("dimmed", state.selected != null && v.route_id !== state.selected);
}

function updateVehicles(list) {
  for (const v of list) {
    const known = state.vehicles.get(v.vehicle_id);
    if (!known || known.ts <= v.ts) state.vehicles.set(v.vehicle_id, v);
  }
  for (const [id, v] of state.vehicles) {
    if (state.clock - v.ts > STALE_S) {
      state.vehicles.delete(id);
      state.markers.get(id)?.remove();
      state.markers.delete(id);
    }
  }
  for (const v of list) if (state.vehicles.get(v.vehicle_id) === v) placeVehicle(v);
  drawBoard();
}

function redrawAllVehicles() {
  for (const v of state.vehicles.values()) placeVehicle(v);
}

// ---------------------------------------------------------------- the board

function drawBoard() {
  $("clock").textContent = state.clock ? hhmm.format(state.clock * 1000) : "--:--";
  const lag = Date.now() / 1000 - state.clock;
  const status = $("status");
  if (replay.data) {
    status.textContent = `snimka od ${day.format(state.clock * 1000)}`;
    status.className = "";
  } else if (!state.clock) {
    status.textContent = "čekam prve položaje";
    status.className = "";
  } else if (lag < 120) {
    status.textContent = "uživo";
    status.className = "live";
  } else {
    status.textContent = `snimka od ${day.format(state.clock * 1000)}`;
    status.className = "";
  }
  let trams = 0;
  for (const v of state.vehicles.values()) if (v.tram) trams += 1;
  $("count").textContent = state.clock ? `${trams} ${plural(trams, "tramvaj", "tramvaja")} na karti` : "";
}

// ---------------------------------------------------------------- lines

function sparkline(history, now) {
  const width = 100;
  const height = 22;
  const top = Math.max(30, ...history.map((w) => w.mean_kmh ?? 0));
  const x = (t) => ((t - (now - HISTORY_MS)) / HISTORY_MS) * width;
  const y = (kmh) => height - (kmh / top) * height;
  const runs = [];
  let run = [];
  let previous = null;
  for (const w of history) {
    if (w.mean_kmh == null || w.end < now - HISTORY_MS) continue;
    if (previous && w.start - previous.end > 60_000) {
      runs.push(run);
      run = [];
    }
    run.push(`${x((w.start + w.end) / 2).toFixed(1)},${y(w.mean_kmh).toFixed(1)}`);
    previous = w;
  }
  runs.push(run);
  const ns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("preserveAspectRatio", "none");
  svg.setAttribute("aria-hidden", "true");
  const slow = document.createElementNS(ns, "line"); // 12 km/h: below it a line is crawling
  for (const [key, value] of Object.entries({ x1: 0, x2: width, y1: y(12), y2: y(12) })) slow.setAttribute(key, value);
  svg.append(slow);
  for (const points of runs) {
    if (points.length === 0) continue;
    const line = document.createElementNS(ns, "polyline");
    line.setAttribute("points", points.length === 1 ? `${points[0]} ${points[0]}` : points.join(" "));
    svg.append(line);
  }
  return svg;
}

function drawLines() {
  const body = $("lines").tBodies[0];
  const now = state.clock * 1000;
  const routes = Object.keys(state.lines)
    .filter(isTramLine)
    .sort((a, b) => Number(a) - Number(b));
  body.replaceChildren();
  for (const route of routes) {
    const history = state.lines[route];
    const latest = history[history.length - 1];
    if (!latest || latest.end < now - HISTORY_MS) continue;
    const quiet = latest.end < now - QUIET_MS;
    const mean = latest.mean_kmh;
    const row = el(
      "tr",
      {
        class: [quiet ? "quiet" : "", state.selected === route ? "selected" : ""].join(" ").trim(),
        tabindex: "0",
        "aria-label": `Linija ${route}: ${mean == null ? "bez podataka" : `${decimal.format(mean)} km/h`}${quiet ? ", sada ne vozi" : ""}`,
        "aria-pressed": String(state.selected === route),
        onclick: () => select(route),
        onkeydown: (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            select(route);
          }
        },
      },
      el("td", {}, badge(route)),
      el("td", { class: "spark" }, sparkline(history, now)),
      el("td", { class: `num${!quiet && mean != null && mean < 12 ? " slow-now" : ""}` }, mean == null ? "–" : decimal.format(mean)),
      el("td", { class: "num" }, latest.stopped_share == null ? "–" : `${whole.format(latest.stopped_share * 100)} %`),
    );
    body.append(row);
  }
  $("lines-empty").hidden = body.rows.length > 0;
}

function select(route) {
  state.selected = state.selected === route ? null : route;
  redrawAllVehicles();
  drawLines();
}

// ---------------------------------------------------------------- bunching

function drawAlerts() {
  const list = $("alerts");
  const recent = state.alerts.filter((a) => state.clock - a.ts < 3600).slice(0, 6);
  list.replaceChildren(
    ...recent.map((a) =>
      el(
        "li",
        {},
        el(
          "button",
          { type: "button", onclick: () => showAlert(a) },
          badge(a.route_id),
          el("span", { class: "what" }, `${a.vehicles.join(" i ")}`, el("small", {}, `razmak ${whole.format(a.distance_m)} m`)),
          el("span", { class: "when" }, hhmm.format(a.ts * 1000)),
        ),
      ),
    ),
  );
  $("alerts-empty").hidden = recent.length > 0;
}

function showAlert(alert) {
  focus(L.latLng(alert.lat, alert.lon).toBounds(250), 16);
  for (const id of alert.vehicles) {
    const tram = state.markers.get(id)?.getElement()?.querySelector(".tram");
    if (!tram) continue;
    tram.classList.remove("flash");
    void tram.offsetWidth;
    tram.classList.add("flash");
  }
}

// ---------------------------------------------------------------- slow spots

function cellBounds(cell) {
  const [row, col] = cell.split(":").map(Number);
  const dLat = CELL_M / 111_320;
  const dLon = CELL_M / (111_320 * Math.cos((45.8 * Math.PI) / 180));
  return [
    [row * dLat, col * dLon],
    [(row + 1) * dLat, (col + 1) * dLon],
  ];
}

function tramRoutes(h) {
  return h.routes.filter(isTramLine).sort((a, b) => Number(a) - Number(b)).join(", ");
}

function slowest() {
  return state.hotspots.filter((h) => h.mean_kmh < 12).sort((a, b) => a.mean_kmh - b.mean_kmh);
}

function drawSpots() {
  spotLayer.clearLayers();
  const spots = slowest();
  if (state.showSpots) {
    for (const h of spots) {
      const color = css(h.mean_kmh < 5 ? "--s0" : "--s1");
      L.rectangle(cellBounds(h.cell), { color, weight: 1, opacity: 0.5, fillColor: color, fillOpacity: h.mean_kmh < 5 ? 0.24 : 0.14 })
        .bindTooltip(
          `<b>Sporo: ${decimal.format(h.mean_kmh)} km/h</b><br>linije ${tramRoutes(h)} · ${h.vehicles} ${plural(h.vehicles, "tramvaj", "tramvaja")}`,
          { className: "tip", sticky: true },
        )
        .addTo(spotLayer);
    }
  }
  const list = $("spots");
  list.replaceChildren(
    ...spots.slice(0, 6).map((h) =>
      el(
        "li",
        {},
        el(
          "button",
          { type: "button", onclick: () => focus(cellBounds(h.cell), 16) },
          el("span", { class: "kmh" }, decimal.format(h.mean_kmh)),
          el(
            "span",
            { class: "what" },
            `linije ${tramRoutes(h)}`,
            el("small", {}, `km/h u prosjeku, ${h.vehicles} ${plural(h.vehicles, "tramvaj", "tramvaja")}`),
          ),
          el("span", { class: "when" }, hhmm.format(h.end)),
        ),
      ),
    ),
  );
  $("spots-empty").hidden = spots.length > 0;
}

// ---------------------------------------------------------------- data

async function refresh() {
  const response = await fetch("api/live", { cache: "no-store" });
  if (!response.ok) throw new Error(`api/live: ${response.status}`);
  const live = await response.json();
  if (live.version < state.version) {
    // the server restarted and counts from zero again: follow its count, or the stream sends nothing for hours
    state.version = live.version;
    if (stream) listen();
  }
  state.clock = Math.max(state.clock, live.clock);
  state.version = Math.max(state.version, live.version);
  state.lines = live.lines;
  state.alerts = live.alerts;
  state.hotspots = live.hotspots;
  updateVehicles(live.vehicles);
  drawLines();
  drawAlerts();
  drawSpots();
}

let stream = null;
function listen() {
  stream?.close();
  stream = new EventSource(`api/stream?since=${state.version}`);
  stream.onmessage = (event) => {
    const update = JSON.parse(event.data);
    state.version = update.version;
    state.clock = Math.max(state.clock, update.clock);
    updateVehicles(update.vehicles);
  };
  stream.onerror = () => {
    stream.close();
    $("status").textContent = "veza je pukla, spajam se ponovo";
    $("status").className = "";
    setTimeout(start, 5000);
  };
}

async function start() {
  try {
    await refresh();
    listen();
  } catch {
    if (!replay.tried && (await startReplay())) return;
    $("status").textContent = "poslužitelj se ne javlja";
    setTimeout(start, 5000);
  }
}

// ---------------------------------------------------------------- playback without a server

const replay = {
  tried: false,
  data: null,
  vehicles: new Map(), // id -> tracks, absolute times
  t: 0,
  playing: true,
  speed: 10,
  shown: "", // which windows are drawn, to redraw the panel only when that changes
};

async function startReplay() {
  replay.tried = true;
  let data;
  try {
    const response = await fetch("data/replay.json");
    if (!response.ok) return false;
    data = await response.json();
  } catch {
    return false;
  }
  if (!data.tracks?.length) return false;
  replay.data = data;
  for (const track of data.tracks) {
    const points = track.p.map(([dt, lat, lon, kmh, heading]) => ({ ts: data.start + dt, lat, lon, kmh, heading }));
    const list = replay.vehicles.get(track.v) ?? [];
    list.push({ route: track.r, tram: track.tram === 1, points });
    replay.vehicles.set(track.v, list);
  }
  // start a quarter of an hour in, when the first windows have closed and the panel has something to show
  replay.t = Math.min(data.end, data.start + 900);
  mapEl.classList.remove("glide"); // positions are interpolated every frame instead
  setupPlayer();
  document.querySelector(".colophon .api").hidden = true;
  document.querySelector(".colophon .replay-note").hidden = false;
  showFrame(true);
  requestAnimationFrame(tick);
  return true;
}

function positionAt(points, t) {
  let lo = 0;
  let hi = points.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (points[mid].ts <= t) lo = mid;
    else hi = mid - 1;
  }
  const a = points[lo];
  const b = points[lo + 1];
  if (!b || b.ts - a.ts > 120) return { ...a, report: a.ts };
  // between two reports: move along the line between them, at the speed measured over it
  const f = (t - a.ts) / (b.ts - a.ts);
  return { lat: a.lat + (b.lat - a.lat) * f, lon: a.lon + (b.lon - a.lon) * f, kmh: b.kmh, heading: b.heading, report: a.ts };
}

function vehiclesAt(t) {
  const out = [];
  for (const [id, list] of replay.vehicles) {
    // a vehicle that changed line has a track per line: take the newest one that has started
    let track = null;
    for (const candidate of list) {
      const first = candidate.points[0].ts;
      const last = candidate.points[candidate.points.length - 1].ts;
      if (t >= first && t <= last + STALE_S && (!track || first > track.points[0].ts)) track = candidate;
    }
    if (track) {
      const p = positionAt(track.points, t);
      out.push({
        vehicle_id: id,
        route_id: track.route,
        tram: track.tram,
        lat: p.lat,
        lon: p.lon,
        ts: p.report,
        speed_kmh: p.kmh,
        stopped: p.kmh < 3,
        heading: p.heading < 0 ? null : p.heading,
      });
    }
  }
  return out;
}

function showFrame(force = false) {
  const t = replay.t;
  state.clock = Math.floor(t);
  const list = vehiclesAt(t);
  const ids = new Set(list.map((v) => v.vehicle_id));
  for (const [id, marker] of state.markers) {
    if (!ids.has(id)) {
      marker.remove();
      state.markers.delete(id);
    }
  }
  state.vehicles = new Map(list.map((v) => [v.vehicle_id, v]));
  for (const v of list) placeVehicle(v);
  drawBoard();

  // the panel shows what the processor had published by this moment
  const ms = t * 1000;
  const lines = replay.data.lines.filter((w) => w.end <= ms);
  const spots = replay.data.hotspots.filter((h) => h.end <= ms);
  const alerts = replay.data.alerts.filter((a) => a.ts <= t);
  const shown = `${lines.length}|${spots.length}|${alerts.length}`;
  if (force || shown !== replay.shown) {
    replay.shown = shown;
    state.lines = {};
    for (const w of lines) (state.lines[w.route_id] ??= []).push(w);
    for (const route of Object.keys(state.lines)) state.lines[route] = state.lines[route].slice(-24);
    const newest = Math.max(0, ...spots.map((h) => h.end));
    state.hotspots = spots.filter((h) => h.end === newest);
    state.alerts = alerts.slice().reverse();
    drawLines();
    drawAlerts();
    drawSpots();
  }
  const scrub = $("scrub");
  if (document.activeElement !== scrub) scrub.value = Math.floor(t);
}

let lastFrame = 0;
let lastDraw = 0;
function tick(now) {
  const elapsed = lastFrame ? (now - lastFrame) / 1000 : 0;
  lastFrame = now;
  if (replay.playing) {
    replay.t += elapsed * replay.speed;
    if (replay.t >= replay.data.end) {
      replay.t = replay.data.end;
      setPlaying(false);
    }
  }
  if (replay.playing && now - lastDraw > 100) {
    lastDraw = now;
    showFrame();
  }
  requestAnimationFrame(tick);
}

function setPlaying(playing) {
  replay.playing = playing;
  const button = $("play");
  button.setAttribute("aria-label", playing ? "Zaustavi" : "Pokreni");
  button.classList.toggle("paused", !playing);
}

function setupPlayer() {
  $("player").hidden = false;
  const scrub = $("scrub");
  scrub.min = replay.data.start;
  scrub.max = replay.data.end;
  scrub.step = 15;
  scrub.addEventListener("input", () => {
    replay.t = Number(scrub.value);
    showFrame();
  });
  $("play").addEventListener("click", () => {
    if (!replay.playing && replay.t >= replay.data.end) replay.t = replay.data.start;
    setPlaying(!replay.playing);
  });
  for (const button of document.querySelectorAll("#speeds button")) {
    button.addEventListener("click", () => {
      replay.speed = Number(button.dataset.speed);
      for (const other of document.querySelectorAll("#speeds button")) other.setAttribute("aria-pressed", String(other === button));
    });
  }
  $("replay-span").textContent = `${hhmm.format(replay.data.start * 1000)}–${hhmm.format(replay.data.end * 1000)}`;
  setPlaying(true);
}

$("show-buses").addEventListener("change", (event) => {
  state.showBuses = event.target.checked;
  redrawAllVehicles();
});
$("show-spots").addEventListener("change", (event) => {
  state.showSpots = event.target.checked;
  drawSpots();
});

setInterval(() => {
  if (!replay.data) refresh().catch(() => {});
}, 30_000);
start();
