"""Distances and headings on Zagreb's scale, where a flat-earth error is centimetres."""

from __future__ import annotations

import math

EARTH_M = 6_371_000
CELL_M = 250  # hotspot grid size


def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_M * math.asin(math.sqrt(a))


def heading_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compass heading from the first point to the second, 0 = north, 90 = east."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def move(lat: float, lon: float, heading: float, metres: float) -> tuple[float, float]:
    """The point `metres` away in direction `heading` (flat approximation, fine under a few km)."""
    north = metres * math.cos(math.radians(heading))
    east = metres * math.sin(math.radians(heading))
    return lat + north / 111_320, lon + east / (111_320 * math.cos(math.radians(lat)))


def angle_between(a: float, b: float) -> float:
    d = abs(a - b) % 360
    return 360 - d if d > 180 else d


def cell(lat: float, lon: float, size_m: int = CELL_M) -> str:
    """A square grid cell id, about size_m on each side at Zagreb's latitude."""
    row = math.floor(lat * 111_320 / size_m)
    col = math.floor(lon * 111_320 * math.cos(math.radians(45.8)) / size_m)
    return f"{row}:{col}"


def cell_center(cell_id: str, size_m: int = CELL_M) -> tuple[float, float]:
    row, col = (int(x) for x in cell_id.split(":"))
    lat = (row + 0.5) * size_m / 111_320
    lon = (col + 0.5) * size_m / (111_320 * math.cos(math.radians(45.8)))
    return round(lat, 6), round(lon, 6)
