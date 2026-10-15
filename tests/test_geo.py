from tramvaj.geo import angle_between, cell, cell_center, distance_m, heading_deg


def test_distance_across_the_main_square():
    # Ban Jelačić Square to the cathedral, about 380 m
    assert 350 < distance_m(45.8131, 15.9772, 45.8146, 15.9817) < 400


def test_heading():
    assert round(heading_deg(45.8, 15.9, 45.9, 15.9)) == 0
    assert round(heading_deg(45.8, 15.9, 45.8, 16.0)) == 90


def test_angle_between_wraps():
    assert angle_between(350, 10) == 20


def test_cell_round_trip():
    c = cell(45.8131, 15.9772)
    lat, lon = cell_center(c)
    assert distance_m(45.8131, 15.9772, lat, lon) < 250
