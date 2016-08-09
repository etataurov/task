import os
import unittest
import unittest.mock
import prepare_data


class TestPrepareData(unittest.TestCase):
    def setUp(self):
        prepare_data.redis_client = unittest.mock.MagicMock()

    def test_process_data(self):
        files = [os.path.join("test_data", f) for f in [
            "test_route_segments.csv", "test_rides.csv",
            "test_segments.csv", "test_tickets.csv"]]
        pax, revenue = prepare_data.process_data(*files)
        assert pax == {1: 3, 2: 3, 3: 1, 4: 1}
        assert revenue == {
            1: 5 / 2 + 15 / 2 + 32 / 3,
            2: 5 / 2 + 15 / 2 + 32 / 3,
            3: 32 / 3,
            4: 12
        }
