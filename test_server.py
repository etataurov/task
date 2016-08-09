import unittest
import unittest.mock
import flixbus_server
import json


class FlixBusServerTestCase(unittest.TestCase):
    def setUp(self):
        flixbus_server.app.app.config['TESTING'] = True
        self.app = flixbus_server.app.app.test_client()
        self.redis_mock = unittest.mock.MagicMock()
        flixbus_server.app.redis_client = self.redis_mock

    def test_segment_pax(self):
        segment_id = 1234
        pax_value = 342
        self.redis_mock.hget.return_value = pax_value
        response = self.app.get("/pax/{}".format(segment_id))
        assert json.loads(response.data.decode()) == {"segment_id": segment_id,
                                                      "pax": pax_value}

    def test_segment_pax_missing(self):
        segment_id = 1234
        self.redis_mock.hget.return_value = None
        response = self.app.get("/pax/{}".format(segment_id))
        assert json.loads(response.data.decode()) == {"segment_id": segment_id,
                                                      "error": "Segment not found"}
        assert response.status_code == 404

    def test_pax_mallformed_segment_id(self):
        segment_id = "abcbc"
        response = self.app.get("/pax/{}".format(segment_id))
        assert json.loads(response.data.decode()) == {"error": "Invalid segment"}
        assert response.status_code == 400

    def test_segment_revenue(self):
        segment_id = 1234
        revenue_value = 342.4
        self.redis_mock.hget.return_value = revenue_value
        response = self.app.get("/revenue/{}".format(segment_id))
        assert json.loads(response.data.decode()) == {"segment_id": segment_id,
                                                      "revenue": revenue_value}

    def test_revenue_segment_missing(self):
        segment_id = 1234
        self.redis_mock.hget.return_value = None
        response = self.app.get("/revenue/{}".format(segment_id))
        assert json.loads(response.data.decode()) == {"segment_id": segment_id,
                                                      "error": "Segment not found"}
        assert response.status_code == 404

    def test_revenue_mallformed_segment_id(self):
        segment_id = "abcbc"
        response = self.app.get("/revenue/{}".format(segment_id))
        assert json.loads(response.data.decode()) == {"error": "Invalid segment"}
        assert response.status_code == 400
