from flask import Flask, jsonify
import redis

redis_client = redis.Redis(decode_responses=True)

app = Flask(__name__)


class SegmentNotFound(Exception):
    def __init__(self, segment_id):
        self.segment_id = segment_id


@app.errorhandler(404)
def invalid_segment(e):
    return jsonify(error="Invalid segment"), 400


@app.errorhandler(SegmentNotFound)
def segment_not_found(e):
    return jsonify(segment_id=e.segment_id, error="Segment not found"), 404


@app.route("/pax/<int:segment_id>")
def segment_pax(segment_id):
    pax = redis_client.hget(segment_id, "pax")
    if pax is None:
        raise SegmentNotFound(segment_id)
    return jsonify(segment_id=segment_id, pax=int(pax))


@app.route("/revenue/<int:segment_id>")
def segment_revenue(segment_id):
    revenue = redis_client.hget(segment_id, "revenue")
    if revenue is None:
        raise SegmentNotFound(segment_id)
    return jsonify(segment_id=segment_id, revenue=round(float(revenue), 2))
