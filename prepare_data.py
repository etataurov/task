from collections import defaultdict, namedtuple
import logging

import pandas
import redis

redis_client = redis.Redis(decode_responses=True)

Segment = namedtuple("Segment", ["segment_id", "from_stop", "to_stop", "distance"])

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())


class Route:
    def __init__(self, segments_data):
        self.segments_data = segments_data
        self.segments = []
        self.process_data()

    def process_data(self):
        for segment_id, segment in self.segments_data:
            from_, to, distance = segment
            self.segments.append(Segment(segment_id, from_, to, distance))

    def get_segments(self, from_stop, to_stop):
        segments = []
        within = False
        segment_distances = []

        def add_segment(segment):
            segments.append(segment.segment_id)
            segment_distances.append(segment.distance)

        for segment in self.segments:
            if from_stop == segment.from_stop:
                within = True
                add_segment(segment)
                if to_stop == segment.to_stop:
                    break
            elif to_stop == segment.to_stop:
                add_segment(segment)
                break
            elif within:
                add_segment(segment)
        total_distance = sum(segment_distances)
        distance_parts = [d / total_distance for d in segment_distances]
        return list(zip(segments, distance_parts))


def store_data(pax, revenue):
    pipeline = redis_client.pipeline()
    for segment_id, segment_pax in pax.items():
        segment_revenue = revenue.get(segment_id)
        pipeline.hmset(
            int(segment_id),
            {"pax": segment_pax, "revenue": segment_revenue}
        )
    pipeline.execute()
    log.info("all data has been written")


def process_data(route_segments_file="homework_route_segments.csv",
                 rides_file="homework_rides.csv",
                 segments_file="homework_segments.csv",
                 tickets_file="homework_tickets.csv"):
    pax = defaultdict(int)
    revenue = defaultdict(int)

    route_segments = pandas.read_csv(
        route_segments_file
    ).set_index(
        "route_id", drop=False
    ).sort_index()

    rides = pandas.read_csv(
        rides_file,
        index_col=0)  # ride_id as index

    segments_data = pandas.read_csv(
        segments_file,
        index_col=0)  # segment_id as index

    tickets = pandas.read_csv(
        tickets_file
    ).set_index(
        "ride_id", drop=False
    ).sort_index()

    unique_routes = route_segments["route_id"].unique()
    for route_id in unique_routes:
        segments = route_segments.ix[route_id]
        if isinstance(segments, pandas.DataFrame):
            segments = segments.sort_values("sequence")
            segments = segments["segment_id"].values
        else:
            # only one segment
            segments = segments[["segment_id"]].values
        data = segments_data[segments_data.index.isin(segments)]
        route = Route(zip(data.index.values, data.values))

        route_rides = rides.loc[rides["route_id"] == route_id].index.values
        for ride_id in route_rides:
            ride_tickets = tickets.ix[ride_id][["from_stop", "to_stop", "price"]]
            if isinstance(ride_tickets, pandas.DataFrame):
                ride_tickets = ride_tickets.values
            else:
                ride_tickets = [ride_tickets.values]
            for ticket in ride_tickets:
                segments = route.get_segments(ticket[0], ticket[1])
                for seg_id, distance_part in segments:
                    pax[seg_id] += 1
                    revenue[seg_id] += distance_part * ticket[-1]
        log.info("processed route #{}".format(route_id))
    return dict(pax), dict(revenue)


def load_all():
    processed_key = "segments_data_processed"
    if redis_client.exists(processed_key):
        log.info("all the data have been already processed")
        return
    pax, revenue = process_data()
    store_data(pax, revenue)
    redis_client.set(processed_key, 1)

if __name__ == '__main__':
    load_all()
