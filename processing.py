from datetime import datetime

from data_generation import FEEDBACK_CATEGORY


class TaxiDataProcessor:
    
    def __init__(self, df):
        self.df = df
        self.drivers_rating = self.df.filter(lambda trip: trip['driver_rate'] > 0).map(self._trip_driver_rate_map).reduceByKey(self._driver_rate_reduce).map(self._driver_avg_map)
        self.clients_rating = self.df.map(self._trip_client_map).reduceByKey(self._client_reduce).map(self._client_map)
        self.client_feedback = self.df.map(self._rate).reduce(self._client_feedback_reduce)

    @staticmethod
    def _trip_driver_rate_map(trip):
        return trip['driver'], (trip['driver_rate'], 1)

    @staticmethod
    def _driver_rate_reduce(acc, n):
        return acc[0] + n[0], acc[1] + n[1]

    @staticmethod
    def _driver_avg_map(driver):
        avg_rating = round(driver[1][0] / driver[1][1], 2)
        return driver[0], avg_rating

    @staticmethod
    def _trip_client_map(trip):
        return trip['client'], (trip['client_rate'], 1)

    @staticmethod
    def _client_map(client):
        avg_rating = round(client[1][0] / client[1][1], 2)
        return client[0], avg_rating

    @staticmethod
    def _client_reduce(acc, n):
        return acc[0] + n[0], acc[1] + n[1]

    def top_drivers(self, n):
        return self.drivers_rating.takeOrdered(n, key=lambda driver: -driver[1])

    def drivers_rating_lower_than(self, rating):
        return self.drivers_rating.filter(lambda a: a[1] < rating).collect()

    def most_intensive_timeframe(self, n_hours=5):

        return self.df.map(lambda x: (datetime.strptime(x['start_date'], "%Y-%m-%d %H:%M").hour, 1)).\
            reduceByKey(lambda a, b: a + b).\
            takeOrdered(n_hours, key=lambda trip: -trip[1])

    def top_clients(self, n):
        return self.clients_rating.takeOrdered(n, key=lambda client: -client[1])
    
    def count_drivers(self):
        return self.df.map(lambda x: x['driver']).distinct().count()

    def count_clients(self):
        return self.df.map(lambda x: x['client']).distinct().count()

    def driver_comment(self):
        map_result = self.df.map(lambda x: x['driver_comment']).distinct().collect()
        return max(map_result, key=len)


    def top_earners(self, n):
        return self.df.map(lambda x: (x['driver'], x['cost'])) \
            .reduceByKey(lambda x, y: x + y) \
            .takeOrdered(n, lambda x: -x[1])

    @staticmethod
    def _trip_hour_filter(trip):
        night_start = 23
        night_end = 6
        trip_hour = datetime.strptime(trip['start_date'], "%Y-%m-%d %H:%M").hour
        return trip_hour >= night_start or trip_hour <= night_end

    def top_nightwolves(self, n):
        return self.df.filter(self._trip_hour_filter).\
            map(lambda trip: (trip['driver'], 1)).\
            reduceByKey(lambda x, y: x + y).\
            takeOrdered(n, key=lambda driver: -driver[1])

    @staticmethod
    def _trip_driver_map(trip):
        return trip['driver'], (trip['driver_rate'], 1)

    @staticmethod
    def _rate(trip):
        if len(trip['driver_feedback']) != 0:
            return trip['driver_feedback']

    @staticmethod
    def _client_feedback_reduce(acc, n):
        if (acc == 0):
            return [0, 0, 0]
        return [acc[0] + n[0], acc[1] + n[1], acc[2] + n[2]]

    def most_praised_driver_quality(self):
        return FEEDBACK_CATEGORY[self.client_feedback.index(max(self.client_feedback))]
    
    def most_complained_driver_quality(self):
        return FEEDBACK_CATEGORY[self.client_feedback.index(min(self.client_feedback))]

    def most_len_comment(self):
        return self.driver_comment[self.driver_comment.index(max(self.driver_comment, key=len))]
