import csv
from datetime import datetime, timedelta
from random import random, randint, choice

import numpy
from faker import Faker
from geopy.distance import geodesic as vincenty


def random_date(start, end):
    """Generate a random datetime between `start` and `end`"""
    random_time =  start + timedelta(
        seconds=randint(0, int((end - start).total_seconds())),
    )
    hour = numpy.random.choice(hours, p=probabilities)
    return random_time.replace(hour=hour)


def flip_a_coin(probability: float) -> bool:
    if probability < 0 or probability > 1:
        raise ValueError("Probability must be between 0 and 1")
    return random.random() < probability


DRIVERS_NUM = 3000
CLIENTS_NUM = 5000
TRIPS_NUM = 40000000
BATCH_SIZE = 10*3

TO_DATE = datetime.today()
FROM_DATE = TO_DATE - timedelta(days=30)

MAX_RATE = 5
FEEDBACK_CATEGORY=['politeness' , 'sociability' ,'punctuality']
FEEDBACK_RATE = [-1, 0, 1]
result_file = 'data.txt'
AVG_SPEED_KM_PER_HOUR = 45

hour_probability = {
    0: 0.03,
    1: 0.02,
    2: 0.01,
    3: 0.01,
    4: 0.005,
    5: 0.005,
    6: 0.01,
    7: 0.03,
    8: 0.09,
    9: 0.08,
    10: 0.07,
    11: 0.04,
    12: 0.03,
    13: 0.02,
    14: 0.03,
    15: 0.03,
    16: 0.04,
    17: 0.06,
    18: 0.08,
    19: 0.09,
    20: 0.07,
    21: 0.06,
    22: 0.05,
    23: 0.04,
}
hours = []
probabilities = []
for hour, probability in hour_probability.items():
    hours.append(hour)
    probabilities.append(probability)

commentsFactory = Faker()


def trip_cost(distance, start_date):
    serving_fee = 30
    price_per_km = 5
    coef = 1
    RUSH_HOURS = {8, 9, 17, 18, 19, 20}
    if start_date.hour in RUSH_HOURS:
        coef = 1.3
    return (serving_fee + distance * price_per_km) * coef


def main(file):
    TRIPS = []

    with open('London.csv', 'r') as postcodes:
        codes = list(csv.DictReader(postcodes, delimiter=','))
        DESTINATIONS_NUM = len(codes)

    for i in range(1, TRIPS_NUM + 1):
        driver_feedback = []
        client_feedback = []
        client_rate = 0
        driver_rate = 0
        if i % 1000 == 0:
            print(str(i) + ' codes')

        driver = randint(0, DRIVERS_NUM - 1)
        client = randint(0, CLIENTS_NUM - 1)

        start, end = randint(0, DESTINATIONS_NUM - 1), randint(0, DESTINATIONS_NUM - 1)
        if start == end:
            end = (end + i) % DESTINATIONS_NUM

        start_point = (float(codes[start]['Latitude']), float(codes[start]['Longitude']))
        end_point = (float(codes[end]['Latitude']), float(codes[end]['Longitude']))
        distance = float(vincenty(start_point, end_point).kilometers)

        start_date = random_date(FROM_DATE, TO_DATE)
        end_date = start_date + timedelta(hours=distance / AVG_SPEED_KM_PER_HOUR)

        if i % 3 == 0:
            driver_rate = randint(0, 5)
            client_rate = randint(3, 5)

        for j in range(len(FEEDBACK_CATEGORY)):
            feedback = 0
            if i % 4 == 0:
                feedback = choice(FEEDBACK_RATE)
            driver_feedback.insert(j, feedback)

        for j in range(len(FEEDBACK_CATEGORY)):
            feedback = 0
            if i % 4 == 0:
                feedback = choice(FEEDBACK_RATE)
            client_feedback.insert(j, feedback)

        driver_comment = commentsFactory.sentence()

        trip = {
            'driver': driver,
            'client': client,
            'start_point': start_point,
            'end_point': end_point,
            'distance': distance,
            'start_date': start_date.strftime("%Y-%m-%d %H:%M"),
            'end_date': end_date.strftime("%Y-%m-%d %H:%M"),
            'driver_rate': driver_rate,
            'driver_comment': driver_comment,
            'driver_feedback': driver_feedback,
            'client_rate': client_rate,
            'client_feedback': client_feedback,
            'cost': trip_cost(distance, start_date)
        }

        TRIPS.append(trip)

        if i % BATCH_SIZE == 0:
            lines = '\n'.join(str(trip) for trip in TRIPS)
            file.write(lines)
            # append ',\n' after every batch except the last one
            if (i + BATCH_SIZE <= TRIPS_NUM):
                file.write('\n')
            TRIPS = []


if __name__ == "__main__":
    with open(result_file, 'w') as f:
        main(f)







