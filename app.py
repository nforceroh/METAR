#!/usr/bin/python
#
from __future__ import print_function
from metar import Metar

import os
import sys
import getopt
import string
import math
import json
import logging
import time
import numpy as np
from metpy.units import units
import metpy.calc as mpcalc
import paho.mqtt.client as mqtt

try:
    from urllib2 import urlopen
except:
    from urllib.request import urlopen

BASE_URL = "http://tgftp.nws.noaa.gov/data/observations/metar/stations"
MQTT_PUB_ROOT = "METAR"
broker_address = "mqtt.nf.lab"
QUERYTIME = os.getenv("QUERYTIME", "900")

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(
    stream=sys.stdout, filemode="w", format=Log_Format, level=logging.ERROR
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


def truncate(f, n):
    return math.floor(f * 10 ** n) / 10 ** n


def mqtt_publish(station, dewpoint, temperature, rh, pressure):
    dict = {
        "station": station,
        "dewpoint": dewpoint,
        "temp": temperature,
        "humidity": rh,
        "pressure": pressure,
    }
    logger.debug("Write points: {0}".format(dict))

    client = mqtt.Client("metar")  # create new instance
    client.connect(broker_address)  # connect to broker
    client.publish(MQTT_PUB_ROOT, payload=json.dumps(dict))  # publish


def fetch_metar():
    stations = ["KMWO", "KHAO"]
    for name in stations:
        url = "%s/%s.TXT" % (BASE_URL, name)
        logger.debug(f"Fetching {url}")
        try:
            response = urlopen(url, timeout=30)
        except Metar.ParserError as exc:
            logger.debug("METAR code: {0}".format(line))
            logger.debug(string.join(exc.args, ", "), "\n")
        except:
            import traceback

            logger.debug(traceback.format_exc())
            logger.debug("Error retrieving", name, "data")
        else:
            report = ""
            for line in response:
                if not isinstance(line, str):
                    line = line.decode()  # convert Python3 bytes buffer to string
                if line.startswith(name):
                    report = line.strip()
                    obs = Metar.Metar(line)
                    logger.debug(obs)

                    try:
                        temp = obs.temp.value("C")
                    except:
                        temp = 0

                    try:
                        dewp = obs.dewpt.value("C")
                    except:
                        dewp = temp
                        
                    if dewp != 0 and temp != 0:
                        relhumcalc = str(mpcalc.relative_humidity_from_dewpoint(temp * units.celsius ,dewp * units.celsius) * units.percent * 100)
                        rawrelhum = relhumcalc.split(" ")
                        hum = str(round(float(rawrelhum[0]),2))
                    else:
                        hum = 0

                    pressure = truncate(obs.press._value * 33.864, 2)
                    logger.debug(
                        f"MQTT -> station_id: {obs.station_id}, dewp: {dewp}, temp: {temp}, hum: {hum}, pressure: {pressure}"
                    )
                    mqtt_publish(
                        obs.station_id, dewp, temp, hum, pressure
                    )
                    break
            if not report:
                logger.debug("No data for ", name)


if __name__ == "__main__":
    while True:
        fetch_metar()
        logger.debug("Sleeping till next go around")
        time.sleep(int(QUERYTIME))
