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
MQTT_PUB_ROOT = os.getenv("MQTT_PUB_ROOT", "METAR")
MQTT_CLIENTID = os.getenv("MQTT_CLIENTID", f'metar-{random.randint(0, 1000)}')
MQTT_HOST = os.getenv("MQTT_HOST", "")
MQTT_PORT = os.getenv("MQTT_PORT", "1883")
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
MQTT_KEEPALIVE = os.getenv("MQTT_KEEPALIVE", "60")
METAR_SNOOZE = os.getenv("METAR_SNOOZE", "300")

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
    data = {
        "station": station,
        "dewpoint": dewpoint,
        "temp": temperature,
        "humidity": rh,
        "pressure": pressure,
    }

    logger.debug("Write points: {0}".format(data))

    client = mqtt.Client(client_id=MQTT_CLIENTID, clean_session=None, userdata=None,
                         transport="tcp", reconnect_on_failure=True)  # create new instance
    client.connect(host=MQTT_HOST, port=int(MQTT_PORT))  # connect to broker
    client.publish(MQTT_PUB_ROOT, payload=json.dumps(data))  # publish


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
                        relhumcalc = str(mpcalc.relative_humidity_from_dewpoint(
                            temp * units.celsius, dewp * units.celsius) * units.percent * 100)
                        rawrelhum = relhumcalc.split(" ")
                        hum = str(round(float(rawrelhum[0]), 2))
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
