#%help


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
import pandas as pd
import random
import string
from faker import Faker
from datetime import datetime, timedelta
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.types import *
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

fake = Faker()
schema_wagon_data = {
    "unique_id": {"type": "integer", "min": 5000, "max": 25000, "uniqueItems": True},
    "wagon_id": {"type": "integer", "min": 0, "max": 5000, "uniqueItems": True},
    "measure_timestamp": {
        "type": "string",
        "faker": "date_time_this_century",
        "format": "%Y-%m-%d %H:%M:%S",
    },
    "measure_lat": {"type": "number", "faker": "latitude"},
    "measure_long": {"type": "number", "faker": "longitude"},
    "measure_wagon_weight": {"type": "number", "min": 1000, "max": 10000},
    "measure_temperature": {"type": "number", "min": -20, "max": 40},
    "measure_gps_signal_quality": {"type": "integer", "min": 1, "max": 5},
    "measure_device_parameter_1": {
        "type": "string",
        "faker": "text",
        "max_nb_chars": 1,
    },
    "measure_device_parameter_2": {"type": "integer", "min": 1, "max": 100},
}

schema_master_data = {
    "wagon_id": {"type": "integer", "min": 1, "max": 1000, "uniqueItems": True},
    "hersteller": {"type": "string"},
    "gattung": {"type": "string"},
    "typ": {"type": "string"},
    "ladelänge": {"type": "integer", "min": 10, "max": 30},
    "ladehöhe": {"type": "number", "min": 2, "max": 3},
    "ladefläche": {"type": "integer", "min": -.3, "max": .3},
    "laderaum": {"type": "integer", "min": 50, "max": 200},
    "leergewicht": {"type": "number", "min": 5, "max": 10},
    "lastgrenze": {"type": "number", "min": 15, "max": 45},
    "inbetriebnahme": {"type": "string"}
}

schema_auftrags_data = {
    "wagon_id": {"type": "integer", "min": 1, "max": 5000, "uniqueItems": True},
    "order_id": {"type": "integer", "min": 3000, "max": 25000, "uniqueItems": True},
    "order_von": {"type": "string", "format": "date-time"},
    "order_bis": {"type": "string", "format": "date-time"},
    "kunden_nr": {"type": "integer", "min": 1000, "max": 10000},
    "ladung": {"type": "string"}
}

schema_geofence_data = {
    "unique_id": {"type": "integer", "min": 1, "max": 100000, "uniqueItems": True},
    "name": {"type": "string"},
    "type": {"type": "string"},
    "geometry": {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "coordinates": {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": [{"type": "number"}, {"type": "number"}],
                    "minItems": 2,
                    "maxItems": 2,
                },
                "minItems": 5,
                "maxItems": 5,
            },
        },
        "required": ["type", "coordinates"],
        "additionalProperties": False,
    },
    "required": ["unique_id", "name", "type", "geometry"],
    "additionalProperties": False,
}

# Listen für hersteller, gattung und typ
hersteller_mapping = {
    "Waggonbau Niesky GmbH":0.7,
    "Waggonbau Graaff GmbH":1,
    "VTG Rail Europe GmbH":1.6,
    "Waggonbau Görlitz GmbH":2
}

gattung_typ_mapping = {
    "E: Offene Wagen": [
        "Ealos-t 058",
        "Eanos-x 052",
        "Eas 066",
    ],
    "H: Gedeckte, großräumige Schiebewandwagen": [
        "Habbiins 344",
        "Hbbins 306",
        "Hcceerrs 330",
    ],
    "R: Drehgestellflachwagen mit 4 Radsätzen": [
        "Rbns 641",
        "Rbns 646.0",
        "Remms 665",
    ]
}

# Funktion zur Generierung eines zufälligen Datensatzes basierend auf dem Schema
def generate_random_data(schema, num_records, master_data, err_rate):
    data = []
    generated_ids = set()  # Verfolgt bereits generierte unique_ids

    while len(data) < num_records:
        new_id = random.randint(schema["unique_id"]["min"], schema["unique_id"]["max"])
        master_record = random.choice(master_data)
        if new_id not in generated_ids:

            # Generate Dataset
            record = {}

            record['unique_id'] = new_id
            record['wagon_id'] = master_record['wagon_id']

            if master_record['inbetriebnahme'] is not None:
                record['measure_timestamp'] = fake.date_time_between(start_date=datetime.strptime(master_record['inbetriebnahme'], '%Y-%m-%d %H:%M:%S'), end_date='now').strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                if random.random() < 0.1:
                    record['measure_timestamp'] = record['measure_timestamp'][:5]+'08'+record['measure_timestamp'][7:]
                elif record['measure_timestamp'][5:7] == '02' and random.random() < .5:
                    record['measure_timestamp'] = record['measure_timestamp'][:5]+'08'+record['measure_timestamp'][7:]
            else:
                record['measure_timestamp'] = fake.date_time_this_century().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
            
            geo_location = fake.local_latlng("DE")
            record['measure_lat'] = round(float(geo_location[0]),4)
            record['measure_long'] = round(float(geo_location[1]),4)

            leergewicht = master_record['leergewicht'] if master_record['leergewicht'] is not None else 10
            if leergewicht <= 0:
                leergewicht = 5
            lastgrenze = master_record['lastgrenze'] if master_record['lastgrenze'] is not None else 45
            if lastgrenze <= 0:
                lastgrenze = leergewicht + 10
            record['measure_wagon_weight'] = round(float(random.uniform(leergewicht, lastgrenze)),2)
            record['measure_temperature'] = round(float(random.triangular(schema['measure_temperature']['min'], schema['measure_temperature']['max'], 20)),2)

            record['measure_gps_signal_quality'] = int(random.choices(list(range(1,6)), weights = [1, 3, 5, 7, 5], k=1)[0])
            record['measure_device_parameter_1'] = random.choice(string.ascii_uppercase)
            letter_num = ord(record['measure_device_parameter_1'])
            record['measure_device_parameter_2'] = random.choice([int(letter_num * record['measure_gps_signal_quality'] * record['wagon_id'] if record['wagon_id'] is not None else 500), None])

            # Generate Errors in the dataset
            if random.random() < random.choice([err_rate, err_rate-(0.5*err_rate), err_rate+(0.5*err_rate)]):
                keys = list(record.keys())
                keys = random.sample(keys, k=3)
                for key in keys:
                    if key == 'measure_lat' or key == 'measure_long':
                        record['measure_lat'] = 0.0
                        record['measure_long'] = 0.0
                    elif key == 'measure_gps_signal_quality' or key == 'measure_device_parameter_2':
                            record[key] = random.choice([0, None])
                    elif key == 'measure_timestamp':
                        if random.random() < 0.7:
                            record[key] = "1900-01-01 00:00:00"
                        else:
                            record[key] = None
                    elif key == 'measure_device_parameter_1':
                        record[key] == random.choice([record[key].lower(), None])
                    elif key in ('measure_wagon_weight', 'measure_temperature'):
                        record[key] = random.choice([round(float(record[key])*random.uniform(0.1,10),2), None])
                    elif key in ('unique_id', 'wagon_id'):
                        continue
            
            # Append the datat set and the id
            data.append(record)  
            generated_ids.add(new_id)
                   
    return data

# Funktion zur Generierung eines zufälligen Datensatzes basierend auf dem Schema für "master_data"
def generate_master_data(schema, num_records, err_rate):
    data = []    
    generated_ids = set()  # Verfolgt bereits generierte unique_ids

    while len(data) < num_records:
        new_id = random.randint(schema["wagon_id"]["min"], schema["wagon_id"]["max"])
        if new_id not in generated_ids:
            record = {}

            record['wagon_id'] = new_id
            record['hersteller'] = random.choice(list(hersteller_mapping.keys()))

            gattung = random.choice(list(gattung_typ_mapping.keys()))
            record["gattung"] = gattung
            record["typ"] = random.choice(gattung_typ_mapping[gattung])

            lade_min = schema['ladelänge']['min']*hersteller_mapping[record['hersteller']]
            lade_max = schema['ladelänge']['max']*hersteller_mapping[record['hersteller']]

            record['ladelänge'] = int(random.uniform(lade_min, lade_max))
            record['ladehöhe'] = round(float(random.uniform(schema['ladehöhe']['min'], schema['ladehöhe']['max'])),2)
            record['ladefläche'] = int((random.uniform(schema['ladefläche']['min'], schema['ladefläche']['max'])+2.6)*record['ladelänge'])
            record['laderaum'] = int(record['ladehöhe']*record['ladefläche'])

            max_volume = lade_max*schema['ladehöhe']['max']*(schema['ladefläche']['max']+2.6)

            record['leergewicht'] = round(random.uniform(
                schema['leergewicht']['min']*record['laderaum']/max_volume+schema['leergewicht']['min']-1, 
                schema['leergewicht']['min']*record['laderaum']/max_volume+schema['leergewicht']['min']+1,
                ), 3)
            record['lastgrenze'] = round(random.uniform(
                schema['lastgrenze']['min']*record['laderaum']/max_volume+schema['lastgrenze']['min'], 
                (schema['lastgrenze']['max']-schema['lastgrenze']['min'])*record['laderaum']/max_volume+schema['lastgrenze']['min'],
                ), 3)
            record['inbetriebnahme'] = fake.date_time_this_century().strftime("%Y-%m-%d %H:%M:%S")
            if random.random() < 0.1:
                record['inbetriebnahme'] = record['inbetriebnahme'][:5]+'05'+record['inbetriebnahme'][7:]
            elif record['inbetriebnahme'][5:7] == '10' and random.random() < .5:
                record['inbetriebnahme'] = record['inbetriebnahme'][:5]+'08'+record['inbetriebnahme'][7:]


            # Generate Errors in the dataset
            if random.random() < random.choice([err_rate, err_rate-(0.5*err_rate), err_rate+(0.5*err_rate)]):
                keys = list(record.keys())
                keys = random.sample(keys, k=4)
                for key in keys:
                    if key in ('ladelänge', 'ladefläche', 'laderaum'):
                            record[key] = random.choice([0, None])
                    elif key in ('ladehöhe', 'leergewicht', 'lastgrenze'):
                            record[key] = random.choice([float(0), None])
                    elif key == 'inbetriebnahme':
                        if random.random() < 0.7:
                            record[key] = "1900-01-01 00:00:00"
                        else:
                            record[key] = None
                    elif key in ('hersteller', 'typ'):
                        record[key] = random.choice([record[key].upper(), randomly_change_letters(record[key],random.randint(3,6)), None])
                    elif key in ('wagon_id', 'gattung'):
                        continue

            data.append(record)
            generated_ids.add(new_id)
            
    return data

def randomly_change_letters(input_string, num_changes):
    return ''.join(
        chr(random.randint(ord('a'), ord('z'))) if random.random() < num_changes / len(input_string) else char
        for char in input_string
    )
# Generiere zufällige Datensätze
random_master_data = generate_master_data(schema_master_data, 500, 0.10)

random_wagon_data = generate_random_data(schema_wagon_data, 2500, random_master_data, 0.10)

sp_schema_wagon_data = StructType(
    [
        StructField("unique_id", IntegerType(), True),
        StructField("wagon_id", IntegerType(), True),
        StructField("measure_timestamp", StringType(), True),
        StructField("measure_lat", FloatType(), True),
        StructField("measure_long", FloatType(), True),
        StructField("measure_wagon_weight", FloatType(), True),
        StructField("measure_temperature", FloatType(), True),
        StructField("measure_gps_signal_quality", IntegerType(), True),
        StructField("measure_device_parameter_1", StringType(), True),
        StructField("measure_device_parameter_2", IntegerType(), True),
    ]
)

wagon_data = spark.createDataFrame(random_wagon_data, schema=sp_schema_wagon_data, verifySchema=False)

sp_schema_master_data = StructType(
    [
        StructField("wagon_id", IntegerType(), True),
        StructField("hersteller", StringType(), True),
        StructField("gattung", StringType(), True),
        StructField("typ", StringType(), True),
        StructField("ladelänge", IntegerType(), True),
        StructField("ladehöhe", FloatType(), True),
        StructField("ladefläche", IntegerType(), True),
        StructField("laderaum", IntegerType(), True),
        StructField("leergewicht", FloatType(), True),
        StructField("lastgrenze", FloatType(), True),
        StructField("inbetriebnahme", StringType(), True),
    ]
)
master_data = spark.createDataFrame(random_master_data, schema=sp_schema_master_data, verifySchema=False)

wagon_data.write.parquet("s3://db-cargo-glue-plattform-test/raw/wagon_data", mode='append')
master_data.write.parquet("s3://db-cargo-glue-plattform-test/raw/master_data", mode='append')
job.commit()