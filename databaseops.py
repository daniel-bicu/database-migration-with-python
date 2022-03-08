__doc__ = "This module helps with connections to the databases"

import json

from pymongo import MongoClient, InsertOne
import psycopg2


def connect_postgresql_db(connection_object):
    conn = psycopg2.connect(
        host=connection_object['host'],
        database=connection_object['database'],
        user=connection_object['user'],
        password=connection_object['password']
    )
    return conn


def connect_mongo_db(connection_string):
    client = MongoClient(connection_string)
    return client


def import_data_to_MongoDb(client, datafile):
    db = client['dvdrental']
    collection_name = 'rental_migration_data2'
    collection_list = db.list_collection_names()

    json_objects = []

    if not collection_name in collection_list:
        print('Creating the collection ... ')
        db.create_collection(collection_name)
        print(f'The {collection_name} collection has been created.')
        collectionObject = db[f'{collection_name}']
        print(f'Importing data into {collection_name}...')
        with open(datafile) as f:
            data = json.load(f)
            for jsonObj in data:
                json_objects.append(InsertOne(jsonObj))
                # collectionObject.insert_one(jsonObj) #another approach to insert into mongoDb

        collectionObject.bulk_write(json_objects)
        print('Migration done.')
    else:
        print('The collection already exists. Try with another name!')
