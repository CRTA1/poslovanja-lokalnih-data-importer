# -*- coding: utf-8 -*-
import csv
import os
import requests
from pymongo import MongoClient
from slugify import slugify

# Connect to defualt local instance of MongoClient
client = MongoClient()

# Get database and collection
db = client.poslavanja

# Set to True to enable geocoding.
# WARNING: Will significantly slow down the importer.
geocode = False

def import_data():

    db.data.remove({})

    for filename in os.listdir('data'):
        if(filename.endswith(".csv")):
            year = int(filename.replace('.csv', ''))

            with open('data/%s' % filename, 'rb') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')

                

                # Loop through each row in the CSV
                for row_index, row in enumerate(reader):
                    if row_index == 0:
                        # Keep the header handy, we will use the column names as the JSON document keys.
                        header = row

                    else:
                        # Import data from each colum for the current row.
                        
                        # The document we will store in MongoDB.
                        doc = {}
                        for column_index in range(0, 24):
                            
                            sub_doc = create_sub_document(year, header, row, column_index)

                            sub_doc_key = slugify(header[column_index], to_lower=True, separator='_')
                            doc[sub_doc_key] = sub_doc

                        print "\n%i: %s - %s" % (year, doc['maticni_broj']['value'], doc['naziv_preduzeca']['value'])
                        
                        # Fetch and set address coordinates
                        if geocode:
                            geocode_address(doc)

                        # Save in database
                        db.data.insert(doc)
                        

def create_sub_document(year, header, row, column_index):
    '''
        Create a sub document from the given column value for the given row.
    '''
    

    if column_index in [0, 4, 6] or column_index > 11:
        if row[column_index] != '':
            val = int(row[column_index])
        else:
            val = 0
    else:
        val = row[column_index]

    sub_doc = {
        'name': header[column_index],
        'value': val
    }

    return sub_doc

def geocode_address(doc):
    '''
        Geocoding. Get coordinates from the address.
        Use MapQuest's free Nominatim-based service.
    '''

    city = doc['sediste']['value']
    street = doc['adresa']['value']

    if city != '' and street != '':
        url = 'http://open.mapquestapi.com/nominatim/v1/search.php?format=json&renderBasicSearchNarrative&q=%s,%s,Serbia'
        url = url % (street, city)

        r = requests.get(url)

        if(r.status_code == 200):
            json_resp = r.json()

            if(len(json_resp) > 0):
                lat = float(json_resp[0]['lat'])
                lon = float(json_resp[0]['lon'])

                doc['coordinates'] = {
                    'lat': lat,
                    'lon': lon
                }

                print"    Geocoded '%s, %s' (%f, %f)" % (street, city, lat, lon)

            else:  
                print "    Failed to geocode address for '%s, %s'" % (street, city)
    
        else:
            print "    Geocode request returned %i status" % r.status_code

    else:
        print '   Cannot geocode blank address values.'

# Let's import data.
import_data()