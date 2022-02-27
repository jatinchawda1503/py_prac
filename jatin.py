import pymongo as pymongo
from pymongo.server_api import ServerApi

from bson.objectid import ObjectId


#### TO CHANGE: CHANGE CONNECTION STRING
client = pymongo.MongoClient(
    "mongodb+srv://j1503c:jatin15031996@cluster0.ralqq.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",
    server_api=ServerApi('1'))
db = client.test

countries = db.countries
continents = db.continents


# find country (not needed # TO REMOVE: not needed)
def find_country():
    for country in countries.find():
        print(country['name'])

# find continents (not needed # TO REMOVE: not needed)
def find_continents():
    for continent in continents.find():
        print(continent)


# Find country by id (not needed # TO REMOVE: not needed)
def find_country_by_id(country_id):
    for con in countries.find({'_id': ObjectId(country_id)}):
        print("The country id", con['name'])


# 1. Get all the countries where the letter or the word is in the name
def find_by_query(name):
    for con in countries.find({'name': {"$regex": name, "$options": 'i'}}):
        print(con['name'])


# 2. Send list of continents with their number of countries
def get_continents_and_countries():
    agg_pipeline = [
        {
            '$project': {
                'name': "$name",
                'countries': {'$size': "$countries"}}
        }
    ]
    cont = continents.aggregate(agg_pipeline)
    for continent in cont:
        print(continent)


# 3. Send back the fourth countries of a continent by alphabetical order
# def fourth_country():
#     for continent in continents.find():
#         for country_id in continent['countries']:
#             countr = countries.find({'_id': ObjectId(country_id)}).sort("name")
#             print(continent['name'],':',countr[0]['name'])

# 4. Get all countries ordered by the number of people
def order_by_population():
    for con in countries.find({}).sort("population"):
        print("The country name is", con['name'], con['population'])



# 5. Get countries which have u in their name and population greater than 100 000
def search_by_name_and_order_by_population():
    for con in countries.find(
            {
                'name':
                    {'$regex': 'u', '$options': 'i'}, 'population': {'$gt': 100000}
            }
            ).sort("population"):
        print("The country name is", con['name'], con['population'])

if __name__ == '__main__':
    find_by_query('')  # TO REMOVE: change the search string
    # get_continents_and_countries()
    # order_by_population()
    # search_by_name_and_order_by_population()