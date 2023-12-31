import pandas as pd
from pymongo import MongoClient
import calendar

def connect_to_mongodb(username, password, cluster_url, database_name):
    mongo_conn_str = f'mongodb+srv://{username}:{password}@{cluster_url}/{database_name}?retryWrites=true&w=majority'
    client = MongoClient(mongo_conn_str)
    return client

def get_unique_states(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]
    return sorted(collection.distinct('State'))

def get_month_year_timeline(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]
    pipeline_timeline = [
        {'$project': {'year': {'$year': '$Date'}, 'month': {'$month': '$Date'}}},
        {'$group': {'_id': {'year': '$year', 'month': '$month'}}},
        {'$sort': {'_id.year': 1, '_id.month': 1}}
    ]
    results = collection.aggregate(pipeline_timeline)
    return [f"{calendar.month_abbr[result['_id']['month']]} {result['_id']['year']}" for result in results]

def query_data(client, database_name, collection_name, selected_states, start_date, end_date):
    db = client[database_name]
    collection = db[collection_name]
    pipeline_output = [
        {'$match': {'State': {'$in': selected_states}, 'Date': {'$gte': start_date, '$lt': end_date}}},
        {'$project': {'Year': {'$year': '$Date'}, 'Month': {'$month': '$Date'}, 'State': 1, 'New_Cases': 1, 'New_Deaths': 1}},
        {'$group': {'_id': {'State': '$State', 'Year': '$Year', 'Month': '$Month'}, 'Total_Cases': {'$sum': '$New_Cases'}, 'Total_Deaths': {'$sum': '$New_Deaths'}}},
        {'$lookup': {'from': 'StatesPop', 'localField': '_id.State', 'foreignField': 'State', 'as': 'Population'}},
        {'$unwind': '$Population'},
        {'$project': {'_id': 0, 'State': '$_id.State', 'YearMonth': { '$concat': [ { '$toString': '$_id.Year' }, ' ', { '$cond': [ { '$lt': [ '$_id.Month', 10 ] }, '0', '' ] } , { '$toString': '$_id.Month' } ] }, 'Total_Cases': 1, 'Cases_per_100k': {'$multiply': [{'$divide': ['$Total_Cases', '$Population.Population']}, 100000]}, 'Total_Deaths': 1, 'Deaths_per_100k': {'$multiply': [{'$divide': ['$Total_Deaths', '$Population.Population']}, 100000]} }},
        {'$sort': {'State': 1, 'YearMonth': 1}}
    ]
    result = list(collection.aggregate(pipeline_output))
    return pd.DataFrame(result)

def close_mongodb_connection(client):
    client.close()
