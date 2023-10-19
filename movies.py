import pandas as pd
import numpy as np
import os
import requests
from pymongo import MongoClient
from urllib.parse import quote_plus

movies_total_data = []
username = "raajteja"
password = "Raajteja@26"
escaped_username = quote_plus(username)
escaped_password = quote_plus(password)
uri = f"mongodb+srv://{escaped_username}:{escaped_password}@cluster0.vysw3oc.mongodb.net"
client = MongoClient(uri)
db = client.movies_data
collection = db.movies
documents = list(collection.find())
for document in documents:
    movies_total_data.append(document)
client.close()

def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directory '{directory}' created successfully.")
    else:
        print(f"Directory '{directory}' already exists.")

directory_name = "movies"
create_directory_if_not_exists(directory_name)

# Task 1
def create_movies_with_comments(movies_data, comments_data):
    movies_with_comments = pd.merge(movies_data, comments_data, how='left', on='movie_id')
    movies_with_no_comments = movies_with_comments[movies_with_comments['comment'].isna()]
    movies_with_no_comments.to_csv('movies/movies_with_no_comments.csv', index=False)
    return movies_with_comments

# Task 2
def create_runtime_columns(movies_with_comments):
    movies_with_comments['low_runtime'] = np.where(movies_with_comments['runtime'] > 50, 'yes', 'no')
    movies_with_comments['high_runtime'] = np.where(movies_with_comments['runtime'] > 50, 'no', 'yes')
    movies_with_comments.to_csv('movies/movies_with_comments.csv', index=False)
    return movies_with_comments

# Task 3
def filter_movies_data(movies_data):
    filtered_movies = movies_data[(movies_data['imdb_rating'] > 8) & (movies_data['year'] >= 2000) & (movies_data['awards'] > 3)].sort_values('released')
    filtered_movies.to_csv('movies/movies_rating_8_released_aft_2000.csv', index=False)
    return filtered_movies

# Task 4
def simplify_theatre_data(theatre_data):
    theatre_data['theaterId'] = theatre_data['theatre'].apply(lambda x: x['theaterId'])
    theatre_data['street1'] = theatre_data['theatre'].apply(lambda x: x['address'][0]['street1'])
    theatre_data['city'] = theatre_data['theatre'].apply(lambda x: x['address'][0]['city'])
    theatre_data['street2'] = theatre_data['theatre'].apply(lambda x: x['address'][0]['street2'])
    theatre_data[0] = theatre_data['theatre'].apply(lambda x: x['geo'][0])
    theatre_data[1] = theatre_data['theatre'].apply(lambda x: x['geo'][1])
    theatre_simplified = theatre_data[['theaterId', 'street1', 'city', 'street2', 0, 1]]
    theatre_simplified.to_csv('movies/theatre_simplified.csv', index=False)
    return theatre_simplified

# Task 5
def fetch_lat_long(theatre_simplified):
    def get_lat_long(city):
        url = f"https://nominatim.openstreetmap.org/search?q={city}&format=json"
        response = requests.get(url).json()
        if response:
            if isinstance(response, list) and len(response) > 0 and 'lat' in response[0] and 'lon' in response[0]:
                return response[0]['lat'], response[0]['lon']
            else:
                print(f"No latitude or longitude found for {city}.")
                return None, None
        else:
            print(f"No response received for {city}.")
            return None, None

    theatre_simplified[['lat', 'long']] = theatre_simplified['city'].apply(lambda x: pd.Series(get_lat_long(x)))
    theatre_simplified.to_csv('movies/theatre_simplified_with_lat_long.csv', index=False)
    return theatre_simplified

# Task 6
def movies_outside_usa(movies_data):
    released_outside_usa = movies_data[movies_data['countries'] != 'USA']
    released_outside_usa.to_csv('movies/released_outside_usa.csv', index=False)
    return released_outside_usa

movies_data = pd.DataFrame(movies_total_data[0]["movies_data"])
comments_data = pd.DataFrame(movies_total_data[0]["comments_data"])
theatre_data = pd.DataFrame(movies_total_data[0]["theatre_data"])

movies_with_comments = create_movies_with_comments(movies_data, comments_data)
movies_with_runtime = create_runtime_columns(movies_with_comments)
filtered_movies = filter_movies_data(movies_data)
theatre_simplified = simplify_theatre_data(theatre_data)
theatre_with_lat_long = fetch_lat_long(theatre_simplified)
released_outside_usa = movies_outside_usa(movies_data)
