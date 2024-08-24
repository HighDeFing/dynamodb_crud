from fastapi import HTTPException

import boto3
from fastapi import FastAPI

from movies.movies import Movies

app = FastAPI()

dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
movie = Movies(dynamodb)
movie_table = movie.create_table("movies")

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI DynamoDB Todo App!"}

@app.post("/create_table", response_model=None)
def create_table():
    movie_table = movie.create_table("movies")
    return movie_table

@app.put("/add_movie", response_model=None)
def add_movie(title: str, year: int, plot: str, rating: float):
    # print(title, year, plot, rating)
    added_movie = movie.add_movie(title, year, plot, rating)
    return added_movie

@app.get("/get_all_movies")
def get_all_movies(table_name):
    items = movie.list_all_items(table_name)
    return items

@app.get("/get_movie")
def read_root(title: str, year: int):
    movie_info = movie.get_movie(title, year)
    return movie_info

@app.put("/update_movie")
def update_movie(title: str, year: int, plot: str, rating: float):
    movie_info = movie.update_movie(title, year, rating, plot)
    if not movie_info:
        raise HTTPException(status_code=404, detail="Item not found")
    return movie_info

@app.delete("/delete_movie")
def delete_movie(title: str, year: int):
    movie_info = movie.delete_movie(title, year)
    print(movie_info)
    if 'ResponseMetadata' not in movie_info:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"message": f"Item {title} from year {year} deleted successfully"}
