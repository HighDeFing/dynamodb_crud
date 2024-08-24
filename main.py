from movies.movies import Movies
import boto3

if __name__ == '__main__':
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
    movie = Movies(dynamodb)
    movie_table = movie.create_table("movies")

    #movie_table.wait_until_exists()

    # Print out some data about the table.
    #print(movie_table.item_count)

    #movie.add_movie("The Color of Pomegranates", 1969, "The Color of Pomegranates is a biography", 5.0 )

    details = movie.get_movie("The Color of Pomegranates", 1969)
    print(details)

    #items = movie.list_all_items("movies")
    #for item in items:
    #    print(item)