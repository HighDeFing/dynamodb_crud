from decimal import Decimal
from fastapi import HTTPException

import boto3
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class Movies:
    """Encapsulates an Amazon DynamoDB table of movie data.

    Example data structure for a movie record in this table:
        {
            "year": 1999,
            "title": "For Love of the Game",
            "info": {
                "directors": ["Sam Raimi"],
                "release_date": "1999-09-15T00:00:00Z",
                "rating": 6.3,
                "plot": "A washed up pitcher flashes through his career.",
                "rank": 4987,
                "running_time_secs": 8220,
                "actors": [
                    "Kevin Costner",
                    "Kelly Preston",
                    "John C. Reilly"
                ]
            }
        }
    """

    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        # The table variable is set during the scenario in the call to
        # 'exists' if the table exists. Otherwise, it is set by 'create_table'.
        self.table = None

    def check_table_exists(self, table_name):

        # Get the table resource
        table = self.dyn_resource.Table(table_name)

        try:
            # Try to load the table's metadata
            table.load()
            return True  # If no exception, the table exists
        except ClientError as e:
            # If the table does not exist, a ResourceNotFoundException is thrown
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                raise e  # For other errors, re-raise the exception


    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table that can be used to store movie data.
        The table uses the release year of the movie as the partition key and the
        title as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        if self.check_table_exists(table_name):
            self.table = self.dyn_resource.Table(table_name)
        else:
            try:
                self.table = self.dyn_resource.create_table(
                    TableName=table_name,
                    KeySchema=[
                        {"AttributeName": "year", "KeyType": "HASH"},  # Partition key
                        {"AttributeName": "title", "KeyType": "RANGE"},  # Sort key
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "year", "AttributeType": "N"},
                        {"AttributeName": "title", "AttributeType": "S"},
                    ],
                    ProvisionedThroughput={
                        "ReadCapacityUnits": 10,
                        "WriteCapacityUnits": 10,
                    },
                )
                self.table.wait_until_exists()
            except ClientError as err:
                logger.error(
                    "Couldn't create table %s. Here's why: %s: %s",
                    table_name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
                raise
            else:
                return self.table

    def add_movie(self, title, year, plot, rating):
        """
        Adds a movie to the table.

        :param title: The title of the movie.
        :param year: The release year of the movie.
        :param plot: The plot summary of the movie.
        :param rating: The quality rating of the movie.
        """
        if self.get_movie(title, year):
            raise HTTPException(status_code=400, detail="Item already exists")
        try:
            Item = {
                "year": year,
                "title": title,
                "info": {"plot": plot, "rating": Decimal(str(rating))},
            }
            self.table.put_item(Item = {
                "year": year,
                "title": title,
                "info": {"plot": plot, "rating": Decimal(str(rating))},
            })
            return Item
        except ClientError as err:
            logger.error(
                "Couldn't add movie %s to table %s. Here's why: %s: %s",
                title,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def list_all_items(self, table_name):

        # Get the table resource
        table = self.dyn_resource.Table(table_name)

        # Scan the table and retrieve all items
        response = table.scan()
        data = response['Items']

        # Handle pagination if needed
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        return data

    def get_movie(self, title, year):
        """
        Gets movie data from the table for a specific movie.

        :param title: The title of the movie.
        :param year: The release year of the movie.
        :return: The data about the requested movie.
        """
        try:
            response = self.table.get_item(Key={"year": year, "title": title})
        except ClientError as err:
            logger.error(
                "Couldn't get movie %s from table %s. Here's why: %s: %s",
                title,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            try:
                return response['Item']
            except KeyError:
                return None

    def update_movie(self, title, year, rating, plot):
        """
        Updates rating and plot data for a movie in the table.

        :param title: The title of the movie to update.
        :param year: The release year of the movie to update.
        :param rating: The updated rating to the give the movie.
        :param plot: The updated plot summary to give the movie.
        :return: The fields that were updated, with their new values.
        """
        #print("THIS RATING", rating)
        try:
            response = self.table.update_item(
                Key={"year": year, "title": title},
                UpdateExpression="set info.rating=:r, info.plot=:p",
                ExpressionAttributeValues={":r": Decimal(str(rating)), ":p": plot},
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as err:
            logger.error(
                "Couldn't update movie %s in table %s. Here's why: %s: %s",
                title,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Attributes"]

    def delete_movie(self, title, year):
        if not self.get_movie(title, year):
            raise HTTPException(status_code=400, detail="Item doesn't exists")
        try:
            response = self.table.delete_item(Key={"year": year, "title": title})
        except ClientError as e:
            raise HTTPException(status_code=500, detail=str(e))
        return response


