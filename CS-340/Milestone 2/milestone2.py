import os
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import logging
from pymongo.errors import ConnectionError, ConfigurationError, OperationFailure, PyMongoError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AnimalShelter")

class AnimalShelter(object):
    """ CRUD operations for animal collection in MongoDB """

    def __init__(self, username=None, password=None):
        # Use environment variables for credentials
        username = username or os.getenv("MONGO_USERNAME")
        password = password or os.getenv("MONGO_PASSWORD")

        if not username or not password:
            raise ValueError("Username and password must be provided")

        try:
            self.client = MongoClient(
                f'mongodb://{username}:{password}@nv-desktop-services.apporto.com:31046/?authMechanism=DEFAULT&authSource=admin'
            )
            self.database = self.client['AAC']
            self.collection = self.database['animals']
            logger.info("Connected to Database")
        except (ConnectionError, ConfigurationError) as e:
            logger.error(f"Error connecting to the database: {e}")
            raise

    def create(self, data):
        if not data:
            raise ValueError("Data parameter cannot be empty when calling create")

        try:
            inserted = self.collection.insert_one(data)
            if inserted.inserted_id:
                logger.info("Item inserted successfully.")
                return True
            else:
                logger.error("Error: Item not inserted!")
                return False
        except PyMongoError as e:
            logger.error(f"Error inserting item: {e}")
            return False

    def read(self, query):
        if not query:
            raise ValueError("Nothing to find. Target is empty.")

        try:
            result = list(self.collection.find(query, {"_id": False}))
            return result
        except PyMongoError as e:
            logger.error(f"Error reading items: {e}")
            return []

    def update(self, findAnimal, updateAnimal):
        if not findAnimal or not updateAnimal:
            raise ValueError("Both find and update parameters must be provided when calling update.")

        try:
            result = self.collection.update_one(findAnimal, {"$set": updateAnimal})
            if result.modified_count > 0:
                logger.info(f"Matched 1 document and updated {result.modified_count} document(s).")
            else:
                logger.info("No documents matched the filter criteria.")
        except PyMongoError as e:
            logger.error(f"Error updating item: {e}")

    def delete(self, remove):
        if not remove:
            raise ValueError("No filter criteria provided for deletion")

        try:
            result = self.collection.delete_one(remove)
            if result.deleted_count > 0:
                logger.info(f"Matched 1 document and deleted {result.deleted_count} document(s).")
            else:
                logger.info("No documents matched the filter criteria.")
        except PyMongoError as e:
            logger.error(f"Error deleting item: {e}")
