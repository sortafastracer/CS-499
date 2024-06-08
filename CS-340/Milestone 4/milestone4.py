import os
import motor.motor_asyncio
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
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                f'mongodb://{username}:{password}@nv-desktop-services.apporto.com:31046/?authMechanism=DEFAULT&authSource=admin'
            )
            self.database = self.client['AAC']
            self.collection = self.database['animals']
            logger.info("Connected to Database")
        except (ConnectionError, ConfigurationError) as e:
            logger.error(f"Error connecting to the database: {e}")
            raise

    async def create(self, data):
        if not data:
            raise ValueError("Data parameter cannot be empty when calling create")

        try:
            result = await self.collection.insert_one(data)
            if result.inserted_id:
                logger.info("Item inserted successfully.")
                return True
            else:
                logger.error("Error: Item not inserted!")
                return False
        except PyMongoError as e:
            logger.error(f"Error inserting item: {e}")
            return False

    async def read(self, query):
        if not query:
            raise ValueError("Nothing to find. Target is empty.")

        try:
            cursor = self.collection.find(query, {"_id": False})
            result = []
            async for document in cursor:
                result.append(document)
            return result
        except PyMongoError as e:
            logger.error(f"Error reading items: {e}")
            return []

    async def update(self, findAnimal, updateAnimal):
        if not findAnimal or not updateAnimal:
            raise ValueError("Both find and update parameters must be provided when calling update.")

        try:
            result = await self.collection.update_one(findAnimal, {"$set": updateAnimal})
            if result.modified_count > 0:
                logger.info(f"Matched 1 document and updated {result.modified_count} document(s).")
            else:
                logger.info("No documents matched the filter criteria.")
        except PyMongoError as e:
            logger.error(f"Error updating item: {e}")

    async def delete(self, remove):
        if not remove:
            raise ValueError("No filter criteria provided for deletion")

        try:
            result = await self.collection.delete_one(remove)
            if result.deleted_count > 0:
                logger.info(f"Matched 1 document and deleted {result.deleted_count} document(s).")
            else:
                logger.info("No documents matched the filter criteria.")
        except PyMongoError as e:
            logger.error(f"Error deleting item: {e}")

    async def aggregate(self, pipeline):
        if not pipeline:
            raise ValueError("Pipeline parameter cannot be empty when calling aggregate")

        try:
            cursor = self.collection.aggregate(pipeline)
            result = []
            async for document in cursor:
                result.append(document)
            return result
        except PyMongoError as e:
            logger.error(f"Error aggregating items: {e}")
            return []