import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import pprint

class AnimalShelter(object):
    """ CRUD operations for animal collection in MongoDB """

    def __init__(self, username, password):
        self.client = MongoClient('mongodb://%s:%s@nv-desktop-services.apporto.com:31046/?authMechanism=DEFAULT&authSource=admin' % (username, password))
        self.database = self.client['AAC']
        self.collection = self.database['animals']
        pprint.pprint("Connected to Database")
            
    def create(self, data):
        if data is not None:
            inserted = self.collection.insert_one(data)
            if inserted != 0:
                pprint.pprint("Item inserted successfully.")
                return True
            else:
                pprint.pprint("Error: Item not inserted!")
                return False
        else:
            raise Exception("Nothing to save, because data parameter is empty")

    def read(self, query):
        try:
            if query is not None:
                result = list(self.collection.find(query, {"_id": False}))
                return result
            else:
                raise Exception("Nothing to find. Target is empty.")
                return False
        except Exception as e:
            print("Exception has occured: ", e)
        
    def update(self, findAnimal, updateAnimal):
        result = self.collection.update_one(findAnimal, updateAnimal)
        if result.modified_count > 0:
            pprint.pprint(f"Matched 1 document and updated {result.modified_count} document(s).")
        else:
            pprint.pprint("No documents matched the filter criteria.")
        
    def delete(self, remove):
        if remove:
            result = self.collection.delete_one(remove)
            pprint.pprint(f"Matched 1 document and deleted {result.deleted_count} document(s).")
        else:
            pprint.pprint("No documents matched the filter criteria.")
