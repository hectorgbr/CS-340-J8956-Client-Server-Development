
# Student: Hector Banos Ramos

from pymongo import MongoClient
from bson.objectid import ObjectId

class CRUD(object):
    """ CRUD operations for MongoDB """
           
    def __init__(self, user, password, host, port, db_name, collection_name):
        """ Initialize the CRUD object with connection details """
        # Arguments:
                    # user (str): Username for MongoDB authentication.
                    # password (str): Password for MongoDB authentication.
                    # host (str): MongoDB host address.
                    # port (int): MongoDB port number.
                    # db_name (str): Name of the MongoDB database.
                    # collection_name (str): Name of the MongoDB collection.   
        self.client = MongoClient(f'mongodb://{user}:{password}@{host}:{port}')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def create(self, data):
        """ Insert a document into the specified MongoDB collection """
         # Arguments:
                     # data (dict): Data to be inserted into the collection.
         # Returns:
                     # bool: True if insertion is successful, False otherwise.
         # Raises:
                     # Exception: If the data parameter is empty.
        if data is not None:
            result = self.collection.insert_one(data) # data should be dictionary 
            if result.inserted_id:
                return True # Insertion successful
            else:
                return False # Insertion failed
        else:
            raise Exception("Nothing to save, because data parameter is empty")

    def read(self, query):
        """ Query documents from the specified MongoDB collection """
        # Arguments: 
                    # query (dict): The key/value lookup pair to use with the MongoDB driver find API call
        cursor = self.collection.find(query)
        if cursor is not None:                        
        # Returns: 
                    # list: Resulting documents if the command is successful
            result = list(cursor)
            return result
        # an else statement is executed
        else:
            print("Failed to retrieve documents from the collection.")
            return []
        
    def update(self, query, update_data):
        """ Update document(s) in the specified MongoDB collection """
        # Update documents matching the query with the provided update_data
        # Arguments:
                    # query (dict): The query to select documents for updating.
                    # update_data (dict): The data to update in the selected  documents.
        result = self.collection.update_many(query, {'$set': update_data})
        if result is not None:
        # Return the count of modified documents
            return result.modified_count
        else:
        # Return an empty dictionary if result is None
            print("Update operation failed.")
            return {}

    def delete(self, query):
        """ Delete document(s) from the specified MongoDB collection """
        # Delete documents matching the query from the collection
        # Arguments:
                    # query (dict): The query to select documents for deletion.
        result = self.collection.delete_many(query)
        if result is not None:
        # Return the count of deleted documents
            return result.deleted_count
        else:
        # Return an empty dictionary if result is None
            print("Delete operation failed.")
            return {}
