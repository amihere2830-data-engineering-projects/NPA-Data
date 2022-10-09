from pymongo import MongoClient

class NPAdb:
    def __init__(self, client, db_name:str) -> None:
        self.client = client
        self.db = self.client[db_name]

    def create_db(self):
        ...

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def create_collection(self):
        ...

    def available_collections(self)->list[str]:
        collection_names = self.db.list_collection_names()
        return collection_names

    def insert_document(self,data:list[dict], collection_name:str)->None:
        collection = self.get_collection(collection_name)
        try:
            inserted_ids = collection.insert_many(data)
        except pymongo.errors.OperationFailure as exc:
            inserted_ids = [doc['_id'] for doc in data if not is_failed(doc, exc)] 
            # print(f"{len(inserted_ids)} documents were not inserted")
        else:
            inserted_ids = inserted_ids.inserted_ids
            print(f"{len(inserted_ids)} documents inserted successfully!!!!!")
    def query(self, q_name:str, collection_name:str, query_by_name=''):
        collection = self.get_collection(collection_name)
        if q_name == 'find':
            return collection.find({})
        elif q_name =='find_one':
            return collection.find_one({'name': query_by_name})
