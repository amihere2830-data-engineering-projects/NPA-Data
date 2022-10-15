from pymongo import MongoClient
import pymongo
from requests import delete

class NPAdb:
    def __init__(self, client, db_name:str) -> None:
        self.client = client
        self.db = self.client[db_name]

    def create_db(self):
        ...

    def get_collection(self, collection_name):
        return self.db[collection_name]

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
    def query(self, q_name:str,collection_name:str,query_by_name='',\
        relation: bool=False,query_by_id:str=''):
        """
        Query mongo collection using query type (find and find_one), and 
        query by collection name and id
        """
        collection = self.get_collection(collection_name)
        if q_name == 'find':
            if relation:
                return collection.find({}, {'_id': 1})
            else:
                return collection.find({})
        elif q_name =='find_one':
            return collection.find_one({'name': query_by_name})

        if query_by_id !='':
            from bson.objectid import ObjectId
            _id = ObjectId(query_by_id)
            return collection.find_one({'_id': _id})


    def count_documents(self,collection_name:str, filter=''):
        if filter=='':
            return self.get_collection(collection_name).count_documents(filter={})

    
    def update_one(self,collection_name:str, document_id:str,set:list[tuple]=[], rename:list[tuple]=[], remove:tuple=(False,''))->None:
        """
        Update a document by its id
        by creating new field, rename field and removing field
        """
        from bson.objectid import ObjectId

        _id = ObjectId(document_id)
        if len(set)>0 and len(rename)>0:
            all_updates = {
                "$set": {st[0]: st[1] for st in set},   # create a new field
                "$rename": {name[0]: name[1] for name in rename},   # rename a field
            }
            self.get_collection(collection_name).updte_one({"_id": _id}, all_updates)


        if len(set)>0:
            set_new_field = {
                "$set": {st[0]: st[1] for st in set},   # create a new field
            }
            self.get_collection(collection_name).updte_one({"_id": _id}, set_new_field)

        if len(rename)>0:
            rename_field = {
                "$rename": {name[0]: name[1] for name in rename},   # rename a field
            }
            self.get_collection(collection_name).updte_one({"_id": _id}, rename_field)

        if remove[0]:
            self.get_collection(collection_name).updte_one({"_id": _id}, {"$unset": {remove[1]: ''}})


    def replace_one(self,collection_name:str, document_id:str, new_doc:dict)->None:
        """
        Replace existing document with a new document without chaning the id
        """
        from bson.objectid import ObjectId

        _id = ObjectId(document_id)

        self.get_collection(collection_name).replace_one({"_id", _id}, new_doc)

    def delete(self,delete_type:str,collection_name:str, document_id:str)->None:
        """
        Delete a document by id
        """
        if delete_type =='one':
            from bson.objectid import ObjectId

            _id = ObjectId(document_id)

            self.get_collection(collection_name).delete_one({"_id": _id})
        elif delete_type =='many':
            self.get_collection(collection_name).delete_many({})