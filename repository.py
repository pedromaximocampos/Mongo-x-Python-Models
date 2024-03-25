from pymongo import MongoClient
from pymongo.errors import (DuplicateKeyError, BulkWriteError, WriteConcernError, CursorNotFound, OperationFailure,
                            InvalidOperation, ConfigurationError, ConnectionFailure, DocumentTooLarge)
from bson.objectid import ObjectId
from datetime import timedelta


def insert_exceptions_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except DuplicateKeyError as e:
            raise DuplicateKeyError(f"Error to insert a document':{e}\n"
                                    f"Check if you are not inserting one document with with a repeated unique index")

        except (CursorNotFound, OperationFailure, WriteConcernError) as e:
            raise type(e)(f"Error to insert a document {e}\n"
                          f"Check your permission to insert, your cluster infrastructure and your connection to the Bank.")

        except ConfigurationError as e:
            raise ConfigurationError(f"Error to insert a document':{e}\n"
                                     f"Check if the parameters are valid or present!")
        except DocumentTooLarge as e:
            raise DocumentTooLarge(f"Error to insert a document:{e}\n"
                                   f"Apparently the document you are trying to insert is larger than the Bank's capacity")

    return wrapper


def select_exceptions_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except (CursorNotFound, OperationFailure, ConnectionFailure) as e:
            raise type(e)(f"Error to find  the documents.{e}\n"
                          f"Check your permission to select, your cluster infrastructure and your connection to the Bank.")

        except ConfigurationError as e:
            raise f"Error to find  the documents:{e}\nCheck if the parameters are valid or present!"

    return wrapper


def delete_exceptions_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DuplicateKeyError as e:
            raise DuplicateKeyError(f"Error to delete documents:{e}\n"
                                    f"Check if you are not deleting one document with with a repeated unique index")

        except (CursorNotFound, OperationFailure, WriteConcernError, InvalidOperation) as e:
            raise e(f"Error to delete documents{e}\n"
                    f"Check your permission to delete, your cluster infrastructure and your connection to the Bank.")

        except (ConfigurationError, BulkWriteError) as e:
            raise e(f"Error to delete documents:{e}\n"
                    f"Check if the parameters are valid or present!")
    return wrapper

def update_exceptions_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except DuplicateKeyError as e:
            raise DuplicateKeyError(f"Error to update the documents:{e}\n"
                                    f"Check that you are not updating a document with a repeated unique index")

        except (CursorNotFound, OperationFailure, WriteConcernError, InvalidOperation) as e:
            raise type(e)(f"Error to update the documents:{e}\n"
                          f"Check your permission to update, your cluster infrastructure and your connection to the Bank. ")

        except (ConfigurationError, BulkWriteError) as e:
            raise ConfigurationError(f"Error to update the documents:{e}\n"
                                     f"Check if the parameters are valid or present!")
        except DocumentTooLarge as e:
            raise DocumentTooLarge(f"Error to update the documents:{e}\n"
                                   f"Apparently the document you are trying to update is larger than the Bank's capacity")

    return wrapper


def index_exceptions_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DuplicateKeyError as e:
            raise DuplicateKeyError(f"Error to create the index:{e}\n"
                                    f"Check that you are not creating an index with a repeated unique index")

        except (OperationFailure, WriteConcernError, InvalidOperation) as e:
            raise e(f"Error to create the index:{e}\n"
                    f"Check your permission to create index, your cluster infrastructure and your connection to the Bank. ")

        except (ConfigurationError, DocumentTooLarge) as e:
            raise e(f"Error to create the index:{e}\n"
                    f"Check if the parameters are valid or present!")

    return wrapper


class Repository:

    def __init__(self, db_connection: MongoClient, collection_name: str):
        """
        Repository Constructor, a class that has the goal of performing CRUD and other actions on the collection passed by
        parameter in 'collection_name', after receiving a connection created by the MongoDBConnectionHandler class.

        :param db_connection: (MongoClient) Connection previously created with the MongoDBConnectionHandler class.

        :param collection_name: (str) Name of the collection to be searched.
        """

        self.__db_connection = db_connection
        self.__collection_name = collection_name
        self.__id_query = "_id"
        self.__or_query = "$or"
        self.__set_query = "$set"
        self.__increment_query = "$inc"
        self.__unset_query = "$unset"

    def __get_collection(self):
        try:
            collection = self.__db_connection.get_collection(self.__collection_name)
        except OperationFailure as e:
            raise OperationFailure(f"Error trying to collect collection  '{self.__collection_name}':{e},\n"
                                   f"Check the name of your collection, your permission or authentication")
        return collection

    @insert_exceptions_decorator
    def insert_one_document(self, document: dict) -> dict:
        """
        Function that receives a single document in dict format to be inserted into the passed collection
         when instantiating the class. If inserted successfully, this document returns.

        :param document: (Dict) document to be inserted.

        :return: 'Document' if it is successfully inserted into the MongoDB collection.
        """
        collection = self.__get_collection()

        'Always call collections in CRUD functions to avoid the risk of becoming outdated'

        collection.insert_one(document)

        return document

    @insert_exceptions_decorator
    def insert_list_of_documents(self, list_of_documents: list[dict]) -> list[dict]:
        """
        Function that receives a list of documents in dict format to be inserted into the passed collection
        when instantiating the class. If inserted successfully, this list of documents is returned.

        :param list_of_documents: (Dict) documents to be inserted.

        :return: List[documents], if it is successfully inserted into the MongoDB collection.
        """

        collection = self.__get_collection()

        collection.insert_many(list_of_documents)

        return list_of_documents

    @select_exceptions_decorator
    def select_many_documents(self, search_filter: dict, return_options: dict = None) -> list[dict]:
        """
        This function aims to return a list of documents based on the mandatory search_filter parameter,
        the return can be more specific by passing a return_options parameter.

        :param search_filter: (Dict) Filter dictionary that will base the document search.
                      Example: {'nome': 'pedro', 'address': {'$exists': True}}; if the search filter has more than one
                       {key: value} the result will only bring documents with both keys and values (inclusive).

        :param return_options: (Dict)Optional Dictionaty that could be used to specify which fields must be returned or
                       excluded at the consult, or to add another search criteria.
                       If not provided, all fields will be returned.
                       REMINDER: Does not accept queries with $ prepositions, such as $exists...!!!
                       Example 1: {'_id': 0, 'name': 1, 'cpf': 1} to include just the fields 'name' and 'cpf' (excluding '_id').
                       Example 2: {'name': 1, 'cpf': 1} to include 'name' and 'cpf' at the returned data.

        :return: (list[dict]) List of documents found in MongoDB.
        """
        collection = self.__get_collection()

        if return_options is None:
            response = collection.find(search_filter)
        else:
            response = collection.find(search_filter, return_options)

        data_list = [data for data in response]

        return data_list

    @select_exceptions_decorator
    def select_one_document(self, search_filter: dict, return_options: dict = None) -> dict:
        """
        This function returns one (or the first)  document found in the database and meets the 'search_filter';
                the return could be more specific passing the 'return_options' parameter.

        :param search_filter:  (Dict) Filter dictionary that will base the document search.
                      Example: {'nome': 'pedro', 'address': {'$exists': True}}; if the search filter has more than one
                       {key: value} the result will only bring documents with both keys and values (inclusive).


        :param return_options: (Dict) Optional Dictionaty that could be used to specify which fields must be returned or
                       excluded at the consult, or to add another search criteria.
                       If not provided, all fields will be returned.
                       REMINDER: Does not accept queries with $ prepositions, such as $exists...!!!
                       Example 1: {'_id': 0, 'name': 1, 'cpf': 1} to include just the fields 'name' and 'cpf' (excluding '_id').
                       Example 2: {'name': 1, 'cpf': 1} to include 'name' and 'cpf' at the returned data.

        :return: (Dict) Returns the first document found in MondoDB that meets the filters.
        """
        collection = self.__get_collection()

        if return_options is not None:
            data = collection.find_one(search_filter, return_options)
        else:
            data = collection.find_one(search_filter)

        return data

    @select_exceptions_decorator
    def select_many_by_order(self, search_filter: dict, order_by: list[tuple], return_options: dict = None) -> list[dict]:
        """
        This functions has the goal to return an ordered list of documents, based on the mandatory params 'search_filter'
         and 'order_by' being able to have a return more specific passing the optional parameter 'return_options'.

        :param search_filter: (Dict) Filter dictionary that will base the document search.
                      Example: {'nome': 'pedro', 'address': {'$exists': True}}; if the search filter has more than one
                       {key: value} the result will only bring documents with both keys and values (inclusive).

        :param order_by: (List[tuple]) List of tuples, each one contains info of how to order the search results.
                 Each tuple has two elements ('key', direction) where the 'key'
                 is the attribute that will be ordenated by the 'direction', where 1 is ascending and -1 is descending.
                 Example: [('name', 1), ('age', -1)] order the 'name' attribute in ascending order and the 'age' in descending order.

        :param return_options: (Dict) Optional Dictionaty that could be used to specify which fields must be returned or
                       excluded at the consult, or to add another search criteria.
                       If not provided, all fields will be returned.
                       REMINDER: Does not accept queries with $ prepositions, such as $exists...!!!
                       Example 1: {'_id': 0, 'name': 1, 'cpf': 1} to include just the fields 'name' and 'cpf' (excluding '_id').
                       Example 2: {'name': 1, 'cpf': 1} to include 'name' and 'cpf' at the returned data.

        :return: (list[dict]) List of documents found in the database that corresponding the search criteria, ordered by the
                            order_by param.
        """

        collection = self.__get_collection()

        if return_options is None:
            response = collection.find(search_filter).sort(order_by)
        else:
            response = collection.find(search_filter, return_options).sort(order_by)

        data_list = [data for data in response]

        return data_list

    @select_exceptions_decorator
    def select_by_object_id(self, object_id: str, return_options: dict = None) -> dict:
        """
        This function will return onde document based on the unique '_id' passed by the 'object_id' param.

        :param object_id: (str) '_id' of the document existent in MongoDB.

        :param return_options: (Dict) Optional Dictionaty that could be used to specify which fields must be returned or
                       excluded at the consult, or to add another search criteria.
                       If not provided, all fiels will be returned.
                       REMINDER: Does not accept queries with $ prepositions, such as $exists...!!!
                       Example 1: {'_id': 0, 'name': 1, 'cpf': 1} to include just the fields 'name' and 'cpf' (excluding '_id').
                       Example 2: {'name': 1, 'cpf': 1} to include 'name' and 'cpf' at the returned data.

        :return: (dict) the document with the corresponding '_id'.
        """

        collection = self.__get_collection()

        if return_options is None:
            data = collection.find_one({self.__id_query: ObjectId(object_id)})

        else:
            data = collection.find_one({self.__id_query: ObjectId(object_id)}, return_options)

        return data

    @select_exceptions_decorator
    def search_many_or(self, search_filter: list[dict], return_options: dict = None, order_by: list[tuple] = None) -> list[dict]:
        """
        This function aims to return a list of documents found in MongoDB that match the 'search_filter' parameter.
        However, this parameter is no longer inclusive, meaning that documents will be returned that match one applied
        filter OR another that is also present. The return can be more specific using the optional parameters 'return_options' and 'order_by'.

        :param search_filter: (list[dict]) A list of dictionaries that will determine a search filter in MongoDB.
                Example: search_filter = [{"name": "pedro"}, {"cpf": {"$exists": True}}], in this case,
                the return will bring documents that have the attribute "name" equal to "pedro" or documents that have the existing "cpf" attribute.

        :param return_options: (Dict) Optional Dictionaty that could be used to specify which fields must be returned or
                       excluded at the consult, or to add another search criteria.
                       If not provided, all fields will be returned.
                       REMINDER: Does not accept queries with $ prepositions, such as $exists...!!!
                       Example 1: {'_id': 0, 'name': 1, 'cpf': 1} to include just the fields 'name' and 'cpf' (excluding '_id').
                       Example 2: {'name': 1, 'cpf': 1} to include 'name' and 'cpf' at the returned data.

        :param order_by: (List[tuple]) List of tuples, each tuple contains information about how to order the search results.
                        Each tuple can have two elements ('key', direction), where 'key' would be the information that will base the sorting and 'direction'
                        will inform the query whether it will be sorted in ascending or descending order (1, -1), respectively.
                        Example: [('name', 1), ('age', -1)] sorts the results first by the 'name' field in ascending order and then by the 'age' field in descending order.

        :return: (list[dict]) A list of documents found in the database that match any of the search criteria,
                                which can be further specified by the return_options and possibly sorted by the order_by.

        """

        collection = self.__get_collection()

        if return_options is None and order_by is None:
            response = collection.find({self.__or_query: search_filter})

        elif return_options is None:
            response = collection.find({self.__or_query: search_filter}).sort(order_by)

        elif order_by is None:
            response = collection.find({self.__or_query: search_filter}, return_options)

        else:
            response = collection.find({self.__or_query: search_filter}, return_options).sort(order_by)

        data = [data for data in response]

        return data

    @update_exceptions_decorator
    def update_one_registry(self, search_filter: dict, properties: dict, query: str = None) -> int:
        """
        Updates a unique document based on the 'search filter'.
        If the filter is recurrent in the MongoDB documents, it will update the first one found!
            REMINDER:
                    Object_id at 'search_filter' it's a good practice!!!!!!


        :param search_filter: (dict) Dictionary containing the filter criteria to identify the documents to be updated.

        :param properties: (dict) Dictionary containing the new values to be set in the updated documents.

        :param query: (str) Optional query string to define the type of update operation to be performed.
                    If not passed as a parameter, '$set' will be used as the default query.

        :return: (int) The number of documents modified by the update operation.
                    The returned number will be 0 if the requested changes are already registered in MongoDB, meaning that,
                    if you attempt to change the 'name' property to 'pedro' and it already has that value in the database, it will return 0.
                    Additionally, if the expected record specified by 'search_filter' is not found, the return will also be 0.
        """
        collection = self.__get_collection()

        if query is None:
            response = collection.update_one(search_filter, {self.__set_query: properties})
        else:
            response = collection.update_one(search_filter, {query: properties})

        return response.modified_count

    @update_exceptions_decorator
    def update_many_registries(self, search_filter: dict, properties: dict, query: str = None) -> int:
        """
        Updates all documents in MongoDB that have properties similar to the 'search_filter'.

        REMINDER: In this case, the use of the Object_id isn't necessary!

        :param search_filter: (dict) Dictionary containing the filter criteria to identify the documents to be updated.

        :param properties: (dict) Dictionary containing the new values to be set in the updated documents.

        :param query: (str) Optional query string to define the type of update operation to be performed.
                    If not passed as a parameter, '$set' will be used as the default query.

        :return: (int) The number of documents modified by the update operation.
                    The returned number will be 0 if the requested changes are already registered in MongoDB, meaning that,
                    if you attempt to change the 'name' property to 'pedro' and it already has that value in the database, it will return 0.
                    Additionally, if the expected record specified by 'search_filter' is not found, the return will also be 0.
        """

        collection = self.__get_collection()

        if query is None:
            response = collection.update_many(search_filter, {self.__set_query: properties})
        else:
            response = collection.update_many(search_filter, {query: properties})

        return response.modified_count

    @update_exceptions_decorator
    def update_many_with_increment(self, search_filter: dict, key_and_number: dict) -> int:
        """
        Updates multiple documents at MongoDB based on the criteria provided by the 'search_filter' param,
        incrementing/decrementing the values of the properties specified at the 'key_and_number' param.

        :param search_filter: (dict) Dictionary containing the filter criteria to identify documents to be updated.

        :param key_and_number: (dict) Dictionary contaning atributes to be modify and the values of that modificantion.
                                Therefore, the 'dict' must be something similar to this:
                                    dict = {"key1": 10, "key2": -5, ...}
                                Remembering the key will a str and decrement/increment will be an int/float.

        :return: (int) The number of documents modified by the update operation.
                    The number returned will be 0 when what was passed is already registered in MongoDB, that is,
                    If you want to change the 'name' property to 'pedro' and it is already like that in the database, it will return 0.
                    Or if the expected record passed through 'search_filter' is not found, the return will also be 0.
         """

        collection = self.__get_collection()

        modified_count = 0

        for key, number in key_and_number.items():
            if type(number) not in [int, float]:
                raise ValueError("The value of increment/decrement must be only an int or a float!")

            response = collection.update_many(search_filter, {self.__increment_query: {key: number}})
            modified_count += response.modified_count

        return modified_count

    @delete_exceptions_decorator
    def delete_registry(self, search_filter: dict) -> int:
        """
        Deletes one document that has the properties equals to those passed by the 'search_filter' param.
            REMINDER:
                DELETE ALWAYS A GOOD PRACTICE TO INCLUDE THE Object_id AT THE 'search_filter'!!!!

        :param search_filter: (dict) Dictionary with the filter criteria to identify the document to be deleted.

        :return: (int) Number of deletions made successfully.
                    The return number will be zero when the function can't find the document or your property.
                    Therefore, if the document doesn't exist or the property passed as a parameter doesn't exist.
        """

        collection = self.__get_collection()

        response = collection.delete_one(search_filter)

        return response.deleted_count

    @delete_exceptions_decorator
    def delete_many_registries(self, search_filter: dict) -> int:
        """
        Deletes many documents that have keys/values equals to those passed by the 'search_filter' parameter.
        REMINDER:
            IF YOU WANT TO DELETE ONE SPECIFIC DOCUMENT, CONSIDER THE 'delete_registry' FUNCTION.

        :param search_filter: (dict) Dictionary with the filter criteria to identify the documents to be deleted.

        :return: (int) Number of deletions made successfully.
                    The return number will be zero when the function cant find the document or your property.
                    Therefore, if the document doesn't exist or the property passed as a parameter doesn't exist.
        """

        collection = self.__get_collection()

        response = collection.delete_many(search_filter)

        return response.deleted_count

    @index_exceptions_decorator
    def create_new_index_ttl(self, time_metric: str, time_to_live: int, name_index: str, order_by: int = 1) -> bool:
        """
        Create an index_ttl (time to live), o meaning an index that, after a specified time,
         will cause MongoDB to automatically delete documents that have the attribute with the same 'key' as the 'name_index'
         parameter passed to the function.

            REMINDER:
                THIS INDEX DELETES ALL DOCUMENTS THAT HAS OU WILL HAVE ONE ATTRIBUTER WITH THE SAME NAME AS 'name_index'!!!
                FOR THIS INDEX WORKS, IT'S NECESSARY THAT THE ATTRIBUTE WITH THE SAME NAME OF 'name_index' BE OF THE TYPE 'datetime'
                object, MORE SPECIFICAlLY 'datetime.utcnow()' (RECOMENDATION), REMEMBER THAT THE DATABASE USES THE TIMEZONE WHERE YOUR
                CLUSTER IS HOSTED!!!
                EXAMPLE: index_time_ttl with the name of "data_de_criacao",  therefore, documents with the same attribute will be deleted.


        :param time_metric: (str) Attribute that allows choosing which time unit to use as a parameter.
                        Supported: "seconds"; "minutes"; "hours"; "days"; in English.

        :param time_to_live: (int) Attribute that determines the period in which MongoDB will delete documents
                            that have the attribute named 'name_index', based on the 'time_metric' parameter.

        :param name_index: (str) Attribute that defines the name of the TTL index and consequently the name of the attribute MongoDB
                            will look for when deleting documents.

        :param order_by: (int) optional parameter that receives '1' or '-1' which maintains an order of index respectively ascending(1)
                        or descending (-1).
                        By ‘DEFAULT,’ it's defined as one (ascending).

        :return: Returns True when the TTL index creation is successful, it may raise an error if the time measure passed
                 is not consistent with what was exemplified in the documentation 'time metric'...
        """

        collection = self.__get_collection()

        time_units = {
            "seconds": timedelta(seconds=1),
            "minutes": timedelta(minutes=1),
            "hours": timedelta(hours=1),
            "days": timedelta(days=1)
        }

        index_time_life = time_units.get(time_metric.lower())

        if index_time_life is not None:

            if order_by in [1, -1]:
                expire_after_option = timedelta(seconds=(time_to_live * index_time_life.total_seconds()))
                collection.create_index([(name_index, order_by)],

                                        expireAfterSeconds=expire_after_option.total_seconds())
                return True
            else:
                raise ValueError("Invalid order_by parameter. Please use either 1 (ascending) or -1 (descending).")
        else:
            raise ValueError("Invalid time metric. Please use one of: seconds, minutes, hours, days.")

    @index_exceptions_decorator
    def create_new_index(self, index_dict: dict) -> bool:
        """
        Create an index in MongoDB to improve search efficiency.
        This index can be unique or composite depending on what is passed in the 'index_dict' dictionary.

        REMINDER: ALL RECORDS THAT HAVE OR WILL HAVE ATTRIBUTES WITH NAMES FROM THE KEYS PASSED IN 'index_dict'
         WILL HAVE THIS INDEX POINTING IN MONGODB.

        :param index_dict: (dict) A dictionary that receives, in the case of a unique index, a "key"
                        along with an 'int' specifying its ordering, where 1 is ascending and -1 is descending.
                            EXAMPLE:
                                    Unic Index: dict = {"name": 1}
                                    Multiple Index: dict = {"name":-1, "age": 1}

        :return: (bool) Returns True if the index creation was successful.
        """

        collection = self.__get_collection()

        for key, order in index_dict.items():
            if order not in [-1, 1]:
                raise ValueError("Invalid order_by parameter. Please use either")

            collection.create_index([(key, order)])

        return True
