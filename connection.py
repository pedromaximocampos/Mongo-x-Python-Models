from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure


class MongoConnectionHandler:

    def __init__(self, connection_string: str, db_name: str):
        """
        Constructor for the MongoDBConnectionHandler class. The goal of this class is to create and manage a pymongo client and,
         from there, create a connection with the MongoDB of interest.

        :param connection_string: (‘String’) The connection ‘string.’

        :param db_name: (‘String’) The name of the database. If provided, allows you to create the class by passing it as a parameter.
        """

        self.__connection_string = connection_string
        self.__db_name = db_name
        self.__client = None
        self.__db_connection = None

    def connect(self):
        """
        This method uses the private attribute of the class (client) to make a connection to mongoDB through the
        pymongo library, so we associate the client private attribute with pymongo's MongoClient and later
        we associate another private attribute db_connection to our client[database_name], just as passed in
        pymongo documentation.
        """
        try:
            self.__client = MongoClient(self.__connection_string)
            self.__db_connection = self.__client[self.__db_name]

        except ServerSelectionTimeoutError:
            raise ServerSelectionTimeoutError('TimeoutError while connecting to MongoDB.\n'
                   'Unable to connect to the bank during the available timeout.\n '
                    'Check the connection_string in your cluster infrastructure')

        except ConnectionFailure:
            raise ConnectionFailure(f'ConnectionFailure.\n '
                  f'Check your connection string and whether you have permission to access the{self.__db_name}')

    def get_connection(self):
        """
        :return: private attribute db_connection which in turn is an instance of client[name_of_database]
        which is the connection to the bank made by pymongo.
        """
        return self.__db_connection

    def get_client(self):
        """
        :return: The client private attribute, which in turn is an instance of Pymongo's MongoClient.
        """
        return self.__client

    def disconnect_from_mongodb(self):
        """
        This method closes our connection to the database, closing our client attribute (MongoClient instance)
         from pymongo.
        """
        self.__client.close()
