from socket import IPPORT_USERRESERVED

from matplotlib.pyplot import connect
from oneStreamExtractor import BATCH_EXTRACTOR, abstractmethod
from batch_extractor import Batch_Extractor, abstractmethod
import os
import oracledb

'''Che parametri inserire per fare in modo di usarlo -> get_data'''
'''defin the return type'''
class Batch_Extractor(Batch_Extractor):
    @abstractmethod
    def get_connection(self):
        pass
    @abstractmethod
    def get_data(self,connection,query):
        pass
    @abstractmethod
    def write_data(self,connection,data):
        pass


class OneStreamExtractor(Batch_Extractor):
    def get_connection(self):
        server = os.environ.get('SERVER')
        db = os.environ.get('DB')
        user = os.environ.get('USER')
        userpwd = os.environ.get('PASSWORD')
        port = os.environ.get('PORT')
        return  oracledb.connect(user=user, password=userpwd,host=server, port=port, service_name=db)

    def get_data(self,connection,query):
        connection.execute(query)
        '''Cosa ritornare??'''
        pass
    def write_data(self,connection,data):
        '''Cosa usare per scrivere?'''
        '''Che tipo di data deve dare in input?'''
        pass
