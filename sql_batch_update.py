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
    def write_data(self,connection,data,where):
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
        results = connection.execute(query)
        '''Cosa ritornare??'''
        return results
        pass
    def write_data(self,connection,data,where):
        cur = connection.cursor()
        stringOfColumns = ",".join([str(i[0]) for i in data.description])
        columnsToInsert = ",".join([":"+str(i) for i in range(1,len(data.description[0])+1)])
        cur.executemany("insert into "+where+"("+stringOfColumns+") values ("+columnsToInsert+")", data, batcherrors = True)
        listOfErrors = []
        for error in cur.getbatcherrors():
            listOfErrors.append("Error"+ error.message.rstrip()+ "at row offset"+ error.offset)
        return (listOfErrors > 0,listOfErrors)
        '''Cosa usare per scrivere?'''
        '''Che tipo di data deve dare in input?'''
        pass
