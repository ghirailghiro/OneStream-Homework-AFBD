from socket import IPPORT_USERRESERVED

from matplotlib.pyplot import connect
from oneStreamExtractor import BATCH_EXTRACTOR, abstractmethod
from batch_extractor import Batch_Extractor, abstractmethod
import os
import oracledb

'''La classe astratta è definita con tre metodi:
- Uno da utilizzare per instaurare la connessione con la fonte dati SQL
- Uno per estrarre i dati da una fonte dati SQL
- Uno per andare a scrivere dati su una fonte SQL
'''
class Batch_Extractor(Batch_Extractor):
    '''
    Il seguente metodo non prende parametri in input,
    ma come output avrà un connettore al db
    '''
    @abstractmethod
    def get_connection(self):
        pass
    '''
    Il seguente metodo prende in input i valori per comporre una query:
        - connection: il connettore al db SQL
        - select: le colonne da estrarre dalla query
        - from: la table da cui estrarre
        - where: i filtri per la query
        - limit: il numero di row da estrarre, settato a None di default
    come output i risultati della query
    Si è deciso di non dare libertà all'utente che chiama il metodo,
    di poter eseguire le query che vuole genericamente,
    ma solo query con clausole select.
    Per quanto riguarda le join si è deciso di non dare questa libertà all'utente 
    per non complicare i parametri di input e l'implementazione della classe astratta stessa.
    ***Essendo che la composizione delle query sarà fatta con dei parametri inseriti dall'utente bisognerà occuparsi della sanitizzazione***
    '''
    @abstractmethod
    def get_data(self,connection,select,from,where,limit = None):
        pass
    '''
    Il seguente metodo prende in input i valori per comporre una query:
        - connection: il connettore al db SQL
        - data: rappresenta i dati che vogliamo scrivere, si è deciso di dare abbastanza libertà poi nello sviluppo della classe
        - from: il nome della table su cui inserire
    come output i risultati della query
    Si è deciso di non dare libertà all'utente che chiama il metodo,
    di poter eseguire le query che vuole genericamente,
    ma solo query con clausole INSERT, dando solo la possibilità di specificare la table in cui salvare.
    ***Essendo che la composizione delle query sarà fatta con dei parametri inseriti dall'utente bisognerà occuparsi della sanitizzazione***
    '''
    @abstractmethod
    def write_data(self,connection,data,from):
        pass


'''
***L'implementazione non è stata completamente testata, è stata inserita come esempio*** 
Qui si può trovare un esempio di possibile implementazione,
si è usata la libreria oracledb per simulare una estrazione
in un db oracle.
'''

class OneStreamExtractor(Batch_Extractor):
    def get_connection(self):
        server = os.environ.get('SERVER')
        db = os.environ.get('DB')
        user = os.environ.get('USER')
        userpwd = os.environ.get('PASSWORD')
        port = os.environ.get('PORT')
        return  oracledb.connect(user=user, password=userpwd,host=server, port=port, service_name=db)

    def get_data(self,connection,select,from,where,limit):
        '''***Gli  Input non sono stati sanitizzati ***'''
        if(limit == None):
            query= "SELECT "+select+" FROM "+from+" WHERE "+ where
        else:
            query= "SELECT "+select+" FROM "+from+" WHERE "+ where + " LIMIT "+limit
        results = connection.execute(query)
        return results
        pass
    def write_data(self,connection,data,where):
        '''***Gli  Input non sono stati sanitizzati ***'''
        cur = connection.cursor()
        '''Spachettamento dei dati in stringa'''
        stringOfColumns = ",".join([str(i[0]) for i in data.description])
        columnsToInsert = ",".join([":"+str(i) for i in range(1,len(data.description[0])+1)])
        cur.executemany("insert into "+where+"("+stringOfColumns+") values ("+columnsToInsert+")", data, batcherrors = True)
        listOfErrors = []
        for error in cur.getbatcherrors():
            listOfErrors.append("Error"+ error.message.rstrip()+ "at row offset"+ error.offset)
        return (listOfErrors > 0,listOfErrors)
        pass
