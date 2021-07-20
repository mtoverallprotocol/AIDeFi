from past.builtins import execfile
from . import loadVersioning as loadVersioning
loadVersioning = loadVersioning.loadVersioning()
execfile(loadVersioning.loader('utilities'))

frameworkInfo = loadVersioning.getFrameworkInfo()
CONNECTION_STRING = frameworkInfo['ConnectionString']['connectionString']
ETHERSCAN_APIKEY = frameworkInfo['APIKeys']['etherscan']
BSCSCAN_APIKEY = frameworkInfo['APIKeys']['bscscan']
BITQUERY_APIKEY = frameworkInfo['APIKeys']['bitquery']

from past.builtins import execfile
libraryPath = '{0}/libraries_v{1}.py'.format(frameworkInfo['Folders']['classes'], frameworkInfo['Metadata']['version'])
execfile(libraryPath)

class contractEvents:
    
    def ___init__(self, name):
        self.name = name
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to save events name in an array from ABI
    # Input: ABI of a smart contract
    # Output: array with events name
    
    def getEventsFromABI(self, _ABI):
        
        import re
        
        events = []
        for match_type in re.finditer(r'\"\,\"type":\"event\"', _ABI):
            for match_name in re.finditer(r'\"name\"\:\"', _ABI[0:match_type.start()]):
                True
                #no-op
            events.append(_ABI[match_name.end():match_type.start()])
        return events
    
    def getEventsFromSmartContract(self, _contract):
        
        try:
            events = list(filter(lambda k: '_' not in k, dir(_contract.events))) 
        except:
            events = []
            
        return events
    
    
    def getLogsEventsBitQuery(self, _addressList, _limit):
        #execfile(loadVersioning.loader('blockchain'))
        #print(loadVersioning.loader('blockchain'))
        from . import blockchain_v1_0_0_0 as blockchain
        blockchain = blockchain.blockchain()
        from classes import bitQuery_v1_0_0_0 as bitQuery
        query = bitQuery.query()
        bq = bitQuery.bitQuery()

        from datetime import datetime
        import pandas as pd

        logsFound = 0
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")

        for address in _addressList:
            (smartContract, ABI) = blockchain.getContract(address)
            if(smartContract != None): 
                blockchain.saveContractData(smartContract)
                for event in self.getEventsFromSmartContract(smartContract):
                    if event != 'abi':
                        q = query.getEvents(address, event, _limit)
                        print('{} - {} downloading events log...'.format(address, event))
                        result = bq.runQuery(q)
                        # Data transformation
                        listResult = result['data']['ethereum']['smartContractEvents']
                        isFirst = True
                        df = []
                        for indexList in range(0, len(listResult)):
                            blockNumber = listResult[indexList]['block']['height']
                            timestamp_iso8601 = listResult[indexList]['block']['timestamp']['iso8601']
                            timestamp_unixtime = listResult[indexList]['block']['timestamp']['unixtime']
                            arguments = listResult[indexList]['arguments']
                            tmp_df = pd.DataFrame()
                            tmp_df['blockNumber'] = [blockNumber]
                            tmp_df['timestamp_iso8601'] = [timestamp_iso8601]
                            tmp_df['timestamp_unixtime'] = [timestamp_unixtime]

                            for indexArgs in range(0, len(arguments)):
                                tmp_df[arguments[indexArgs]['argument']] = [arguments[indexArgs]['value']]

                            if(isFirst):
                                df = tmp_df
                                isFirst = False
                            else:
                                df = pd.concat([df, tmp_df])

                        if len(df) > 0:
                            print('{} - {} correctly saved'.format(address, event))
                            df.to_csv('data/contracts/{}/event_{}_{}.csv'.format(address.lower(),event, dt))
                            logsFound = logsFound + len(df)
                        else:
                            print('{} - {} no logs found'.format(address, event))

#################################################################################################################
    
    # @dev: This function allow to merge all events for each contract present inside data/contracts/{addressContract}
    # based on a blocknumber timeline
    # Input: nothing
    # Output: Dataframe with merged logs based on blocknumber timeline
    
    def mergeAllLogsData(self):
        
        CONTRACT_FOLDER = 'data/contracts'
        contractList = [f for f in listdir(CONTRACT_FOLDER) if isdir(join(CONTRACT_FOLDER, f))]
        
        eventPrefix = 'event_'
        blockNumberDf = pd.DataFrame()
        dfList = {}
        for contractFolder in contractList:
            if(contractFolder != '.ipynb_checkpoints'):
                cFolder = CONTRACT_FOLDER+'/'+contractFolder
                eventsDataList = [f for f in listdir(cFolder) if isfile(join(cFolder, f))]
                for eventsData in eventsDataList:
                    if(eventsData[0:len(eventPrefix)] == eventPrefix):
                        tmpDf = pd.read_csv(cFolder+'/'+eventsData)
                        # KEYNAME EXAMPLE: A6F6BF_CASHPRIOR. A = ADDRESS, 6F6BF = LAST 5 DIGITS OF THE ADDRESS, CASHPRIOR = EVENT NAME
                        keyName = 'A'+contractFolder.replace(" ", "_")[len(contractFolder)-5:len(contractFolder)]+"_"+eventsData[len(eventPrefix):len(eventsData)-20] 
                        keyName = keyName.upper()
                        if len(tmpDf.keys()) > 0:
                            tmpDf = tmpDf.drop(columns=[tmpDf.keys()[0]])
                            dfList[keyName] = tmpDf
                            dfList[keyName]['blockNumber'] = tmpDf['blockNumber']
                            blockNumberDf = pd.concat([blockNumberDf, tmpDf['blockNumber']])
                            #print('##### {}'.format(keyName))
                            #print("Total logs {}. Keys: {}".format(len(dfList[keyName]), " ".join(dfList[keyName].keys())))
                            
                        else:
                            print("Keys not found at {}".format(keyName))
                        
        blockNumberList = pd.DataFrame(blockNumberDf[0].unique(), columns = ['blockNumber'])
        
        sql = "SELECT {0} FROM blockNumberList A "
        colsBase = 'A.blockNumber, '
        cols = colsBase
        for key in dfList.keys():
            #print(dfList[key].keys())
            groupCols = []
            afterCols = []
            for keyColumn in dfList[key].keys():
                
                if(keyColumn == 'from'): #special word
                    dfList[key].rename(columns={'from':'fromAddress'}, inplace=True)
                    keyColumn = 'fromAddress'
                if(keyColumn == 'to'): #special word
                    dfList[key].rename(columns={'to':'toAddress'}, inplace=True)
                    keyColumn = 'toAddress'
                if(keyColumn != 'blockNumber'):
                    groupCols.append('MAX('+keyColumn+') AS '+key+'_'+keyColumn+',')
                    afterCols.append(key+'_'+keyColumn+', ')
            #print(groupCols)
            selectGroup = ' '.join(groupCols)
            selectGroupBase = ' '.join(afterCols)
            cols = cols + selectGroup
            df = dfList[key]
            joinStatement = "LEFT JOIN df B on A.blockNumber = B.blockNumber "
            groupByStatement = "GROUP BY A.blockNumber ORDER BY A.blockNumber"
            finalSql = sql.format(cols[0:len(cols)-1])+joinStatement+groupByStatement
            colsBase = colsBase + selectGroupBase
            cols = colsBase
            print("{0} event was imported".format(key))
            blockNumberList = psql.sqldf(finalSql)
            
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        blockNumberList.to_csv('{0}/smartContractsLogsGrouped_{1}.csv'.format(CONTRACT_FOLDER, dt))
        print('File saved on {0}/smartContractsLogsGrouped_{1}.csv'.format(CONTRACT_FOLDER, dt))