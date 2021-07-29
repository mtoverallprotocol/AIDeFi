from past.builtins import execfile
from . import settings as settings
settings = settings.settings()
execfile(settings.loader('utilities'))

frameworkInfo = settings.getFrameworkInfo()
CONNECTION_STRING = frameworkInfo['ConnectionString']['connectionString']
ETHERSCAN_APIKEY = frameworkInfo['APIKeys']['etherscan']
BSCSCAN_APIKEY = frameworkInfo['APIKeys']['bscscan']
BITQUERY_APIKEY = frameworkInfo['APIKeys']['bitquery']
CONTRACTS_FOLDER = frameworkInfo['Folders']['contracts']
DATA_FOLDER = frameworkInfo['Folders']['data']

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
    
    def downloadAllLogsEvents(self, _path, _limit = 90000): #main useful function to be run
        # CSV has 3 columns: Network, Address, NameAddress
        import pandas as pd

        addressListRaw = pd.read_csv(_path)

        networks = addressListRaw['Network'].unique()

        addressList = {}
        for network in networks:
            addressList[network] = []

        for indexAddress in range(0, len(addressListRaw)):
            for network in networks:
                if addressListRaw['Network'][indexAddress] == network:
                    addressList[network].append(addressListRaw['Address'][indexAddress])

        bitQueryAllowedNetworks = ['ethereum', 'bsc']
        terraAllowedNetwoks = ['columbus-4', 'tequila-0004', 'Mombay-0008', 'Localterra']
        for network in networks:
            if network in bitQueryAllowedNetworks: self.getLogsEventsBitQuery(addressList[network], _limit, network)
            if network in terraAllowedNetwoks: True #terraChain.getLogsEvents(addressList[network], _limit, network) #to-do 
    
    def getAddressMetaData(self, _address):
        result = None
        try:
            df = pd.read_csv('{}/smartContract.csv'.format(DATA_FOLDER))
        except:
            df = pd.DataFrame(columns = ['Address'])
            print('File not found.')
        finally:
            dfAddress = pd.DataFrame(columns = ['Address'], data = [_address])
            result = pd.merge(df, dfAddress, on='Address', how='inner')
        return result

    def getLogsEventsBitQuery(self, _addressList, _limit, _network = 'ethereum'):
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
        if _network in ['ethereum', 'bsc']:
            for address in _addressList:
                addressMetaData = self.getAddressMetaData(address)
                (smartContract, ABI) = blockchain.getContract(address, _network)
                if(smartContract != None): 
                    blockchain.saveContractData(smartContract)
                    for event in self.getEventsFromSmartContract(smartContract):
                        if event != 'abi':
                            q = query.getEvents(address, event, _limit, _network)

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
                                tmp_df['network'] = [addressMetaData['Network']]
                                tmp_df['nameAddress'] = [addressMetaData['NameAddress']]
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
        else: 
            print ('Network not valid')

#################################################################################################################
    
    # @dev: This function allow to merge all events for each contract present inside data/contracts/{addressContract}
    # based on a blocknumber timeline
    # Input: nothing
    # Output: Dataframe with merged logs based on blocknumber timeline
    
    def mergeAllLogsData(self, _backupPath = None, _overloadData = False):
        
        import numpy as np
        dfList = {}
        
        contractList = [f for f in listdir(CONTRACTS_FOLDER) if isdir(join(CONTRACTS_FOLDER, f))]
        
        eventPrefix = 'event_'
        if _backupPath == None:
            # New Dataframe
            blockNumberDf = pd.DataFrame()
            columnList = []
        else:
            # Load existing dataframer with relative columns
            print('Importing backup...')
            bck = pd.read_csv(_backupPath, low_memory=False)
            blockNumberDf = bck['blockNumber']
            columnList = bck.keys()
            print('Imported {} rows and {} columns'.format(len(blockNumberDf), len(columnList)))
        
        print('Importing smart contract event data...')
        for contractFolder in contractList:
            if(contractFolder != '.ipynb_checkpoints'):
                cFolder = CONTRACTS_FOLDER+'/'+contractFolder
                eventsDataList = np.sort([f for f in listdir(cFolder) if isfile(join(cFolder, f))])[::-1]
                for eventsData in eventsDataList:
                    if(eventsData[0:len(eventPrefix)] == eventPrefix):
                        keyName = contractFolder.replace(" ", "_")                +"_"+eventsData[len(eventPrefix):len(eventsData)-20] 
                        isPresent = False
                        for columnBck in columnList:
                            if len(columnBck.split('_')) > 1: 
                                if columnBck.split('_')[0]+'_'+columnBck.split('_')[1] == keyName:
                                    isPresent = True
                        if isPresent == False or _overloadData:
                            tmpDf = pd.read_csv(cFolder+'/'+eventsData, low_memory=False)
                            keyName = keyName.upper()
                            if len(tmpDf.keys()) > 0:
                                tmpDf = tmpDf.drop(columns=[tmpDf.keys()[0]])
                                dfList[keyName] = tmpDf.add_prefix('{}_'.format(keyName))
                                dfList[keyName]['blockNumber'] = tmpDf['blockNumber']
                                dfList[keyName].set_index('blockNumber')
                                blockNumberDf = pd.concat([blockNumberDf, tmpDf['blockNumber']])
                            else:
                                print("Keys not found at {}".format(keyName))
                        else:
                            print('{} already taken from Backup'.format(keyName))
        
        if _backupPath is not None: 
            blockNumberList = bck #set backup to start dataframe
        else:
            blockNumberList = pd.DataFrame(blockNumberDf[0].unique(), columns=['blockNumber']) # start dataframe

        blockNumberList.set_index('blockNumber')
        
        print('Starting merge...')

        numberOfKeys = len(dfList.keys())
        startIndex = 1
        
        # Merging
        
        for key in dfList.keys():
            try:
                #print('Merging event {}...'.format(key))
                blockNumberList = pd.merge(blockNumberList, dfList[key], on='blockNumber', how='left')
                dfList[key] = None #save space memory
                print("{}/{} - {} event was merged".format(startIndex, numberOfKeys, key))
            except:
                print('Error on sql query. Key: {}'.format(key))
            startIndex = startIndex + 1
        
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Backup pre-optimization
        
        print('Backup before optimization...')
        blockNumberList.to_csv('{0}/bck_smartContractsLogsGrouped_{1}.csv'.format(CONTRACTS_FOLDER, dt))

        # Optimize merged logs:
        blockNumberList = self.optimizeMergedLogs(blockNumberList)
    
        # After All - Save data
        
        blockNumberList.to_csv('{0}/smartContractsLogsGrouped_{1}.csv'.format(CONTRACTS_FOLDER, dt))
        print('File saved on {0}/smartContractsLogsGrouped_{1}.csv'.format(CONTRACTS_FOLDER, dt))

       
    #################################################################################################################
    
    # @dev: 
    # Input: Dataframe of merged logs
    # Output: Reduced dataframe where timestamp_iso8601, timestamp_unixtime are not duplicated on columns
    # but merged in a unique column

    def optimizeMergedLogs(self, _df):
        
        import datetime
        date_time_str = '1970-01-01 00:00:00.000000'
        date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
        indexList_timestamp_iso8601 = []
        indexList_timestamp_unixtime = []

        columnsToDrop = []
        index = 1
        for column in _df.keys():
            if '_timestamp_iso8601' in column:
                _df[column] = pd.to_datetime(_df[column], utc=True)
                _df[column] = _df[column].fillna(date_time_obj)
                indexList_timestamp_iso8601.append(column)
            if '_timestamp_unixtime' in column:
                indexList_timestamp_unixtime.append(column)
            if "_blockNumber" in column: columnsToDrop.append(column)
            if "Unnamed:" in column: columnsToDrop.append(column)
            print('Analyzed {} of {} columns'.format(index, len(_df.keys())))
            index = index + 1

        print('Optimizing timestamp_iso8601...')
        _df['timestamp_iso8601'] = _df[indexList_timestamp_iso8601].astype(str).max(axis=1)
        print('Optimizing timestamp_unixtime...')
        _df['timestamp_unixtime'] = _df[indexList_timestamp_unixtime].max(axis=1)
        print('Dropping redundante columns...')

        _df = _df.drop(columns = indexList_timestamp_iso8601)
        _df = _df.drop(columns = indexList_timestamp_unixtime)
        _df = _df.drop(columns = columnsToDrop)

        print('Dataframe optimized with success.')
        
        return _df
            
        
    
    