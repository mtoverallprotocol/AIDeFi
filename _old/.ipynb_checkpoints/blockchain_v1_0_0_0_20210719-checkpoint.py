# Import Libraries

from past.builtins import execfile
from . import loadVersioning as loadVersioning
loadVersioning = loadVersioning.loadVersioning()
execfile(loadVersioning.loader('utilities'))
execfile(loadVersioning.loader('contractEvents'))
execfile(loadVersioning.loader('contractFunctions'))



frameworkInfo = utilities.getFrameworkInfo()
CONNECTION_STRING = frameworkInfo['ConnectionString']['connectionString']
ETHERSCAN_APIKEY = frameworkInfo['APIKeys']['etherscan']
BSCSCAN_APIKEY = frameworkInfo['APIKeys']['bscscan']
BITQUERY_APIKEY = frameworkInfo['APIKeys']['bitquery']
CLASSES_FOLDER = frameworkInfo['Folders']['classes']
CONTRACTS_FOLDER = frameworkInfo['Folders']['contracts']
COSTUMERS_FOLDER = frameworkInfo['Folders']['costumers']

from past.builtins import execfile
libraryPath = '{0}/libraries_v{1}.py'.format(frameworkInfo['Folders']['classes'], frameworkInfo['Metadata']['version'])
execfile(libraryPath)

class blockchain:
    
    def ___init__(self, name):
        self.name = name
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to receive the web3 connection to catch data from blockchain
    # Input: connection String
    # Output: Web3 connection
    
    def blockchainConnection(self):
        _connectionString = CONNECTION_STRING
        if (re.search("(http(|s)\:\/\/)", _connectionString) != None):
            # HTTP Provider
            w3 = Web3(Web3.HTTPProvider(_connectionString))
        elif (re.search("(\.ipc)", _connectionString) != None):
            # IPC Provider
            w3 = Web3(Web3.IPCProvider(connectionString))
        elif (re.search("(ws\:\/\/)", _connectionString) != None):
            # Web Socket Provider
            w3 = Web3(Web3.WebsocketProvider(_connectionString))    
        else:
            w3 = None
        return w3

    
    
    #################################################################################################################
    
    # @dev: This function allow us to know how many items there are inside the source
    # Input: he source name
    # Output: source number
    
    def getNumberOfMaps(self, _source):
        return self.getMaps(-1, _source)[2]
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to read data from files that has dex, token, smartcontracts addresses
    # Input: the data index, the source name
    # Output: value of address or addressName of a specific provider
    
    def getMaps(self, _index, _source):
        indexAddress = 0
        
        nameAddress = None
        address = None
        
        if(_source == 'DEX'): 
            
            dexes = pd.read_csv('data/dex.csv')
            for indexAddress in range(0, len(dexes)):
                if(_index == indexAddress):     
                    address = dexes['Address'].values[_index]
                    nameAddress = dexes['NameAddress'].values[_index]
        
        if(_source == 'TOKEN'): 
            
            tokens = pd.read_csv('data/token.csv')
            for indexAddress in range(0, len(tokens)):
                if(_index == indexAddress):     
                    address = tokens['Address'].values[_index]
                    nameAddress = tokens['NameAddress'].values[_index].replace(".","").replace("$","").replace("+","")
        
        if(_source == 'SMART_CONTRACT'): 
            
            dexes = pd.read_csv('data/smartContract.csv')
            for indexAddress in range(0, len(dexes)):
                if(_index == indexAddress):     
                    address = dexes['Address'].values[_index]
                    nameAddress = dexes['NameAddress'].values[_index]
                    
        return nameAddress, address, indexAddress + 1
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to download the block info
    # Input: web3 connection, block list array (a block has 1 or more transactions), if it's a full import or not 
    # Output: Dataframe of the block or a tuple of dataframe that has block transaction and transaction receipt
    
    def importTransactionList(self, _w3, _block, _blockArgs, _df_transaction, _transactionArgs, _df_transactionReceipt, _transactionReceiptArgs):
        transactions = _block["transactions"]
        for indexTransaction in range(0, len(transactions)):
            #Importing transactions on df_transaction
            transaction = _w3.eth.getTransaction(str(transactions[indexTransaction].hex()))
            self.addBlockOnDataFrame(transaction, _df_transaction, _transactionArgs)
            #Importing transaction receipts on df_transactionReceipt
            transactionReceipt = _w3.eth.getTransactionReceipt(str(transaction["hash"].hex()))
            self.addBlockOnDataFrame(transactionReceipt, _df_transactionReceipt, _transactionReceiptArgs)
            
            
            
    #################################################################################################################
    
    # @dev: This function allow us to download the block info
    # Input: web3 connection, block list array (a block has 1 or more transactions), if it's a full import or not 
    # Output: Dataframe of the block or a tuple of dataframe that has block transaction and transaction receipt
    
    def importBlockList(self, _w3, _listBlock, _fullImport):
        if(_fullImport):
            transactionArgs = self.getTransactionArgs(_w3)
            df_transaction = pd.DataFrame(columns = transactionArgs)
            transactionReceiptArgs = self.getTransactionReceiptArgs(_w3)
            df_transactionReceipt = pd.DataFrame(columns = transactionReceiptArgs)
        
        blockArgs = self.getBlockArgs(_w3)
        df_block = pd.DataFrame(columns = blockArgs)
        
        for indexBlock in _listBlock:
            block = _w3.eth.get_block(indexBlock)
            #Importing block data
            self.addBlockOnDataFrame(block, df_block, blockArgs)
            if(_fullImport):
                #Importing transaction and transaction receipt data
                self.importTransactionList(_w3, block, blockArgs, df_transaction, transactionArgs, df_transactionReceipt, transactionReceiptArgs)  
        
        if(_fullImport):
            return (df_block, df_transaction, df_transactionReceipt)
        else:
            return df_block
        
        
        
        
    #################################################################################################################
    
    # @dev: This function allow us to download the transaction list considering the limit of providers such etherscan
    # Input: etherscsan api key, provider (see API_String function), action, address to be analysed, number of block we want to
    # consider from the start, blockstart usualy set to zero (it means from last block less zero), block limit means how much
    # iteration we have to consider. when blockinterval = 'MAX' it takes automatically the maximum number of txs with the block lmit
    # Output: Dataframe with the transaction list
    
    def importTransactionListFromAddress(self, _provider, _action, _address, _blockInterval, _blockStart, _blockLimit):
        _etherscanApiKey = ETHERSCAN_APIKEY
        df = pd.DataFrame()
        START_BLOCK = '0'
        END_BLOCK = '9999999999999999'
        maxDf = []
        print(self.API_String(_provider, _action, [_address, START_BLOCK, END_BLOCK, _etherscanApiKey]))
        r = requests.get(self.API_String(_provider, _action, [_address, START_BLOCK, END_BLOCK, _etherscanApiKey]))
        tx = r.json()['result']
        #print(tx[0])
        try:
            if(len(tx) > 0):

                cols = tx[0].keys()
                df = pd.DataFrame(columns = cols)
                lastBlock = int(tx[0]['blockNumber'])
                if(_blockInterval == 'MAX'):

                    TRANSACTION_LIMIT = 9999
                    if(len(tx) < TRANSACTION_LIMIT):
                        TRANSACTION_LIMIT = len(tx) * 2

                    blockInterval = abs(int(tx[0]['blockNumber']) - int(tx[TRANSACTION_LIMIT]['blockNumber']))
                else:
                    blockInterval = _blockInterval # Uniswap API is limited to 10000 transictions data each time, max 2 request / s on free api account

                indexBlock = _blockStart
                blockStart = _blockStart
                blockLimit = _blockLimit

                unsplittable = True
                lenMax_df = 0
                while (blockLimit > indexBlock and unsplittable):

                    endBlock = lastBlock - blockInterval * (indexBlock - 1)
                    startBlock = endBlock - blockInterval

                    try:
                        r = requests.get(self.API_String(_provider, _action, [_address, startBlock, endBlock, _etherscanApiKey]))
                        tx = r.json()['result']
                        #print(tx)
                        #print(len(tx))
                        tmp_df = pd.DataFrame.from_dict(tx).filter(items = cols)
                        if(len(tmp_df) > 0 and len(tmp_df) < 10000):

                            df = pd.concat([df, tmp_df])
                            if(len(df) > lenMax_df):
                                maxDf = df
                                lenMax_df = len(df)  

                            print("Index Block Interval n. {0} - Imported {1} transactions - DateTime: {2}".format(indexBlock, len(tmp_df), datetime.now().strftime("%d/%m/%Y_%H:%M:%S")))
                        else: #restart

                            df = pd.DataFrame(columns = cols)
                            indexBlock = _blockStart
                            if(blockInterval / 2 == .5):
                                unsplittable = False
                            blockInterval = math.ceil(blockInterval / 2)

                            print("Maximim number of transaction. Restarting import process. Block Interval set to {0}".format(blockInterval))

                    except:
                        r = None
                        print("Error on request.")

                    indexBlock = indexBlock + 1
            
        except:
            print("Undefined error for address {0}".format(_address))
        return maxDf[1:len(maxDf)]
    

    
    #################################################################################################################
    
    # @dev: This function allow us to create tye correct API string from a specific provider
    # Input: provider name, action, a tuple-array of data that will be put inside the API string
    # Output: String with the correct API String
    
    def API_String(self, _provider, _action, _data):
        apiString = None
        if(_provider == 'ETHERSCAN'):
            if(_action == 'TXLIST'):
                apiString = 'https://api.etherscan.io/api?module=account&action=txlist&address={0}&startblock={1}&endblock={2}&sort=desc&apikey={3}'.format(_data[0], _data[1], _data[2], _data[3])
                
            if(_action == 'ABI'):
                apiString = 'https://api.etherscan.io/api?module=contract&action=getabi&address={0}&apikey={1}'.format(_data[0], _data[1]) 
                
        if(_provider == 'BSCSCAN'):
            if(_action == 'TXLIST'):
                apiString = 'https://api.bscscan.com/api?module=account&action=txlist&address={0}&startblock={1}&endblock={2}&sort=desc&apikey={3}'.format(_data[0], _data[1], _data[2], _data[3])
        
        return apiString
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to download a huge list of transactions splitting the import considering the limitations
    # of some providers that restrics the metrics into a well defined range (limited to 10000 for etherscan)
    # Input: etherscan api key, provider ('ETHERSCAN' or 'BSCSCAN'), action (see API_String function), source (referred to getMaps function)
    # Output: Dataframe with all transaction from the source addresses (csv files inside data)
    
    def importTransactionsFromDexCustomers(self, _provider, _action, _source): #directFunction
        _etherscanApiKey = ETHERSCAN_APIKEY
        df = pd.DataFrame()
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        export_files = [f for f in listdir(COSTUMERS_FOLDER) if isfile(join(COSTUMERS_FOLDER, f))]
        
        NUMBER_OF_MAPPED_ADDRESS = self.getNumberOfMaps(_source)
        
        dfCustomers = pd.DataFrame(columns = ['from'])
        
        for indexMap in range(0,NUMBER_OF_MAPPED_ADDRESS):
            
            addressName = self.getMaps(indexMap, _source)[0]
            fileIndex = utilities.linearReSearch(export_files, "TRANSACTION_LIST_{0}".format(addressName))
            
            if(fileIndex >= 0):
                
                csv_transactionList = export_files[fileIndex]
                
                print("Importing transactions from {0}".format(csv_transactionList))
                
                df = pd.read_csv('{0}/{1}'.format(COSTUMERS_FOLDER, csv_transactionList))
                df = df.filter(items = ['from', 'value']) # Save cache memory space selecting the 2 usefuls columns
                df = pd.DataFrame(df[df['value'] != '0']['from'].unique(), columns = ['from']) # Save cache memory space deleting duplicates and 0 values
                dfCustomers = pd.concat([dfCustomers, df])
                
                print("Imported {0} address transactions".format(len(df)))
                
        dfCustomers = pd.DataFrame(dfCustomers['from'].unique(), columns = ['from']) #Save more memory and duplicates on final result
        
        dfCustomers.to_csv('data/customers/CUSTOMERS_LIST_FROM_DEX_{0}.csv'.format(dt))
        
        foundAddresses = len(dfCustomers)
        
        print("Found {0} addresses".format(foundAddresses))
            
        indexImport = 0

        df = dfCustomers
        
        cols = ['fromAddress', 'blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice']
        dfCustomers = pd.DataFrame(columns = cols) #reset dfCustomers

        for fromAddress in df['from']:

            START_BLOCK = '0'
            END_BLOCK = '9999999999999999'

            try:

                r = requests.get(self.API_String(_provider, _action, [fromAddress, START_BLOCK, END_BLOCK, _etherscanApiKey]))
                tx = r.json()['result']
                if(len(tx) > 0):
                    tmp_df = pd.DataFrame.from_dict(tx).filter(items = cols)
                    tmp_df['fromAddress'] = fromAddress

                    dfCustomers = pd.concat([dfCustomers,tmp_df])

                    indexImport = indexImport + 1

                    print("Index Import: {0} - Imported {1} transactions - Datetime: {2} - Cumulative dimension: {3}".format(indexImport, len(tmp_df), datetime.now().strftime("%Y%m%d_%H%M%S"), len(dfCustomers)))

            except OSError as err:
                print("OS error: {0}".format(err))
            except ValueError:
                print("Could not convert data to an integer.")
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise

        if(len(dfCustomers) > 0):
            dfCustomers.to_csv('data/customers/CUSTOMERS_TRANSACTIONS_FROM_DEX_{1}.csv'.format(fromAddress, dt))
        
    def importAllDexTransactions(self, _provider, _action, _source, _blockInterval, _blockStart, _blockLimit): #only DEXes
        
        _etherscanApiKey = ETHERSCAN_APIKEY
        
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for indexMap in range(0,self.getNumberOfMaps(_source)):
            
            address = self.getMaps(indexMap, _source)[1]
            addressName = self.getMaps(indexMap, _source)[0]
            df = self.importTransactionListFromAddress(_provider, _action, address, _blockInterval, _blockStart, _blockLimit)
            if(len(df) > 0):
                print("Dataset of {}'s lenght is: {}".format(addressName, len(df)))
                df.to_csv("{}/TRANSACTION_LIST_{}_{}.csv".format(COSTUMERS_FOLDER, addressName, dt))

                
                
    #################################################################################################################
    
    # @dev: This function allow us to save in a variable the contract datatype
    # Input: contract address, etherscan api key, connection string to web3 (Infura)
    # Output: Contract Data Type with all events and functions and other data and ABI
    
    def getContract(self, _address):
        _apiKey = ETHERSCAN_APIKEY
        _connectionString = CONNECTION_STRING
        ABI = requests.get(self.API_String('ETHERSCAN', 'ABI', [_address, _apiKey])).json()['result']
        w3 = self.blockchainConnection()
        address = w3.toChecksumAddress(_address)
        try:
            return (w3.eth.contract(address=address, abi=ABI), ABI)
        except:
            return (None, None)
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to decode the args of transaction log
    # Input: transaction hash, contract function namd,  onnection string to web3 (Infura)
    # Output: Dataframe with merged logs based on blocknumber timeline
    
    def getLogFromTransactionHash(self, _tx, _contractFunction):
        _connectionString = CONNECTION_STRING

        w3 = self.blockchainConnection()
        txR = w3.eth.getTransactionReceipt(_tx)
        keyLog = []
        valueLog = []
        try:
            log = _contractFunction.processReceipt(txR)[0]['args']
            for key in log.keys():
                keyLog.append(key)
                valueLog.append(log[key])
        except:
            True #no-op
       
        return keyLog, valueLog, txR
    
    
    
    #################################################################################################################
    
    # @dev: This function allow to decode all logs in a tuple of variables
    # Input: transaction hash, contract address, contract datatype, contract ABI, connection string to web3 (Infura)
    # Output: Dataframe with merged logs based on blocknumber timeline
    
    def decodeLogs(self, _tx, _contractAddress, _contract, _ABI):
        execfile(loadVersioning.loader('contractEvents'))

        _connectionString = CONNECTION_STRING

        eventList = contractEvents.eventList(_contract) # callable functions
        eventsName = contractEvents.getEventsFromABI(_ABI) #only function list
        
        keyLogList = {}
        valueLogList = {}
        txR = {}
        index = 0
        
        for event in eventList:
            
            (keyLogList[eventsName[index]], valueLogList[eventsName[index]], txR) = self.getLogFromTransactionHash(_tx, event)
            
            index = index + 1
            
        return (keyLogList, valueLogList, txR)
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to save the events and the functions name of a specific address
    # Input: contract address, contract datatype, contract ABI
    # Output: A couple of csv (events.csv and functions.csv)
    
    def saveContractData(self,_contract):
        execfile(loadVersioning.loader('contractEvents'))
        execfile(loadVersioning.loader('contractFunctions'))
        contractName = _contract.address.lower()
        eventsList = contractEvents.getEventsFromSmartContract(_contract)
        functionList = contractFunctions.getFunctionsFromSmartContract(_contract)
        
        #print(eventsList)
        #print(functionList)
        utilities.createFolder('{}/{}'.format(CONTRACTS_FOLDER, contractName))     
            
        filePath = "{}/{}/{}.csv"
        
        pd.DataFrame(eventsList, columns = ['event']).to_csv(filePath.format(CONTRACTS_FOLDER, contractName, 'events'))
        pd.DataFrame(functionList, columns = ['function']).to_csv(filePath.format(CONTRACTS_FOLDER, contractName, 'functions'))
        
    def getABIFromSmartContract(self, _contract):
        return str(_contract.abi).replace("\n","").replace(", ",",").replace(": ",":").replace("'",'"')
        
    #################################################################################################################
    
    # @dev: This function downloads Logs content of a specific address from the last blocknumber catching a specific
    # volume of transactions
    # Input: contract address, etherscan API key, connection string to Infura
    # Output: List of dataframes for each event of the contract address specified
    
    def downloadContractLogsContent(self, _contractAddress, _depth, limit):
        _connectionString = CONNECTION_STRING
        _apiKey = ETHERSCAN_APIKEY

        from . import dynamics_v1_0_0_0 as dynamics
        dynamics = dynamics.dynamics()
        
        dynamics.generateDynamicSmartContractEventsAndFunctions(_apiKey, _connectionString)
        
        filePath = "{}/{}/{}{}.csv"

        (contract, ABI) = self.getContract(_contractAddress)
        
        self.saveContractData(contract)
        
        contractName = _contractAddress.lower()

        df_txHashList = self.importTransactionListFromAddress('ETHERSCAN', 'TXLIST', _contractAddress, 'MAX', 0 , _depth)
        
        if(len(df_txHashList) > 0):
            
            df_txHashList = df_txHashList['hash'][1:] #set here dimension of transactions volume (to be defined or dev can set it using other variables)
            if(limit > 0):
                df_txHashList = df_txHashList[0:limit]

            print("Found {0} transaction(s)".format(len(df_txHashList)))
            (keyLog, valueLog, txR) = self.decodeLogs(df_txHashList.iloc[0], _contractAddress, contract, ABI)

            txR_columns = []
            for key in txR.keys():
                if(key != 'logs'):
                    txR_columns.extend([key])
            txR_values = []
            for key in txR.keys():
                if(key != 'logs'):
                    txR_values.extend([txR[key]])


            warnings.filterwarnings('ignore') 
            dfList = {}

            isFirst = True
            index = 1

            createdKeyList = []

            for tx in df_txHashList:

                print("Downloading logs from {0} - {1} of {2} at {3}".format(tx, index, len(df_txHashList), datetime.now().strftime("%Y%m%d_%H%M%S")))

                (keyLog, valueLog, txR) = self.decodeLogs(tx, _contractAddress, contract, ABI)

                txR_columns = []
                for key in txR.keys():
                    if(key != 'logs'):
                        txR_columns.extend([key])
                txR_values = []
                for key in txR.keys():
                    if(key != 'logs'):
                        txR_values.extend([txR[key]])

                for key in keyLog:
                    if(len(keyLog[key])>0): #if key has a value

                        keyLog[key].extend(txR_columns)
                        valueLog[key].extend(txR_values)

                        existKey = key in createdKeyList

                        if(existKey):
                            dfList[key] = pd.concat([dfList[key], pd.DataFrame([valueLog[key]], columns = keyLog[key])])
                        else:
                            dfList[key] = pd.DataFrame([valueLog[key]], columns = keyLog[key]) #create empty dataframe
                            createdKeyList.append(key)

                index = index + 1
            print("-----------------------------------")
            print("Log results for {0}:".format(contractName))
            for key in keyLog:
                try:
                    print("Found {0} on key {1}".format(len(dfList[key]), key))
                except:
                    print("Key {0} not found".format(key))  

            dt = datetime.now().strftime("%Y%m%d_%H%M%S")

            filePath = "{}/{}/{}{}_{}.csv"
            for dfKey in dfList.keys():
                dfList[dfKey].to_csv(filePath.format(CONTRACTS_FOLDER, contractName, 'event_', dfKey, dt))
    
    
    
    def getLogs(self, _addresList, _limit):
        _connectionString = CONNECTION_STRING
        _apiKey = ETHERSCAN_APIKEY
        _bitQueryKey = BITQUERY_APIKEY
        import pandas as pd
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        smartContracts = _addresList
        filePathEvents = "{}/{}/events.csv"
        filePathTo = "{}/{}/{}{}_{}.csv"
        from classes import bitQuery_v1_0_0_0 as bitQuery
        query = bitQuery.query()
        bq = bitQuery.bitQuery()
        for indexContract in range(0,len(smartContracts)):
            address = smartContracts[indexContract].lower()
            contract = self.getContract(address)[0]
            self.saveContractData(contract)
            eventList = pd.read_csv(filePathEvents.format(address.lower()))
            for indexEvent in range(0, len(eventList)):
                eventName = eventList['event'][indexEvent]
                q = query.getEvents(address, eventName, _limit)
                print("Quering {} - {}".format(address, eventName))
                try:
                    result = bq.directQuery(q, _bitQueryKey)
                    result.to_csv(filePathTo.format(CONTRACTS_FOLDER, address, 'event_', eventName, dt))
                except:
                    print("Error on selected query")
            
        
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to create all folders based on smartcontracts.csv file (inside data). For each smart contract
    # we will save all events and functions in dataframes
    # Input: nothing, indirectly the smartcontracts.csv has to be filled with address/namdeAddress info
    # Output: List of folders and files inside data/contracts/{contractAddress}
    
    def inizializeSmartContracts(self):
        _connectionString = CONNECTION_STRING
        _apiKey = ETHERSCAN_APIKEY
        for indexMap in range(0,self.getNumberOfMaps('SMART_CONTRACT')):
            address = self.getMaps(indexMap, 'SMART_CONTRACT')[1]
            ABI = requests.get(self.API_String('ETHERSCAN', 'ABI', [address, _apiKey])).json()['result']
            contract = self.getContract(address)
            self.saveContractData(contract)

    def plotLogs(self, _arrayColumn, _fileLogDf, _savePlot = False):
        columnsIndex = _arrayColumn
        plt.figure(figsize = (16,8))
        fig, axs = plt.subplots(len(columnsIndex), 1, figsize=(16, 5*len(columnsIndex)), sharex=True, sharey=False)
        i = 0
        columns = []
        cond = []
        names = []
        values = []

        for index in columnsIndex :
            try:
                columns.append(_fileLogDf.keys()[index])
                print(columns[i])
                cond.append(_fileLogDf[columns[i]].fillna(0).astype(float) > 0)
                names.append(list(_fileLogDf[cond[i]]['blockNumber'].astype(float)))
                values.append(list(_fileLogDf[cond[i]][columns[i]]))
                axs[i].scatter(names[i], values[i])
                axs[i].set_title(columns[i] + ' - min: ' + str(min(values[i])) + ' - max: ' + str(max(values[i]))) #add min max
                axs[i].axes.get_yaxis().set_visible(False)
                i = i + 1
            except:
                print('Error on: {}'.format(index))

        dt = datetime.now().strftime("%Y%m%d_%H%M%S")

        fig.suptitle('plotLogs_{}'.format(dt))
        
        if(_savePlot):
            fig.savefig('plotLogs_{}'.format(dt))  