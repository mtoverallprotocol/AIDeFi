# Import Libraries

from . import utilities_v1_0_0_0 as utilities
from . import contractEvents_v1_0_0_0 as contractEvents
from . import contractFunctions_v1_0_0_0 as contractFunctions

utilities = utilities.utilities()
frameworkInfo = utilities.getFrameworkInfo()

contractEvents = contractEvents.contractEvents()
contractFunctions = contractFunctions.contractFunctions()

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
    
    def blockchainConnection(self, _connectionString):
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
            
            dexes = pd.read_csv('data/dex.csv', delimiter='|')
            for indexAddress in range(0, len(dexes)):
                if(_index == indexAddress):     
                    address = dexes['Address'].values[_index]
                    nameAddress = dexes['NameAddress'].values[_index]
        
        if(_source == 'TOKEN'): 
            
            tokens = pd.read_csv('data/token.csv', delimiter='|')
            for indexAddress in range(0, len(tokens)):
                if(_index == indexAddress):     
                    address = tokens['Address'].values[_index]
                    nameAddress = tokens['NameAddress'].values[_index].replace(".","").replace("$","").replace("+","")
        
        if(_source == 'SMART_CONTRACT'): 
            
            dexes = pd.read_csv('data/smartContract.csv', delimiter='|')
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
    
    def importTransactionListFromAddress(self, _etherscanApiKey, _provider, _action, _address, _blockInterval, _blockStart, _blockLimit):
        df = pd.DataFrame()
        START_BLOCK = '0'
        END_BLOCK = '9999999999999999'
        print(self.API_String(_provider, _action, [_address, START_BLOCK, END_BLOCK, _etherscanApiKey]))
        r = requests.get(self.API_String(_provider, _action, [_address, START_BLOCK, END_BLOCK, _etherscanApiKey]))
        tx = r.json()['result']
        #print(tx[0])
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
            maxDf = []
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
            
        return maxDf
    

    
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
    
    def importTransactionsFromDexCustomers(self, _etherscanApiKey, _provider, _action, _source): #directFunction
        
        df = pd.DataFrame()
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        EXPORT_FOLDER = 'export'
        
        export_files = [f for f in listdir(EXPORT_FOLDER) if isfile(join(EXPORT_FOLDER, f))]
        
        NUMBER_OF_MAPPED_ADDRESS = self.getNumberOfMaps(_source)
        
        dfCustomers = pd.DataFrame(columns = ['from'])
        
        for indexMap in range(0,NUMBER_OF_MAPPED_ADDRESS):
            
            addressName = self.getMaps(indexMap, _source)[0]
            fileIndex = utilities.linearReSearch(export_files, "TRANSACTION_LIST_{0}".format(addressName))
            
            if(fileIndex >= 0):
                
                csv_transactionList = export_files[fileIndex]
                
                print("Importing transactions from {0}".format(csv_transactionList))
                
                df = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, csv_transactionList), delimiter='|')
                df = df.filter(items = ['from', 'value']) # Save cache memory space selecting the 2 usefuls columns
                df = pd.DataFrame(df[df['value'] != '0']['from'].unique(), columns = ['from']) # Save cache memory space deleting duplicates and 0 values
                dfCustomers = pd.concat([dfCustomers, df])
                
                print("Imported {0} address transactions".format(len(df)))
                
        dfCustomers = pd.DataFrame(dfCustomers['from'].unique(), columns = ['from']) #Save more memory and duplicates on final result
        
        dfCustomers.to_csv('export/CUSTOMERS_LIST_FROM_DEX_{0}.csv'.format(dt), sep='|')
        
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
            dfCustomers.to_csv('export/CUSTOMERS_TRANSACTIONS_FROM_DEX_{1}.csv'.format(fromAddress, dt), sep='|')
        
    def importAllDexTransactions(self, _etherscanApiKey, _provider, _action, _source, _blockInterval, _blockStart, _blockLimit): #only DEXes
        
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for indexMap in range(0,self.getNumberOfMaps(_source)):
            
            address = self.getMaps(indexMap, _source)[1]
            addressName = self.getMaps(indexMap, _source)[0]
            df = self.importTransactionListFromAddress(_etherscanApiKey, _provider, _action, address, _blockInterval, _blockStart, _blockLimit)
            if(len(df) > 0):
                print("Dataset of {0}'s lenght is: {1}".format(addressName, len(df)))
                df.to_csv("export/TRANSACTION_LIST_{0}_{1}.csv".format(addressName, dt), sep='|')

                
                
    #################################################################################################################
    
    # @dev: This function allow us to save in a variable the contract datatype
    # Input: contract address, etherscan api key, connection string to web3 (Infura)
    # Output: Dataframe with merged logs based on blocknumber timeline
    
    def getContract(self, _address, _apiKey, _connectionString):
        ABI = requests.get(self.API_String('ETHERSCAN', 'ABI', [_address, _apiKey])).json()['result']
        w3 = self.blockchainConnection(_connectionString)
        address = w3.toChecksumAddress(_address)
        return (w3.eth.contract(address=address, abi=ABI), ABI)
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to decode the args of transaction log
    # Input: transaction hash, contract function namd,  onnection string to web3 (Infura)
    # Output: Dataframe with merged logs based on blocknumber timeline
    
    def getLogFromTransactionHash(self, _tx, _contractFunction, _connectionString):
        w3 = self.blockchainConnection(_connectionString)
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
    
    def decodeLogs(self, _tx, _contractAddress, _contract, _ABI, _connectionString): 
        
        eventList = contractEvents.eventList(_contractAddress, _contract) # callable functions
        eventsName = contractEvents.getEventsFromABI(_ABI) #only function list
        
        keyLogList = {}
        valueLogList = {}
        txR = {}
        index = 0
        
        for event in eventList:
            
            (keyLogList[eventsName[index]], valueLogList[eventsName[index]], txR) = self.getLogFromTransactionHash(_tx, event, _connectionString)
            
            index = index + 1
            
        return (keyLogList, valueLogList, txR)
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to save the events and the functions name of a specific address
    # Input: contract address, contract datatype, contract ABI
    # Output: A couple of csv (events.csv and functions.csv)
    
    def saveContractData(self,_contractAddress, _contract, _ABI):
        
        contractName = _contractAddress
        eventsList = contractEvents.getEventsFromABI(_ABI)
        functionList = contractFunctions.getFunctionsFromABI(_ABI)
        
        utilities.createFolder('data/contracts/{0}'.format(contractName))     
            
        filePath = "data/contracts/{0}/{1}.csv"
        
        pd.DataFrame(eventsList, columns = ['event']).to_csv(filePath.format(contractName, 'events'))
        pd.DataFrame(functionList, columns = ['function']).to_csv(filePath.format(contractName, 'functions'))
        
        
        
        
    #################################################################################################################
    
    # @dev: This function downloads Logs content of a specific address from the last blocknumber catching a specific
    # volume of transactions
    # Input: contract address, etherscan API key, connection string to Infura
    # Output: List of dataframes for each event of the contract address specified
    
    def downloadContractLogsContent(self, _contractAddress, _apiKey, _connectionString):
        
        filePath = "data/contracts/{0}/{1}{2}.csv"

        (contract, ABI) = self.getContract(_contractAddress, _apiKey, _connectionString)
        
        self.saveContractData(_contractAddress, contract, ABI)
        
        contractName = _contractAddress

        df_txHashList = self.importTransactionListFromAddress(_apiKey, 'ETHERSCAN', 'TXLIST', _contractAddress, 'MAX', 0 , 2)['hash'][1:] #set here dimension of transactions volume (to be defined or dev can set it using other variables)
        
        print("Found {0} transaction(s)".format(len(df_txHashList)))
        (keyLog, valueLog, txR) = self.decodeLogs(df_txHashList.iloc[0], _contractAddress, contract, ABI, _connectionString)
        
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

            (keyLog, valueLog, txR) = self.decodeLogs(tx, _contractAddress, contract, ABI, _connectionString)

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
        
        filePath = "data/contracts/{0}/{1}{2}_{3}.csv"
        for dfKey in dfList.keys():
            dfList[dfKey].to_csv(filePath.format(contractName, 'event_', dfKey, dt))
    
    
    
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
                        indexKey = 0
                        foundKey = 0
                        for key in tmpDf.keys():
                            if(key == 'blockHash'):
                                foundKey = indexKey
                            indexKey = indexKey + 1
                        keyName = contractFolder.replace(" ", "_")+"_"+eventsData[0:len(eventsData)-4]
                        dfList[keyName] = tmpDf.iloc[:, 1:foundKey]
                        dfList[keyName]['blockNumber'] = tmpDf['blockNumber']
                        blockNumberDf = pd.concat([blockNumberDf, tmpDf['blockNumber']])
                        
        blockNumberList = pd.DataFrame(blockNumberDf[0].unique(), columns = ['blockNumber'])
        
        sql = "SELECT {0} FROM blockNumberList A "
        colsBase = 'A.blockNumber, '
        cols = colsBase
        for key in dfList.keys():
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
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to create all folders based on smartcontracts.csv file (inside data). For each smart contract
    # we will save all events and functions in dataframes
    # Input: nothing, indirectly the smartcontracts.csv has to be filled with address/namdeAddress info
    # Output: List of folders and files inside data/contracts/{contractAddress}
    
    def inizializeSmartContracts(self, _apiKey, _connectionString):
        for indexMap in range(0,self.getNumberOfMaps('SMART_CONTRACT')):
            address = self.getMaps(indexMap, 'SMART_CONTRACT')[1]
            ABI = requests.get(self.API_String('ETHERSCAN', 'ABI', [address, _apiKey])).json()['result']
            contract = self.getContract(address, _apiKey, _connectionString)
            self.saveContractData(address, contract, ABI)

    