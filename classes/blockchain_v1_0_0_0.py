# Import Libraries

from past.builtins import execfile
from . import settings as settings
settings = settings.loadVersioning()
execfile(settings.loader('utilities'))
execfile(settings.loader('contractEvents'))
execfile(settings.loader('contractFunctions'))

frameworkInfo = settings.getFrameworkInfo()
CONNECTION_STRING = frameworkInfo['ConnectionString']['connectionString']
ETHERSCAN_APIKEY = frameworkInfo['APIKeys']['etherscan']
BSCSCAN_APIKEY = frameworkInfo['APIKeys']['bscscan']
BITQUERY_APIKEY = frameworkInfo['APIKeys']['bitquery']
CLASSES_FOLDER = frameworkInfo['Folders']['classes']
CONTRACTS_FOLDER = frameworkInfo['Folders']['contracts']
TRANSACTIONS_FOLDER = frameworkInfo['Folders']['transactions']

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

