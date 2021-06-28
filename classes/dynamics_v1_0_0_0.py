# This class allow to create dynamic meta-code creating a specific text file converted to .py where will be imported inside another class

class dynamics:
    
    def ___init__(self, name):
        self.name = name

    #################################################################################################################
    
    # @dev: It generates a file that it will be imported such a piece of code on python class
    # Input: -
    # Output: .py file with dynamic python code that will be run inside contractEventas Class
    
    def generateDynamicSmartContractEventsAndFunctions(self, _apiKey, _connectionString):
        
        from . import blockchain_v1_0_0_0 as blockchain
        import pandas as pd
        blockchain = blockchain.blockchain()
        dynamicClassFolder = 'classes'
        #_smartContractAddress = '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4'
        fileName = 'dynamicEvents.py'
        file = open("{0}/{1}".format(dynamicClassFolder, fileName), 'w')

        dfSmartContract = pd.read_csv('data/smartContract.csv')
        for indexSmartContract in range(0, len(dfSmartContract)):
            (nameAddress, address) = (dfSmartContract['NameAddress'][indexSmartContract], dfSmartContract['Address'][indexSmartContract])
            (smartContract, ABI) = blockchain.getContract(address, _apiKey, _connectionString)   
            blockchain.saveContractData(address, smartContract, ABI) #save events and functions names on data/contracts/{address}
            dfEvents = pd.read_csv('data/contracts/{0}/events.csv'.format(address))
            if(len(dfEvents)>0):
                file.writelines("\n\nif(contractName == '{0}'): # {1}\n\n".format(address.lower(), nameAddress))
                for indexEvents in range(0, len(dfEvents)):
                    file.writelines("    eventList.append(_contract.events.{0}())\n".format(dfEvents['event'][indexEvents]))

        #ADD THE SAME LOGIC TO FUNCTIONS
        file.close() 
