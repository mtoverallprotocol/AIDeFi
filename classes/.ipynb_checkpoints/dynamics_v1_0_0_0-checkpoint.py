from past.builtins import execfile
from . import loadVersioning as loadVersioning
loadVersioning = loadVersioning.loadVersioning()

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
        blockchain = blockchain.blockchain()
        #execfile(loadVersioning.loader('blockchain'))
        import pandas as pd
        dynamicClassFolder = 'classes/smartContracts'
        #_smartContractAddress = '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4'
        fileNameEvents = 'dynamicEvents.py'
        
        fileEvents = open("{0}/{1}".format(dynamicClassFolder, fileNameEvents), 'w')
        
        dfSmartContract = pd.read_csv('data/smartContract.csv')
        for indexSmartContract in range(0, len(dfSmartContract)):
            (nameAddress, address) = (dfSmartContract['NameAddress'][indexSmartContract], dfSmartContract['Address'][indexSmartContract])
            address = address.lower()
            (smartContract, ABI) = blockchain.getContract(address, _apiKey, _connectionString)   
            blockchain.saveContractData(smartContract) #save events and functions names on data/contracts/{address}
            dfEvents = pd.read_csv('data/contracts/{0}/events.csv'.format(address))
            if(len(dfEvents)>0):
                fileEvents.writelines("\n\nif(contractName == '{0}'): # {1}\n\n".format(address.lower(), nameAddress))
                for indexEvents in range(0, len(dfEvents)):
                    fileEvents.writelines("    try:\n        eventList.append(_contract.events.{0}())\n".format(dfEvents['event'][indexEvents]))
                    fileEvents.writelines("    except:\n        True #no-op\n\n".format(dfEvents['event'][indexEvents]))

        #ADD THE SAME LOGIC TO FUNCTIONS
        fileEvents.close() 
        
    def generateCallableFunction(self, _ABI, _functionName, args):

        #execfile(loadVersioning.loader('contractFunctions')) #versioning doesn't work...test more!
        from . import contractFunctions_v1_0_0_0 as contractFunctions
        contractFunctions = contractFunctions.contractFunctions()
        
        dynamicClassFolder = 'classes/smartContracts'
        fileNameFunction = 'dynamicFunction_{0}.py'.format(_functionName)    
        fileFunction = open('{0}/{1}'.format(dynamicClassFolder, fileNameFunction), 'w')
        
        #args on solidity can be Boolean, Int256, Address, Bytes32 #to test if we need apex or not
        
        (inputs, outputs) = contractFunctions.getFunctionsArgsFromPartialABI(contractFunctions.getPartialABIFromFunctionName(_ABI, _functionName))
        
        inputString = "(" + ", ".join(args) + ")"
        
        print("Inputs: {0}".format(", ".join(inputs)))
        print("Outputs: {0}".format(", ".join(outputs)))
        
        fileFunction.writelines("return _contract.functions.{0}{1}".format(_functionName, inputString))
        
        fileFunction.close()
        
        
        
        