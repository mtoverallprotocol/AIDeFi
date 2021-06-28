class unitTest():

    def ___init__(self, name):
        self.name = name
        
    def runAllTests(self):
        print("Running blockchain_v1_0_0_0 class test...")
        blockchainTest.test_web3Connection_infura(self)

class blockchainTest(unitTest):
    
        def ___init__(self, name):
            self.name = name
        
        def test_web3Connection_infura(self):
        
            from . import blockchain_v1_0_0_0 as blockchain

            blockchain = blockchain.blockchain()
            INFURA_ID = '40816bb42ce34f808b4afe219e9d41cb' #change it with your own id
            connectionString = 'https://mainnet.infura.io/v3/{0}'.format(INFURA_ID)

            if(blockchain.blockchainConnection(connectionString).isConnected() == True):
                testResult = 'PASSED'
            else:
                testResult = 'NOT_PASSED'
            
            print('Test test_web3Connection_infura ==> {0}'.format(testResult))
            
            return testResult
        
        