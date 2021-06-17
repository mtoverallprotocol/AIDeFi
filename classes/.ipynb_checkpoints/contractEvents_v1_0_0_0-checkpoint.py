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
    
    
    
    #################################################################################################################
    
    # @dev: This function allow us to have an array with callable smart contract events
    # Input: contract address, contract datatype
    # Output: array with callable smart contract events  (event list are in the same order of data/smartcontracts/{contract address}/events.csv)
    
    def eventList(self, _contractAddress, _contract):
        contractName = _contractAddress 
        
        eventList = []
        
        if(contractName == '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4'): #COMPOUND
                        
            eventList.append(_contract.events.AccrueInterest())
            eventList.append(_contract.events.Approval())
            eventList.append(_contract.events.Borrow())
            eventList.append(_contract.events.Failure())
            eventList.append(_contract.events.LiquidateBorrow())
            eventList.append(_contract.events.Mint())
            eventList.append(_contract.events.NewAdmin())
            eventList.append(_contract.events.NewComptroller())
            eventList.append(_contract.events.NewImplementation())
            eventList.append(_contract.events.NewMarketInterestRateModel())
            eventList.append(_contract.events.NewPendingAdmin())
            eventList.append(_contract.events.NewReserveFactor())
            eventList.append(_contract.events.Redeem())
            eventList.append(_contract.events.RepayBorrow())
            eventList.append(_contract.events.ReservesAdded())
            eventList.append(_contract.events.ReservesReduced())
            eventList.append(_contract.events.Transfer())
            
        if(contractName == '0xB53C1a33016B2DC2fF3653530bfF1848a515c8c5'): #AAVE_LendingPoolAddressesProvider
                        
            eventList.append(_contract.events.AddressSet())
            eventList.append(_contract.events.ConfigurationAdminUpdated())
            eventList.append(_contract.events.EmergencyAdminUpdated())
            eventList.append(_contract.events.LendingPoolCollateralManagerUpdated())
            eventList.append(_contract.events.LendingPoolConfiguratorUpdated())
            eventList.append(_contract.events.LendingPoolUpdated())
            eventList.append(_contract.events.LendingRateOracleUpdated())
            eventList.append(_contract.events.MarketIdSet())
            eventList.append(_contract.events.OwnershipTransferred())
            eventList.append(_contract.events.PriceOracleUpdated())
            eventList.append(_contract.events.ProxyCreated())
            
        if(contractName == '0x52D306e36E3B6B02c153d0266ff0f85d18BCD413'): #AAVE_LendingPoolAddressesProviderRegistry
                        
            eventList.append(_contract.events.AddressesProviderRegistered())
            eventList.append(_contract.events.AddressesProviderUnregistered())
            eventList.append(_contract.events.OwnershipTransferred())
            
        if(contractName == '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'): #AAVE_LendingPool
                        
            eventList.append(_contract.events.Upgraded())
            
        if(contractName == '0xbd4765210d4167CE2A5b87280D9E8Ee316D5EC7C'): #AAVE_LendingPoolCollateralManager
                        
            eventList.append(_contract.events.LiquidationCall())
            eventList.append(_contract.events.ReserveUsedAsCollateralDisabled())
            eventList.append(_contract.events.ReserveUsedAsCollateralEnabled())
            
        if(contractName == '0x311Bb771e4F8952E6Da169b425E7e92d6Ac45756'): #AAVE_LendingPoolConfigurator
                        
            eventList.append(_contract.events.AccrueInterest())
            
        if(contractName == '0x8A32f49FFbA88aba6EFF96F45D8BD1D4b3f35c7D'): #AAVE_LendingRateOracle
                        
            eventList.append(_contract.events.MarketBorrowRateSet())
            eventList.append(_contract.events.OwnershipTransferred())
            
        if(contractName == '0xA50ba011c48153De246E5192C8f9258A2ba79Ca9'): #AAVE_Price_Oracle
                        
            eventList.append(_contract.events.AssetSourceUpdated())
            eventList.append(_contract.events.FallbackOracleUpdated())
            eventList.append(_contract.events.OwnershipTransferred())
            eventList.append(_contract.events.WethSet())
            
        if(contractName == '0xB9062896ec3A615a4e4444DF183F0531a77218AE'): #AAVE_Pool_Admin
                        
            True #no-event emitted
            
        if(contractName == '0xB9062896ec3A615a4e4444DF183F0531a77218AE'): #AAVE_Emergency_Admin
                        
            eventList.append(_contract.events.AccrueInterest())
            
        if(contractName == '0xB9062896ec3A615a4e4444DF183F0531a77218AE'): #AAVE_ProtocolDataProvider
                        
            eventList.append(_contract.events.AccrueInterest())
            
        if(contractName == '0xcc9a0B7c43DC2a5F023Bb9b738E45B0Ef6B06E04'): #AAVE_WETHGateway
                        
            eventList.append(_contract.events.OwnershipTransferred())
            
        if(contractName == '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c'): #AAVE_AaveCollector
                        
            eventList.append(_contract.events.AccrueInterest())
            
        if(contractName == '0xd784927Ff2f95ba542BfC824c8a8a98F3495f6b5'): #AAVE_IncentivesController
                        
            eventList.append(_contract.events.Upgrated())
            
        return eventList