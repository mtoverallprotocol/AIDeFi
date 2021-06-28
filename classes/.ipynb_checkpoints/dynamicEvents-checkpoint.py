

if(contractName == '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4'): # Compound

    eventList.append(_contract.events.AccrueInterest()):
    eventList.append(_contract.events.Approval()):
    eventList.append(_contract.events.Borrow()):
    eventList.append(_contract.events.Failure()):
    eventList.append(_contract.events.LiquidateBorrow()):
    eventList.append(_contract.events.Mint()):
    eventList.append(_contract.events.NewAdmin()):
    eventList.append(_contract.events.NewComptroller()):
    eventList.append(_contract.events.NewImplementation()):
    eventList.append(_contract.events.NewMarketInterestRateModel()):
    eventList.append(_contract.events.NewPendingAdmin()):
    eventList.append(_contract.events.NewReserveFactor()):
    eventList.append(_contract.events.Redeem()):
    eventList.append(_contract.events.RepayBorrow()):
    eventList.append(_contract.events.ReservesAdded()):
    eventList.append(_contract.events.ReservesReduced()):
    eventList.append(_contract.events.Transfer()):


if(contractName == '0xb53c1a33016b2dc2ff3653530bff1848a515c8c5'): # AAVE_LendingPoolAddressesProvider

    eventList.append(_contract.events.AddressSet()):
    eventList.append(_contract.events.ConfigurationAdminUpdated()):
    eventList.append(_contract.events.EmergencyAdminUpdated()):
    eventList.append(_contract.events.LendingPoolCollateralManagerUpdated()):
    eventList.append(_contract.events.LendingPoolConfiguratorUpdated()):
    eventList.append(_contract.events.LendingPoolUpdated()):
    eventList.append(_contract.events.LendingRateOracleUpdated()):
    eventList.append(_contract.events.MarketIdSet()):
    eventList.append(_contract.events.OwnershipTransferred()):
    eventList.append(_contract.events.PriceOracleUpdated()):
    eventList.append(_contract.events.ProxyCreated()):


if(contractName == '0x52d306e36e3b6b02c153d0266ff0f85d18bcd413'): # AAVE_LendingPoolAddressesProviderRegistry

    eventList.append(_contract.events.AddressesProviderRegistered()):
    eventList.append(_contract.events.AddressesProviderUnregistered()):
    eventList.append(_contract.events.OwnershipTransferred()):


if(contractName == '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'): # AAVE_LendingPool

    eventList.append(_contract.events.Upgraded()):


if(contractName == '0xbd4765210d4167ce2a5b87280d9e8ee316d5ec7c'): # AAVE_LendingPoolCollateralManager

    eventList.append(_contract.events.LiquidationCall()):
    eventList.append(_contract.events.ReserveUsedAsCollateralDisabled()):
    eventList.append(_contract.events.ReserveUsedAsCollateralEnabled()):


if(contractName == '0x311bb771e4f8952e6da169b425e7e92d6ac45756'): # AAVE_LendingPoolConfigurator

    eventList.append(_contract.events.Upgraded()):


if(contractName == '0x8a32f49ffba88aba6eff96f45d8bd1d4b3f35c7d'): # AAVE_LendingRateOracle

    eventList.append(_contract.events.MarketBorrowRateSet()):
    eventList.append(_contract.events.OwnershipTransferred()):


if(contractName == '0xa50ba011c48153de246e5192c8f9258a2ba79ca9'): # AAVE_Price_Oracle

    eventList.append(_contract.events.AssetSourceUpdated()):
    eventList.append(_contract.events.FallbackOracleUpdated()):
    eventList.append(_contract.events.OwnershipTransferred()):
    eventList.append(_contract.events.WethSet()):


if(contractName == '0xcc9a0b7c43dc2a5f023bb9b738e45b0ef6b06e04'): # AAVE_WETHGateway

    eventList.append(_contract.events.OwnershipTransferred()):


if(contractName == '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c'): # AAVE_AaveCollector

    eventList.append(_contract.events.AdminChanged()):
    eventList.append(_contract.events.Upgraded()):


if(contractName == '0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5'): # AAVE_IncentivesController

    eventList.append(_contract.events.Upgraded()):
