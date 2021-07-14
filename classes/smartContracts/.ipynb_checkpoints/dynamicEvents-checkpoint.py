

if(contractName == '0xb8c77482e45f1f44de1745f52c74426c631bdd52'): # Binance

    eventList.append(_contract.events.Transfer())
    eventList.append(_contract.events.Burn())
    eventList.append(_contract.events.Freeze())
    eventList.append(_contract.events.Unfreeze())


if(contractName == '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4'): # Compound

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
