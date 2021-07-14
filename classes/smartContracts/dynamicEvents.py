

if(contractName == '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4'): # Compound

    try:
        eventList.append(_contract.events.AccrueInterest())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.Approval())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.Borrow())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.Failure())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.LiquidateBorrow())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.Mint())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.NewAdmin())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.NewComptroller())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.NewImplementation())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.NewMarketInterestRateModel())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.NewPendingAdmin())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.NewReserveFactor())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.Redeem())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.RepayBorrow())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.ReservesAdded())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.ReservesReduced())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.Transfer())
    except:
        True #no-op

    try:
        eventList.append(_contract.events._events())
    except:
        True #no-op

    try:
        eventList.append(_contract.events.abi())
    except:
        True #no-op

