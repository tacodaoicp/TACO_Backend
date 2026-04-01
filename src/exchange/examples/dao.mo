import Result "mo:base/Result";

module {
  public type TransferFee = Nat;
  public type Token = Text;

  public type TokenInfo = {
    address : Text;
    name : Text;
    symbol : Text;
    decimals : Nat;
    transfer_fee : Nat;
    icp_price : Nat;
  };

  public type Vote = { token : Text; basisPoints : Nat };

  public type VouchedInfo = {
    tokenId : Text;
    decimals : Nat;
    transferFee : Nat;
    name : Text;
    totalAmount : Nat;
    vouchers : [(Principal, Nat)];
  };

  public type Holdings = {
    amount : Amount;
    totalValue : ICPPrice;
  };

  public type Time = Int;

  public type SNSstyleAuction = {
    active : Bool;
    from : Int;
    to : Int;
    AcceptedTokens : [Text];
    TACOAmount : Nat;
    TotalVouched : [VouchedInfo];
    minimumAmount : [Nat];
  };

  public type ProposalID = Nat;

  public type Proposal = {
    ProposalID : ProposalID;
    Active : { #Active; #Ended };
    CoveredVotingPower : CoveredVotingPower;
    NonCoveredVotingPower : NonCoveredVotingPower;
    IncrementCovered : IncrementCovered;
    AllocationDetailsLastIncrement : AllocationDetailsLastIncrement;
    AllocationDetailsPast : AllocationDetailsPast;
    AllocationDetailsPastIncrements : [[{
      token : Text;
      basisPoints : Nat;
    }]];
    epochStarted : Int;
    epochEndingOrEnded : Int;
    holdingsStart : [{ address : Text; holdings : Nat; icpValue : Nat }];
    holdingsEnd : [{ address : Text; holdings : Nat; icpValue : Nat }];
  };

  public type NonCoveredVotingPower = Nat;
  public type IncrementCovered = Nat;
  public type ICPPrice = Nat;
  public type Decimals = Nat;
  public type CoveredVotingPower = Nat;
  public type BasisPoint = Nat;
  public type Amount = Nat;

  public type TokenDetails = {
    Active : Bool;
    isPaused : Bool;
    lastUnbuggedPrice : Nat;
    lastPrices : [Nat];
    highestPrice : Nat;
    lowestPrice : Nat;
    tokenType : { #ICP; #ICRC12; #ICRC3 };
    address : Token;
    decimals : Decimals;
    transferFee : TransferFee;
    name : Text;
    symbol : Text;
    holdingsBeforeVote : Holdings;
    currentHoldings : Holdings;
    minimumAmountAuction : Nat;
    priceUSD : Float;
    epochLastPriceUpdate : Int;
    epochAdded : Int;
  };

  public type AllocationDetailsPast = [Vote];
  public type AllocationDetailsLastIncrement = [Vote];

  public type RecalibratedPosition = {
    poolId : (Text, Text);
    accesscode : Text;
    amountInit : Nat;
    amountSell : Nat;
    fee : Nat;
    revokeFee : Nat;
  };

  public type TokenAmount = (Text, Nat);

  public type TransactionType = {
    #Burn;
    #Mint;
    #Vouch : Time;
  };

  public type Transaction = {
    txType : TransactionType;
    sentToDAO : [TokenAmount];
    sentFromDAO : [TokenAmount];
    when : Time;

  };

  public type PermissionUpdate = {
    changeParameters : ?Bool;
    forceStopProposal : ?Bool;
    pauseToken : ?Bool;
    checkDiffs : ?Bool;
    getErrorLog : ?Bool;
    get_cycles : ?Bool;
    CriticalPause : ?Bool;
    addAcceptedToken : ?Bool;
    forceEndSNSstyleAuction : ?Bool;
    createSNSstyleAuction : ?Bool;
    NextProposal : ?Bool;
  };

  public type Self = actor {
    getUserTransactions : shared query () -> async ?{
      transactions : [{
        txType : TransactionType;
        sentToDAO : [TokenAmount];
        sentFromDAO : [TokenAmount];
        when : Int;

      }];
      totalTransactions : Nat;
    };

    BurnTaco : shared (Nat64) -> async Text;
    CriticalPause : shared () -> async ();
    NextProposal : shared (Bool, Bool, Bool) -> async ();
    addAcceptedToken : shared ({ #Add; #Remove; #Opposite }, Principal, Nat, { #ICP; #ICRC12; #ICRC3 }) -> async ();

    changeParameters : shared {
      createOrdersIfNotDone : ?Bool;
      frontendCanister : ?Principal;
      SNSmanagementCanister : ?Principal;
      snapshotCanister : ?Principal;
      TacoDecimals : ?Nat;
      MinimumTACOMint : ?Nat;
      BurnFees : ?Nat;
      MintFees : ?Nat;
      MaxPriceChange : ?Nat;
      incrementAmounts : ?Nat;
      manualSwitchMintBurn : ?Bool;
      deleteFromDayBan : ?[Principal];
      deleteFromAllTimeBan : ?[Principal];
      addToAllTimeBan : ?[Principal];
      changeAllowedCalls : ?Nat;
      changeallowedSilentWarnings : ?Nat;
      addAllowedCanisters : ?[Principal];
      deleteAllowedCanisters : ?[Principal];
    } -> async ();

    createSNSstyleAuction : shared (Nat, Nat, Nat) -> async Bool;
    deleteTokensForTest : shared () -> async ();
    forceHoldingsUpdate : shared Bool -> async ();
    setBurnCase : shared (Bool, Bool, Bool, Bool) -> async ();
    forceRecalibratePositions : shared (Bool, Bool) -> async ();
    forceStopProposal : shared () -> async ();

    getAcceptedTokens : shared query () -> async ?[Text];
    getAllTokensEverAccepted : shared query () -> async ?[Text];
    getEmptyAllocation : shared query () -> async ?[{
      token : Text;
      basisPoints : Nat;
    }];
    getErrorLog : shared query () -> async [(Int, Text)];
    getIndividualPastVotes : shared query Principal -> async ?[{
      ProposalID : ProposalID;
      vote : [{
        token : Text;
        basisPoints : Nat;
      }];
    }];
    getOldOrders : shared query () -> async ?[RecalibratedPosition];
    getOrders : shared query () -> async ?[RecalibratedPosition];

    getParameters : shared query () -> async ?{
      TacoDecimals : Nat;
      TacoExchangePrice : Float;
      MinimumTACOMint : Nat;
      BurnFees : Nat;
      MintFees : Nat;
      incrementAmounts : Nat;
      mintPossible : Bool;
      burnPossible : Bool;
      portofolioICPValue : Nat;
      TACOSupply : Nat;
      TacoHoldingsForMint : Nat;
      manualSwitchMintBurn : Bool;
    };

    getProposals : shared query Nat -> async ?[Proposal];
    getSNSstyleAuction : shared query () -> async ?[SNSstyleAuction];
    getTACOMintBurnPrice : shared query () -> async ?(
      {
        TacoHoldings : Nat;
        liveICPworthPortofolioHIGH : Nat;
        liveICPworthPortofolioLOW : Nat;
        tokens : [Text];
        TACOSupply : Nat;
      },
      Bool,
      Bool,
    );

    get_cycles : shared query () -> async Nat;
    forceEndSNSstyleAuction : shared Bool -> async ();
    pauseToken : shared ({ #Add; #Remove; #Opposite }, Principal) -> async ();
    returncontractprincipal : shared query () -> async Text;
    mintTaco : shared (Principal, Nat64) -> async Text;
    setTest : shared Bool -> async ();
    vote : shared [Vote] -> async ();
    vouchSNSstyleAuction : shared (Principal, Nat, Nat) -> async Bool;
    getTokenDetails : shared query () -> async [TokenDetails];
    recoverWronglysent : shared (Principal, Nat, { #ICP; #ICRC12; #ICRC3 }, Bool) -> async Bool;
    addTACOforMintBurn : shared (Nat64) -> async Bool;
    checkDiffs : shared Bool -> async ?{
      error : Bool;
      differences : {
        actualTotalSupply : Int;
        expectedTotalSupply : Int;
        difference : Int;
        tacoBalance : Int;
        tacoHoldingsForMint : Int;
      };
    };
    getLogging : shared query (
      {
        #addAcceptedToken;
        #OTCTrades;
        #revokeTrades;
        #endSNSAuction;
        #endWeeklyProposal;
        #retryWithBackOff;
        #createSNSstyleAuction;
      },
      Nat,
    ) -> async {
      entries : [(Nat, Text)];
      totalEntries : Nat;
      functionType : Text;
    };
    updateControllerPermissions : shared PermissionUpdate -> async Result.Result<Text, Text>;

  };

};
