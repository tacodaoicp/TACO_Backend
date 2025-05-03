module {
  public type NatResult = { #ok : Nat; #err : Text };
  public type OldPublicTokenOverview = {
    id : Nat;
    totalVolumeUSD : Float;
    name : Text;
    priceUSDChangeWeek : Float;
    volumeUSD : Float;
    feesUSD : Float;
    priceUSDChange : Float;
    tvlUSD : Float;
    address : Text;
    volumeUSDWeek : Float;
    txCount : Int;
    priceUSD : Float;
    volumeUSDChange : Float;
    tvlUSDChange : Float;
    standard : Text;
    tvlToken : Float;
    symbol : Text;
  };
  public type PoolInfo = {
    fee : Int;
    token0Id : Text;
    token1Id : Text;
    pool : Text;
    token1Price : Float;
    token1Standard : Text;
    token1Decimals : Float;
    token0Standard : Text;
    token0Symbol : Text;
    token0Decimals : Float;
    token0Price : Float;
    token1Symbol : Text;
  };
  public type PoolTvlData = {
    token0Id : Text;
    token1Id : Text;
    pool : Text;
    tvlUSD : Float;
    token0Symbol : Text;
    token1Symbol : Text;
  };
  public type PublicTokenChartDayData = {
    id : Int;
    volumeUSD : Float;
    timestamp : Int;
    txCount : Int;
  };
  public type PublicTokenOverview = {
    id : Nat;
    volumeUSD1d : Float;
    volumeUSD7d : Float;
    totalVolumeUSD : Float;
    name : Text;
    volumeUSD : Float;
    feesUSD : Float;
    priceUSDChange : Float;
    address : Text;
    txCount : Int;
    priceUSD : Float;
    standard : Text;
    symbol : Text;
  };
  public type PublicTokenPricesData = {
    id : Int;
    low : Float;
    high : Float;
    close : Float;
    open : Float;
    timestamp : Int;
  };
  public type Transaction = {
    to : Text;
    action : TransactionType;
    token0Id : Text;
    token1Id : Text;
    liquidityTotal : Nat;
    from : Text;
    hash : Text;
    tick : Int;
    token1Price : Float;
    recipient : Text;
    token0ChangeAmount : Float;
    sender : Text;
    liquidityChange : Nat;
    token1Standard : Text;
    token0Fee : Float;
    token1Fee : Float;
    timestamp : Int;
    token1ChangeAmount : Float;
    token1Decimals : Float;
    token0Standard : Text;
    amountUSD : Float;
    amountToken0 : Float;
    amountToken1 : Float;
    poolFee : Nat;
    token0Symbol : Text;
    token0Decimals : Float;
    token0Price : Float;
    token1Symbol : Text;
    poolId : Text;
  };
  public type TransactionType = {
    #decreaseLiquidity;
    #claim;
    #swap;
    #addLiquidity;
    #increaseLiquidity;
  };
  public type Self = actor {
    addOwners : shared [Principal] -> async ();
    batchInsert : shared (Text, [Transaction]) -> async ();
    batchUpdatePoolTvl : shared [PoolTvlData] -> async ();
    clean : shared () -> async ();
    cycleAvailable : shared () -> async NatResult;
    cycleBalance : shared query () -> async NatResult;
    getAllTokens : shared query () -> async [PublicTokenOverview];
    getOwners : shared () -> async [Principal];
    getPoolTvl : shared query () -> async [PoolTvlData];
    getPoolsForToken : shared query Text -> async [PoolInfo];
    getToken : shared query Text -> async PublicTokenOverview;
    getTokenChartData : shared query (Text, Nat, Nat) -> async [
      PublicTokenChartDayData
    ];
    getTokenPricesData : shared query (Text, Int, Int, Nat) -> async [
      PublicTokenPricesData
    ];
    getTokenTransactions : shared query (Text, Nat, Nat) -> async [Transaction];
    insert : shared (Text, Transaction) -> async ();
    updateDayData : shared (Text, Nat, Float, Float, Float, Float) -> async ();
    updateOverview : shared [OldPublicTokenOverview] -> async ();
  };
};
