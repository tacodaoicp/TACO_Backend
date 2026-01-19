import Principal "mo:base/Principal";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Nat64 "mo:base/Nat64";
import Text "mo:base/Text";
import Int "mo:base/Int";
import Float "mo:base/Float";
import Result "mo:base/Result";

module {
  // Basic Types
  public type Token = {
    address : Text;
    standard : Text;
  };

  public type Value = {
    #Nat : Nat;
    #Int : Int;
    #Text : Text;
    #Blob : Blob;
  };

  // ICRC1 Types
  public type ICRC1Account = {
    owner : Principal;
    subaccount : ?[Nat8];
  };

  public type ICRC1TransferArgs = {
    to : ICRC1Account;
    fee : ?Nat;
    memo : ?[Nat8];
    from_subaccount : ?[Nat8];
    created_at_time : ?Nat64;
    amount : Nat;
  };

  public type ICRC1TransferError = {
    #BadFee : { expected_fee : Nat };
    #BadBurn : { min_burn_amount : Nat };
    #InsufficientFunds : { balance : Nat };
    #TooOld;
    #CreatedInFuture : { ledger_time : Nat64 };
    #Duplicate : { duplicate_of : Nat };
    #TemporarilyUnavailable;
    #GenericError : { error_code : Nat; message : Text };
  };

  public type ICRC1TokenMetadata = {
    fee : Nat;
    decimals : Nat8;
    name : Text;
    symbol : Text;
  };

  public type ICRC1 = actor {
    icrc1_transfer : shared ICRC1TransferArgs -> async {
      #Ok : Nat;
      #Err : ICRC1TransferError;
    };
    icrc1_balance_of : shared query ICRC1Account -> async Nat;
    icrc1_metadata : shared query () -> async [(Text, Value)];
    icrc1_name : shared query () -> async Text;
    icrc1_symbol : shared query () -> async Text;
    icrc1_decimals : shared query () -> async Nat8;
    icrc1_fee : shared query () -> async Nat;
  };

  // ICPSwap Types
  public type PoolMetadata = {
    fee : Nat;
    key : Text;
    liquidity : Nat;
    maxLiquidityPerTick : Nat;
    nextPositionId : Nat;
    sqrtPriceX96 : Nat;
    tick : Int;
    token0 : Token;
    token1 : Token;
  };

  public type ICPSwapError = {
    #CommonError;
    #InternalError : Text;
    #UnsupportedToken : Text;
    #InsufficientFunds;
  };

  public type SwapArgs = {
    amountIn : Text;
    amountOutMinimum : Text;
    zeroForOne : Bool;
  };

  public type DepositArgs = {
    amount : Nat;
    fee : Nat;
    token : Text;
  };

  public type WithdrawArgs = {
    amount : Nat;
    fee : Nat;
    token : Text;
  };

  // Combined deposit and swap args - saves one inter-canister call
  public type DepositAndSwapArgs = {
    zeroForOne : Bool;
    tokenInFee : Nat;
    tokenOutFee : Nat;
    amountIn : Text;
    amountOutMinimum : Text;
  };

  public type ICPSwapPool = actor {
    metadata : query () -> async Result.Result<PoolMetadata, ICPSwapError>;
    //metadata2 : query Principal -> async Result.Result<PoolMetadata, ICPSwapError>;
    quote : query (SwapArgs) -> async Result.Result<Nat, ICPSwapError>;
    //quote2 : query (SwapArgs, Principal) -> async Result.Result<Nat, ICPSwapError>;
    getUserUnusedBalance : query (Principal) -> async Result.Result<{ balance0 : Nat; balance1 : Nat }, Text>;
    deposit : shared (DepositArgs) -> async Result.Result<Nat, ICPSwapError>;
    //deposit2 : shared (Principal, DepositArgs) -> async Result.Result<Nat, ICPSwapError>;
    swap : shared (SwapArgs) -> async Result.Result<Nat, ICPSwapError>;
    //swap2 : shared (Principal, SwapArgs) -> async Result.Result<Nat, ICPSwapError>;
    withdraw : shared (WithdrawArgs) -> async Result.Result<Nat, ICPSwapError>;
    //withdraw2 : shared (Principal, WithdrawArgs) -> async Result.Result<Nat, ICPSwapError>;
    // Combined deposit+swap - reduces 2 calls to 1, saving ~5-8s per trade
    depositAndSwap : shared (DepositAndSwapArgs) -> async Result.Result<Nat, ICPSwapError>;
  };

  public type ICPSwapPriceInfo = {
    price : Float;
    timestamp : Int;
    fee : Nat;
    liquidity : Nat;
    priceX96 : Nat;
    tick : Int;
    token0 : Token;
    token1 : Token;
  };

  public type ICPSwapQuoteParams = {
    poolId : Principal;
    amountIn : Nat;
    amountOutMinimum : Nat;
    zeroForOne : Bool;
  };

  public type ICPSwapQuoteResult = {
    amountOut : Nat;
    slippage : Float;
    fee : Nat;
    token0 : Token;
    token1 : Token;
  };

  public type ICPSwapBalance = {
    balance0 : Nat;
    balance1 : Nat;
  };

  public type ICPSwapParams = {
    poolId : Principal;
    amountIn : Nat;
    minAmountOut : Nat;
    zeroForOne : Bool;
  };

  public type ICPSwapWithdrawParams = {
    poolId : Principal;
    token : Principal;
    amount : Nat;
    fee : Nat;
  };

  public type ICPSwapDepositParams = {
    poolId : Principal;
    token : Principal;
    amount : Nat;
    fee : Nat;
  };

  // ICPSwap Factory Types
  public type GetPoolArgs = {
    token0 : Token;
    token1 : Token;
    fee : Nat;
  };

  public type PoolData = {
    key : Text;
    token0 : Token;
    token1 : Token;
    fee : Nat;
    tickSpacing : Int;
    canisterId : Principal;
  };

  public type SwapFactory = actor {
    getPool : query (GetPoolArgs) -> async Result.Result<PoolData, Text>;
  };

  // Kong Swap Types
  public type KongSwapPriceInfo = {
    tokenA : Text;
    tokenB : Text;
    price : Float;
    timestamp : Int;
    fee : Nat;
    liquidity : Nat;
    midPrice : Float;
    slippage : Float;
  };

  public type KongSwapBalance = {
    token : Text;
    amount : Nat;
    symbol : Text;
    decimals : Nat8;
  };

  public type KongSwapParams = {
    token0_ledger : Principal;
    token0_symbol : Text;
    token1_ledger : Principal;
    token1_symbol : Text;
    amountIn : Nat;
    minAmountOut : Nat;
    deadline : ?Int;
    recipient : ?Principal;
    txId : ?Nat;
    slippageTolerance : Float;
  };

  public type TxId = {
    #BlockIndex : Nat;
    #TransactionId : Text;
  };

  public type KongSwapArgs = {
    pay_token : Text;
    pay_amount : Nat;
    pay_tx_id : ?TxId;
    receive_token : Text;
    receive_amount : ?Nat;
    receive_address : ?Principal;
    max_slippage : ?Float;
    referred_by : ?Principal;
  };

  public type PoolsResult = { #Ok : PoolsReply; #Err : Text };
  public type TokensResult = { #Ok : [TokenReply]; #Err : Text };
  public type SwapAmountsResult = { #Ok : SwapAmountsReply; #Err : Text };
  public type SwapResult = { #Ok : SwapReply; #Err : Text };
  public type SwapAsyncResult = { #Ok : Nat64; #Err : Text };
  public type RequestsResult = { #Ok : [RequestsReply]; #Err : Text };

  public type KongSwap = actor {
    pools : query (?Text) -> async PoolsResult;
    tokens : query (?Text) -> async TokensResult;
    swap_amounts : query (Text, Nat, Text) -> async SwapAmountsResult;
    swap : (KongSwapArgs) -> async SwapResult;
    swap_async : (KongSwapArgs) -> async SwapAsyncResult;
    requests : query (?Nat64) -> async RequestsResult;
    // Claim functions for recovering tokens from failed swaps
    claims : query (Text) -> async ClaimsResult;  // text = principal as string
    claim : (Nat64) -> async ClaimResult;  // nat64 = claim_id
  };

  public type TokenReply = {
    #IC : ICTokenReply;
    #LP : LPTokenReply;
  };

  public type ICTokenReply = {
    token_id : Nat32;
    chain : Text;
    canister_id : Text;
    name : Text;
    symbol : Text;
    decimals : Nat8;
    fee : Nat;
    icrc1 : Bool;
    icrc2 : Bool;
    icrc3 : Bool;
    is_removed : Bool;
  };

  public type LPTokenReply = {
    token_id : Nat32;
    chain : Text;
    address : Text;
    name : Text;
    symbol : Text;
    pool_id_of : Nat32;
    decimals : Nat8;
    fee : Nat;
    total_supply : Nat;
    is_removed : Bool;
  };

  public type PoolsReply = {
    pools : [PoolReply];
    total_tvl : Nat;
    total_24h_volume : Nat;
    total_24h_lp_fee : Nat;
    total_24h_num_swaps : Nat;
  };

  public type PoolReply = {
    pool_id : Nat32;
    name : Text;
    symbol : Text;
    chain_0 : Text;
    symbol_0 : Text;
    address_0 : Text;
    balance_0 : Nat;
    lp_fee_0 : Nat;
    chain_1 : Text;
    symbol_1 : Text;
    address_1 : Text;
    balance_1 : Nat;
    lp_fee_1 : Nat;
    price : Float;
    lp_fee_bps : Nat8;
    tvl : Nat;
    rolling_24h_volume : Nat;
    rolling_24h_lp_fee : Nat;
    rolling_24h_num_swaps : Nat;
    rolling_24h_apy : Float;
    lp_token_symbol : Text;
    is_removed : Bool;
  };

  public type SwapAmountsReply = {
    pay_chain : Text;
    pay_symbol : Text;
    pay_address : Text;
    pay_amount : Nat;
    receive_chain : Text;
    receive_symbol : Text;
    receive_address : Text;
    receive_amount : Nat;
    price : Float;
    mid_price : Float;
    slippage : Float;
    txs : [SwapAmountsTxReply];
  };

  public type SwapAmountsTxReply = {
    pool_symbol : Text;
    pay_chain : Text;
    pay_symbol : Text;
    pay_address : Text;
    pay_amount : Nat;
    receive_chain : Text;
    receive_symbol : Text;
    receive_address : Text;
    receive_amount : Nat;
    price : Float;
    lp_fee : Nat;
    gas_fee : Nat;
  };

  public type SwapReply = {
    tx_id : Nat64;
    request_id : Nat64;
    status : Text;
    pay_chain : Text;
    pay_address : Text;
    pay_symbol : Text;
    pay_amount : Nat;
    receive_chain : Text;
    receive_address : Text;
    receive_symbol : Text;
    receive_amount : Nat;
    mid_price : Float;
    price : Float;
    slippage : Float;
    txs : [SwapTxReply];
    transfer_ids : [TransferIdReply];
    claim_ids : [Nat64];
    ts : Nat64;
  };

  public type SwapTxReply = {
    pool_symbol : Text;
    pay_chain : Text;
    pay_address : Text;
    pay_symbol : Text;
    pay_amount : Nat;
    receive_chain : Text;
    receive_address : Text;
    receive_symbol : Text;
    receive_amount : Nat;
    price : Float;
    lp_fee : Nat;
    gas_fee : Nat;
    ts : Nat64;
  };

  public type TransferIdReply = {
    transfer_id : Nat64;
    transfer : TransferReply;
  };

  public type TransferReply = {
    #IC : ICTransferReply;
  };

  public type ICTransferReply = {
    chain : Text;
    symbol : Text;
    is_send : Bool;
    amount : Nat;
    canister_id : Text;
    block_index : Nat;
  };

  public type RequestsReply = {
    request_id : Nat64;
    statuses : [Text];
    request : RequestRequest;
    reply : RequestReply;
    ts : Nat64;
  };

  public type RequestRequest = {
    #Swap : KongSwapArgs;
    #AddPool;
    #AddLiquidity;
    #RemoveLiquidity;
  };

  public type RequestReply = {
    #Pending;
    #Swap : SwapReply;
    #AddPool;
    #AddLiquidity;
    #RemoveLiquidity;
  };

  // Optional withdraw parameters for combined operations
  public type OptionalWithdrawParams = {
    token : Principal;
    amount : ?Nat; // If null, withdraw all received tokens
  };

  // Combined operation result
  public type TransferDepositSwapWithdrawResult = {
    swapAmount : Nat;     // Amount from swap (use for price calculation)
    receivedAmount : Nat; // Amount after withdraw fee (actual tokens received)
  };

    // Kong Swap Transaction Types
  public type SwapTxStatus = {
    #SwapPending;
    #SwapFailed : Text;
    #SwapSucceeded;
  };

  // DEPRECATED: SwapTxRecord - kept for stable variable backwards compatibility only
  // Kong now tracks failed swaps as claims - we use recoverKongswapClaims() instead
  public type SwapTxRecord = {
    txId : Nat;
    token0_ledger : Principal;
    token0_symbol : Text;
    token1_ledger : Principal;
    token1_symbol : Text;
    amount : Nat;
    minAmountOut : Nat;
    lastAttempt : Int;
    attempts : Nat;
    status : SwapTxStatus;
  };

  // Kong Swap Claim Types (for recovering tokens from failed swaps)
  public type ClaimsReply = {
    claim_id : Nat64;
    status : Text;
    chain : Text;
    symbol : Text;
    canister_id : ?Text;
    amount : Nat;
    fee : Nat;
    to_address : Text;
    desc : Text;
    ts : Nat64;
  };

  public type ClaimReply = {
    claim_id : Nat64;
    status : Text;
    chain : Text;
    symbol : Text;
    canister_id : ?Text;
    amount : Nat;
    fee : Nat;
    to_address : Text;
    desc : Text;
    transfer_ids : [TransferIdReply];
    ts : Nat64;
  };

  public type ClaimsResult = { #Ok : [ClaimsReply]; #Err : Text };
  public type ClaimResult = { #Ok : ClaimReply; #Err : Text };
};
