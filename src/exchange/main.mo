// Different types of positions:
// 1. Public positions that consist of assets within the regular pools
//    - Created with `pub = true` in `addPosition` function
//    - Involve token pairs listed in `pool_canister` array
//    - Visible to all users and included in public orderbooks
//    - Stored in `tradeStorePublic` map
//
// 2. Public positions that consist of assets within foreign pools
//    - Created with `pub = true` in `addPosition` function
//    - Involve token pairs not listed in `pool_canister` array
//    - Tracked in `foreignPools` map
//    - Public but do not have orderBooks
//    - Visible to users but using an interface that allows alot of filtering
//    - Are encouraged to include OC handle so other actors can discuss with them about the trade specifics
//
// 3. Private positions
//    - Created with `pub = false` in `addPosition` function
//    - Not visible in public orderbooks
//    - Stored in `tradeStorePrivate` map
//    - Accessible only with specific access code
//    - DAO can interact unless specifically excluded (see 4)
//    - Can be seen as OTC trades. Made for actors that want to trade larger amounts, without scaring the market
//
// 4. Private positions that exclude the DAO from accessing them
//    - Created with `pub = false` and `excludeDAO = true` in `addPosition`
//    - Stored in `tradeStorePrivate` map
//    - Access code has additional "excl" suffix
//    - DAO prevented from interacting or viewing
//    - Highest level of privacy (not accessed by DAO), typically for OTC trades
//
// 5. AMM Liquidity Positions
//    - Created using `addLiquidity` function
//    - In userLiquidityPositions the user's share in an AMM liquidity pool are stored
//    - Users earn fees from AMM trades proportional to their share of the pool
//
// These position types offer varying levels of visibility and accessibility,
// catering to different trading needs and privacy requirements.
//
//
//
//
// Different ways positions can be made, fulfilled or cancelled:
//
// 1. Creating Positions:
//    - Public positions: Created using `addPosition` with `pub = true` or `addPositionDAO`, of which the latter is called by DAO functions when
//      the exchanges liquidity is not enough to fulfill the DAO's needs
//    - Private positions: Created using `addPosition` with `pub = false`
//    - DAO-excluded private positions: Created with `pub = false` and `excludeDAO = true`
//    - AMM Liquidity positions: Created using `addLiquidity` function
//    - All positions are created by sending funds first, then calling the respective function
//    - Functions check for received funds and create the position if valid
//
// 2. Fulfilling Positions:
//    a. For Public Positions:
//       - Automatic fulfillment through `orderPairing` when a matching order is found
//       - Automatic fulfillment through AMM if better price is available
//       - Manual fulfillment using `FinishSell` function for specific trades
//       - Batch fulfillment using `FinishSellBatch` for multiple trades at once
//    b. For Private Positions:
//       - Manual fulfillment only, using `FinishSell` function
//       - Requires knowledge of the specific access code, expeciallyy when excluded from the DAO
//    c. DAO Operations:
//       - `FinishSellBatchDAO` for bulk operations by the DAO
//       - Can interact with both public and non-excluded private positions
//    d. AMM Swaps:
//       - Automatic execution through `swapWithAMM` function
//       - Integrated with orderbook for best execution price
//
// 3. Cancelling Positions:
//    - Users can cancel their own positions using `revokeTrade` with type #Initiator
//    - Sellers can cancel accepted trades using `revokeTrade` with type #Seller
//    - DAO can cancel multiple trades using `revokeTrade` with type #DAO
//    - AMM liquidity can be removed using `removeLiquidity` function
//    - When the assets gets removed from the exchange (addAcceptedToken),
//      all positions assotiated with it get deleted
//    - Cancelled trades incur a revoke fee, which is a fraction of the full fee
//
// 4. Partial Fulfillment:
//    - All position types support partial fulfillment
//    - Remaining amounts are kept as active positions
//    - Fees are proportionally applied to the fulfilled part
//
// 5. Error Handling and Recovery:
//    - `FixStuckTX` function to recover from interrupted transactions
//    - Automatic retry mechanisms for failed DAO operations
//
// 6. Position Lifecycle:
//    - Created -> Active -> Partially Filled / Fully Filled / Cancelled
//    - Positions older than 30 days are automatically removed by `cleanupOldTrades`
//    - AMM positions remain active until liquidity is removed
//
// This flexible system allows for various trading strategies and caters to
// different user needs, from public orderbook trading to private OTC deals,
// while maintaining security and efficiency in trade execution.
//
//
//
//
//
// All types of trades can be fulfilled partly (or fully) and are bound to the
// same fees (revokeFees and normal Fees). RevokeFees are applied when a trade
// is canceled (when the variable is 10, it means 1/10th of the normal fees),
// while normal Fees are applied upon successful completion of a trade.
//
// Referrer System and Fees:
// - Referrer links are established only through the `addPosition` function
// - When a user adds a position for the first time, they can specify a referrer
// - The within the addPosition function some logic is added that verifies and sets the referrer link (userReferrerLink Map):
//   - If the user already has a referrer, no changes are made
//   - If no referrer is provided or the provided referrer is invalid, it's set to null
//   - If a valid referrer is provided, it's stored in the `userReferrerLink` map
// - Referrer information is stored in:
//   - `userReferrerLink`: Maps users to their referrers
//   - `referrerFeeMap`: Tracks accumulated fees for each referrer
//   - `lastFeeAdditionByTime`: Helps manage and trim old referrer data
// - When fees are collected (via `addFees`), a portion goes to the referrer if one exists
// - The referral fee percentage is stored in `ReferralFees` and can be changed by admins
// - Referrer fees are automatically calculated and distributed when trades are executed
// - Referrers can claim their accumulated fees using the `claimFeesReferrer` function
// - Old referrer fee data is periodically trimmed to maintain system efficiency.
//   If nothing has been claimed for 2 months or if no fees were added for the referrer in 2 months
//   the fees are deleted and added to the collected exchange fees
//
//
//
//
// AMM System and Fees:
// - AMM pools are created for token pairs and stored in `AMMpools` map
// - Users can add liquidity to pools using `addLiquidity` function
// - AMM swaps are executed through `swapWithAMM` function
// - Fees from AMM trades are split between liquidity providers and the TACO exchange (70% of fees to liq providers, 30% to TACO)
// - Liquidity provider fees are accumulated in the `totalFees` variables
// - Users can remove liquidity and claim accumulated fees using `removeLiquidity` function
// - AMM is integrated with the orderbook system for best execution price
// - The system includes functions like `getAMMPoolInfo` and `getUserLiquidityDetailed` for querying AMM state
//
// Swap Functionality and getExpectedReceiveAmount:
// - The `getExpectedReceiveAmount` function provides essential information for token swaps:
//   - It takes parameters: tokenSell, tokenBuy, and amountSell
//   - Returns: expectedBuyAmount, fee, priceImpact, routeDescription, canFulfillFully, and potentialOrderDetails
//   - potentialOrderDetails: if not enough liquidity it also tells what klind or position will created if swap is done
// - This function can be used to create a user-friendly swap interface:
//   - Provides real-time estimates as users input swap amounts
//   - Displays expected receive amount, fees, and price impact
//   - Informs users about the execution route (AMM, Orderbook, or both)
//   - Handles partial fills by offering order creation for unfulfilled amounts
// - Slippage protection can be implemented using the function's output:
//   - User sets slippage tolerance (e.g., 1%)
//   - Calculate minimum acceptable amount: minAmount = expectedBuyAmount * (100% - slippage)
//   - Use minAmount when executing the swap to protect against price movements
//
// This hybrid system combines orderbook trading, AMM functionality, a referrer system, and user-friendly swap features,
// providing a comprehensive trading platform with various options for liquidity provision, trading strategies, and user incentives.

// Fixed RVVR-TACOX-7 by making nowVar set locally. In functions that have awaits this nowVar is a var, so its re-assigned after an await.

// RVVR-TACOX-2:
// DISCLAIMER: The "private" or "accesscode" features in this contract do not guarantee
// absolute confidentiality. Due to the current architecture of the Internet Computer,
// boundary nodes processing requests and node providers with access to node memory
// could potentially view or capture this information. Users should be aware that
// while efforts are made to protect this data, true secrecy cannot be guaranteed
// in the current IC environment.

// RVVR-TACOX-23:
// DISCLAIMER: This contract intentionally ignores subaccount information for simplicity
// and ease of use. This design decision means that all transactions are treated as
// coming from the main account of a principal, regardless of the subaccount used.
// Users should be aware of this limitation when interacting with the contract.
// ICRC tokens sent from an account with non-null subaccount are recoverable, however this
// is not the case for ICP transactions: these are not recoverable, maybe even not when
// contacting support.

// --compute-allocation 3

import Text "mo:base/Text";
import Iter "mo:base/Iter";
import Utils "./src/Utils";
import Cycles "mo:base/ExperimentalCycles";
import Principal "mo:base/Principal";
import PrincipalExt "./src/PrincipalExt";
import Error "mo:base/Error";
import Array "mo:base/Array";
import Option "mo:base/Option";
import Float "mo:base/Float";
import Buffer "mo:base/Buffer";
import Map "mo:map/Map";
import Prim "mo:prim";
import Int "mo:base/Int";
import Nat64 "mo:base/Nat64";
import Nat32 "mo:base/Nat32";
import Nat8 "mo:base/Nat8";
import Nat "mo:base/Nat";
import Random "mo:base/Random";
import ICRC1 "mo:icrc1/ICRC1";
import ICRC3 "mo:icrc3-mo/service";
import Ledger "src/Ledger";
import Fuzz "mo:fuzz";
import ICRC2 "./src/icrc.types";
import Debug "mo:base/Debug"; //
import Blob "mo:base/Blob";
import Vector "mo:vector";
import TrieSet "mo:base/TrieSet";
import Time = "mo:base/Time";
import treasuryType "./src/treasuryType";
import Logger "../helper/logger";
import AdminAuth "../helper/admin_authorization";
import { setTimer; cancelTimer; recurringTimer } = "mo:base/Timer";
//documentation: https://canscale.github.io/StableRBTree/StableRBTree.html
import RBTree "mo:stable-rbtree/StableRBTree";
import ExTypes "./exchangeTypes";
import LedgerType "mo:ledger-types";
import {
  getTimeFrameDetails;
  createEmptyKline;
  aggregateScanResult;
  alignTimestamp;
  compareTime;
  calculateKlineStats;
  mergeKlineData;
} "KLineHelperFunctions";
import {
  calculateFee;
  hashRatio;
  hashKlineKey;
  hashTextText;
  compareRatio;
  isLessThanRatio;
  sqrt;
  compareTextTime;
} "miscHelperFunctions";

// import Migration "./migration"; // migration already applied
shared (deployer) persistent actor class create_trading_canister() = this {
  stable var treasury_text = "qbnpl-laaaa-aaaan-q52aq-cai"; // Set via parameterManagement after deploy
  stable var treasury_principal = Principal.fromText(treasury_text);

  transient let treasury = actor (treasury_text) : treasuryType.Treasury;
  transient let logger = Logger.Logger();

  private func isAdmin(caller : Principal) : Bool {
    AdminAuth.isMasterAdmin(caller, func(_ : Principal) : Bool { false }) or caller == self or caller == deployer.caller;
  };

  stable var test = false;
  //to afat regaring notes: any reference of the testing will be deleted in production, so also this function
  public func setTest(a : Bool) : async () {
    test := a;
    let currentTreasury = actor (treasury_text) : treasuryType.Treasury;
    await currentTreasury.setTest(a);
  };

  // Test-only: wipe all exchange state (orders, pools, fees, V3) without transfers.
  // Call before re-running stress tests to avoid redeployment.
  public shared ({ caller }) func resetAllState() : async Text {
    if (not test) return "Not in test mode";
    if (not ownercheck(caller)) return "Not authorized";
    // Orders
    Map.clear(tradeStorePublic);
    Map.clear(tradeStorePrivate);
    for (k in Map.keys(liqMapSort)) { ignore Map.remove(liqMapSort, hashtt, k) };
    for (k in Map.keys(liqMapSortForeign)) { ignore Map.remove(liqMapSortForeign, hashtt, k) };
    for (k in Map.keys(privateAccessCodes)) { ignore Map.remove(privateAccessCodes, hashtt, k) };
    // AMM
    for (k in Map.keys(AMMpools)) { ignore Map.remove(AMMpools, hashtt, k) };
    for (k in Map.keys(poolV3Data)) { ignore Map.remove(poolV3Data, hashtt, k) };
    Map.clear(concentratedPositions);
    Map.clear(userLiquidityPositions);
    // Fees
    Map.clear(feescollectedDAO);
    Map.clear(referrerFeeMap);
    // Misc
    Vector.clear(tempTransferQueue);
    AMMMinimumLiquidityDone := TrieSet.empty();
    // Reset accepted tokens to base only
    acceptedTokens := ["ryjl3-tyaaa-aaaaa-aaaba-cai", "xevnm-gaaaa-aaaar-qafnq-cai"];
    "State reset complete";
  };
  type hashtt<K> = (
    getHash : (K) -> Nat32,
    areEqual : (K, K) -> Bool,
  );
  type Ratio = {
    #Max;
    #Zero;
    #Value : Nat;
  };
  type SwapHop = {
    tokenIn : Text;
    tokenOut : Text;
  };
  type SplitLeg = {
    amountIn : Nat;
    route : [SwapHop];
    minLegOut : Nat;
  };
  transient let {
    ihash;
    nhash;
    thash;
    bhash;
    phash;
    calcHash;
    hashText;
    n64hash;
    hashNat32;
    hashNat;
  } = Map;
  transient let {
    natToNat64;
    nat64ToNat;
    intToNat64Wrap;
    nat8ToNat;
    natToNat8;
    nat64ToInt64;
  } = Prim;

  // Module-level constants

  // according to RVVR-TACOX-5 and RVVR-TACOX-16
  transient let tenToPower256 : Nat = 1_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000;
  transient let tenToPower60 : Nat = 10 ** 60;
  transient let tenToPower120 : Nat = 10 ** 120;
  transient let tenToPower64 : Nat = 10 ** 64;
  transient let tenToPower30 : Nat = 10 ** 30;
  transient let tenToPower200 : Nat = 1_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000;
  transient let tenToPower80 : Nat = 1_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000;
  transient let twoToPower256 : Nat = 115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_936_937_788_164_706_601_208_502_937_451_870_474_002_309_074_206_031_068_203_496_252_451_749_399_651_431_429_809_190_659_250_937_221_696_461_515_709_858_386_744_464_207_952_318;
  transient let twoToPower70 : Nat = 1_180_591_620_717_411_303_424;
  transient let twoToPower256MinusOne : Nat = 115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_936_937_788_164_706_601_208_502_937_451_870_474_002_309_074_206_031_068_203_496_252_451_749_399_651_431_429_809_190_659_250_937_221_696_461_515_709_858_386_744_464_207_952_317;
  transient let tenToPower70 : Nat = 1_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000;
  transient let tenToPower20 : Nat = 100_000_000_000_000_000_000;

  

  type Time = Int;
  type PoolTrackingInfo = {
    var lastAggregationTime : Time;
    var hasTradedSinceLastAggregation : Bool;
  };

  type Pool_History = Map.Map<(Text, Text), RBTree.Tree<Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }]>>;

  type pool = {
    pool_canister : [(Text, Text)];
    asset_names : [(Text, Text)];
    asset_symbols : [(Text, Text)];
    asset_decimals : [(Nat8, Nat8)];
    asset_transferfees : [(Nat, Nat)];
    asset_minimum_amount : [(Nat, Nat)];
    last_traded_price : [Float];
    price_day_before : [Float];
    volume_24h : [Nat];
    amm_reserve0 : [Nat];
    amm_reserve1 : [Nat];
  };

  transient let hashtt = (hashTextText, func(a, b) = a == b) : hashtt<(Text, Text)>;
  transient let hashkl = (hashKlineKey, func(a, b) = a == b) : hashtt<KlineKey>;

  transient let rhash = (hashRatio, func(a, b) = a == b) : hashtt<Ratio>;

  //The fee per transaction (both for the initiator and the finaliser. Its in Basispoints so 1 represents 0.01%
  stable var ICPfee : Nat = 5;
  //RevokeFee represents 1/RevokeFee, so a 3 says that 1third of the total fee will be kept if trade is revoked
  stable var RevokeFeeNow : Nat = 5;
  //Referralfees. For instance 20 means 20% of the total fees go to the refferer
  stable var ReferralFees : Nat = 20;
  // Percentage of the AMM swap fee that goes to LPs. Protocol keeps (100 - this).
  // Must be referenced in BOTH swapWithAMMV3 (fee split) AND getPoolStats (reporting)
  // so the two never diverge.
  transient let LP_FEE_SHARE_PERCENT : Nat = 70;
  stable var verboseLogging : Bool = true;

  // Unified fee calculation helpers — use these everywhere to ensure consistent integer division.
  type BlockData = {
    #ICP : LedgerType.QueryBlocksResponse;
    #ICRC12 : [ICRC2.Transaction];
    #ICRC3 : ICRC3.GetBlocksResult;
  };

  type TradeEntry = {
    accesscode : Text;
    amount_sell : Nat;
    amount_init : Nat;
    token_sell_identifier : Text;
    token_init_identifier : Text;
    Fee : Nat;
    InitPrincipal : Text;
  };

  type TradePosition = {
    amount_sell : Nat;
    amount_init : Nat;
    token_sell_identifier : Text;
    token_init_identifier : Text;
    trade_number : Nat;
    Fee : Nat;
    trade_done : Nat;
    strictlyOTC : Bool;
    allOrNothing : Bool;
    OCname : Text;
    time : Int;
    filledInit : Nat;
    filledSell : Nat;
    initPrincipal : Text;
  };

  type TradePrivate = {
    amount_sell : Nat;
    amount_init : Nat;
    token_sell_identifier : Text;
    token_init_identifier : Text;
    trade_done : Nat;
    seller_paid : Nat;
    init_paid : Nat;
    trade_number : Nat;
    SellerPrincipal : Text;
    initPrincipal : Text;
    Fee : Nat;
    seller_paid2 : Nat;
    init_paid2 : Nat;
    RevokeFee : Nat;
    time : Int;
    OCname : Text;
    filledInit : Nat;
    filledSell : Nat;
    allOrNothing : Bool;
    strictlyOTC : Bool;
  };

  type TradePrivate2 = {
    amount_sell : Nat;
    amount_init : Nat;
    token_sell_identifier : Text;
    token_init_identifier : Text;
    trade_done : Nat;
    seller_paid : Nat;
    init_paid : Nat;
    trade_number : Nat;
    SellerPrincipal : Text;
    initPrincipal : Text;
    Fee : Nat;
    seller_paid2 : Nat;
    init_paid2 : Nat;
    RevokeFee : Nat;
    time : Int;
    OCname : Text;
    accesscode : Text;
    filledInit : Nat;
    filledSell : Nat;
    allOrNothing : Bool;
    strictlyOTC : Bool;
  };

  //RBtree that saves all current liquidity in a pool. RBtree as order is important for orders suuch as orderPairing, which tries to connect a new order to existing liquidity.
  type liqmapsort = RBTree.Tree<Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]>;

  // Map that saves all the liqmapsorts
  type BigLiqMapSort = Map.Map<(Text, Text), liqmapsort>;

  //KLines sent to frontend tgo make the graphs.
  type KlineData = {
    timestamp : Int;
    open : Float;
    high : Float;
    low : Float;
    close : Float;
    volume : Nat;
  };
  // Timerframes of the KLine chart data
  type TimeFrame = {
    #fivemin;
    #hour;
    #fourHours;
    #day;
    #week;
  };
  //token 1, token 2 , TimeFrame
  type KlineKey = (Text, Text, TimeFrame);

  type TransferRecipient = {
    #principal : Principal;
    #accountId : { owner : Principal; subaccount : ?Subaccount };
  };

  type Account = { owner : Principal; subaccount : ?Subaccount };
  type TransferArgs = {
    from_subaccount : ?Subaccount;
    to : Account;
    amount : Nat;
    fee : ?Nat;
    memo : ?Blob;
    created_at_time : ?Nat64;
  };
  type TransferError = {
    #BadFee : { expected_fee : Nat };
    #BadBurn : { min_burn_amount : Nat };
    #InsufficientFunds : { balance : Nat };
    #TooOld;
    #CreatedInFuture : { ledger_time : Nat64 };
    #Duplicate : { duplicate_of : Nat };
    #TemporarilyUnavailable;
    #GenericError : { error_code : Nat; message : Text };
  };
  type ICRC1Ledger = actor {
    icrc1_balance_of : (Account) -> async (Nat);
    icrc1_transfer : (TransferArgs) -> async ({
      #Ok : Nat;
      #Err : TransferError;
    });
  };

  transient let Faketrade : TradePrivate = {
    amount_sell = 0;
    amount_init = 0;
    token_sell_identifier = "0";
    token_init_identifier = "0";
    trade_done = 0;
    seller_paid = 0;
    init_paid = 0;
    trade_number = 0;
    SellerPrincipal = "0";
    initPrincipal = "0";
    Fee = ICPfee;
    seller_paid2 = 0;
    init_paid2 = 0;
    RevokeFee = 3;  // sentinel — must be non-zero to avoid div-by-zero if ever reached; callers skip via trade_number == 0

    time = 0;
    OCname = "";
    filledInit = 0;
    filledSell = 0;
    allOrNothing = false;
    strictlyOTC = false;
  };

  type AMMPool = {
    token0 : Text;
    token1 : Text;
    reserve0 : Nat;
    reserve1 : Nat;
    totalLiquidity : Nat;
    totalFee0 : Nat;
    totalFee1 : Nat;
    lastUpdateTime : Int;
    providers : TrieSet.Set<Principal>;
  };

  type LiquidityPosition = {
    token0 : Text;
    token1 : Text;
    liquidity : Nat;
    fee0 : Nat;
    fee1 : Nat;
    lastUpdateTime : Int;
  };

  stable let AMMpools = Map.new<(Text, Text), AMMPool>();
  // Map to store user's liquidity positions
  stable let userLiquidityPositions = Map.new<Principal, [LiquidityPosition]>();

  // ═══════════════════════════════════════════════════════════════
  // Concentrated Liquidity (V3) Types and State
  // ═══════════════════════════════════════════════════════════════

  type RangeData = {
    liquidityNet : Int;          // net liquidity change when crossing this price (+ for lower bounds, - for upper)
    liquidityGross : Nat;        // total liquidity referencing this price point
    feeGrowthOutside0 : Nat;     // fee growth on the other side of this tick
    feeGrowthOutside1 : Nat;
  };

  type PoolV3Data = {
    activeLiquidity : Nat;       // currently active liquidity (sum of in-range positions)
    currentSqrtRatio : Nat;      // sqrt(price) scaled by tenToPower60
    feeGrowthGlobal0 : Nat;      // cumulative fee0 per unit of liquidity (scaled by tenToPower60)
    feeGrowthGlobal1 : Nat;      // cumulative fee1 per unit of liquidity (scaled by tenToPower60)
    totalFeesCollected0 : Nat;   // actual token0 fees collected (prevents negative drift)
    totalFeesCollected1 : Nat;
    totalFeesClaimed0 : Nat;     // token0 fees already claimed by LPs
    totalFeesClaimed1 : Nat;
    ranges : RBTree.Tree<Nat, RangeData>; // keyed by ratio value (Nat, price always positive)
  };

  type ConcentratedPosition = {
    positionId : Nat;
    token0 : Text;
    token1 : Text;
    liquidity : Nat;             // virtual liquidity within this range
    ratioLower : Nat;            // lower price bound (ratio scaled by tenToPower60)
    ratioUpper : Nat;            // upper price bound (ratio scaled by tenToPower60)
    lastFeeGrowth0 : Nat;        // snapshot of feeGrowthGlobal0 at last update
    lastFeeGrowth1 : Nat;
    lastUpdateTime : Int;
  };

  // 0.1% tick spacing (10 basis points)
  transient let TICK_SPACING_BPS : Nat = 10;

  // Snap any ratio to nearest 0.1% tick boundary
  func snapToTick(ratio : Nat) : Nat {
    let tickSize = ratio * TICK_SPACING_BPS / 10000;
    if (tickSize == 0) return ratio;
    (ratio / tickSize) * tickSize;
  };

  // Full-range boundaries (used for V2-compatible positions)
  transient let FULL_RANGE_LOWER : Nat = tenToPower20; // sqrtRatio for very low price (~10^-40 in raw terms)
  transient let FULL_RANGE_UPPER : Nat = tenToPower120;

  // V3 stable state
  stable var nextPositionId : Nat = 0;
  stable let poolV3Data = Map.new<(Text, Text), PoolV3Data>();
  stable let concentratedPositions = Map.new<Principal, [ConcentratedPosition]>();
  stable var v3Migrated = false;
  stable var v3MigratedV2 = false;
  stable var v3MigratedV3 = false;
  // Keep these for stable compat (were added by earlier migrations, can't drop)
  stable var v3LiquidityFixed = true;
  stable var predrainCleanupDone = true;
  // Legacy: kept for stable compat so upgrade doesn't need an explicit migration.
  stable var refundStuckFundsCalled = true;

  stable var feeGrowthInsideMigrated = false;

  // ── V3 Math Helpers ──

  // sqrt(ratio * tenToPower60) → result scaled by tenToPower60
  func ratioToSqrtRatio(ratio : Nat) : Nat {
    if (ratio == 0) return 0;
    sqrt(ratio * tenToPower60);
  };

  // Compute liquidity from token amounts and price range (overflow-safe)
  func liquidityFromAmounts(amount0 : Nat, amount1 : Nat, sqrtLower : Nat, sqrtUpper : Nat, sqrtCurrent : Nat) : Nat {
    if (sqrtLower >= sqrtUpper or sqrtLower == 0) return 0;
    if (sqrtCurrent <= sqrtLower) {
      // Below range: all token0. L = amount0 * sqrtLower * sqrtUpper / (SCALE * (sqrtUpper - sqrtLower))
      let denom = safeSub(sqrtUpper, sqrtLower);
      if (denom == 0) return 0;
      mulDiv(mulDiv(amount0, sqrtLower, tenToPower60), sqrtUpper, denom);
    } else if (sqrtCurrent >= sqrtUpper) {
      // Above range: all token1. L = amount1 * SCALE / (sqrtUpper - sqrtLower)
      let denom = safeSub(sqrtUpper, sqrtLower);
      if (denom == 0) return 0;
      mulDiv(amount1, tenToPower60, denom);
    } else {
      // In range: min of both
      let denomUpper = safeSub(sqrtUpper, sqrtCurrent);
      let L0 = if (denomUpper > 0) {
        mulDiv(mulDiv(amount0, sqrtCurrent, tenToPower60), sqrtUpper, denomUpper);
      } else { 0 };
      let denomLower = safeSub(sqrtCurrent, sqrtLower);
      let L1 = if (denomLower > 0) {
        mulDiv(amount1, tenToPower60, denomLower);
      } else { 0 };
      if (L0 == 0) L1 else if (L1 == 0) L0 else Nat.min(L0, L1);
    };
  };

  // Compute token amounts from liquidity and price range (overflow-safe)
  func amountsFromLiquidity(liquidity : Nat, sqrtLower : Nat, sqrtUpper : Nat, sqrtCurrent : Nat) : (Nat, Nat) {
    if (sqrtLower >= sqrtUpper or liquidity == 0) return (0, 0);
    if (sqrtCurrent <= sqrtLower) {
      // Below range: all token0
      // amount0 = L * SCALE / sqrtLower - L * SCALE / sqrtUpper
      let t1 = mulDiv(liquidity, tenToPower60, sqrtLower);
      let t2 = mulDiv(liquidity, tenToPower60, sqrtUpper);
      (safeSub(t1, t2), 0);
    } else if (sqrtCurrent >= sqrtUpper) {
      // Above range: all token1
      // amount1 = L * (sqrtUpper - sqrtLower) / SCALE
      let delta = safeSub(sqrtUpper, sqrtLower);
      (0, mulDiv(liquidity, delta, tenToPower60));
    } else {
      // In range: both tokens
      let t1 = mulDiv(liquidity, tenToPower60, sqrtCurrent);
      let t2 = mulDiv(liquidity, tenToPower60, sqrtUpper);
      let amount0 = safeSub(t1, t2);
      let amount1 = mulDiv(liquidity, safeSub(sqrtCurrent, sqrtLower), tenToPower60);
      (amount0, amount1);
    };
  };

  // Overflow-safe a*b/c: divides the larger factor first to keep intermediates under 2^256
  func mulDiv(a : Nat, b : Nat, c : Nat) : Nat {
    if (c == 0 or a == 0 or b == 0) return 0;
    let (big, small) = if (a >= b) { (a, b) } else { (b, a) };
    if (big >= c) {
      (big / c) * small + mulDiv(big % c, small, c);
    } else {
      big * small / c;
    };
  };

  // Safe Nat subtraction: floors at 0
  func safeSub(a : Nat, b : Nat) : Nat { if (a >= b) { a - b } else { 0 } };

  // Uniswap V3 tick-initialization convention for feeGrowthOutside:
  // When a tick is first created/referenced, feeGrowthOutside captures the pool's
  // current global fee growth IF the tick is "below" the current price (currentSqrtRatio
  // >= tickSqrtRatio), else 0. On subsequent tick crossings, the value is flipped
  // (feeGrowthOutside := feeGrowthGlobal - feeGrowthOutside). This lets us compute
  // feeGrowthInside for any range without iterating over all historical swaps.
  func initialFeeGrowthOutside(tickSqrtRatio : Nat, currentSqrtRatio : Nat, feeGrowthGlobal0 : Nat, feeGrowthGlobal1 : Nat) : (Nat, Nat) {
    if (currentSqrtRatio >= tickSqrtRatio) {
      (feeGrowthGlobal0, feeGrowthGlobal1)
    } else {
      (0, 0)
    };
  };

  // Uniswap V3 feeGrowthInside: per-unit-liquidity fee growth accumulated strictly
  // while the pool's current price was inside [tickLower, tickUpper]. This is what
  // concentrated LP claim math should use, NOT raw feeGrowthGlobal.
  //
  // Formula:
  //   feeBelow(tickLower) = currentSqrtRatio >= tickLower
  //     ? feeGrowthOutside(tickLower)
  //     : feeGrowthGlobal - feeGrowthOutside(tickLower)
  //   feeAbove(tickUpper) = currentSqrtRatio < tickUpper
  //     ? feeGrowthOutside(tickUpper)
  //     : feeGrowthGlobal - feeGrowthOutside(tickUpper)
  //   feeGrowthInside = feeGrowthGlobal - feeBelow - feeAbove
  //
  // Short-circuit for full-range positions: inside == global by construction.
  // This keeps full-range mainnet positions unaffected when the tick values are
  // correctly initialized (which Phase A migration ensures).
  func feeGrowthInside(
    tickLower : Nat,
    tickUpper : Nat,
    currentSqrtRatio : Nat,
    v3 : PoolV3Data,
  ) : (Nat, Nat) {
    // Short-circuit for full-range positions
    if (tickLower == FULL_RANGE_LOWER and tickUpper == FULL_RANGE_UPPER) {
      // After correct tick init + flipping, inside = global for full range.
      // But as a belt-and-suspenders short-circuit, just return global directly.
      return (v3.feeGrowthGlobal0, v3.feeGrowthGlobal1);
    };

    let (lowerOutside0, lowerOutside1) = switch (RBTree.get(v3.ranges, Nat.compare, tickLower)) {
      case (?r) { (r.feeGrowthOutside0, r.feeGrowthOutside1) };
      case null { initialFeeGrowthOutside(tickLower, currentSqrtRatio, v3.feeGrowthGlobal0, v3.feeGrowthGlobal1) };
    };
    let (upperOutside0, upperOutside1) = switch (RBTree.get(v3.ranges, Nat.compare, tickUpper)) {
      case (?r) { (r.feeGrowthOutside0, r.feeGrowthOutside1) };
      case null { initialFeeGrowthOutside(tickUpper, currentSqrtRatio, v3.feeGrowthGlobal0, v3.feeGrowthGlobal1) };
    };

    let (feeBelow0, feeBelow1) = if (currentSqrtRatio >= tickLower) {
      (lowerOutside0, lowerOutside1)
    } else {
      (safeSub(v3.feeGrowthGlobal0, lowerOutside0), safeSub(v3.feeGrowthGlobal1, lowerOutside1))
    };
    let (feeAbove0, feeAbove1) = if (currentSqrtRatio < tickUpper) {
      (upperOutside0, upperOutside1)
    } else {
      (safeSub(v3.feeGrowthGlobal0, upperOutside0), safeSub(v3.feeGrowthGlobal1, upperOutside1))
    };

    (
      safeSub(safeSub(v3.feeGrowthGlobal0, feeBelow0), feeAbove0),
      safeSub(safeSub(v3.feeGrowthGlobal1, feeBelow1), feeAbove1),
    );
  };

  // Position-level wrapper for feeGrowthInside: handles the RBTree key convention
  // (full-range positions store FULL_RANGE_LOWER/UPPER directly as ratio AND tree key;
  // concentrated positions store ratio values and the tree is keyed by ratioToSqrtRatio).
  //
  // IMPORTANT — migration dependency: the full-range short-circuit returns feeGrowthGlobal
  // directly. The feeGrowthInside hard-reset migration (stable var feeGrowthInsideMigrated)
  // snapshots lastFeeGrowth = positionFeeGrowthInside(pos, v3_post_migration). For full-range
  // positions that snapshot is feeGrowthGlobal_atMigration (via this short-circuit), NOT the
  // 0 that the full formula would give after tick re-init. Do NOT remove the short-circuit
  // without also re-snapshotting all full-range positions' lastFeeGrowth consistently.
  func positionFeeGrowthInside(pos : ConcentratedPosition, v3 : PoolV3Data) : (Nat, Nat) {
    if (pos.ratioLower == FULL_RANGE_LOWER and pos.ratioUpper == FULL_RANGE_UPPER) {
      // Full-range: inside == global (the full-range-short-circuit of Uniswap math)
      (v3.feeGrowthGlobal0, v3.feeGrowthGlobal1);
    } else {
      feeGrowthInside(
        ratioToSqrtRatio(pos.ratioLower),
        ratioToSqrtRatio(pos.ratioUpper),
        v3.currentSqrtRatio,
        v3,
      );
    };
  };

  // Sync AMMPool from V3 data: totalLiquidity = v3.activeLiquidity, fees always 0
  func syncPoolFromV3(poolKey : (Text, Text)) {
    switch (Map.get(poolV3Data, hashtt, poolKey), Map.get(AMMpools, hashtt, poolKey)) {
      case (?v3, ?pool) {
        Map.set(AMMpools, hashtt, poolKey, { pool with totalLiquidity = v3.activeLiquidity; totalFee0 = 0; totalFee1 = 0 });
      };
      case _ {};
    };
  };

  // Derive sqrtRatio from reserves, handling one-sided pools correctly.
  // For a drained pool (reserve1 = 0): price floor (currentSqrtRatio = 1).
  // For a drained pool (reserve0 = 0): price ceiling (currentSqrtRatio = tenToPower120).
  // For fully empty pool: keep current value (pool will be deleted elsewhere).
  // This lets the V3 state self-heal after clamp events without leaving stale currentSqrtRatio.
  func getPoolPriceV3(poolKey : (Text, Text)) : (Float, Float) {
    let dec0 = switch (Map.get(tokenInfo, thash, poolKey.0)) { case (?i) { i.Decimals }; case null { 8 } };
    let dec1 = switch (Map.get(tokenInfo, thash, poolKey.1)) { case (?i) { i.Decimals }; case null { 8 } };
    switch (Map.get(poolV3Data, hashtt, poolKey), Map.get(AMMpools, hashtt, poolKey)) {
      case (?v3, ?pool) {
        if (v3.currentSqrtRatio > 0) {
          let ratioScaled = (v3.currentSqrtRatio * v3.currentSqrtRatio) / tenToPower60;
          let price0 = (Float.fromInt(ratioScaled) * Float.fromInt(10 ** dec0)) / (Float.fromInt(tenToPower60) * Float.fromInt(10 ** dec1));
          let price1 = if (price0 > 0.0) { 1.0 / price0 } else { 0.0 };
          (price0, price1);
        } else if (pool.reserve0 > 0 and pool.reserve1 > 0) {
          let p0 = (Float.fromInt(pool.reserve1) * Float.fromInt(10 ** dec0)) / (Float.fromInt(pool.reserve0) * Float.fromInt(10 ** dec1));
          let p1 = if (p0 > 0.0) { 1.0 / p0 } else { 0.0 };
          (p0, p1);
        } else { (0.0, 0.0) };
      };
      case (_, ?pool) {
        if (pool.reserve0 > 0 and pool.reserve1 > 0) {
          let p0 = (Float.fromInt(pool.reserve1) * Float.fromInt(10 ** dec0)) / (Float.fromInt(pool.reserve0) * Float.fromInt(10 ** dec1));
          let p1 = if (p0 > 0.0) { 1.0 / p0 } else { 0.0 };
          (p0, p1);
        } else { (0.0, 0.0) };
      };
      case _ { (0.0, 0.0) };
    };
  };

  func getPoolRatioV3(poolKey : (Text, Text)) : Nat {
    switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case (?v3) {
        if (v3.currentSqrtRatio > 0) {
          (v3.currentSqrtRatio * v3.currentSqrtRatio) / tenToPower60;
        } else { 0 };
      };
      case null { 0 };
    };
  };

  // V3-aware overall price impact: |1 − (actualOutput / spotOutputForFullRequest)|.
  // Compares the actual quote against what the user *would* receive at zero slippage for
  // the FULL requested amount — so partial fills (e.g. 5000 ckUSDC into a 50-deep pool
  // returning only ~50 ckUSDT) honestly report ~99% impact, instead of hiding the
  // missing 4950 behind a per-filled-token weighted average.
  // CRITICAL: caller must pass the PRE-SWAP V3 ratio (snapshot before orderPairing).
  // orderPairing mutates v3.currentSqrtRatio during its tick walk; reading getPoolRatioV3
  // here would return the post-swap ratio, dividing by the (much smaller for outsized
  // swaps) post-swap spot and producing impact > 1 (= 100%+) — giving readings like
  // 59,482% for a 999 ICP swap that should be ~68%.
  // IMPORTANT: spotOut is computed in Nat arithmetic before converting to Float, because
  // Float (f64) only represents integers up to 2^53 exactly — multiplying 1e60-scale
  // canonical ratios in Float-space silently loses ~7 orders of magnitude of precision.
  func computeBlendedAmmImpact(
    poolKey : (Text, Text),
    tokenSell : Text,
    expectedBuyAmount : Nat,
    amountSell : Nat,
    preSwapV3Ratio : Nat,           // pre-swap snapshot of getPoolRatioV3(poolKey); 0 if absent
    preSwapV2 : ?(Nat, Nat),        // (reserveIn, reserveOut) in tokenSell→tokenBuy direction
  ) : Float {
    if (amountSell == 0 or expectedBuyAmount == 0) return 0.0;
    let spotOutNat : Nat = if (preSwapV3Ratio > 0) {
      if (tokenSell == poolKey.0) {
        // tokenSell is token0 → output is token1; v3Ratio = (raw_reserve1 × 1e60) / raw_reserve0
        preSwapV3Ratio * amountSell / tenToPower60;
      } else {
        // tokenSell is token1 → output is token0; spot rate is reciprocal
        tenToPower60 * amountSell / preSwapV3Ratio;
      };
    } else {
      switch (preSwapV2) {
        case (?(rIn, rOut)) {
          if (rIn > 0) { rOut * amountSell / rIn } else { 0 };
        };
        case null { 0 };
      };
    };
    if (spotOutNat == 0) return 0.0;
    let actual = Float.fromInt(expectedBuyAmount);
    let spot = Float.fromInt(spotOutNat);
    if (spot <= 0.0) return 0.0;
    Float.abs(1.0 - actual / spot);
  };

  func sqrtRatioFromReserves(reserve0 : Nat, reserve1 : Nat, fallback : Nat) : Nat {
    if (reserve0 > 0 and reserve1 > 0) {
      ratioToSqrtRatio((reserve1 * tenToPower60) / reserve0);
    } else if (reserve0 > 0 and reserve1 == 0) {
      1;  // Price floor — pool drained of token1
    } else if (reserve0 == 0 and reserve1 > 0) {
      tenToPower120;  // Price ceiling — pool drained of token0
    } else {
      fallback;
    };
  };

  // Recalculate currentSqrtRatio from reserves + activeLiquidity from tick tree.
  // Reserves are ground truth — orderbook fills and LP operations change reserves
  // without updating V3 sqrtRatio, causing drift. This corrects both.
  // Pure variant of recalculateActiveLiquidity: recomputes sqrtRatio + activeLiquidity
  // from the given pool's reserves without touching Maps. Safe to use in simulation
  // chains (simulateSwap) where writes to AMMpools / poolV3Data would corrupt real state.
  // Fixes the stale-sqrtRatio problem that caused swapV3 clamps during route ranking.
  // when booking an AMM protocol fee to feescollectedDAO, we must also
  // increment v3.totalFeesClaimed by the same amount. Otherwise v3.totalFeesCollected -
  // totalFeesClaimed (= the "outstanding fees in pool" that checkDiffs counts into ammbalance)
  // still includes the protocol portion — double-counting it with feescollectedDAO.
  // After this sync: residual = totalCollected - totalClaimed represents ONLY the LP portion
  // still owed to LPs. The 30% DAO portion is cleanly routed to feescollectedDAO.
  func claimProtocolFeeInV3(tokenIn : Text, tokenOut : Text, protocolFeeAmt : Nat) {
    if (protocolFeeAmt == 0) return;
    let poolKey = getPool(tokenIn, tokenOut);
    switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case null {};
      case (?v3) {
        let isTokenIn0 = (tokenIn == poolKey.0);
        let updatedV3 = if (isTokenIn0) {
          { v3 with totalFeesClaimed0 = v3.totalFeesClaimed0 + protocolFeeAmt }
        } else {
          { v3 with totalFeesClaimed1 = v3.totalFeesClaimed1 + protocolFeeAmt }
        };
        Map.set(poolV3Data, hashtt, poolKey, updatedV3);
      };
    };
  };

  func recalcV3Pure(pool : AMMPool, v3 : PoolV3Data) : (AMMPool, PoolV3Data) {
    let sqrtCurrent = if (v3.currentSqrtRatio > 0) { v3.currentSqrtRatio } else {
      sqrtRatioFromReserves(pool.reserve0, pool.reserve1, 0)
    };
    var active : Int = 0;
    for ((tick, data) in RBTree.entries(v3.ranges)) {
      if (tick <= sqrtCurrent) { active += data.liquidityNet };
    };
    let activeLiq = if (active > 0) { Int.abs(active) } else { 0 };
    (
      { pool with totalLiquidity = activeLiq; totalFee0 = 0; totalFee1 = 0 },
      { v3 with activeLiquidity = activeLiq; currentSqrtRatio = sqrtCurrent }
    );
  };

  func recalculateActiveLiquidity(poolKey : (Text, Text)) {
    switch (Map.get(poolV3Data, hashtt, poolKey), Map.get(AMMpools, hashtt, poolKey)) {
      case (?v3, ?pool) {
        let sqrtCurrent = if (v3.currentSqrtRatio > 0) { v3.currentSqrtRatio } else {
          sqrtRatioFromReserves(pool.reserve0, pool.reserve1, 0)
        };

        var active : Int = 0;
        for ((tick, data) in RBTree.entries(v3.ranges)) {
          if (tick <= sqrtCurrent) {
            active += data.liquidityNet;
          };
        };
        let activeLiq = if (active > 0) { Int.abs(active) } else { 0 };

        Map.set(poolV3Data, hashtt, poolKey, {
          v3 with
          activeLiquidity = activeLiq;
          currentSqrtRatio = sqrtCurrent;
        });
        syncPoolFromV3(poolKey);
      };
      case _ {};
    };
  };

  // Recalculate activeLiquidity for all V3 pools
  // Uses pool_canister vector (not Map.entries) to avoid hash iteration issues
  func recalculateAllActiveLiquidity() {
    for (poolKey in Vector.vals(pool_canister)) {
      recalculateActiveLiquidity(poolKey);
    };
  };

  // Overflow-safe a*b/c rounded UP (for amountIn — user pays slightly more, pool accumulates dust)
  func mulDivUp(a : Nat, b : Nat, c : Nat) : Nat {
    if (c == 0 or a == 0 or b == 0) return 0;
    mulDiv(a, b, c) + 1;
  };

  // Compute swap step within a single liquidity range (overflow-safe)
  // Returns: (amountIn consumed, amountOut produced, new sqrtRatio)
  func computeSwapStep(sqrtRatioCurrent : Nat, sqrtRatioTarget : Nat, liquidity : Nat, amountRemaining : Nat, zeroForOne : Bool) : (Nat, Nat, Nat) {
    if (liquidity == 0) return (0, 0, sqrtRatioCurrent);

    if (zeroForOne) {
      // Selling token0, buying token1: price decreases
      // Δx = L * SCALE / sqrtTarget - L * SCALE / sqrtCurrent (overflow-safe, round UP for inputs)
      let maxAmountIn = if (sqrtRatioCurrent > sqrtRatioTarget and sqrtRatioTarget > 0) {
        let term1 = mulDivUp(liquidity, tenToPower60, sqrtRatioTarget);
        let term2 = mulDiv(liquidity, tenToPower60, sqrtRatioCurrent);
        safeSub(term1, term2);
      } else { 0 };

      if (maxAmountIn == 0) return (0, 0, sqrtRatioCurrent);

      let (actualIn, newSqrt) = if (amountRemaining >= maxAmountIn) {
        (maxAmountIn, sqrtRatioTarget);
      } else {
        // newSqrt = sqrtCurrent * L / (L + amountIn * sqrtCurrent / SCALE)
        let addend = mulDiv(amountRemaining, sqrtRatioCurrent, tenToPower60);
        let denominator = liquidity + addend;
        if (denominator == 0) return (0, 0, sqrtRatioCurrent);
        let newSqrt2 = mulDiv(sqrtRatioCurrent, liquidity, denominator);
        (amountRemaining, Nat.max(newSqrt2, sqrtRatioTarget));
      };

      // Δy = L * (sqrtOld - sqrtNew) / SCALE
      let amountOut = mulDiv(liquidity, safeSub(sqrtRatioCurrent, newSqrt), tenToPower60);
      (actualIn, amountOut, newSqrt);
    } else {
      // Selling token1, buying token0: price increases
      // Δy_in = L * (sqrtTarget - sqrtCurrent) / SCALE (round UP for inputs)
      let maxAmountIn = if (sqrtRatioTarget > sqrtRatioCurrent) {
        mulDivUp(liquidity, safeSub(sqrtRatioTarget, sqrtRatioCurrent), tenToPower60);
      } else { 0 };

      if (maxAmountIn == 0) return (0, 0, sqrtRatioCurrent);

      let (actualIn, newSqrt) = if (amountRemaining >= maxAmountIn) {
        (maxAmountIn, sqrtRatioTarget);
      } else {
        // newSqrt = sqrtCurrent + amountIn * SCALE / L
        let sqrtDelta = mulDiv(amountRemaining, tenToPower60, liquidity);
        let newSqrt2 = sqrtRatioCurrent + sqrtDelta;
        (amountRemaining, Nat.min(newSqrt2, sqrtRatioTarget));
      };

      // Δx_out = L * SCALE / sqrtOld - L * SCALE / sqrtNew
      let amountOut = if (newSqrt > 0 and sqrtRatioCurrent > 0) {
        let outTerm1 = mulDiv(liquidity, tenToPower60, sqrtRatioCurrent);
        let outTerm2 = mulDiv(liquidity, tenToPower60, newSqrt);
        safeSub(outTerm1, outTerm2);
      } else { 0 };
      (actualIn, amountOut, newSqrt);
    };
  };

  // Find next initialized tick boundary in the given direction
  func findNextRange(ranges : RBTree.Tree<Nat, RangeData>, currentSqrtRatio : Nat, ascending : Bool) : ?Nat {
    if (ascending) {
      // Find smallest tick > currentSqrtRatio
      let scan = RBTree.scanLimit(ranges, Nat.compare, currentSqrtRatio + 1, tenToPower120, #fwd, 1);
      if (scan.results.size() > 0) { ?scan.results[0].0 } else { null };
    } else {
      // Find largest tick < currentSqrtRatio
      let scan = RBTree.scanLimit(ranges, Nat.compare, 0, currentSqrtRatio, #bwd, 1);
      if (scan.results.size() > 0 and scan.results[0].0 < currentSqrtRatio) { ?scan.results[0].0 } else { null };
    };
  };

  // Concentrated swap engine: iterates through tick ranges
  func swapWithAMMV3(
    pool : AMMPool, v3 : PoolV3Data, tokenInIsToken0 : Bool, amountIn : Nat, fee : Nat,
    callerCtx : Text, sqrtPriceLimit : ?Nat
  ) : (Nat, Nat, Nat, Nat, AMMPool, PoolV3Data) {
    // Returns: (totalAmountIn, totalAmountOut, protocolFee, poolFee, updatedPool, updatedV3)
    // sqrtPriceLimit (optional): stop the swap once V3 currentSqrtRatio reaches this bound.
    //   - tokenInIsToken0  → price decreasing → limit is a floor (target = max(rawTarget, limit))
    //   - !tokenInIsToken0 → price increasing → limit is a ceiling (target = min(rawTarget, limit))
    //   `null` preserves the legacy budget-driven behaviour (no price clamp).
    var amountRemaining = amountIn;
    var totalAmountOut : Nat = 0;
    var totalPoolFee : Nat = 0;
    var totalProtocolFee : Nat = 0;
    var currentSqrtRatio = v3.currentSqrtRatio;
    var currentLiquidity = v3.activeLiquidity;
    var feeGrowth0 = v3.feeGrowthGlobal0;
    var feeGrowth1 = v3.feeGrowthGlobal1;
    var feesCollected0 = v3.totalFeesCollected0;
    var feesCollected1 = v3.totalFeesCollected1;
    var updatedRanges = v3.ranges;
    var iterations = 0;
    let maxIterations = 2000; // safety cap

    label swapLoop while (amountRemaining > 0 and currentLiquidity > 0 and iterations < maxIterations) {
      iterations += 1;

      // Find next tick boundary
      let nextBoundary = findNextRange(updatedRanges, currentSqrtRatio, not tokenInIsToken0);
      // zeroForOne (token0 in) = price decreasing = scan backward
      // oneForZero (token1 in) = price increasing = scan forward

      let rawTarget = switch (nextBoundary) {
        case null {
          // No more ticks: swap within remaining liquidity until exhausted
          if (tokenInIsToken0) { 1 } else { tenToPower120 };
        };
        case (?t) { t };
      };
      let target = switch (sqrtPriceLimit) {
        case null { rawTarget };
        case (?lim) {
          if (tokenInIsToken0) { Nat.max(rawTarget, lim) }  // price decreasing — stop at the higher of the two
          else { Nat.min(rawTarget, lim) };                  // price increasing — stop at the lower
        };
      };

      // Compute swap step
      let stepFeeRate = fee; // basis points
      let amountBeforeFee = amountRemaining;
      let stepFee = (amountBeforeFee * stepFeeRate * LP_FEE_SHARE_PERCENT) / (100 * 10000);
      let stepProtocolFee = (amountBeforeFee * stepFeeRate * (100 - LP_FEE_SHARE_PERCENT)) / (100 * 10000);
      let amountAfterFee = if (amountBeforeFee > stepFee + stepProtocolFee) {
        amountBeforeFee - stepFee - stepProtocolFee;
      } else { 0 };

      let (stepIn, stepOut, newSqrt) = computeSwapStep(currentSqrtRatio, target, currentLiquidity, amountAfterFee, tokenInIsToken0);

      if (stepIn == 0 and stepOut == 0) {
        break swapLoop;
      };

      // Actual fee is proportional to stepIn consumed
      let actualFee = if (amountAfterFee > 0) { stepFee * stepIn / amountAfterFee } else { 0 };
      let actualProtocolFee = if (amountAfterFee > 0) { stepProtocolFee * stepIn / amountAfterFee } else { 0 };

      totalPoolFee += actualFee;
      totalProtocolFee += actualProtocolFee;

      // Update fee growth (overflow-safe)
      if (currentLiquidity > 0) {
        if (tokenInIsToken0) {
          let growth = mulDiv(actualFee, tenToPower60, currentLiquidity);
          feeGrowth0 += growth;
          feesCollected0 += actualFee + actualProtocolFee;
        } else {
          let growth = mulDiv(actualFee, tenToPower60, currentLiquidity);
          feeGrowth1 += growth;
          feesCollected1 += actualFee + actualProtocolFee;
        };
      };

      let totalDeducted = stepIn + actualFee + actualProtocolFee;
      amountRemaining := if (amountRemaining > totalDeducted) { amountRemaining - totalDeducted } else { 0 };
      totalAmountOut += stepOut;
      currentSqrtRatio := newSqrt;

      // Cross tick boundary if reached
      if (nextBoundary != null and newSqrt == target) {
        switch (RBTree.get(updatedRanges, Nat.compare, target)) {
          case (?rangeData) {
            if (tokenInIsToken0) {
              // Price decreasing: subtract liquidityNet (crossing from right to left)
              let netChange = rangeData.liquidityNet;
              if (netChange >= 0) {
                currentLiquidity := if (currentLiquidity >= Int.abs(netChange)) { currentLiquidity - Int.abs(netChange) } else { 0 };
              } else {
                currentLiquidity += Int.abs(netChange);
              };
            } else {
              // Price increasing: add liquidityNet (crossing from left to right)
              let netChange = rangeData.liquidityNet;
              if (netChange >= 0) {
                currentLiquidity += Int.abs(netChange);
              } else {
                currentLiquidity := if (currentLiquidity >= Int.abs(netChange)) { currentLiquidity - Int.abs(netChange) } else { 0 };
              };
            };
            // Flip feeGrowthOutside at this tick (saturating subtraction to prevent underflow)
            let flippedRange = {
              rangeData with
              feeGrowthOutside0 = safeSub(feeGrowth0, rangeData.feeGrowthOutside0);
              feeGrowthOutside1 = safeSub(feeGrowth1, rangeData.feeGrowthOutside1);
            };
            updatedRanges := RBTree.put(updatedRanges, Nat.compare, target, flippedRange);
          };
          case null {};
        };
      };
    };

    let totalIn = amountIn - amountRemaining;

    if (test and amountRemaining > 0) {
      Debug.print("DRIFT_TRACE swapV3: amountIn=" # Nat.toText(amountIn)
        # " totalIn=" # Nat.toText(totalIn)
        # " remaining=" # Nat.toText(amountRemaining)
        # " poolFee=" # Nat.toText(totalPoolFee)
        # " protocolFee=" # Nat.toText(totalProtocolFee)
        # " iterations=" # Nat.toText(iterations));
    };

    // Safety clamp: prevents insolvency when output would exceed pool reserves. KEEP THIS —
    // without it, the transfer would pull tokens from OTHER pools' shared treasury balance,
    // enabling a drain attack. Goal: fix upstream so this NEVER fires during testing.
    // The CLAMP_TRIGGERED debug message + logger.error surface the drift instead of hiding it.
    let maxOutput = if (tokenInIsToken0) { pool.reserve1 } else { pool.reserve0 };
    if (totalAmountOut > maxOutput and totalAmountOut > 0) {
      let originalOut = totalAmountOut;
      totalAmountOut := maxOutput;
      // Sim-mode callers (e.g. getAMMLiquidity probes, route ranking) can hit this
      // legitimately during exploration; only escalate for real swaps.
      if (callerCtx != "sim") {
        Debug.print("CLAMP_TRIGGERED swapV3 ctx=" # callerCtx # ": tokenInIsToken0=" # debug_show(tokenInIsToken0)
          # " originalOut=" # Nat.toText(originalOut)
          # " clampedOut=" # Nat.toText(maxOutput)
          # " totalIn=" # Nat.toText(totalIn)
          # " pool.reserve0=" # Nat.toText(pool.reserve0)
          # " pool.reserve1=" # Nat.toText(pool.reserve1)
          # " currentSqrtRatio=" # Nat.toText(currentSqrtRatio)
          # " activeLiquidity=" # Nat.toText(currentLiquidity));
        logger.error("AMM", "swapV3 CLAMP fired ctx=" # callerCtx # " origOut=" # Nat.toText(originalOut) # " clampedOut=" # Nat.toText(maxOutput) # " reserve0=" # Nat.toText(pool.reserve0) # " reserve1=" # Nat.toText(pool.reserve1), "swapWithAMMV3");
      };
    };

    // totalInFinal preserved for symmetry with reserve math below
    let totalInFinal = totalIn;

    // Compute new reserves from sqrtRatio (overflow-safe)
    let newReserve0 = if (currentSqrtRatio > 0) {
      mulDiv(currentLiquidity, tenToPower60, currentSqrtRatio);
    } else { pool.reserve0 };
    let newReserve1 = mulDiv(currentLiquidity, currentSqrtRatio, tenToPower60);

    let updatedPool = {
      pool with
      // Subtract both poolFee and protocolFee from reserves.
      // Both accumulate in v3.totalFeesCollected (= actualFee + actualProtocolFee).
      // poolFee → attributable to LPs via feeGrowthGlobal (claimed by claimLPFees / removeConcentratedLiquidity).
      // protocolFee → parked in the (totalFeesCollected - totalFeesClaimed) residual; flushed to
      //               feescollectedDAO only when the pool is deleted (addAcceptedToken #Remove).
      // Use totalInFinal (post-clamp reduced input) so refunded excess isn't counted.
      reserve0 = if (tokenInIsToken0) {
        let total = pool.reserve0 + totalInFinal;
        let fees = totalPoolFee + totalProtocolFee;
        safeSub(total, fees);
      } else {
        safeSub(pool.reserve0, totalAmountOut);
      };
      reserve1 = if (tokenInIsToken0) {
        safeSub(pool.reserve1, totalAmountOut);
      } else {
        let total = pool.reserve1 + totalInFinal;
        let fees = totalPoolFee + totalProtocolFee;
        safeSub(total, fees);
      };
      lastUpdateTime = Time.now();
    };

    let updatedV3 = {
      activeLiquidity = currentLiquidity;
      currentSqrtRatio = currentSqrtRatio;
      feeGrowthGlobal0 = feeGrowth0;
      feeGrowthGlobal1 = feeGrowth1;
      totalFeesCollected0 = feesCollected0;
      totalFeesCollected1 = feesCollected1;
      totalFeesClaimed0 = v3.totalFeesClaimed0;
      totalFeesClaimed1 = v3.totalFeesClaimed1;
      ranges = updatedRanges;
    };

    (totalIn, totalAmountOut, totalProtocolFee, totalPoolFee, updatedPool, updatedV3);
  };

  //Map that indexes trades by accesscode
  type TradeMap = Map.Map<Text, TradePrivate>;

  //Map that indexes fees by token canister address
  type feemap = Map.Map<Text, Nat>;

  type TokenInfo = {
    address : Text;
    name : Text;
    symbol : Text;
    decimals : Nat;
    transfer_fee : Nat;
    minimum_amount : Nat;
    asset_type : { #ICP; #ICRC12; #ICRC3 };
  };

  // When a new AMM is created, 10000 is extracted of both tokens so the balance never goes below 0
  stable var AMMMinimumLiquidityDone = TrieSet.empty<Text>();
  transient let minimumLiquidity = 10000;

  // Daily pool snapshots for TVL/APR history
  type PoolDailySnapshot = {
    timestamp : Int;
    reserve0 : Nat;
    reserve1 : Nat;
    volume : Nat;
    totalLiquidity : Nat;
    activeLiquidity : Nat;
  };
  stable let poolDailySnapshots = Map.new<(Text, Text), RBTree.Tree<Int, PoolDailySnapshot>>();

  private func takePoolDailySnapshots() {
    let nowVar = Time.now();
    let dayStart = alignTimestamp(nowVar, 86400);

    for ((poolKey, pool) in Map.entries(AMMpools)) {
      if (pool.reserve0 > 0 or pool.reserve1 > 0) {
        // Get daily volume from K-line
        let kKey : KlineKey = (poolKey.0, poolKey.1, #day);
        let volume = switch (Map.get(klineDataStorage, hashkl, kKey)) {
          case (?tree) {
            switch (RBTree.get(tree, compareTime, dayStart)) {
              case (?kline) { kline.volume }; case null { 0 };
            };
          };
          case null { 0 };
        };

        let activeLiq = switch (Map.get(poolV3Data, hashtt, poolKey)) {
          case (?v3) { v3.activeLiquidity }; case null { pool.totalLiquidity };
        };

        var tree = switch (Map.get(poolDailySnapshots, hashtt, poolKey)) {
          case null { RBTree.init<Int, PoolDailySnapshot>() };
          case (?t) { t };
        };

        tree := RBTree.put(tree, Int.compare, dayStart, {
          timestamp = dayStart;
          reserve0 = pool.reserve0; reserve1 = pool.reserve1;
          volume = volume;
          totalLiquidity = pool.totalLiquidity;
          activeLiquidity = activeLiq;
        });

        // Keep max 365 days of history
        if (RBTree.size(tree) > 365) {
          let oldest = RBTree.scanLimit(tree, Int.compare, 0, nowVar, #fwd, 1);
          for ((k, _) in oldest.results.vals()) { tree := RBTree.delete(tree, Int.compare, k) };
        };

        Map.set(poolDailySnapshots, hashtt, poolKey, tree);
      };
    };
  };

  // In this trades are being stored that are current not fulfiled/done yet. The private map is only accessible if someone has the accesscode of the trade or if the entity is the DAO
  stable let tradeStorePrivate : TradeMap = Map.new<Text, TradePrivate>();

  // In this map all the public trades are stored by accesscode
  stable let tradeStorePublic : TradeMap = Map.new<Text, TradePrivate>();

  // Map that stores all users trades for queries that retrieve liquidity of an user
  stable let userCurrentTradeStore = Map.new<Text, TrieSet.Set<Text>>();

  // Trieset to save the trades that are being processed, this set is being checked on when deleting old trades, so no async problems happen.
  stable var tradesBeingWorkedOn = TrieSet.empty<Text>();

  // Map that has all (Foreign) Pools with liquidity with private trades
  stable let privateAccessCodes = Map.new<(Text, Text), TrieSet.Set<Text>>();

  // Map that saves all trades accoreding to thew time the were made, this is used to easily delete trades older than X days
  stable var timeBasedTrades : RBTree.Tree<Time, [Text]> = RBTree.init<Time, [Text]>();

  // Per-user swap history — keyed by timestamp for cheap range deletion
  type SwapRecord = {
    swapId : Nat;
    tokenIn : Text;
    tokenOut : Text;
    amountIn : Nat;
    amountOut : Nat;
    route : [Text];
    fee : Nat;
    swapType : { #direct; #multihop; #limit; #otc };
    timestamp : Int;
  };
  stable var nextSwapId : Nat = 0;
  stable let userSwapHistory = Map.new<Principal, RBTree.Tree<Int, SwapRecord>>();

  private func recordSwap(user : Principal, record : SwapRecord) {
    var tree = switch (Map.get(userSwapHistory, phash, user)) {
      case null { RBTree.init<Int, SwapRecord>() };
      case (?t) { t };
    };
    // Use swapId as tiebreaker to avoid timestamp collisions (nanosecond precision already unique enough)
    let key = record.timestamp + (record.swapId % 1000);
    tree := RBTree.put(tree, Int.compare, key, record);

    // Cap at 500 per user — remove oldest if over
    if (RBTree.size(tree) > 500) {
      let oldest = RBTree.scanLimit(tree, Int.compare, 0, 9_999_999_999_999_999_999_999, #fwd, 1);
      for ((k, _) in oldest.results.vals()) {
        tree := RBTree.delete(tree, Int.compare, k);
      };
    };

    Map.set(userSwapHistory, phash, user, tree);
  };

  // Map that saves all Foreign pools that have liquidity, so other functions know that the have to check those pools when an order has to be cancelled
  stable let foreignPools = Map.new<(Text, Text), Nat>();
  stable let foreignPrivatePools = Map.new<(Text, Text), Nat>();

  // Map that saves all the blocks that have been used for exchange transactions. So no-one can  make 2 orders with 1 transfer.
  stable let BlocksDone : Map.Map<Text, Time> = Map.new<Text, Time>();

  // One-shot marker: set when adminRecoverWronglysent has dispatched a refund
  // for a trap-orphaned deposit. Prevents repeated admin calls from double-paying
  // the same block. Independent of BlocksDone (which gates regular recoverWronglysent).
  stable let BlocksAdminRecovered : Map.Map<Text, Time> = Map.new<Text, Time>();

  // liqidity map that has all the order ratios (asset a amount/ asset b) in order
  stable let liqMapSort : BigLiqMapSort = Map.new<(Text, Text), liqmapsort>();

  stable let liqMapSortForeign : BigLiqMapSort = Map.new<(Text, Text), liqmapsort>();
  stable let tokenInfo = Map.new<Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text }>();
  Map.set(tokenInfo, thash, "ryjl3-tyaaa-aaaaa-aaaba-cai", { TransferFee = 10000; Decimals = 8; Name = "ICP"; Symbol = "ICP" });
  Map.set(tokenInfo, thash, "xevnm-gaaaa-aaaar-qafnq-cai", { TransferFee = 10000; Decimals = 6; Name = "USDC"; Symbol = "USDC" });

  stable var tokenInfoARR : [(Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text })] = [];

  // Stores tokeninfo update timer IDs to prevent exponential timer growth and enable cancellations
  stable var timerIDs = Vector.new<Nat>();

  // In this map the canister saves how many fees are already available to be picked up by the DAO. They can be picked up by calling collectFees()
  stable let feescollectedDAO : feemap = Map.new<Text, Nat>();

  // Non-drainable surplus counter: tracks tokens that remain in the wallet after token removal
  // (order Tfees gaps, V3 protocol fees, reserve rounding dust from pool deletion).
  // NOT drained by collectFees — included only in checkDiffs equation.
  // Reset when a token is re-added via addAcceptedToken(#Add).
  transient let tokenRemovalSurplus = Map.new<Text, Nat>();



  // Map to check whether a trader was referred, if null 100% of fees go to the DAO and the val will be Null.
  // Entries in this map get set to Null from Text if referrerFeeMap has null as val in referrerFeeMap (the key in referrerFeeMap is the val in userReferrerLink)
  stable let userReferrerLink = Map.new<Text, ?Text>();
  // Map that has referrers as key. The vals are tuples, of which the first item saves all the fees per token (Text), and the second item saves the last time it was updated
  // the Time is saved to access lastFeeAdditionByTime and delete the old entry to add a new one
  stable let referrerFeeMap = Map.new<Text, ?(Vector.Vector<(Text, Nat)>, Time)>();
  // RBTree to make it easy to delete referrers that havent been updated for more than 2 months
  stable var lastFeeAdditionByTime = RBTree.init<(Text, Time), Null>();

  stable var trade_number : Nat = 1;

  // If frozen, trading is impossible
  stable var exchangeState : { #Active; #Frozen } = #Active;

  // Emergency drain state machine
  stable var drainState : {
    #Idle;
    #DrainingOrders;
    #DrainingV2;
    #DrainingV3;
    #SweepingFees;
    #SweepingRemainder;
    #Done;
  } = #Idle;
  stable var drainTarget : Principal = Principal.fromText("aaaaa-aa");

  // Self-managed whitelist for who can call collectFees + manage the list
  stable var feeCollectors : [Principal] = [
    Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe")
  ];

  stable var baseTokens = ["ryjl3-tyaaa-aaaaa-aaaba-cai", "xevnm-gaaaa-aaaar-qafnq-cai"];

  // This Array saves what tokens are okay to be traded within the OTC exchange. They have to be either ICP, ICRC1-2 or ICRC3. Minimum amount is the minum amount positions can be made for.
  stable var acceptedTokens : [Text] = ["ryjl3-tyaaa-aaaaa-aaaba-cai", "xevnm-gaaaa-aaaar-qafnq-cai"]; // ICP + ckUSDC (base tokens); other tokens added via addAcceptedToken
  stable var acceptedTokensInfo : [TokenInfo] = [];
  stable var minimumAmount = [100000, 100000];
  stable var tokenType : [{ #ICP; #ICRC12; #ICRC3 }] = [#ICP, #ICRC12];

  // Cache of position-offset archives: token canister id -> firstIndex.
  // Some ICRC-1 archives (e.g., CLOWN at iwv6l-6iaaa-aaaal-ajjjq-cai) use
  // position-based indexing so that archive position N corresponds to global
  // index N + offset. Auto-populated on first successful probe in getBlockData.
  stable let tokenArchiveOffset = Map.new<Text, Nat>();

  assert (acceptedTokens.size() == minimumAmount.size());
  assert (acceptedTokens.size() > 1);

  //Array to store tokens that cant be traded. This will be used when its knows a certain token has a consolidated processqueue or is going to change its characteristics.
  stable var pausedTokens : [Text] = [];

  stable let klineDataStorage = Map.new<KlineKey, RBTree.Tree<Int, KlineData>>();

  stable let last24hPastPriceUpdate = Map.new<(Text, Text), Time>();

  // The different enities that have something to say about this canister, will change in the future (outside the tests)
  transient let self = Principal.fromActor(this);
  stable var owner2 = Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe"); // will be the sole Admin account
  stable var owner3 = Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe"); // will be the sns management canister
  stable var DAOentry = Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe"); //change in production
  stable var DAOTreasury = Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe"); //change in production
  stable var DAOTreasuryText = Principal.toText(DAOTreasury); //change in production

  // Retained for stable-storage compatibility with previous deployments
  // (persistent actor `let` bindings are part of the stable signature; dropping
  // one requires an explicit migration function). Superseded by FLASH_ARB_CALLERS.
  let BUYBACK_CANISTER_ID : Principal = Principal.fromText("cfl3o-5qaaa-aaaan-q6fga-cai");

  // Flash-arb-trusted callers — used by adminFlashArb AND by the same-token
  // reject in addPosition / swapMultiHop / swapSplitRoutes. Cyclic flash-arb
  // routes are legitimate from these callers; everyone else gets their deposit
  // refunded with the standard rejection fee.
  // Add/remove requires a code edit + redeploy — no runtime mutator.
  let FLASH_ARB_CALLERS : [Principal] = [
    BUYBACK_CANISTER_ID, // buyback canister (kept above for stable-compat)
    Principal.fromText("r7wkx-fqqi2-nwydg-gcytg-75vgc-7d33n-d4hxs-wsfqe-6inmj-6oiic-pae"),
  ];
  private func isFlashArbCaller(p : Principal) : Bool {
    for (allowed in FLASH_ARB_CALLERS.vals()) {
      if (allowed == p) return true;
    };
    false;
  };

  // variable that stores all the transfer made within 1 exchangeblock. It gets cleared when its sent to the treasury canister.
  // This way, if the intercanister call to the treasury errors out, no funds are lost.
  stable let tempTransferQueue = Vector.new<(TransferRecipient, Nat, Text, Text)>();

  // Per-transaction idempotency counter for transfer deduplication
  stable var nextTxId : Nat = 0;
  private func genTxId() : Text {
    nextTxId += 1;
    Nat.toText(nextTxId);
  };

  // check the function named ownercheck and isAllowed and their description for the following variables.
  stable var allowedCanisters = [treasury_principal, DAOentry, DAOTreasury, owner2, owner3, Principal.fromActor(this)];

  // DAO treasury principals that bypass the public-only orderbook filter when
  // matching against private orders with excludeDAO=false. Both production and
  // staging are listed because the exchange canister is shared between networks.
  // Adminable via parameterManagement.
  stable var daoTreasuryPrincipalsText : [Text] = [
    "v6t5d-6yaaa-aaaan-qzzja-cai",  // IC mainnet DAO treasury
    "tptia-syaaa-aaaai-atieq-cai"   // staging DAO treasury
  ];

  stable let spamCheck = Map.new<Principal, Nat>();

  stable let spamCheckOver10 = Map.new<Principal, Nat>();
  stable var warnings = TrieSet.empty<Principal>();

  stable var allowedCalls = 21;
  stable var allowedSilentWarnings = 11;

  stable var dayBan = TrieSet.empty<Principal>();
  stable var dayBanRegister = TrieSet.empty<Principal>();
  stable var allTimeBan = TrieSet.empty<Principal>();
  stable var over10 = TrieSet.empty<Principal>();

  stable var timeStartSpamCheck = Time.now();
  stable var timeStartSpamDayCheck = Time.now();
  stable var timeWindowSpamCheck = 90000000000;

  // Map set with pools as keys and RBTree as val, to save all the past trades. Gets trimmed occasionally
  stable let pool_history : Pool_History = Map.new<(Text, Text), RBTree.Tree<Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }]>>();

  // Array that saves all the pools
  stable var pool_canister = Vector.new<(Text, Text)>();

  // Transient index for O(1) pool lookups: (token0, token1) → index in pool_canister
  transient var poolIndexMap = Map.new<(Text, Text), Nat>();

  private func rebuildPoolIndex() {
    poolIndexMap := Map.new<(Text, Text), Nat>();
    for (i in Iter.range(0, Vector.size(pool_canister) - 1)) {
      let p = Vector.get(pool_canister, i);
      Map.set(poolIndexMap, hashtt, p, i);
      Map.set(poolIndexMap, hashtt, (p.1, p.0), i);
    };
  };

  stable var amm_reserve0Array : [Nat] = Array.tabulate(Vector.size(pool_canister), func(_ : Nat) : Nat { 0 });
  stable var amm_reserve1Array : [Nat] = Array.tabulate(Vector.size(pool_canister), func(_ : Nat) : Nat { 0 });

  stable var asset_names = Vector.new<(Text, Text)>();
  stable var asset_symbols = Vector.new<(Text, Text)>();
  stable var asset_decimals = Vector.new<(Nat8, Nat8)>();
  stable var asset_transferfees = Vector.new<(Nat, Nat)>();
  stable var asset_minimum_amount = Vector.new<(Nat, Nat)>();

  stable var last_traded_price = Vector.new<Float>();
  stable var price_day_before = Vector.new<Float>();

  // Variables made to do certain things such as update the token info when upgrading or initialising the canister
  
  stable var first_time_running = 1;
  transient var first_time_running_after_upgrade = 1;

  if (first_time_running == 1) {
    for (index in Iter.range(0, acceptedTokens.size() - 1)) {
      for (baseToken in baseTokens.vals()) {
        if (acceptedTokens[index] != baseToken and Array.find<Text>(baseTokens, func(t) { t == acceptedTokens[index] }) == null) {
          Vector.add(pool_canister, (acceptedTokens[index], baseToken));
          let baseTokenIndex = Array.indexOf<Text>(baseToken, acceptedTokens, Text.equal);
          switch (baseTokenIndex) {
            case (?i) {
              Vector.add(asset_minimum_amount, (minimumAmount[index], minimumAmount[i]));
            };
            case null {
              Vector.add(asset_minimum_amount, (minimumAmount[index], minimumAmount[index]));
            };
          };
          Vector.add(
            last_traded_price,
            (
              0.000000000001
            ),
          );
          Vector.add(price_day_before, 0.000000000001);
        };
      };
    };
    // Create pools between base tokens
    label a for (i in Iter.range(0, baseTokens.size() - 1)) {
      label b for (j in Iter.range(i + 1, baseTokens.size() - 1)) {
        if (i >= j) {
          continue a;
        };
        Vector.add(pool_canister, (baseTokens[i], baseTokens[j]));
        Vector.add(asset_minimum_amount, (minimumAmount[switch (Array.indexOf<Text>(baseTokens[i], acceptedTokens, Text.equal)) { case (?a) { a }; case null { 0 } }], minimumAmount[switch (Array.indexOf<Text>(baseTokens[j], acceptedTokens, Text.equal)) { case (?a) { a }; case null { 0 } }]));
        Vector.add(
          last_traded_price,
          0.000000000001,
        );
        Vector.add(price_day_before, 0.000000000001);
      };
    };
  };
  rebuildPoolIndex();
  stable var volume_24hArray : [Nat] = Array.tabulate(Vector.size(pool_canister), func(_ : Nat) : Nat { 0 });

  stable var AllExchangeInfo : pool = {
    pool_canister = Vector.toArray(pool_canister);
    asset_names = Vector.toArray(asset_names);
    asset_symbols = Vector.toArray(asset_symbols);
    asset_decimals = Vector.toArray(asset_decimals);
    asset_transferfees = Vector.toArray(asset_transferfees);
    asset_minimum_amount = Vector.toArray(asset_minimum_amount);
    last_traded_price = Vector.toArray(last_traded_price);
    price_day_before = Vector.toArray(price_day_before);
    volume_24h = volume_24hArray;
    amm_reserve0 = amm_reserve0Array;
    amm_reserve1 = amm_reserve1Array;
  };

  //@afat added more randomness like this, is this enough? Random Blob needs await and not sure how random a generator would be.
  transient let fuzz = Fuzz.fromSeed(Fuzz.fromSeed(Int.abs(Time.now()) + Fuzz.Fuzz().nat.randomRange(0, 10000000)).nat.randomRange(0, 1000000));
  public type Subaccount = Blob;

  // function that gives the right order of tokens according to existing pools.
  func getPool(token1 : Text, token2 : Text) : (Text, Text) {
    switch (Map.get(poolIndexMap, hashtt, (token1, token2))) {
      case (?idx) { Vector.get(pool_canister, idx) };
      case null {
        if (Map.has(foreignPools, hashtt, (token1, token2))) {
          (token1, token2);
        } else if (Map.has(foreignPools, hashtt, (token2, token1))) {
          (token2, token1);
        } else {
          (token1, token2);
        };
      };
    };
  };

  private func isKnownPool(t1 : Text, t2 : Text) : Bool {
    Map.has(poolIndexMap, hashtt, (t1, t2));
  };

  // Register a new pool pair in pool_canister + poolIndexMap + ALL per-pool vectors/arrays.
  // Called when addLiquidity/addConcentratedLiquidity creates a pair not yet in pool_canister.
  // All updates are synchronous (no await) = atomic in Motoko's actor model.
  private func registerPoolPair(token0 : Text, token1 : Text) {
    let pk = (token0, token1);
    if (Map.has(poolIndexMap, hashtt, pk)) return;

    // 1. Pool canister + index map
    Vector.add(pool_canister, pk);
    let idx = Vector.size(pool_canister) - 1 : Nat;
    Map.set(poolIndexMap, hashtt, pk, idx);
    Map.set(poolIndexMap, hashtt, (token1, token0), idx);

    // 2. Per-pool price/volume vectors
    Vector.add(last_traded_price, 0.0);
    Vector.add(price_day_before, 0.0);

    // 3. Per-pool metadata vectors (asset_names, asset_symbols, asset_decimals, asset_transferfees)
    let info0 = Map.get(tokenInfo, thash, token0);
    let info1 = Map.get(tokenInfo, thash, token1);
    let name0 = switch (info0) { case (?i) { i.Name }; case null { "" } };
    let name1 = switch (info1) { case (?i) { i.Name }; case null { "" } };
    let sym0 = switch (info0) { case (?i) { i.Symbol }; case null { "" } };
    let sym1 = switch (info1) { case (?i) { i.Symbol }; case null { "" } };
    let dec0 = switch (info0) { case (?i) { natToNat8(i.Decimals) }; case null { 8 : Nat8 } };
    let dec1 = switch (info1) { case (?i) { natToNat8(i.Decimals) }; case null { 8 : Nat8 } };
    let fee0 = switch (info0) { case (?i) { i.TransferFee }; case null { 10000 } };
    let fee1 = switch (info1) { case (?i) { i.TransferFee }; case null { 10000 } };

    Vector.add(asset_names, (name0, name1));
    Vector.add(asset_symbols, (sym0, sym1));
    Vector.add(asset_decimals, (dec0, dec1));
    Vector.add(asset_transferfees, (fee0, fee1));

    // 4. Per-pool minimum amounts
    let min0 = switch (Array.indexOf<Text>(token0, acceptedTokens, Text.equal)) {
      case (?i) { minimumAmount[i] }; case null { 100000 };
    };
    let min1 = switch (Array.indexOf<Text>(token1, acceptedTokens, Text.equal)) {
      case (?i) { minimumAmount[i] }; case null { 100000 };
    };
    Vector.add(asset_minimum_amount, (min0, min1));

    // 5. Per-pool arrays
    volume_24hArray := Array.tabulate<Nat>(volume_24hArray.size() + 1, func(i : Nat) : Nat {
      if (i < volume_24hArray.size()) { volume_24hArray[i] } else { 0 };
    });
    amm_reserve0Array := Array.tabulate<Nat>(amm_reserve0Array.size() + 1, func(i : Nat) : Nat {
      if (i < amm_reserve0Array.size()) { amm_reserve0Array[i] } else { 0 };
    });
    amm_reserve1Array := Array.tabulate<Nat>(amm_reserve1Array.size() + 1, func(i : Nat) : Nat {
      if (i < amm_reserve1Array.size()) { amm_reserve1Array[i] } else { 0 };
    });

    // 6. Update AllExchangeInfo snapshot so queries see the new pool immediately
    updateStaticInfo();
    doInfoBeforeStep2();
  };

  // Enumerate all valid 1-3 hop paths between two tokens.
  // Returns routes ranked by AMM-simulated output (best first).
  private func findRoutes(
    tokenIn : Text, tokenOut : Text, amountIn : Nat
  ) : [{ hops : [SwapHop]; estimatedOut : Nat }] {
    let results = Vector.new<{ hops : [SwapHop]; estimatedOut : Nat }>();

    // 1-hop: direct pool
    if (isKnownPool(tokenIn, tokenOut)) {
      let pk = getPool(tokenIn, tokenOut);
      switch (Map.get(AMMpools, hashtt, pk)) {
        case (?pool) {
          let v3 = Map.get(poolV3Data, hashtt, pk);
          let (est, _, _) = simulateSwap(pool, v3, tokenIn, amountIn, ICPfee);
          if (est > 0) Vector.add(results, {
            hops = [{ tokenIn; tokenOut }]; estimatedOut = est;
          });
        };
        case null {};
      };
    };

    // 2-hop: tokenIn -> mid -> tokenOut
    for (midToken in acceptedTokens.vals()) {
      if (midToken != tokenIn and midToken != tokenOut and isKnownPool(tokenIn, midToken) and isKnownPool(midToken, tokenOut)) {
        let pk1 = getPool(tokenIn, midToken);
        let pk2 = getPool(midToken, tokenOut);
        switch (Map.get(AMMpools, hashtt, pk1), Map.get(AMMpools, hashtt, pk2)) {
          case (?p1, ?p2) {
            let v3_1 = Map.get(poolV3Data, hashtt, pk1);
            let (out1, _, _) = simulateSwap(p1, v3_1, tokenIn, amountIn, ICPfee);
            if (out1 > 0) {
              let v3_2 = Map.get(poolV3Data, hashtt, pk2);
              let (out2, _, _) = simulateSwap(p2, v3_2, midToken, out1, ICPfee);
              if (out2 > 0) Vector.add(results, {
                hops = [{ tokenIn; tokenOut = midToken }, { tokenIn = midToken; tokenOut }];
                estimatedOut = out2;
              });
            };
          };
          case _ {};
        };
      };
    };

    // 3-hop: tokenIn -> mid1 -> mid2 -> tokenOut
    for (mid1 in acceptedTokens.vals()) {
      if (mid1 == tokenIn or mid1 == tokenOut or not isKnownPool(tokenIn, mid1)) { /* skip */ } else {
        for (mid2 in acceptedTokens.vals()) {
          if (mid2 == tokenIn or mid2 == tokenOut or mid2 == mid1 or not isKnownPool(mid1, mid2) or not isKnownPool(mid2, tokenOut)) { /* skip */ } else {
            let pk1 = getPool(tokenIn, mid1);
            let pk2 = getPool(mid1, mid2);
            let pk3 = getPool(mid2, tokenOut);
            switch (Map.get(AMMpools, hashtt, pk1), Map.get(AMMpools, hashtt, pk2), Map.get(AMMpools, hashtt, pk3)) {
              case (?p1, ?p2, ?p3) {
                let v3_1 = Map.get(poolV3Data, hashtt, pk1);
                let (out1, _, _) = simulateSwap(p1, v3_1, tokenIn, amountIn, ICPfee);
                if (out1 > 0) {
                  let v3_2 = Map.get(poolV3Data, hashtt, pk2);
                  let (out2, _, _) = simulateSwap(p2, v3_2, mid1, out1, ICPfee);
                  if (out2 > 0) {
                    let v3_3 = Map.get(poolV3Data, hashtt, pk3);
                    let (out3, _, _) = simulateSwap(p3, v3_3, mid2, out2, ICPfee);
                    if (out3 > 0) Vector.add(results, {
                      hops = [
                        { tokenIn; tokenOut = mid1 },
                        { tokenIn = mid1; tokenOut = mid2 },
                        { tokenIn = mid2; tokenOut },
                      ];
                      estimatedOut = out3;
                    });
                  };
                };
              };
              case _ {};
            };
          };
        };
      };
    };

    // Sort by estimated output descending (best first)
    Array.sort<{ hops : [SwapHop]; estimatedOut : Nat }>(Vector.toArray(results), func(a, b) {
      if (a.estimatedOut > b.estimatedOut) #less
      else if (a.estimatedOut < b.estimatedOut) #greater
      else #equal;
    });
  };

  // in this function 2 directions are sent. This corresponds to the positions surrounding the last traded price (the orderbook)
  // RVVR-TACOX4 Fix: Removed magic numbers (1500 limit) and improved flexibility:
  // - Client specifies limit and direction (forward/backward)
  // - Implemented cursor-based pagination
  // - Clear end-of-data signaling (Max/Zero cursor)
  // - Maintained efficiency with RBTree.scanLimit
  public query ({ caller }) func getCurrentLiquidity(
    token1 : Text,
    token2 : Text,
    direction : { #forward; #backward },
    limit : Nat,
    cursor : ?Ratio,
  ) : async {
    liquidity : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
    nextCursor : Ratio;
  } {
    if (isAllowedQuery(caller) != 1) {
      return { liquidity = []; nextCursor = #Zero };
    };



    let pair = switch (direction) {
      case (#forward) (token1, token2);
      case (#backward) (token2, token1);
    };

    switch (Map.get(liqMapSort, hashtt, pair)) {
      case (null) {
        return {
          liquidity = [];
          nextCursor = #Max;
        };
      };
      case (?tree) {
        let endBound = switch (cursor) {
          case (null) #Max;
          case (?c) switch (c) {
            case (#Value(a)) { #Value(a -1) };
            case (a) { a };
          };
        };

        let startCursor = #Zero;

        let result = RBTree.scanLimit(
          tree,
          compareRatio,
          startCursor,
          endBound,
          #bwd,
          limit,
        );

        let filteredResults = filterPublicOrders(result.results);

        return {
          liquidity = filteredResults;
          nextCursor = if (filteredResults.size() < limit) endBound else switch (filteredResults.size()) {
            case 0 startCursor;
            case n filteredResults[n -1].0;
          };
        };
      };
    };
  };

  func filterPublicOrders(entries : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])]) : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])] {
    Array.map<(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]), (Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])>(
      entries,
      func(entry : (Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])) : (Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]) {
        (
          entry.0,
          Array.filter(
            entry.1,
            func(order : { time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }) : Bool {
              Text.startsWith(order.accesscode, #text "Public");
            },
          ),
        );
      },
    );
  };

  type PoolLiquidity = {
    forward : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
    backward : [(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])];
  };

  type ForeignPoolLiquidity = {
    pool : (Text, Text);
    liquidity : PoolLiquidity;
    forwardCursor : Ratio;
    backwardCursor : Ratio;
  };

  type PoolQuery = {
    pool : (Text, Text);
    forwardCursor : ?Ratio;
    backwardCursor : ?Ratio;
  };

  type ForeignPoolsResponse = {
    pools : [ForeignPoolLiquidity];
    nextPoolCursor : ?PoolQuery;
  };

  // RVVR-TACOX-4 Fix: Improved getCurrentLiquidityForeignPools
  // This function retrieves liquidity information for foreign pools with the following features:
  // 1. Client-specified limit instead of hardcoded 1500
  // 2. Separate forward and backward liquidity as per liqMapSort structure
  // 3. Global limit across all queried pools
  // 4. Support for querying multiple specific pools or all pools
  // 5. Cursor-based pagination for efficient data retrieval
  //
  // How it works:
  // - If poolQuery is provided:
  //   a) It processes only the specified pools if onlySpecifiedPools is true
  //   b) It processes the specified pools first, then continues with remaining pools if onlySpecifiedPools is false
  // - If poolQuery is not provided:
  //   a) It starts from the beginning of all foreign pools
  // - For each pool:
  //   a) Retrieves forward and backward liquidity up to the remaining limit
  //   b) Applies public order filtering
  //   c) Adds results to the output
  // - If the global limit is reached:
  //   a) It stops processing more pools
  //   b) Trims excess entries from the last processed pool if necessary
  // - Returns:
  //   a) Processed pool liquidity data
  //   b) A nextPoolCursor for the next query, if more data is available
  //
  // Usage:
  // 1. Initial query: Don't provide poolQuery to start from the beginning
  // 2. Continuation query: Provide poolQuery with the nextPoolCursor from the previous response
  // 3. Specific pools query: Provide poolQuery with desired pools, their cursors, and set onlySpecifiedPools to true
  //
  // This implementation allows for flexible and efficient querying of foreign pool liquidity,
  // addressing the issues raised in RVVR-TACOX-4 while maintaining the existing structure of liqMapSort.
  public query ({ caller }) func getCurrentLiquidityForeignPools(
    limit : Nat,
    poolQuery : ?[PoolQuery],
    onlySpecifiedPools : Bool,
  ) : async ForeignPoolsResponse {
    if (isAllowedQuery(caller) != 1) {
      return { pools = []; nextPoolCursor = null };
    };

    let results = Vector.new<ForeignPoolLiquidity>();
    var totalEntries = 0;
    var nextPoolCursor : ?PoolQuery = null;

    let allPools = Iter.toArray(Map.keys(foreignPools));

    let poolsToProcess = switch (poolQuery) {
      case (?queries) {
        if (onlySpecifiedPools) {
          queries;
        } else {
          let lastQueriedPool = if (queries.size() > 0) ?queries[queries.size() - 1].pool else null;
          let remainingPools = switch (lastQueriedPool) {
            case null allPools;
            case (?lastPool) {
              let startIndex = Option.get(Array.indexOf<(Text, Text)>(lastPool, allPools, func(a, b) { a.0 == b.0 and a.1 == b.1 }), 0) + 1;
              Array.tabulate<(Text, Text)>(allPools.size() - startIndex, func(i) { allPools[i + startIndex] });
            };
          };
          let combined = Vector.fromArray<PoolQuery>(queries);
          Vector.addFromIter(combined, Array.map<(Text, Text), PoolQuery>(remainingPools, func((t1, t2)) { { pool = (t1, t2); forwardCursor = null; backwardCursor = null } }).vals());
          Vector.toArray(combined);
        };
      };
      case null {
        Array.map<(Text, Text), PoolQuery>(allPools, func((t1, t2)) { { pool = (t1, t2); forwardCursor = null; backwardCursor = null } });
      };
    };

    if (poolsToProcess.size() == 0) {
      return { pools = []; nextPoolCursor = null };
    };
    var i2 = 0;
    var Query = poolsToProcess[0];
    label poolProcessing for (i in Iter.range(0, poolsToProcess.size() - 1)) {
      i2 := i;
      Query := poolsToProcess[i];
      let (token1, token2) = Query.pool;

      let forwardTree = switch (Map.get(liqMapSortForeign, hashtt, (token1, token2))) {
        case (null) {
          RBTree.init<Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]>();
        };
        case (?tree) tree;
      };


      let backwardTree = switch (Map.get(liqMapSortForeign, hashtt, (token2, token1))) {
        case (null) {
          RBTree.init<Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]>();
        };
        case (?tree) tree;
      };


      let forwardStartCursor = switch (Query.forwardCursor) {
        case (?fc) switch (fc) {
          case (#Value(a)) { #Value(a -1) };
          case (a) { a };
        };
        case (null) #Max;
      };

      let backwardStartCursor = switch (Query.backwardCursor) {
        case (?bc) switch (bc) {
          case (#Value(a)) { #Value(a -1) };
          case (a) { a };
        };
        case (null) #Max;
      };

      let remainingLimit = limit - totalEntries;
      let forwardLimit = switch (Query.forwardCursor) {
        case (? #Max) 0;
        case _ switch (Query.backwardCursor) {
          case (? #Max) remainingLimit;
          case _ remainingLimit / 2 + remainingLimit % 2;
        };
      };
      let backwardLimit = remainingLimit - forwardLimit;

      let forwardResult = if (forwardLimit > 0) RBTree.scanLimit(forwardTree, compareRatio, #Zero, forwardStartCursor, #bwd, forwardLimit) else {
        { results = []; next = forwardStartCursor };
      };

      let backwardResult = if (backwardLimit > 0) RBTree.scanLimit(backwardTree, compareRatio, #Zero, backwardStartCursor, #bwd, backwardLimit) else {
        { results = []; next = backwardStartCursor };
      };

      let filteredForward = forwardResult.results;
      let filteredBackward = backwardResult.results;

      let poolLiquidity : ForeignPoolLiquidity = {
        pool = (token1, token2);
        liquidity = {
          forward = filteredForward;
          backward = filteredBackward;
        };
        forwardCursor = if (filteredForward.size() > 0) filteredForward[filteredForward.size() - 1].0 else forwardStartCursor;
        backwardCursor = if (filteredBackward.size() > 0) filteredBackward[filteredBackward.size() - 1].0 else backwardStartCursor;
      };

      Vector.add(results, poolLiquidity);
      totalEntries += filteredForward.size() + filteredBackward.size();

      if (totalEntries >= limit) {
        nextPoolCursor := if (i + 1 < poolsToProcess.size()) ?{
          pool = poolsToProcess[i + 1].pool;
          forwardCursor = null;
          backwardCursor = null;
        } else null;
        break poolProcessing;
      };
    };

    // Trim the last pool's results if we've exceeded the limit
    if (totalEntries > limit) {
      let forwardStartCursor = switch (Query.forwardCursor) {
        case (?fc) switch (fc) {
          case (#Value(a)) { #Value(a -1) };
          case (a) { a };
        };
        case (null) #Max;
      };

      let backwardStartCursor = switch (Query.backwardCursor) {
        case (?bc) switch (bc) {
          case (#Value(a)) { #Value(a -1) };
          case (a) { a };
        };
        case (null) #Max;
      };
      let lastIndex = Vector.size(results) - 1;
      let lastPool = Vector.get(results, lastIndex);
      let excessEntries = totalEntries - limit;

      let forwardSize = lastPool.liquidity.forward.size();
      let backwardSize = lastPool.liquidity.backward.size();

      if (excessEntries < forwardSize + backwardSize) {
        let forwardToKeep = Nat.max(0, forwardSize - excessEntries / 2);
        let backwardToKeep = Nat.max(0, backwardSize - (excessEntries - (forwardSize - forwardToKeep)));

        let newForward = Array.tabulate<(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])>(
          forwardToKeep,
          func(i : Nat) : (Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]) {
            lastPool.liquidity.forward[i];
          },
        );

        let newBackward = Array.tabulate<(Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])>(
          backwardToKeep,
          func(i : Nat) : (Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]) {
            lastPool.liquidity.backward[i];
          },
        );

        Vector.put(
          results,
          lastIndex,
          {
            pool = lastPool.pool;
            liquidity = {
              forward = newForward;
              backward = newBackward;
            };
            forwardCursor = if (newForward.size() > 0) newForward[newForward.size() - 1].0 else lastPool.forwardCursor;
            backwardCursor = if (newBackward.size() > 0) newBackward[newBackward.size() - 1].0 else lastPool.backwardCursor;
          },
        );
        nextPoolCursor := ?{
          pool = poolsToProcess[i2].pool;
          forwardCursor = if (newForward.size() > 0) ?newForward[newForward.size() - 1].0 else ?forwardStartCursor;
          backwardCursor = if (newBackward.size() > 0) ?newBackward[newBackward.size() - 1].0 else ?backwardStartCursor;
        };
      };
    };

    { pools = Vector.toArray(results); nextPoolCursor = nextPoolCursor };
  };

  public query ({ caller }) func getOrderbookCombined(
    token0 : Text, token1 : Text,
    numLevels : Nat,
    stepBasisPoints : Nat,
  ) : async {
    bids : [{ price : Float; ammAmount : Nat; limitAmount : Nat; limitOrders : Nat }];
    asks : [{ price : Float; ammAmount : Nat; limitAmount : Nat; limitOrders : Nat }];
    ammMidPrice : Float;
    spread : Float;
    ammReserve0 : Nat;
    ammReserve1 : Nat;
  } {
    let emptyResult = { bids : [{ price : Float; ammAmount : Nat; limitAmount : Nat; limitOrders : Nat }] = []; asks : [{ price : Float; ammAmount : Nat; limitAmount : Nat; limitOrders : Nat }] = []; ammMidPrice : Float = 0.0; spread : Float = 0.0; ammReserve0 : Nat = 0; ammReserve1 : Nat = 0 };
    if (isAllowedQuery(caller) != 1) { return emptyResult };

    let maxLevels = if (numLevels > 100) { 100 } else if (numLevels == 0) { 25 } else { numLevels };
    let step = if (stepBasisPoints > 1000) { 1000 } else if (stepBasisPoints == 0) { 10 } else { stepBasisPoints };
    let poolKey = getPool(token0, token1);

    // Get AMM reserves, ensuring token0 maps to res0
    var res0 : Nat = 0;
    var res1 : Nat = 0;
    switch (Map.get(AMMpools, hashtt, poolKey)) {
      case (null) {};
      case (?pool) {
        if (pool.token0 == token0) { res0 := pool.reserve0; res1 := pool.reserve1 }
        else { res0 := pool.reserve1; res1 := pool.reserve0 };
      };
    };
    let hasAMM = res0 > 0 and res1 > 0;
    let v3MidRatio = getPoolRatioV3(poolKey);
    var midRatio : Nat = if (v3MidRatio > 0) { v3MidRatio } else if (res0 > 0) { res1 * tenToPower60 / res0 } else { 0 };

    // If no AMM, try to derive mid price from best bid/ask in limit orderbook
    if (midRatio == 0) {
      let bestAskR : ?Nat = switch (Map.get(liqMapSort, hashtt, (token0, token1))) {
        case (?tree) {
          let s = RBTree.scanLimit(tree, compareRatio, #Zero, #Max, #fwd, 1);
          if (s.results.size() > 0) { switch (s.results[0].0) { case (#Value(v)) { ?v }; case _ { null } } } else { null };
        };
        case _ { null };
      };
      let bestBidR : ?Nat = switch (Map.get(liqMapSort, hashtt, (token1, token0))) {
        case (?tree) {
          let s = RBTree.scanLimit(tree, compareRatio, #Zero, #Max, #fwd, 1);
          if (s.results.size() > 0) { switch (s.results[0].0) { case (#Value(v)) { if (v > 0) { ?(tenToPower120 / v) } else { null } }; case _ { null } } } else { null };
        };
        case _ { null };
      };
      midRatio := switch (bestAskR, bestBidR) {
        case (?a, ?b) { (a + b) / 2 };
        case (?a, null) { a };
        case (null, ?b) { b };
        case _ { 0 };
      };
    };

    if (midRatio == 0) { return emptyResult };
    // Decimal-adjust midPrice: midRatio is res1/res0 * 10^60, normalize for display
    let dec0 = switch (Map.get(tokenInfo, thash, token0)) { case (?i) { i.Decimals }; case null { 8 } };
    let dec1 = switch (Map.get(tokenInfo, thash, token1)) { case (?i) { i.Decimals }; case null { 8 } };
    let midPrice : Float = (Float.fromInt(midRatio) * Float.fromInt(10 ** dec0)) / (Float.fromInt(tenToPower60) * Float.fromInt(10 ** dec1));

    // Build level vectors
    let askVec = Vector.new<{ price : Float; ammAmount : Nat; limitAmount : Nat; limitOrders : Nat }>();
    let bidVec = Vector.new<{ price : Float; ammAmount : Nat; limitAmount : Nat; limitOrders : Nat }>();

    if (hasAMM) {
      // V3-aware depth calculation: use concentrated liquidity ranges if available
      switch (Map.get(poolV3Data, hashtt, poolKey)) {
        case (?v3) {
          // ASK side: price increases, token0 depth per level
          var prevSqrtAsk = v3.currentSqrtRatio;
          var askActiveLiq = v3.activeLiquidity;
          for (i in Iter.range(1, maxLevels)) {
            let factor = 10000 + i * step;
            let scaledFactor = sqrt(factor * tenToPower60 / 10000);
            let levelSqrt = mulDiv(v3.currentSqrtRatio, scaledFactor, tenToPower30);

            // Check tick crossings between prevSqrtAsk and levelSqrt (ascending)
            let crossedTicks = RBTree.scanLimit(v3.ranges, Nat.compare, prevSqrtAsk + 1, levelSqrt, #fwd, 100);
            for ((_, tickData) in crossedTicks.results.vals()) {
              if (tickData.liquidityNet >= 0) { askActiveLiq += Int.abs(tickData.liquidityNet) }
              else { askActiveLiq := safeSub(askActiveLiq, Int.abs(tickData.liquidityNet)) };
            };

            // token0 depth = L * SCALE / prevSqrt - L * SCALE / levelSqrt
            let ammAmt = if (askActiveLiq > 0 and prevSqrtAsk > 0 and levelSqrt > 0) {
              safeSub(mulDiv(askActiveLiq, tenToPower60, prevSqrtAsk), mulDiv(askActiveLiq, tenToPower60, levelSqrt));
            } else { 0 };
            Vector.add(askVec, { price = midPrice * Float.fromInt(factor) / 10000.0; ammAmount = ammAmt; limitAmount = 0; limitOrders = 0 });
            prevSqrtAsk := levelSqrt;
          };

          // BID side: price decreases, token0 depth per level
          var prevSqrtBid = v3.currentSqrtRatio;
          var bidActiveLiq = v3.activeLiquidity;
          for (i in Iter.range(1, maxLevels)) {
            if (i * step >= 10000) {
              Vector.add(bidVec, { price = 0.0; ammAmount = 0; limitAmount = 0; limitOrders = 0 });
            } else {
              let factor = 10000 - i * step;
              let scaledFactor = sqrt(factor * tenToPower60 / 10000);
              let levelSqrt = mulDiv(v3.currentSqrtRatio, scaledFactor, tenToPower30);

              // Check tick crossings between levelSqrt and prevSqrtBid (descending)
              let crossedTicks = RBTree.scanLimit(v3.ranges, Nat.compare, levelSqrt, prevSqrtBid, #bwd, 100);
              for ((_, tickData) in crossedTicks.results.vals()) {
                if (tickData.liquidityNet >= 0) { bidActiveLiq := safeSub(bidActiveLiq, Int.abs(tickData.liquidityNet)) }
                else { bidActiveLiq += Int.abs(tickData.liquidityNet) };
              };

              // token0 depth = L * SCALE / levelSqrt - L * SCALE / prevSqrtBid
              let ammAmt = if (bidActiveLiq > 0 and levelSqrt > 0 and prevSqrtBid > 0) {
                safeSub(mulDiv(bidActiveLiq, tenToPower60, levelSqrt), mulDiv(bidActiveLiq, tenToPower60, prevSqrtBid));
              } else { 0 };
              Vector.add(bidVec, { price = midPrice * Float.fromInt(factor) / 10000.0; ammAmount = ammAmt; limitAmount = 0; limitOrders = 0 });
              prevSqrtBid := levelSqrt;
            };
          };
        };
        case null {
          // V2 fallback: constant-product depth calculation
          var prevR0Ask = res0;
          for (i in Iter.range(1, maxLevels)) {
            let factor = 10000 + i * step;
            let newR0 = sqrt(res0 * res0 * 10000 / factor);
            let ammAmt = if (prevR0Ask > newR0) { prevR0Ask - newR0 } else { 0 };
            Vector.add(askVec, { price = midPrice * Float.fromInt(factor) / 10000.0; ammAmount = ammAmt; limitAmount = 0; limitOrders = 0 });
            prevR0Ask := newR0;
          };
          var prevR0Bid = res0;
          for (i in Iter.range(1, maxLevels)) {
            if (i * step >= 10000) {
              Vector.add(bidVec, { price = 0.0; ammAmount = 0; limitAmount = 0; limitOrders = 0 });
            } else {
              let factor = 10000 - i * step;
              let newR0 = sqrt(res0 * res0 * 10000 / factor);
              let ammAmt = if (newR0 > prevR0Bid) { newR0 - prevR0Bid } else { 0 };
              Vector.add(bidVec, { price = midPrice * Float.fromInt(factor) / 10000.0; ammAmount = ammAmt; limitAmount = 0; limitOrders = 0 });
              prevR0Bid := newR0;
            };
          };
        };
      };
    } else {
      // No AMM — create levels with derived prices, zero AMM amounts
      for (i in Iter.range(1, maxLevels)) {
        Vector.add(askVec, { price = midPrice * Float.fromInt(10000 + i * step) / 10000.0; ammAmount = 0; limitAmount = 0; limitOrders = 0 });
        if (i * step >= 10000) {
          Vector.add(bidVec, { price = 0.0; ammAmount = 0; limitAmount = 0; limitOrders = 0 });
        } else {
          Vector.add(bidVec, { price = midPrice * Float.fromInt(10000 - i * step) / 10000.0; ammAmount = 0; limitAmount = 0; limitOrders = 0 });
        };
      };
    };

    // Limit orders — ASK side: show at exact prices (separate from AMM buckets)
    switch (Map.get(liqMapSort, hashtt, (token0, token1))) {
      case (null) {};
      case (?tree) {
        let scan = RBTree.scanLimit(tree, compareRatio, #Zero, #Max, #fwd, maxLevels * 20);
        for ((ratio, orders) in scan.results.vals()) {
          switch (ratio) {
            case (#Value(r)) {
              if (r > 0) {
                var amt : Nat = 0;
                var cnt : Nat = 0;
                for (o in orders.vals()) {
                  if (Text.startsWith(o.accesscode, #text "Public")) { amt += o.amount_init; cnt += 1 };
                };
                if (cnt > 0) {
                  let exactPrice = (Float.fromInt(tenToPower120 / r) * Float.fromInt(10 ** dec0)) / (Float.fromInt(tenToPower60) * Float.fromInt(10 ** dec1));
                  if (exactPrice > 0.0) {
                    Vector.add(askVec, { price = exactPrice; ammAmount = 0; limitAmount = amt; limitOrders = cnt });
                  };
                };
              };
            };
            case _ {};
          };
        };
      };
    };

    // Limit orders — BID side: show at exact prices (separate from AMM buckets)
    switch (Map.get(liqMapSort, hashtt, (token1, token0))) {
      case (null) {};
      case (?tree) {
        let scan = RBTree.scanLimit(tree, compareRatio, #Zero, #Max, #fwd, maxLevels * 20);
        for ((ratio, orders) in scan.results.vals()) {
          switch (ratio) {
            case (#Value(r)) {
              if (r > 0) {
                var amt : Nat = 0;
                var cnt : Nat = 0;
                for (o in orders.vals()) {
                  if (Text.startsWith(o.accesscode, #text "Public")) { amt += o.amount_sell; cnt += 1 };
                };
                if (cnt > 0) {
                  let exactPrice = (Float.fromInt(r) * Float.fromInt(10 ** dec0)) / (Float.fromInt(tenToPower60) * Float.fromInt(10 ** dec1));
                  if (exactPrice > 0.0) {
                    Vector.add(bidVec, { price = exactPrice; ammAmount = 0; limitAmount = amt; limitOrders = cnt });
                  };
                };
              };
            };
            case _ {};
          };
        };
      };
    };

    // Sort: asks ascending by price, bids descending by price
    let sortedAsks = Array.sort<{ price : Float; ammAmount : Nat; limitAmount : Nat; limitOrders : Nat }>(
      Vector.toArray(askVec), func(a, b) { Float.compare(a.price, b.price) }
    );
    let sortedBids = Array.sort<{ price : Float; ammAmount : Nat; limitAmount : Nat; limitOrders : Nat }>(
      Vector.toArray(bidVec), func(a, b) { Float.compare(b.price, a.price) }
    );

    // Compute spread from sorted levels
    let bestAskP = if (sortedAsks.size() > 0) { sortedAsks[0].price } else { 0.0 };
    let bestBidP = if (sortedBids.size() > 0) { sortedBids[0].price } else { 0.0 };
    let spreadVal = if (bestBidP > 0.0 and bestAskP > 0.0 and midPrice > 0.0) { (bestAskP - bestBidP) / midPrice } else { 0.0 };

    {
      bids = sortedBids;
      asks = sortedAsks;
      ammMidPrice = midPrice;
      spread = spreadVal;
      ammReserve0 = res0;
      ammReserve1 = res1;
    };
  };

  public query ({ caller }) func getPoolHistory(token1 : Text, token2 : Text, limit : Nat) : async [(Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }])] {
    if (isAllowedQuery(caller) != 1) {
      return [];
    };

    let pool = getPool(token1, token2);

    switch (Map.get(pool_history, hashtt, pool)) {
      case (null) { [] };
      case (?tree) {
        (
          RBTree.scanLimit<Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }]>(
            tree,
            compareTime,
            0,
            tenToPower256,
            #bwd,
            limit,
          )
        ).results;
      };
    };
  };

  // I could also use a for-loop to remove per-entry. However I think this is more efficient. It keeps between 2000 and 3000 of the newest entries and only starts if size is above 4000
  private func trimPoolHistory() {
    for ((poolKey, tree) in Map.entries(pool_history)) {
      let originalSize = RBTree.size(tree);
      if (originalSize > 8000) {
        var trimmedTree = tree;
        var entriesToRemove = originalSize - 5000;
        var iterationCount = 0;
        let maxIterations = 10;

        label a while (entriesToRemove > 0 and RBTree.size(trimmedTree) > 4000 and iterationCount < maxIterations) {
          switch (RBTree.split(trimmedTree, compareTime)) {
            case (?(leftTree, rightTree)) {
              let leftSize = RBTree.size(leftTree);
              if (RBTree.size(rightTree) >= 4000 and leftSize <= entriesToRemove) {
                trimmedTree := rightTree;
                entriesToRemove -= leftSize;
              } else {
                break a;
              };
            };
            case null {

              break a;
            };
          };
          iterationCount += 1;
        };

        if (iterationCount == maxIterations) {

        };

        // Update the tree in the map
        Map.set(pool_history, hashtt, poolKey, trimmedTree);
      };
    };
  };

  // Remove swap history records older than 90 days
  private func trimSwapHistory() {
    let cutoff = Time.now() - 7_776_000_000_000_000; // 90 days in nanoseconds
    let batchSize = 100;

    for ((user, tree) in Map.entries(userSwapHistory)) {
      let oldEntries = RBTree.scanLimit(tree, Int.compare, 0, cutoff, #fwd, batchSize);
      if (oldEntries.results.size() > 0) {
        var trimmed = tree;
        for ((key, _) in oldEntries.results.vals()) {
          trimmed := RBTree.delete(trimmed, Int.compare, key);
        };
        if (RBTree.size(trimmed) == 0) {
          Map.delete(userSwapHistory, phash, user);
        } else {
          Map.set(userSwapHistory, phash, user, trimmed);
        };
      };
    };
  };

  // Remove old trades (older than 30 days)
  private func cleanupOldTrades() : async () {
    let nowVar = Time.now();
    let thirtyDaysAgo = if test { nowVar - (1 * 1_000_000_000) } else {
      nowVar - (30 * 24 * 3600 * 1_000_000_000);
    }; // 30 days in nanoseconds
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var processedCount = 0;
    var continueCleanup = false;

    //RVVR-TACOX-3 - Inefficient Collection Storage- Fix
    // Iterate through BlocksDone in reverse order
    label cleanup for ((blockKey, timestamp) in Map.entriesDesc(BlocksDone)) {
      if (timestamp < thirtyDaysAgo) {
        if (processedCount >= 4000) {
          continueCleanup := true;
          break cleanup;
        };

        // Remove old entry
        Map.delete(BlocksDone, thash, blockKey);

        processedCount += 1;
      } else {
        // Stop if we've reached entries younger than 30 days
        break cleanup;
      };
    };
    if (not continueCleanup) {

      // Scan the timeBasedTrades tree for old trades
      let oldTrades = RBTree.scanLimit(
        timeBasedTrades,
        compareTime,
        0, // start from the earliest time
        thirtyDaysAgo,
        #fwd,
        4001,
      );

      label a for ((timestamp, accesscodes) in oldTrades.results.vals()) {
        label b for (accesscode in accesscodes.vals()) {
          if (processedCount >= 4000) {
            continueCleanup := true;
            break a;
          };
          if (TrieSet.contains(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal)) {
            continue b;
          };

          var trade : ?TradePrivate = null;
          if (Text.startsWith(accesscode, #text "Public")) {
            trade := Map.get(tradeStorePublic, thash, accesscode);
          } else {
            trade := Map.get(tradeStorePrivate, thash, accesscode);
          };

          switch (trade) {
            case (null) {
              // Trade not found, remove from timeBasedTrades
              removeTrade(accesscode, "", ("", "")); // Use empty strings as we don't have the correct information
            };
            case (?t) {
              // Process the trade for settlement
              if (t.trade_done == 0) {
                // Trade is not completed, settle it
                let RevokeFee = t.RevokeFee;

                if (t.init_paid == 1) {
                  // Refund the initiator
                  let refundAmount = t.amount_init + (((t.amount_init * t.Fee) / (10000 * RevokeFee)) * (RevokeFee - 1));
                  Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(t.initPrincipal)), refundAmount, t.token_init_identifier, genTxId()));
                };

                if (t.seller_paid == 1) {
                  // Refund the seller
                  let refundAmount = t.amount_sell + (((t.amount_sell * t.Fee) / (10000 * RevokeFee)) * (RevokeFee - 1));
                  Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(t.SellerPrincipal)), refundAmount, t.token_sell_identifier, genTxId()));
                };
              };

              // Remove the trade from all data structures
              removeTrade(accesscode, t.initPrincipal, (t.token_init_identifier, t.token_sell_identifier));

              // Call replaceLiqMap to update liquidity map
              replaceLiqMap(
                true, // del
                false, // copyFee
                t.token_init_identifier,
                t.token_sell_identifier,
                accesscode,
                (t.amount_init, t.amount_sell, t.Fee, t.RevokeFee, t.initPrincipal, t.OCname, t.time, t.token_init_identifier, t.token_sell_identifier, t.strictlyOTC, t.allOrNothing),
                #Zero,
                null,
                null,
              );

              processedCount += 1;
            };
          };
        };
      };

      // Update the exchange info
      doInfoBeforeStep2();

      // Transferring the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), false) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
    };

    // If there are more trades to process, set a timer to run cleanupOldTrades again
    if (continueCleanup) {
      ignore setTimer(
        #seconds(fuzz.nat.randomRange(30, 60)),
        func() : async () {
          await cleanupOldTrades();
        },
      );
    };
  };

  public shared ({ caller }) func claimFeesReferrer() : async [(Text, Nat)] {

    if (isAllowed(caller) != 1) {

      return [];
    };
    let nowVar = Time.now();
    let referrer = Principal.toText(caller);

    switch (Map.get(referrerFeeMap, thash, referrer)) {
      case (null) {

        return [];
      };
      case (??(fees, oldTime)) {

        let feesToClaim = Vector.toArray(fees);

        let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
        for ((token, amount) in feesToClaim.vals()) {
          let Tfees = returnTfees(token);

          if (amount > Tfees) {
            Vector.add(tempTransferQueueLocal, (#principal(caller), amount - Tfees, token, genTxId()));

          } else {
            addFees(token, amount, false, "", nowVar);

          };
        };
        let newFees = Vector.new<(Text, Nat)>();
        Map.set(referrerFeeMap, thash, referrer, ?(newFees, nowVar));

        // Update lastFeeAdditionByTime
        lastFeeAdditionByTime := RBTree.put(RBTree.delete(lastFeeAdditionByTime, compareTextTime, (referrer, oldTime)), compareTextTime, (referrer, nowVar), null);

        // RVVR-TACOX-6: Attempt transfer, queue if fails
        if ((
          try {

            await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller));
          } catch (err) {

            false;
          }
        )) {

          return feesToClaim;
        } else {

          Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
          return feesToClaim;
        };
      };
      case (?null) {

        return [];
      };
    };
  };

  public query ({ caller }) func checkFeesReferrer() : async [(Text, Nat)] {
    if (isAllowedQuery(caller) != 1) {
      return [];
    };
    let referrer = Principal.toText(caller);

    switch (Map.get(referrerFeeMap, thash, referrer)) {
      case (null) {
        // No fees available
        return [];
      };
      case (??(fees, _)) {
        return Vector.toArray(fees);
      };
      case (?null) {
        // No fees available
        return [];
      };
    };
  };

  public query ({ caller }) func getUserReferralInfo() : async {
    hasReferrer : Bool;
    referrer : ?Text;
    isFirstTrade : Bool;
    referralEarnings : [(Text, Nat)];
  } {
    if (isAllowedQuery(caller) != 1) {
      return { hasReferrer = false; referrer = null; isFirstTrade = false; referralEarnings = [] };
    };
    let principal = Principal.toText(caller);

    let (hasRef, ref, isFirst) = switch (Map.get(userReferrerLink, thash, principal)) {
      case (null) { (false, null, true) };
      case (?null) { (false, null, false) };
      case (??r) { (true, ?r, false) };
    };

    let earnings : [(Text, Nat)] = switch (Map.get(referrerFeeMap, thash, principal)) {
      case (null) { [] };
      case (?null) { [] };
      case (??(fees, _)) { Vector.toArray(fees) };
    };

    { hasReferrer = hasRef; referrer = ref; isFirstTrade = isFirst; referralEarnings = earnings };
  };

  func trimOldReferralFees<system>() {
    let nowVar = Time.now();
    let twoMonthsAgo = nowVar - 2 * 30 * 24 * 60 * 60 * 1000000000; // 2 months in nanoseconds

    let oldEntries = RBTree.scanLimit(
      lastFeeAdditionByTime,
      compareTextTime,
      ("", 0), // Start from the beginning
      ("", twoMonthsAgo), // Up to 2 months ago
      #fwd,
      1000 // Limit to prevent too long execution
    );

    for (((referrer, time), _) in oldEntries.results.vals()) {
      // Remove from lastFeeAdditionByTime
      lastFeeAdditionByTime := RBTree.delete(lastFeeAdditionByTime, compareTextTime, (referrer, time));

      // Remove from referrerFeeMap
      let toAdd : [(Text, Nat)] = switch (Map.remove(referrerFeeMap, thash, referrer)) {
        case null { [] };
        case (??a) { Vector.toArray(a.0) };
        case (?null) { [] };
      };
      for ((token, amount) in toAdd.vals()) {
        addFees(token, amount, false, "", nowVar);
      };

    };

    // If we hit the limit, schedule another run
    if (oldEntries.results.size() == 1000) {
      ignore setTimer<system>(
        #seconds(fuzz.nat.randomRange(30, 60)), // Run again after 1 minute
        func() : async () {
          trimOldReferralFees<system>();
        },

      );
    };
  };

  public query ({ caller }) func getExpectedReceiveAmount(
    tokenSell : Text,
    tokenBuy : Text,
    amountSell : Nat,
  ) : async {
    expectedBuyAmount : Nat;
    fee : Nat;
    priceImpact : Float;
    routeDescription : Text;
    canFulfillFully : Bool;
    potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
    hopDetails : [HopDetail];
  } {
    if (isAllowedQuery(caller) != 1) {
      return {
        expectedBuyAmount = 0;
        fee = 0;
        priceImpact = 0;
        routeDescription = "Query not allowed";
        canFulfillFully = false;
        potentialOrderDetails = null;
        hopDetails = [];
      };
    };
    let nowVar = Time.now();

    // Snapshot pre-swap reserves AND V3 sqrtRatio for accurate price impact calculation
    // (orderPairing mutates both AMMpools and poolV3Data in-place, even in query context)
    let preSwapPoolKey = getPool(tokenSell, tokenBuy);
    let preSwapReserves : ?(Nat, Nat) = switch (Map.get(AMMpools, hashtt, preSwapPoolKey)) {
      case (?pool) {
        let (rIn, rOut) = if (pool.token0 == tokenSell) { (pool.reserve0, pool.reserve1) } else { (pool.reserve1, pool.reserve0) };
        ?(rIn, rOut);
      };
      case null { null };
    };
    let preSwapV3Ratio : Nat = getPoolRatioV3(preSwapPoolKey);

    let dummyTrade : TradePrivate = {
      Fee = ICPfee;
      amount_sell = 0; // This will be filled by orderPairing
      amount_init = amountSell;
      token_sell_identifier = tokenBuy;
      token_init_identifier = tokenSell;
      trade_done = 0;
      seller_paid = 0;
      init_paid = 1;
      trade_number = 0;
      SellerPrincipal = "0";
      initPrincipal = Principal.toText(caller);
      seller_paid2 = 0;
      init_paid2 = 0;
      RevokeFee = RevokeFeeNow;
      OCname = "";
      time = nowVar;
      filledInit = 0;
      filledSell = 0;
      allOrNothing = false;
      strictlyOTC = false;
    };

    let (remainingAmountInit, totalProtocolFeeAmount, totalPoolFeeAmount, transactions, _, _, ammAmountIn) = orderPairing(dummyTrade);

    let amountFilled = if (amountSell > remainingAmountInit) {
      amountSell - remainingAmountInit;
    } else { 0 };
    var expectedBuyAmount : Nat = 0;

    // Calculate expectedBuyAmount from the transactions
    for (transaction in transactions.vals()) {
      if (transaction.0 == #principal(caller) and transaction.2 == tokenBuy) {
        expectedBuyAmount += transaction.1;
      }; // transaction.1 should be the amount received
    };

    var totalFee = totalProtocolFeeAmount + totalPoolFeeAmount;

    // Always compare direct vs multi-hop to find the best route
    var multiHopUsed = false;
    var multiHopRoute : [SwapHop] = [];
    var multiHopDetails : [HopDetail] = [];
    let routes = findRoutes(tokenSell, tokenBuy, amountSell);
    label routeSearch for (r in routes.vals()) {
      if (r.hops.size() <= 1) continue routeSearch;
      let sim = simulateMultiHop(r.hops, amountSell, caller);
      if (sim.amountOut > expectedBuyAmount) {
        expectedBuyAmount := sim.amountOut;
        totalFee := sim.totalFees;
        multiHopUsed := true;
        multiHopRoute := r.hops;
        multiHopDetails := sim.hopDetails;
      };
      break routeSearch;
    };

    let priceImpact = if (expectedBuyAmount > 0 and amountSell > 10000) {
      if (multiHopUsed) {
        // Multi-hop price impact: sum per-hop impacts from simulation
        var totalMHImpact = 0.0;
        for (hd in multiHopDetails.vals()) {
          totalMHImpact += hd.priceImpact;
        };
        totalMHImpact;
      } else {
        // V3-aware overall price impact: (actual output) vs (spot × full requested input).
        // Uses pre-swap snapshots — orderPairing already mutated post-swap state.
        computeBlendedAmmImpact(preSwapPoolKey, tokenSell, expectedBuyAmount, amountSell, preSwapV3Ratio, preSwapReserves);
      };
    } else {
      0.0;
    };

    let routeDescription = if (multiHopUsed) {
      var desc = "Multi-hop (" # Nat.toText(multiHopRoute.size()) # " hops): " # tokenSell;
      for (hop in multiHopRoute.vals()) {
        desc := desc # " → " # hop.tokenOut;
      };
      desc;
    } else if (expectedBuyAmount > 0) {
      if (totalPoolFeeAmount > 0) {
        if (totalProtocolFeeAmount > 0) {
          "AMM and Orderbook";
        } else {
          "AMM only";
        };
      } else {
        "Orderbook only";
      };
    } else {
      "No liquidity available";
    };

    let canFulfillFully = if (multiHopUsed) { expectedBuyAmount > 0 } else { remainingAmountInit < 10001 };
    let potentialOrderDetails = if (not canFulfillFully and expectedBuyAmount > 0) {
      ?{ amount_init = amountSell; amount_sell = expectedBuyAmount };
    } else {
      null;
    };

    {
      expectedBuyAmount = expectedBuyAmount;
      fee = totalFee;
      priceImpact = priceImpact;
      routeDescription = routeDescription;
      canFulfillFully = canFulfillFully;
      potentialOrderDetails = potentialOrderDetails;
      hopDetails = multiHopDetails;
    };
  };

  // Batch quote: get expected receive amounts for multiple (tokenSell, tokenBuy, amount) tuples in ONE call.
  // Replaces 10 individual getExpectedReceiveAmount calls with 1 inter-canister round-trip.
  // Max 20 quotes per call to bound cycle cost.
  // Mirrors getExpectedReceiveAmount logic exactly for each request.
  public query ({ caller }) func getExpectedReceiveAmountBatch(
    requests : [{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }],
  ) : async [{
    expectedBuyAmount : Nat;
    fee : Nat;
    priceImpact : Float;
    routeDescription : Text;
    canFulfillFully : Bool;
    potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
    hopDetails : [HopDetail];
  }] {
    if (isAllowedQuery(caller) != 1 or requests.size() > 20) { return [] };
    let nowVar = Time.now();

    // Snapshot ALL global state mutated by orderPairing so each batch request runs
    // against the SAME pre-swap pool state — without this, request N is biased by
    // the cumulative mutations of requests 0..N-1 within the same query call.
    let initialSnapshot = snapshotQuoteState();

    let results = Vector.new<{
      expectedBuyAmount : Nat; fee : Nat; priceImpact : Float;
      routeDescription : Text; canFulfillFully : Bool;
      potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
      hopDetails : [HopDetail];
    }>();

    for (req in requests.vals()) {
      // Restore to the pre-batch state before each request (no-op for first iteration)
      restoreQuoteState(initialSnapshot);
      let tokenSell = req.tokenSell;
      let tokenBuy = req.tokenBuy;
      let amountSell = req.amountSell;

      if (amountSell == 0) {
        Vector.add(results, {
          expectedBuyAmount = 0; fee = 0; priceImpact = 0.0;
          routeDescription = ""; canFulfillFully = false;
          potentialOrderDetails = null; hopDetails = [];
        });
      } else {
        // ── Mirror of getExpectedReceiveAmount body ──
        // Snapshot pre-swap reserves AND V3 sqrtRatio for accurate price impact
        let preSwapPoolKeyB = getPool(tokenSell, tokenBuy);
        let preSwapReservesB : ?(Nat, Nat) = switch (Map.get(AMMpools, hashtt, preSwapPoolKeyB)) {
          case (?pool) {
            let (rIn, rOut) = if (pool.token0 == tokenSell) { (pool.reserve0, pool.reserve1) } else { (pool.reserve1, pool.reserve0) };
            ?(rIn, rOut);
          };
          case null { null };
        };
        let preSwapV3RatioB : Nat = getPoolRatioV3(preSwapPoolKeyB);

        let dummyTrade : TradePrivate = {
          Fee = ICPfee;
          amount_sell = 0;
          amount_init = amountSell;
          token_sell_identifier = tokenBuy;
          token_init_identifier = tokenSell;
          trade_done = 0;
          seller_paid = 0;
          init_paid = 1;
          trade_number = 0;
          SellerPrincipal = "0";
          initPrincipal = Principal.toText(caller);
          seller_paid2 = 0;
          init_paid2 = 0;
          RevokeFee = RevokeFeeNow;
          OCname = "";
          time = nowVar;
          filledInit = 0;
          filledSell = 0;
          allOrNothing = false;
          strictlyOTC = false;
        };

        let (remainingAmountInit, totalProtocolFeeAmount, totalPoolFeeAmount, transactions, _, _, ammAmountIn) = orderPairing(dummyTrade);

        var expectedBuyAmount : Nat = 0;
        for (transaction in transactions.vals()) {
          if (transaction.0 == #principal(caller) and transaction.2 == tokenBuy) {
            expectedBuyAmount += transaction.1;
          };
        };

        var totalFee = totalProtocolFeeAmount + totalPoolFeeAmount;

        var multiHopUsed = false;
        var multiHopRoute : [SwapHop] = [];
        var multiHopDetails : [HopDetail] = [];
        // Restore BEFORE findRoutes + simulateMultiHop — otherwise route enumeration
        // and simulation read post-direct-swap state, biasing the multi-hop pick.
        restoreQuoteState(initialSnapshot);
        let routes = findRoutes(tokenSell, tokenBuy, amountSell);
        label routeSearch for (r in routes.vals()) {
          if (r.hops.size() <= 1) continue routeSearch;
          let sim = simulateMultiHop(r.hops, amountSell, caller);
          if (sim.amountOut > expectedBuyAmount) {
            expectedBuyAmount := sim.amountOut;
            totalFee := sim.totalFees;
            multiHopUsed := true;
            multiHopRoute := r.hops;
            multiHopDetails := sim.hopDetails;
          };
          break routeSearch;
        };

        let priceImpact = if (expectedBuyAmount > 0 and amountSell > 10000) {
          if (multiHopUsed) {
            var totalMHImpact = 0.0;
            for (hd in multiHopDetails.vals()) { totalMHImpact += hd.priceImpact };
            totalMHImpact;
          } else {
            // V3-aware overall price impact against full requested amount (partial fills surface as impact)
            computeBlendedAmmImpact(preSwapPoolKeyB, tokenSell, expectedBuyAmount, amountSell, preSwapV3RatioB, preSwapReservesB);
          };
        } else { 0.0 };

        let routeDescription = if (multiHopUsed) {
          var desc = "Multi-hop (" # Nat.toText(multiHopRoute.size()) # " hops): " # tokenSell;
          for (hop in multiHopRoute.vals()) { desc := desc # " → " # hop.tokenOut };
          desc;
        } else if (expectedBuyAmount > 0) {
          if (totalPoolFeeAmount > 0) {
            if (totalProtocolFeeAmount > 0) { "AMM and Orderbook" } else { "AMM only" };
          } else { "Orderbook only" };
        } else { "No liquidity available" };

        let canFulfillFully = if (multiHopUsed) { expectedBuyAmount > 0 } else { remainingAmountInit < 10001 };
        let potentialOrderDetails = if (not canFulfillFully and expectedBuyAmount > 0) {
          ?{ amount_init = amountSell; amount_sell = expectedBuyAmount };
        } else { null };

        Vector.add(results, {
          expectedBuyAmount = expectedBuyAmount;
          fee = totalFee;
          priceImpact = priceImpact;
          routeDescription = routeDescription;
          canFulfillFully = canFulfillFully;
          potentialOrderDetails = potentialOrderDetails;
          hopDetails = multiHopDetails;
        });
      };
    };

    Vector.toArray(results);
  };

  // Per-request multi-route batch quote: returns the TOP N routes per request, not just
  // the canister's single-best pick. Lets the frontend construct cross-route splits
  // (e.g. 50% via direct + 50% via 2-hop) which the single-best batch endpoint can't
  // express because it silently drops the alternatives. Each request runs against the
  // SAME pre-batch state via snapshot+restore — both between requests and between the
  // direct/multi-hop simulations within a single request.
  public query ({ caller }) func getExpectedReceiveAmountBatchMulti(
    requests : [{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }],
    maxRoutesPerRequest : Nat,
  ) : async [{
    routes : [{
      expectedBuyAmount : Nat;
      fee : Nat;
      priceImpact : Float;
      routeDescription : Text;
      canFulfillFully : Bool;
      potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
      hopDetails : [HopDetail];
      routeTokens : [Text]; // [tokenSell, …intermediates, tokenBuy] for cross-fraction matching
      tradingFeeBps : Nat;  // snapshot of ICPfee for the simulation that produced this route
    }];
  }] {
    if (isAllowedQuery(caller) != 1 or requests.size() > 20) { return [] };
    let cap : Nat = if (maxRoutesPerRequest == 0) { 5 }
                    else if (maxRoutesPerRequest > 10) { 10 }
                    else { maxRoutesPerRequest };
    let nowVar = Time.now();
    let initialSnapshot = snapshotQuoteState();

    type QuoteRoute = {
      expectedBuyAmount : Nat;
      fee : Nat;
      priceImpact : Float;
      routeDescription : Text;
      canFulfillFully : Bool;
      potentialOrderDetails : ?{ amount_init : Nat; amount_sell : Nat };
      hopDetails : [HopDetail];
      routeTokens : [Text];
      tradingFeeBps : Nat;
    };
    let allResults = Vector.new<{ routes : [QuoteRoute] }>();

    for (req in requests.vals()) {
      let tokenSell = req.tokenSell;
      let tokenBuy = req.tokenBuy;
      let amountSell = req.amountSell;

      if (amountSell == 0) {
        Vector.add(allResults, { routes = [] });
      } else {
        let perRequestRoutes = Vector.new<QuoteRoute>();

        // ── Route 1: direct (single-hop via orderPairing — AMM + orderbook combined) ──
        restoreQuoteState(initialSnapshot);
        let preSwapPoolKey = getPool(tokenSell, tokenBuy);
        let preSwapReserves : ?(Nat, Nat) = switch (Map.get(AMMpools, hashtt, preSwapPoolKey)) {
          case (?pool) {
            let (rIn, rOut) = if (pool.token0 == tokenSell) { (pool.reserve0, pool.reserve1) } else { (pool.reserve1, pool.reserve0) };
            ?(rIn, rOut);
          };
          case null { null };
        };
        let preSwapV3Ratio : Nat = getPoolRatioV3(preSwapPoolKey);

        let dummyTrade : TradePrivate = {
          Fee = ICPfee;
          amount_sell = 0;
          amount_init = amountSell;
          token_sell_identifier = tokenBuy;
          token_init_identifier = tokenSell;
          trade_done = 0;
          seller_paid = 0;
          init_paid = 1;
          trade_number = 0;
          SellerPrincipal = "0";
          initPrincipal = Principal.toText(caller);
          seller_paid2 = 0;
          init_paid2 = 0;
          RevokeFee = RevokeFeeNow;
          OCname = "";
          time = nowVar;
          filledInit = 0;
          filledSell = 0;
          allOrNothing = false;
          strictlyOTC = false;
        };
        let (remainingAmountInit, totalProtocolFeeAmount, totalPoolFeeAmount, transactions, _, _, ammAmountIn) = orderPairing(dummyTrade);

        var directOut : Nat = 0;
        for (tx in transactions.vals()) {
          if (tx.0 == #principal(caller) and tx.2 == tokenBuy) { directOut += tx.1 };
        };

        if (directOut > 0) {
          let directImpact = if (amountSell > 10000) {
            computeBlendedAmmImpact(preSwapPoolKey, tokenSell, directOut, amountSell, preSwapV3Ratio, preSwapReserves);
          } else { 0.0 };
          let directFulfillFully = remainingAmountInit < 10001;
          let directPotential = if (not directFulfillFully and directOut > 0) {
            ?{ amount_init = amountSell; amount_sell = directOut };
          } else { null };
          let directDesc = if (totalPoolFeeAmount > 0 and totalProtocolFeeAmount > 0) { "AMM and Orderbook" }
                           else if (totalPoolFeeAmount > 0) { "AMM only" }
                           else { "Orderbook only" };
          Vector.add(perRequestRoutes, {
            expectedBuyAmount = directOut;
            fee = totalProtocolFeeAmount + totalPoolFeeAmount;
            priceImpact = directImpact;
            routeDescription = directDesc;
            canFulfillFully = directFulfillFully;
            potentialOrderDetails = directPotential;
            hopDetails = [];
            routeTokens = [tokenSell, tokenBuy];
            tradingFeeBps = ICPfee;
          });
        };

        // ── Routes 2..N: multi-hop alternatives (each via simulateMultiHop) ──
        // Restore BEFORE findRoutes too — its internal simulateSwap reads AMMpools/poolV3Data
        // for route enumeration, so a stale post-direct-swap snapshot would bias route picks.
        restoreQuoteState(initialSnapshot);
        let candidateRoutes = findRoutes(tokenSell, tokenBuy, amountSell);
        for (r in candidateRoutes.vals()) {
          if (r.hops.size() > 1) {
            restoreQuoteState(initialSnapshot);
            let sim = simulateMultiHop(r.hops, amountSell, caller);
            if (sim.amountOut > 0) {
              var totalMHImpact = 0.0;
              for (hd in sim.hopDetails.vals()) { totalMHImpact += hd.priceImpact };
              let tokenList = Vector.new<Text>();
              Vector.add(tokenList, tokenSell);
              for (hop in r.hops.vals()) { Vector.add(tokenList, hop.tokenOut) };
              var mhDesc = "Multi-hop (" # Nat.toText(r.hops.size()) # " hops): " # tokenSell;
              for (hop in r.hops.vals()) { mhDesc := mhDesc # " → " # hop.tokenOut };
              Vector.add(perRequestRoutes, {
                expectedBuyAmount = sim.amountOut;
                fee = sim.totalFees;
                priceImpact = totalMHImpact;
                routeDescription = mhDesc;
                canFulfillFully = true;
                potentialOrderDetails = null;
                hopDetails = sim.hopDetails;
                routeTokens = Vector.toArray(tokenList);
                tradingFeeBps = ICPfee;
              });
            };
          };
        };

        // Sort by expectedBuyAmount desc, take top `cap`
        let sorted = Array.sort<QuoteRoute>(Vector.toArray(perRequestRoutes), func(a, b) {
          if (a.expectedBuyAmount > b.expectedBuyAmount) { #less }
          else if (a.expectedBuyAmount < b.expectedBuyAmount) { #greater }
          else { #equal };
        });
        let takeN : Nat = Nat.min(sorted.size(), cap);
        let topN = Array.tabulate<QuoteRoute>(takeN, func(i) { sorted[i] });

        Vector.add(allResults, { routes = topN });
      };
    };

    // Final restore — defensive; IC reverts at query end anyway.
    restoreQuoteState(initialSnapshot);

    Vector.toArray(allResults);
  };

  // Simulate a multi-leg split — returns the EXACT combined output and per-leg outputs
  // a real `swapSplitRoutes` call would produce against the current pool/orderbook
  // state. Mirrors `swapSplitRoutes`'s pre-check loop verbatim: same `simulateMultiHop`
  // chained sequentially, mutations propagating from leg N to leg N+1, restored at end.
  // Treasury (or any caller) can use this to know the realistic split output BEFORE
  // submitting — instead of trying to derive it from per-route independent quotes.
  public query ({ caller }) func simulateSplitRoutes(
    splits : [{ amountIn : Nat; route : [SwapHop] }],
  ) : async { totalOut : Nat; perLegOut : [Nat]; error : Text } {
    if (isAllowedQuery(caller) != 1) {
      return { totalOut = 0; perLegOut = []; error = "Not authorized" };
    };
    if (splits.size() == 0 or splits.size() > 3) {
      return { totalOut = 0; perLegOut = []; error = "1-3 splits required" };
    };

    let snap = snapshotQuoteState();
    var total : Nat = 0;
    var err : Text = "";
    let perLegBuf = Vector.new<Nat>();

    label simLoop for (legIdx in Iter.range(0, splits.size() - 1)) {
      let leg = splits[legIdx];
      if (leg.route.size() == 0 or leg.route.size() > 3) {
        err := "Leg " # Nat.toText(legIdx) # ": 1-3 hops required";
        Vector.add(perLegBuf, 0);
        break simLoop;
      };
      let res = simulateMultiHop(leg.route, leg.amountIn, caller);
      Vector.add(perLegBuf, res.amountOut);
      if (res.amountOut == 0) {
        err := "Leg " # Nat.toText(legIdx) # ": zero output at simulation";
        break simLoop;
      };
      total += res.amountOut;
    };

    restoreQuoteState(snap);
    { totalOut = total; perLegOut = Vector.toArray(perLegBuf); error = err }
  };

  // Multi-hop route discovery (query = free on ICP).
  // Finds the best 1-3 hop route between any two tokens using AMM + orderbook liquidity.
  public query ({ caller }) func getExpectedMultiHopAmount(
    tokenIn : Text,
    tokenOut : Text,
    amountIn : Nat,
  ) : async {
    bestRoute : [SwapHop];
    expectedAmountOut : Nat;
    totalFee : Nat;
    priceImpact : Float;
    hops : Nat;
    routeTokens : [Text];
    hopDetails : [HopDetail];
  } {
    let emptyResult = {
      bestRoute : [SwapHop] = [];
      expectedAmountOut = 0;
      totalFee = 0;
      priceImpact = 0.0;
      hops = 0;
      routeTokens : [Text] = [];
      hopDetails : [HopDetail] = [];
    };
    if (isAllowedQuery(caller) != 1) { return emptyResult };

    let routes = findRoutes(tokenIn, tokenOut, amountIn);
    if (routes.size() == 0) { return emptyResult };

    // Full hybrid simulation on the best route (by AMM estimate)
    let best = routes[0];
    let sim = simulateMultiHop(best.hops, amountIn, caller);
    var finalRoute = best.hops;
    var finalOut = sim.amountOut;
    var finalFee = sim.totalFees;
    var finalHopDetails = sim.hopDetails;

    // Also try 2nd best if it exists (AMM ranking might differ from hybrid)
    if (routes.size() > 1) {
      let sim2 = simulateMultiHop(routes[1].hops, amountIn, caller);
      if (sim2.amountOut > finalOut) {
        finalRoute := routes[1].hops;
        finalOut := sim2.amountOut;
        finalFee := sim2.totalFees;
        finalHopDetails := sim2.hopDetails;
      };
    };

    // Price impact: compare against a small swap to get spot rate
    // Price impact: sum per-hop mathematical impacts from hopDetails
    let priceImpact = if (finalOut > 0 and amountIn > 0) {
      var totalImpact = 0.0;
      for (hd in finalHopDetails.vals()) {
        totalImpact += hd.priceImpact;
      };
      totalImpact;
    } else { 0.0 };

    // Build route token list for display
    let tokenList = Vector.new<Text>();
    Vector.add(tokenList, tokenIn);
    for (hop in finalRoute.vals()) {
      Vector.add(tokenList, hop.tokenOut);
    };

    {
      bestRoute = finalRoute;
      expectedAmountOut = finalOut;
      totalFee = finalFee;
      priceImpact;
      hops = finalRoute.size();
      routeTokens = Vector.toArray(tokenList);
      hopDetails = finalHopDetails;
    };
  };

  public shared ({ caller }) func addLiquidity(token0i : Text, token1i : Text, amount0i : Nat, amount1i : Nat, block0i : Nat, block1i : Nat, isInitial : ?Bool) : async ExTypes.AddLiquidityResult {
    // isInitial is a hint from the caller that this is a dust-pool / initial-creation deposit.
    // Exchange currently auto-detects dust pools via reserve checks; the flag is accepted for
    // future use and interface compatibility with the treasury wrapper.
    ignore isInitial;
    if (isAllowed(caller) != 1) {
      return #Err(#NotAuthorized);
    };
    if (Text.size(token0i) > 150 or Text.size(token1i) > 150) {
      return #Err(#Banned);
    };

    if (token0i == token1i) {
      return #Err(#InvalidInput("token0 and token1 must be different"));
    };

    let (token0, token1) = getPool(token0i, token1i);
    let tType0 = returnType(token0);
    let tType1 = returnType(token1);
    let poolKey = (token0, token1);
    var amount1 = amount1i;
    var amount0 = amount0i;
    var block0 = block0i;
    var block1 = block1i;
    if (token1i != token1) {
      amount1 := amount0i;
      amount0 := amount1i;
      block0 := block1i;
      block1 := block0i;
    };
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var nowVar = Time.now();
    // Check if the amounts are allowed to be traded (not paused, at least the minimum amount)
    if (
      ((switch (Array.find<Text>(pausedTokens, func(t) { t == token0 })) { case null { false }; case (?_) { true } })) or ((switch (Array.find<Text>(pausedTokens, func(t) { t == token1 })) { case null { false }; case (?_) { true } })) or ((returnMinimum(token0, amount0, true) and returnMinimum(token1, amount1, true)) == false)
    ) {
      label a for ((token, Block, amount, tType) in ([(token1, block1, amount1, tType1), (token0, block0, amount0, tType0)]).vals()) {
        if (Map.has(BlocksDone, thash, token # ":" #Nat.toText(Block))) {
          continue a;
        };
        Map.set(BlocksDone, thash, token # ":" #Nat.toText(Block), nowVar);
        let blockData = try {
          await* getBlockData(if (token == token0) { token0 } else { token1 }, if (token == token0) { block0 } else { block1 }, tType);
        } catch (err) {
          Map.delete(BlocksDone, thash, token # ":" #Nat.toText(Block));
          continue a;
          #ICRC12([]);
        };
        nowVar := Time.now();

        let nowVar2 = nowVar;

        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
      };
      // Transfering the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {

      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#TokenPaused("Token paused or below minimum"));
    };

    // Pre-flight check — if the pool needs creation/recreation and the amounts
    // are below MINIMUM_LIQUIDITY, fail BEFORE accepting any deposits. This prevents
    // the critical bug where pool math fails AFTER tokens are accepted, losing both.
    let prePoolMinLiq0 = if (not (TrieSet.contains(AMMMinimumLiquidityDone, token0, Text.hash(token0), Text.equal))) { minimumLiquidity } else { 0 };
    let prePoolMinLiq1 = if (not (TrieSet.contains(AMMMinimumLiquidityDone, token1, Text.hash(token1), Text.equal))) { minimumLiquidity } else { 0 };
    let existingPoolPre = Map.get(AMMpools, hashtt, poolKey);
    let needsNewPool = switch (existingPoolPre) {
      case null { true };
      case (?p) {
        not (returnMinimum(token0, p.reserve0, false) and returnMinimum(token1, p.reserve1, false) and p.reserve0 > 0 and p.reserve1 > 0);
      };
    };
    if (needsNewPool and (amount0 < prePoolMinLiq0 or amount1 < prePoolMinLiq1)) {
      // DRIFT FIX: refund both tokens before rejecting. Tokens were transferred
      // on-chain to the exchange; without refund they strand (treasury ledger
      // grows, nothing in ord/amm/fee buckets claims them → drift).
      label a for ((token, Block, tType) in ([(token1, block1, tType1), (token0, block0, tType0)]).vals()) {
        if (Map.has(BlocksDone, thash, token # ":" # Nat.toText(Block))) { continue a };
        Map.set(BlocksDone, thash, token # ":" # Nat.toText(Block), nowVar);
        let blockData = try { await* getBlockData(token, Block, tType) } catch (_) {
          Map.delete(BlocksDone, thash, token # ":" # Nat.toText(Block));
          continue a;
          #ICRC12([]);
        };
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token, ICPfee, RevokeFeeNow, true, true, blockData, tType, Time.now())).1.vals());
      };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InsufficientFunds("Amounts below minimum liquidity for new pool (pre-check)"));
    };

    var receiveBool = true;
    // Track per-token acceptance so we can explicitly refund tokens that were
    // accepted when the OTHER token's validation failed. checkReceive with exact amount
    // generates NO refund transfer — an accepted token would be stuck otherwise.
    var token0Accepted = false;
    var token1Accepted = false;
    let receiveTransfersVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    label a for ((token, Block, amount, tType) in ([(token1, block1, amount1, tType1), (token0, block0, amount0, tType0)]).vals()) {
      if (Map.has(BlocksDone, thash, token # ":" #Nat.toText(Block))) {
        receiveBool := false;
        continue a;
      };
      Map.set(BlocksDone, thash, token # ":" #Nat.toText(Block), nowVar);
      let blockData = try {
        await* getBlockData(if (token == token0) { token0 } else { token1 }, if (token == token0) { block0 } else { block1 }, tType);
      } catch (err) {
        Map.delete(BlocksDone, thash, token # ":" #Nat.toText(Block));
        continue a;
        #ICRC12([]);
      };

      let nowVar2 = nowVar;

      let receiveData = checkReceive(Block, caller, amount, token, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2);
      Vector.addFromIter(receiveTransfersVec, receiveData.1.vals());
      let thisResult = receiveData.0;
      if (not thisResult) {
        logger.error("addLiquidity", "checkReceive FAILED for token=" # token # " block=" # Nat.toText(Block) # " amount=" # Nat.toText(amount) # " tType=" # debug_show(tType) # " caller=" # Principal.toText(caller), "addLiquidity");
      } else {
        if (token == token0) { token0Accepted := true } else { token1Accepted := true };
      };
      receiveBool := receiveBool and thisResult;
    };

    Vector.addFromIter(tempTransferQueueLocal, Vector.vals(receiveTransfersVec));
    if (not receiveBool) {
      logger.error("addLiquidity", "receiveBool=false token0=" # token0 # " token1=" # token1 # " amt0=" # Nat.toText(amount0) # " amt1=" # Nat.toText(amount1) # " blk0=" # Nat.toText(block0) # " blk1=" # Nat.toText(block1), "addLiquidity");
      // Explicitly refund any accepted token to prevent one-sided deposit loss.
      // checkReceive only generates refund transfers for overpayment; exact amounts produce
      // no transfers, leaving accepted tokens stuck. Queue explicit refunds here.
      if (token0Accepted and amount0 > returnTfees(token0)) {
        Vector.add(tempTransferQueueLocal, (#principal(caller), amount0 - returnTfees(token0), token0, genTxId()));
      };
      if (token1Accepted and amount1 > returnTfees(token1)) {
        Vector.add(tempTransferQueueLocal, (#principal(caller), amount1 - returnTfees(token1), token1, genTxId()));
      };
      // Transfering the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {

      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InsufficientFunds("Deposit not received"));
    };

    var deleteOld = false;
    let MINIMUM_LIQUIDITY0 = if (not (TrieSet.contains(AMMMinimumLiquidityDone, token0, Text.hash(token0), Text.equal))) {
      AMMMinimumLiquidityDone := TrieSet.put(AMMMinimumLiquidityDone, token0, Text.hash(token0), Text.equal);
      minimumLiquidity;
    } else { 0 };
    let MINIMUM_LIQUIDITY1 = if (not (TrieSet.contains(AMMMinimumLiquidityDone, token1, Text.hash(token1), Text.equal))) {
      AMMMinimumLiquidityDone := TrieSet.put(AMMMinimumLiquidityDone, token1, Text.hash(token1), Text.equal);
      minimumLiquidity;
    } else { 0 };
    var oldProviders = TrieSet.empty<Principal>();
    let (liquidityMinted, refund0, refund1) = switch (Map.get(AMMpools, hashtt, poolKey)) {
      case (null) {
        // Create new pool — register pair if not yet in pool_canister
        if (amount0 < MINIMUM_LIQUIDITY0 or amount1 < MINIMUM_LIQUIDITY1) {
          // DRIFT FIX: tokens have already been checkReceive'd into the treasury.
          // Refund the full accepted amounts before rejecting; otherwise they
          // strand (no AMMpools entry to count them in ammbalance).
          let Tfees0 = returnTfees(token0);
          let Tfees1 = returnTfees(token1);
          if (amount0 > Tfees0) {
            Vector.add(tempTransferQueueLocal, (#principal(caller), amount0 - Tfees0, token0, genTxId()));
          };
          if (amount1 > Tfees1) {
            Vector.add(tempTransferQueueLocal, (#principal(caller), amount1 - Tfees1, token1, genTxId()));
          };
          if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
            Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
          };
          return #Err(#InsufficientFunds("Amounts below minimum liquidity for new pool"));
        };
        registerPoolPair(token0, token1);
        let initialLiquidity = sqrt((amount0 -MINIMUM_LIQUIDITY0) * (amount1 -MINIMUM_LIQUIDITY1));

        let newPool : AMMPool = {
          token0 = token0;
          token1 = token1;
          reserve0 = amount0 -MINIMUM_LIQUIDITY0;
          reserve1 = amount1 -MINIMUM_LIQUIDITY1;
          totalLiquidity = initialLiquidity;
          totalFee0 = 0;
          totalFee1 = 0;
          lastUpdateTime = nowVar;
          providers = TrieSet.put(TrieSet.empty<Principal>(), caller, Principal.hash(caller), Principal.equal);
        };
        Map.set(AMMpools, hashtt, poolKey, newPool);

        deleteOld := true;
        (initialLiquidity, 0, 0);
      };
      case (?existingPool) {
        if (returnMinimum(token0, existingPool.reserve0, false) and returnMinimum(token1, existingPool.reserve1, false) and existingPool.reserve0 > 0 and existingPool.reserve1 > 0) {
          // Add to existing pool
          let amount0Optimal = (amount1 * existingPool.reserve0) / existingPool.reserve1;
          let amount1Optimal = (amount0 * existingPool.reserve1) / existingPool.reserve0;

          let (useAmount0, useAmount1, refund0, refund1) = if (amount0Optimal <= amount0) {
            (amount0Optimal, amount1, amount0 - amount0Optimal, 0);
          } else { (amount0, amount1Optimal, 0, amount1 - amount1Optimal) };

          let liquidity0 = (useAmount0 * existingPool.totalLiquidity) / existingPool.reserve0;
          let liquidity1 = (useAmount1 * existingPool.totalLiquidity) / existingPool.reserve1;
          let liquidityMinted = Nat.min(liquidity0, liquidity1);

          let updatedPool = {
            existingPool with
            reserve0 = existingPool.reserve0 + useAmount0;
            reserve1 = existingPool.reserve1 + useAmount1;
            totalLiquidity = existingPool.totalLiquidity + liquidityMinted;
            lastUpdateTime = nowVar;
            providers = TrieSet.put(existingPool.providers, caller, Principal.hash(caller), Principal.equal);
          };
          Map.set(AMMpools, hashtt, poolKey, updatedPool);
          (liquidityMinted, refund0, refund1);
        } else {
          addFees(existingPool.token0, existingPool.reserve0, false, "", nowVar);
          addFees(existingPool.token1, existingPool.reserve1, false, "", nowVar);
          recordOpDrift("addLiq_recreate", existingPool.token0, existingPool.reserve0);
          recordOpDrift("addLiq_recreate", existingPool.token1, existingPool.reserve1);
          // Recreate pool — register pair if not yet in pool_canister
          if (amount0 < MINIMUM_LIQUIDITY0 or amount1 < MINIMUM_LIQUIDITY1) {
            // DRIFT FIX: same as new-pool branch above — refund full amounts so
            // rejected deposits don't strand.
            let Tfees0 = returnTfees(token0);
            let Tfees1 = returnTfees(token1);
            if (amount0 > Tfees0) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), amount0 - Tfees0, token0, genTxId()));
            };
            if (amount1 > Tfees1) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), amount1 - Tfees1, token1, genTxId()));
            };
            if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
              Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
            };
            return #Err(#InsufficientFunds("Amounts below minimum liquidity for pool recreation"));
          };
          registerPoolPair(token0, token1);
          let initialLiquidity = sqrt((amount0 -MINIMUM_LIQUIDITY0) * (amount1 -MINIMUM_LIQUIDITY1));
          oldProviders := existingPool.providers;
          let newPool : AMMPool = {
            token0 = token0;
            token1 = token1;
            reserve0 = amount0 -MINIMUM_LIQUIDITY0;
            reserve1 = amount1 -MINIMUM_LIQUIDITY1;
            totalLiquidity = initialLiquidity;
            totalFee0 = 0;
            totalFee1 = 0;
            lastUpdateTime = nowVar;
            providers = TrieSet.put(TrieSet.empty<Principal>(), caller, Principal.hash(caller), Principal.equal);
          };

          deleteOld := true;
          Map.set(AMMpools, hashtt, poolKey, newPool);
          (initialLiquidity, 0, 0);

        };
      };
    };

    // Sync V3 data: ensure full-range addLiquidity also creates/updates V3 pool data
    let poolAfterAdd = switch (Map.get(AMMpools, hashtt, poolKey)) { case (?p) { p }; case null { { token0; token1; reserve0 = 0; reserve1 = 0; totalLiquidity = 0; totalFee0 = 0; totalFee1 = 0; lastUpdateTime = nowVar; providers = TrieSet.empty<Principal>() } } };
    if (poolAfterAdd.reserve0 > 0 and poolAfterAdd.reserve1 > 0) {
      let sqrtR = ratioToSqrtRatio((poolAfterAdd.reserve1 * tenToPower60) / poolAfterAdd.reserve0);
      switch (Map.get(poolV3Data, hashtt, poolKey)) {
        case null {
          // Create V3 data for new pool
          var rangeTree = RBTree.init<Nat, RangeData>();
          rangeTree := RBTree.put(rangeTree, Nat.compare, FULL_RANGE_LOWER, {
            liquidityNet = poolAfterAdd.totalLiquidity; liquidityGross = poolAfterAdd.totalLiquidity;
            feeGrowthOutside0 = 0; feeGrowthOutside1 = 0;
          });
          rangeTree := RBTree.put(rangeTree, Nat.compare, FULL_RANGE_UPPER, {
            liquidityNet = -poolAfterAdd.totalLiquidity; liquidityGross = poolAfterAdd.totalLiquidity;
            feeGrowthOutside0 = 0; feeGrowthOutside1 = 0;
          });
          Map.set(poolV3Data, hashtt, poolKey, {
            activeLiquidity = poolAfterAdd.totalLiquidity;
            currentSqrtRatio = sqrtR;
            feeGrowthGlobal0 = 0; feeGrowthGlobal1 = 0;
            totalFeesCollected0 = 0; totalFeesCollected1 = 0;
            totalFeesClaimed0 = 0; totalFeesClaimed1 = 0;
            ranges = rangeTree;
          });
        };
        case (?v3) {
          // Update existing V3 data: add liquidity to full range
          var ranges = v3.ranges;
          let lData = switch (RBTree.get(ranges, Nat.compare, FULL_RANGE_LOWER)) {
            case null { { liquidityNet = 0 : Int; liquidityGross = 0; feeGrowthOutside0 = 0; feeGrowthOutside1 = 0 } };
            case (?d) { d };
          };
          ranges := RBTree.put(ranges, Nat.compare, FULL_RANGE_LOWER, { lData with liquidityNet = lData.liquidityNet + liquidityMinted; liquidityGross = lData.liquidityGross + liquidityMinted });
          let uData = switch (RBTree.get(ranges, Nat.compare, FULL_RANGE_UPPER)) {
            case null { { liquidityNet = 0 : Int; liquidityGross = 0; feeGrowthOutside0 = 0; feeGrowthOutside1 = 0 } };
            case (?d) { d };
          };
          ranges := RBTree.put(ranges, Nat.compare, FULL_RANGE_UPPER, { uData with liquidityNet = uData.liquidityNet - liquidityMinted; liquidityGross = uData.liquidityGross + liquidityMinted });
          Map.set(poolV3Data, hashtt, poolKey, {
            v3 with
            activeLiquidity = v3.activeLiquidity + liquidityMinted;
            ranges = ranges;
          });
        };
      };

      // Create or merge concentrated position for this user (full-range)
      let existingConc = switch (Map.get(concentratedPositions, phash, caller)) { case null { [] }; case (?a) { a } };
      // Check if a full-range position already exists for this pool
      let existingIndex = Array.indexOf<ConcentratedPosition>(
        { positionId = 0; token0; token1; liquidity = 0; ratioLower = FULL_RANGE_LOWER; ratioUpper = FULL_RANGE_UPPER; lastFeeGrowth0 = 0; lastFeeGrowth1 = 0; lastUpdateTime = 0 },
        existingConc,
        func(a, b) { a.token0 == b.token0 and a.token1 == b.token1 and a.ratioLower == FULL_RANGE_LOWER and a.ratioUpper == FULL_RANGE_UPPER },
      );
      switch (existingIndex) {
        case (?idx) {
          // Merge: auto-claim accrued fees to the user before re-snapshotting lastFeeGrowth,
          // then add the new liquidity. Matches claimLPFees behavior so the user receives
          // what they earned instead of abandoning it to feescollectedDAO on pool deletion.
          let old = existingConc[idx];
          let v3Now = switch (Map.get(poolV3Data, hashtt, poolKey)) { case (?v) v; case null { { activeLiquidity = 0; currentSqrtRatio = 0; feeGrowthGlobal0 = 0; feeGrowthGlobal1 = 0; totalFeesCollected0 = 0; totalFeesCollected1 = 0; totalFeesClaimed0 = 0; totalFeesClaimed1 = 0; ranges = RBTree.init<Nat, RangeData>() } } };
          let (insideNow0, insideNow1) = positionFeeGrowthInside(old, v3Now);
          let pendingFee0 = old.liquidity * safeSub(insideNow0, old.lastFeeGrowth0) / tenToPower60;
          let pendingFee1 = old.liquidity * safeSub(insideNow1, old.lastFeeGrowth1) / tenToPower60;
          let maxClaim0 = safeSub(v3Now.totalFeesCollected0, v3Now.totalFeesClaimed0);
          let maxClaim1 = safeSub(v3Now.totalFeesCollected1, v3Now.totalFeesClaimed1);
          let claim0 = Nat.min(pendingFee0, maxClaim0);
          let claim1 = Nat.min(pendingFee1, maxClaim1);
          let Tfees0 = returnTfees(token0);
          let Tfees1 = returnTfees(token1);
          if (claim0 > Tfees0) { Vector.add(tempTransferQueueLocal, (#principal(caller), claim0 - Tfees0, token0, genTxId())) }
          else if (claim0 > 0) { addFees(token0, claim0, false, "", nowVar) };
          if (claim1 > Tfees1) { Vector.add(tempTransferQueueLocal, (#principal(caller), claim1 - Tfees1, token1, genTxId())) }
          else if (claim1 > 0) { addFees(token1, claim1, false, "", nowVar) };
          if (claim0 > 0 or claim1 > 0) {
            Map.set(poolV3Data, hashtt, poolKey, { v3Now with totalFeesClaimed0 = v3Now.totalFeesClaimed0 + claim0; totalFeesClaimed1 = v3Now.totalFeesClaimed1 + claim1 });
          };
          let updated = Array.tabulate<ConcentratedPosition>(existingConc.size(), func(i) {
            if (i == idx) {
              { old with liquidity = old.liquidity + liquidityMinted; lastFeeGrowth0 = insideNow0; lastFeeGrowth1 = insideNow1; lastUpdateTime = nowVar }
            } else { existingConc[i] }
          });
          Map.set(concentratedPositions, phash, caller, updated);
        };
        case null {
          // Create new full-range concentrated position
          nextPositionId += 1;
          let (initInside0, initInside1) = switch (Map.get(poolV3Data, hashtt, poolKey)) {
            case (?v) { (v.feeGrowthGlobal0, v.feeGrowthGlobal1) };  // full-range: inside == global
            case null { (0, 0) };
          };
          let fullRangePos : ConcentratedPosition = {
            positionId = nextPositionId;
            token0; token1;
            liquidity = liquidityMinted;
            ratioLower = FULL_RANGE_LOWER;
            ratioUpper = FULL_RANGE_UPPER;
            lastFeeGrowth0 = initInside0;
            lastFeeGrowth1 = initInside1;
            lastUpdateTime = nowVar;
          };
          let cVec = Vector.fromArray<ConcentratedPosition>(existingConc);
          Vector.add(cVec, fullRangePos);
          Map.set(concentratedPositions, phash, caller, Vector.toArray(cVec));
        };
      };
    };

    // Sync AMMPool from V3 after liquidity addition
    syncPoolFromV3(poolKey);

    if (refund0 > 0 or refund1 > 0) {
      let Tfees0 = returnTfees(token0);
      let Tfees1 = returnTfees(token1);

      if (refund0 > Tfees0) {
        Vector.add(tempTransferQueueLocal, (#principal(caller), refund0 - Tfees0, token0, genTxId()));
      } else { addFees(token0, refund0, false, "", nowVar) };
      if (refund1 > Tfees1) {
        Vector.add(tempTransferQueueLocal, (#principal(caller), refund1 - Tfees1, token1, genTxId()));
      } else {
        addFees(token1, refund1, false, "", nowVar);
      };
    };
    // Transferring the transactions that have to be made to the treasury,
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {

    } else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };
    #Ok({
      liquidityMinted = liquidityMinted;
      token0 = token0;
      token1 = token1;
      amount0Used = amount0 - refund0;
      amount1Used = amount1 - refund1;
      refund0 = refund0;
      refund1 = refund1;
    });
  };

  // ═══════════════════════════════════════════════════════════════
  // Concentrated Liquidity: Add and Remove
  // ═══════════════════════════════════════════════════════════════

  public shared ({ caller }) func addConcentratedLiquidity(
    token0i : Text, token1i : Text,
    amount0i : Nat, amount1i : Nat,
    priceLower : Nat, priceUpper : Nat,
    block0i : Nat, block1i : Nat,
  ) : async ExTypes.AddConcentratedResult {
    if (isAllowed(caller) != 1) { return #Err(#NotAuthorized) };
    if (Text.size(token0i) > 150 or Text.size(token1i) > 150) {
      return #Err(#Banned);
    };
    if (token0i == token1i) {
      return #Err(#InvalidInput("token0 and token1 must be different"));
    };

    let (token0, token1) = getPool(token0i, token1i);
    let tType0 = returnType(token0);
    let tType1 = returnType(token1);
    let poolKey = (token0, token1);
    var amount1 = amount1i;
    var amount0 = amount0i;
    var block0 = block0i;
    var block1 = block1i;
    var priceLowerC = priceLower;
    var priceUpperC = priceUpper;
    if (token1i != token1) {
      amount1 := amount0i; amount0 := amount1i;
      block0 := block1i; block1 := block0i;
      // user supplied bounds in "userToken1 per userToken0" — relabel just
      // flipped the canonical orientation, so price bounds must be inverted (and swapped
      // because 1/big < 1/small) to remain in canonical "token1 per token0" form.
      let prevLower = priceLowerC;
      priceLowerC := if (priceUpperC > 0) { tenToPower120 / priceUpperC } else { 0 };
      priceUpperC := if (prevLower > 0)   { tenToPower120 / prevLower }   else { tenToPower120 };
    };
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var nowVar = Time.now();

    // DRIFT FIX: refund-on-reject helper. Tokens have already been transferred
    // into the exchange via ICRC-1 (blocks block0/block1). On a reject we must
    // verify the deposits via checkReceive(amount=0) which queues full-amount
    // refunds, then flush to treasury. Without this, tokens strand in the
    // treasury ledger, uncountered by any bookkeeping bucket → drift.
    func refundAndReject(errMsg : ExTypes.ExchangeError) : async ExTypes.AddConcentratedResult {
      label a for ((token, Block, tType) in ([(token1, block1, tType1), (token0, block0, tType0)]).vals()) {
        if (Map.has(BlocksDone, thash, token # ":" # Nat.toText(Block))) { continue a };
        Map.set(BlocksDone, thash, token # ":" # Nat.toText(Block), nowVar);
        let blockData = try { await* getBlockData(token, Block, tType) } catch (_) {
          Map.delete(BlocksDone, thash, token # ":" # Nat.toText(Block));
          continue a;
          #ICRC12([]);
        };
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token, ICPfee, RevokeFeeNow, true, true, blockData, tType, Time.now())).1.vals());
      };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      #Err(errMsg);
    };

    // reject full-range endpoints. Full-range positions must go through
    // addLiquidity (V2) so the position's ratioLower/ratioUpper match FULL_RANGE_*
    // AND the tick-tree key uses the raw constant (see main.mo:771 convention).
    // If we allowed this path, the position would store ratioLower=FULL_RANGE_LOWER
    // but its tick-tree entry would be at ratioToSqrtRatio(FULL_RANGE_LOWER), breaking
    // the conditional lookup in removeConcentratedLiquidity.
    if (priceLowerC == FULL_RANGE_LOWER or priceUpperC == FULL_RANGE_UPPER) {
      return await refundAndReject(#InvalidInput("Use addLiquidity for full-range positions"));
    };

    // decode human price → canonical raw ratio. The user's input represents
    // (canonicalToken1_human / canonicalToken0_human) × 10^60. Canonical convention is
    // (raw_reserve1 × 10^60) / raw_reserve0 = human_price × 10^(dec1 − dec0) × 10^60.
    // Apply 10^(dec1 − dec0) in integer math; default to 8 decimals if tokenInfo missing.
    let dec0Int = switch (Map.get(tokenInfo, thash, token0)) { case (?i) { i.Decimals }; case null { 8 } };
    let dec1Int = switch (Map.get(tokenInfo, thash, token1)) { case (?i) { i.Decimals }; case null { 8 } };
    let (lowerCanonical, upperCanonical) = if (dec0Int == dec1Int) {
      (priceLowerC, priceUpperC);
    } else if (dec1Int > dec0Int) {
      let factor = 10 ** (dec1Int - dec0Int);
      (priceLowerC * factor, priceUpperC * factor);
    } else {
      let factor = 10 ** (dec0Int - dec1Int);
      if (priceLowerC < factor or priceUpperC < factor) {
        return await refundAndReject(#InvalidInput("Price below minimum representable for this pair"));
      };
      (priceLowerC / factor, priceUpperC / factor);
    };

    // Snap to tick boundaries
    let ratioLower = snapToTick(lowerCanonical);
    let ratioUpper = snapToTick(upperCanonical);
    if (ratioLower >= ratioUpper or ratioLower == 0) {
      return await refundAndReject(#InvalidInput("Invalid price range"));
    };
    // SECURITY FIX: snapToTick can snap user input just above a sentinel down to the
    // sentinel itself (e.g. priceLower in (10^20, 10^20+1000] → 10^20). The pre-snap
    // check above only tests for exact equality to the sentinels. Without this guard
    // the position would store ratioLower=FULL_RANGE_LOWER while the tick tree is
    // keyed at ratioToSqrtRatio(FULL_RANGE_LOWER) — so removeConcentratedLiquidity's
    // `if (ratioLower == FULL_RANGE_LOWER) raw else sqrt` lookup would miss, leaving
    // phantom liquidity in the tick tree and draining the pool over time. Full-range
    // positions MUST go through addLiquidity (which writes raw sentinels directly).
    if (ratioLower == FULL_RANGE_LOWER or ratioUpper == FULL_RANGE_UPPER) {
      return await refundAndReject(#InvalidInput("Range snaps to full-range sentinel — use addLiquidity for full-range positions"));
    };

    // Validate: not paused, minimums
    if (
      ((switch (Array.find<Text>(pausedTokens, func(t) { t == token0 })) { case null { false }; case (?_) { true } })) or
      ((switch (Array.find<Text>(pausedTokens, func(t) { t == token1 })) { case null { false }; case (?_) { true } })) or
      ((returnMinimum(token0, amount0, true) and returnMinimum(token1, amount1, true)) == false)
    ) {
      // Refund both tokens
      label a for ((token, Block, tType) in ([(token1, block1, tType1), (token0, block0, tType0)]).vals()) {
        if (Map.has(BlocksDone, thash, token # ":" #Nat.toText(Block))) { continue a };
        Map.set(BlocksDone, thash, token # ":" #Nat.toText(Block), nowVar);
        let blockData = try { await* getBlockData(token, Block, tType) } catch (err) {
          Map.delete(BlocksDone, thash, token # ":" #Nat.toText(Block)); continue a; #ICRC12([]);
        };
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token, ICPfee, RevokeFeeNow, true, true, blockData, tType, Time.now())).1.vals());
      };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#TokenPaused("Validation failed"));
    };

    // Verify on-chain transfers
    var receiveBool = true;
    let receiveTransfersVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    label a for ((token, Block, amount, tType) in ([(token1, block1, amount1, tType1), (token0, block0, amount0, tType0)]).vals()) {
      if (Map.has(BlocksDone, thash, token # ":" #Nat.toText(Block))) {
        receiveBool := false; continue a;
      };
      Map.set(BlocksDone, thash, token # ":" #Nat.toText(Block), nowVar);
      let blockData = try { await* getBlockData(token, Block, tType) } catch (err) {
        Map.delete(BlocksDone, thash, token # ":" #Nat.toText(Block)); continue a; #ICRC12([]);
      };
      let receiveData = checkReceive(Block, caller, amount, token, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar);
      Vector.addFromIter(receiveTransfersVec, receiveData.1.vals());
      receiveBool := receiveBool and receiveData.0;
    };
    Vector.addFromIter(tempTransferQueueLocal, Vector.vals(receiveTransfersVec));
    if (not receiveBool) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InsufficientFunds("Deposit not received"));
    };

    // Get or create pool and V3 data — register pair if not yet in pool_canister
    var pool = switch (Map.get(AMMpools, hashtt, poolKey)) {
      case null {
        registerPoolPair(token0, token1);
        let newPool : AMMPool = {
          token0; token1;
          reserve0 = 0; reserve1 = 0;
          totalLiquidity = 0;
          totalFee0 = 0; totalFee1 = 0;
          lastUpdateTime = nowVar;
          providers = TrieSet.empty<Principal>();
        };
        Map.set(AMMpools, hashtt, poolKey, newPool);
        newPool;
      };
      case (?p) { p };
    };

    var v3 = switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case null {
        let sqrtRatio = if (pool.reserve0 > 0 and pool.reserve1 > 0) {
          ratioToSqrtRatio((pool.reserve1 * tenToPower60) / pool.reserve0);
        } else if (amount0 > 0 and amount1 > 0) {
          ratioToSqrtRatio((amount1 * tenToPower60) / amount0);
        } else { tenToPower60 }; // default 1:1
        {
          activeLiquidity = 0;
          currentSqrtRatio = sqrtRatio;
          feeGrowthGlobal0 = 0; feeGrowthGlobal1 = 0;
          totalFeesCollected0 = 0; totalFeesCollected1 = 0;
          totalFeesClaimed0 = 0; totalFeesClaimed1 = 0;
          ranges = RBTree.init<Nat, RangeData>();
        };
      };
      case (?v) { v };
    };

    // Calculate virtual liquidity for this range
    let sqrtLower = ratioToSqrtRatio(ratioLower);
    let sqrtUpper = ratioToSqrtRatio(ratioUpper);
    let sqrtCurrent = if (v3.currentSqrtRatio > 0) { v3.currentSqrtRatio } else { sqrtRatioFromReserves(pool.reserve0, pool.reserve1, 0) };
    let liquidity = liquidityFromAmounts(amount0, amount1, sqrtLower, sqrtUpper, sqrtCurrent);
    let (rawUsed0, rawUsed1) = amountsFromLiquidity(liquidity, sqrtLower, sqrtUpper, sqrtCurrent);
    let used0 = Nat.min(rawUsed0 + 1, amount0);
    let used1 = Nat.min(rawUsed1 + 1, amount1);

    if (liquidity == 0) {
      // Refund
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InvalidInput("Zero liquidity for range"));
    };

    // Update range tree: add liquidityNet at boundaries
    var ranges = v3.ranges;
    // Use sqrtRatio as tree keys (not price ratios) for consistency with swap engine.
    // When creating a new tick, initialize feeGrowthOutside per Uniswap V3 convention
    // so that feeGrowthInside is computed correctly for this range's positions.
    let lowerData = switch (RBTree.get(ranges, Nat.compare, sqrtLower)) {
      case null {
        let (fgo0, fgo1) = initialFeeGrowthOutside(sqrtLower, v3.currentSqrtRatio, v3.feeGrowthGlobal0, v3.feeGrowthGlobal1);
        { liquidityNet = 0 : Int; liquidityGross = 0; feeGrowthOutside0 = fgo0; feeGrowthOutside1 = fgo1 };
      };
      case (?d) { d };
    };
    ranges := RBTree.put(ranges, Nat.compare, sqrtLower, {
      liquidityNet = lowerData.liquidityNet + liquidity;
      liquidityGross = lowerData.liquidityGross + liquidity;
      feeGrowthOutside0 = lowerData.feeGrowthOutside0;
      feeGrowthOutside1 = lowerData.feeGrowthOutside1;
    });

    let upperData = switch (RBTree.get(ranges, Nat.compare, sqrtUpper)) {
      case null {
        let (fgo0, fgo1) = initialFeeGrowthOutside(sqrtUpper, v3.currentSqrtRatio, v3.feeGrowthGlobal0, v3.feeGrowthGlobal1);
        { liquidityNet = 0 : Int; liquidityGross = 0; feeGrowthOutside0 = fgo0; feeGrowthOutside1 = fgo1 };
      };
      case (?d) { d };
    };
    ranges := RBTree.put(ranges, Nat.compare, sqrtUpper, {
      liquidityNet = upperData.liquidityNet - liquidity;
      liquidityGross = upperData.liquidityGross + liquidity;
      feeGrowthOutside0 = upperData.feeGrowthOutside0;
      feeGrowthOutside1 = upperData.feeGrowthOutside1;
    });

    // Update active liquidity if current price is in range
    let currentRatio = if (sqrtCurrent > 0) { (sqrtCurrent * sqrtCurrent) / tenToPower60 } else { 0 };
    let newActiveLiquidity = if (currentRatio >= ratioLower and currentRatio < ratioUpper) {
      v3.activeLiquidity + liquidity;
    } else { v3.activeLiquidity };

    // Update pool reserves with USED amounts only (excess refunded to user)
    pool := {
      pool with
      reserve0 = pool.reserve0 + used0;
      reserve1 = pool.reserve1 + used1;
      totalLiquidity = pool.totalLiquidity + liquidity;
      lastUpdateTime = nowVar;
      providers = TrieSet.put(pool.providers, caller, Principal.hash(caller), Principal.equal);
    };
    Map.set(AMMpools, hashtt, poolKey, pool);

    // Store updated V3 data — do NOT update currentSqrtRatio (maintained by swap engine only)
    Map.set(poolV3Data, hashtt, poolKey, {
      v3 with
      activeLiquidity = newActiveLiquidity;
      ranges = ranges;
    });

    // Sync AMMPool from V3
    syncPoolFromV3(poolKey);

    // Store or merge position for user (merge if same pool + same range exists)
    let existingConc = switch (Map.get(concentratedPositions, phash, caller)) {
      case null { [] }; case (?arr) { arr };
    };
    let existingIndex = Array.indexOf<ConcentratedPosition>(
      { positionId = 0; token0; token1; liquidity = 0; ratioLower; ratioUpper; lastFeeGrowth0 = 0; lastFeeGrowth1 = 0; lastUpdateTime = 0 },
      existingConc,
      func(a, b) { a.token0 == b.token0 and a.token1 == b.token1 and a.ratioLower == b.ratioLower and a.ratioUpper == b.ratioUpper },
    );
    let v3Now = switch (Map.get(poolV3Data, hashtt, poolKey)) { case (?v) v; case null v3 };
    switch (existingIndex) {
      case (?idx) {
        // Merge: re-snapshot lastFeeGrowthInside before adding new liquidity.
        // Pending in-range fees accrued under the old liquidity are credited via
        // totalFeesClaimed increment (so the canister's accounting stays consistent).
        let old = existingConc[idx];
        let (insideNow0, insideNow1) = positionFeeGrowthInside(old, v3Now);
        let pendingFee0 = old.liquidity * safeSub(insideNow0, old.lastFeeGrowth0) / tenToPower60;
        let pendingFee1 = old.liquidity * safeSub(insideNow1, old.lastFeeGrowth1) / tenToPower60;
        let maxClaim0 = safeSub(v3Now.totalFeesCollected0, v3Now.totalFeesClaimed0);
        let maxClaim1 = safeSub(v3Now.totalFeesCollected1, v3Now.totalFeesClaimed1);
        let claimed0 = Nat.min(pendingFee0, maxClaim0);
        let claimed1 = Nat.min(pendingFee1, maxClaim1);
        let Tfees0 = returnTfees(token0);
        let Tfees1 = returnTfees(token1);
        if (claimed0 > Tfees0) { Vector.add(tempTransferQueueLocal, (#principal(caller), claimed0 - Tfees0, token0, genTxId())) }
        else if (claimed0 > 0) { addFees(token0, claimed0, false, "", nowVar) };
        if (claimed1 > Tfees1) { Vector.add(tempTransferQueueLocal, (#principal(caller), claimed1 - Tfees1, token1, genTxId())) }
        else if (claimed1 > 0) { addFees(token1, claimed1, false, "", nowVar) };
        if (claimed0 > 0 or claimed1 > 0) {
          Map.set(poolV3Data, hashtt, poolKey, { v3Now with totalFeesClaimed0 = v3Now.totalFeesClaimed0 + claimed0; totalFeesClaimed1 = v3Now.totalFeesClaimed1 + claimed1 });
        };
        let updated = Array.tabulate<ConcentratedPosition>(existingConc.size(), func(i) {
          if (i == idx) {
            { old with liquidity = old.liquidity + liquidity; lastFeeGrowth0 = insideNow0; lastFeeGrowth1 = insideNow1; lastUpdateTime = nowVar }
          } else { existingConc[i] }
        });
        Map.set(concentratedPositions, phash, caller, updated);
      };
      case null {
        // Create new concentrated position — snapshot feeGrowthInside NOW so future
        // claims correctly isolate to in-range growth only.
        nextPositionId += 1;
        let dummyPos : ConcentratedPosition = {
          positionId = 0; token0; token1; liquidity = 0;
          ratioLower; ratioUpper; lastFeeGrowth0 = 0; lastFeeGrowth1 = 0; lastUpdateTime = 0;
        };
        let (initInside0, initInside1) = positionFeeGrowthInside(dummyPos, v3Now);
        let newPosition : ConcentratedPosition = {
          positionId = nextPositionId;
          token0; token1;
          liquidity;
          ratioLower; ratioUpper;
          lastFeeGrowth0 = initInside0;
          lastFeeGrowth1 = initInside1;
          lastUpdateTime = nowVar;
        };
        let posVec = Vector.fromArray<ConcentratedPosition>(existingConc);
        Vector.add(posVec, newPosition);
        Map.set(concentratedPositions, phash, caller, Vector.toArray(posVec));
      };
    };

    // Refund excess tokens not used by the position
    let refund0 = if (amount0 > used0) { amount0 - used0 } else { 0 };
    let refund1 = if (amount1 > used1) { amount1 - used1 } else { 0 };
    if (refund0 > returnTfees(token0)) {
      Vector.add(tempTransferQueueLocal, (#principal(caller), refund0 - returnTfees(token0), token0, genTxId()));
    };
    if (refund1 > returnTfees(token1)) {
      Vector.add(tempTransferQueueLocal, (#principal(caller), refund1 - returnTfees(token1), token1, genTxId()));
    };

    doInfoBeforeStep2();

    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };

    #Ok({
      liquidity = liquidity;
      positionId = nextPositionId;
      token0 = token0;
      token1 = token1;
      amount0Used = used0;
      amount1Used = used1;
      refund0 = refund0;
      refund1 = refund1;
      priceLower = priceLower;
      priceUpper = priceUpper;
    });
  };

  // Remove concentrated liquidity position
  public shared ({ caller }) func removeConcentratedLiquidity(
    token0i : Text, token1i : Text,
    positionId : Nat,
    liquidityAmount : Nat,
  ) : async ExTypes.RemoveConcentratedResult {
    if (isAllowed(caller) != 1) { return #Err(#NotAuthorized) };

    let (token0, token1) = getPool(token0i, token1i);
    let poolKey = (token0, token1);
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    let nowVar = Time.now();

    // Find user's position
    let positions = switch (Map.get(concentratedPositions, phash, caller)) {
      case null { return #Err(#OrderNotFound("No positions found")) };
      case (?arr) { arr };
    };

    var foundPosition : ?ConcentratedPosition = null;
    var foundIndex : Nat = 0;
    label search for (i in Iter.range(0, positions.size() - 1)) {
      if (positions[i].positionId == positionId and positions[i].token0 == token0 and positions[i].token1 == token1) {
        foundPosition := ?positions[i];
        foundIndex := i;
        break search;
      };
    };

    let position = switch (foundPosition) {
      case null { return #Err(#OrderNotFound("Position not found")) };
      case (?p) { p };
    };

    let actualLiquidityToRemove = Nat.min(liquidityAmount, position.liquidity);
    if (actualLiquidityToRemove == 0) { return #Err(#InvalidInput("Nothing to remove")) };

    // Get pool and V3 data
    let pool = switch (Map.get(AMMpools, hashtt, poolKey)) {
      case null { return #Err(#PoolNotFound("Pool not found")) };
      case (?p) { p };
    };
    var v3 = switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case null { return #Err(#PoolNotFound("V3 data not found")) };
      case (?v) { v };
    };

    // Calculate fees owed using feeGrowthInside (Uniswap V3 isolation — only in-range
    // growth accrues to the position). NOTE: lastFeeGrowth0/1 fields now store
    // lastFeeGrowthInside values post-migration (see migration.mo feeGrowthInsideMigrated).
    let (insideNow0, insideNow1) = positionFeeGrowthInside(position, v3);
    let theoreticalFee0 = position.liquidity * safeSub(insideNow0, position.lastFeeGrowth0) / tenToPower60;
    let maxClaimable0 = safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0);
    let actualFee0 = Nat.min(theoreticalFee0, maxClaimable0);

    let theoreticalFee1 = position.liquidity * safeSub(insideNow1, position.lastFeeGrowth1) / tenToPower60;
    let maxClaimable1 = safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1);
    let actualFee1 = Nat.min(theoreticalFee1, maxClaimable1);

    // Calculate token amounts based on current price vs range.
    // Use derived sqrtCurrent from live reserves (self-heals if v3.currentSqrtRatio stale).
    // DRIFT FIX: full-range positions store FULL_RANGE_LOWER/UPPER directly as BOTH ratio
    // AND tree key (see main.mo:771, addLiquidity V2 at 3209/3213). Concentrated positions
    // store the sqrt-converted key. Without this conditional, full-range tick-tree lookups
    // would miss, leaving the tree stale and causing activeLiquidity to re-inflate via
    // recalculateActiveLiquidity → LP funds orphan to feescollectedDAO on pool deletion.
    let sqrtLower = if (position.ratioLower == FULL_RANGE_LOWER) { FULL_RANGE_LOWER } else { ratioToSqrtRatio(position.ratioLower) };
    let sqrtUpper = if (position.ratioUpper == FULL_RANGE_UPPER) { FULL_RANGE_UPPER } else { ratioToSqrtRatio(position.ratioUpper) };
    let sqrtCurrent = v3.currentSqrtRatio;

    let (rawBase0, rawBase1) = amountsFromLiquidity(actualLiquidityToRemove, sqrtLower, sqrtUpper, sqrtCurrent);

    // Defense in depth: cap base amounts at actual pool reserves. V3 virtual-liquidity math
    // can produce amounts exceeding real reserves when the pool has drifted (stale sqrtCurrent,
    // LP rounding, orderbook-induced drift). Without the cap, removeConcentratedLiquidity would
    // transfer more than the pool holds, draining from other pools via shared treasury balance.
    let baseAmount0 = Nat.min(rawBase0, pool.reserve0);
    let baseAmount1 = Nat.min(rawBase1, pool.reserve1);
    if (rawBase0 > pool.reserve0 or rawBase1 > pool.reserve1) {
      Debug.print("LP_REMOVE_CAPPED: position=" # Nat.toText(position.positionId)
        # " rawBase0=" # Nat.toText(rawBase0) # " reserve0=" # Nat.toText(pool.reserve0)
        # " rawBase1=" # Nat.toText(rawBase1) # " reserve1=" # Nat.toText(pool.reserve1));
      logger.warn("AMM", "LP removal capped — upstream drift. posId=" # Nat.toText(position.positionId) # " raw0=" # Nat.toText(rawBase0) # " res0=" # Nat.toText(pool.reserve0) # " raw1=" # Nat.toText(rawBase1) # " res1=" # Nat.toText(pool.reserve1), "removeConcentratedLiquidity");
    };

    let totalAmount0 = baseAmount0 + (actualFee0 * actualLiquidityToRemove / position.liquidity);
    let totalAmount1 = baseAmount1 + (actualFee1 * actualLiquidityToRemove / position.liquidity);

    // Update range tree: remove liquidity from boundaries
    var ranges = v3.ranges;
    switch (RBTree.get(ranges, Nat.compare, sqrtLower)) {
      case (?d) {
        let newGross = if (d.liquidityGross > actualLiquidityToRemove) { d.liquidityGross - actualLiquidityToRemove } else { 0 };
        if (newGross == 0) {
          ranges := RBTree.delete(ranges, Nat.compare, sqrtLower);
        } else {
          ranges := RBTree.put(ranges, Nat.compare, sqrtLower, {
            d with
            liquidityNet = d.liquidityNet - actualLiquidityToRemove;
            liquidityGross = newGross;
          });
        };
      };
      case null {
        if (test) Debug.print("TICK_TREE_MISS removeConcentratedLiquidity lower posId=" # Nat.toText(position.positionId) # " sqrtLower=" # Nat.toText(sqrtLower) # " ratioLower=" # Nat.toText(position.ratioLower));
        logger.warn("AMM", "removeConcentratedLiquidity lower-tick miss posId=" # Nat.toText(position.positionId) # " ratioLower=" # Nat.toText(position.ratioLower), "removeConcentratedLiquidity");
      };
    };
    switch (RBTree.get(ranges, Nat.compare, sqrtUpper)) {
      case (?d) {
        let newGross = if (d.liquidityGross > actualLiquidityToRemove) { d.liquidityGross - actualLiquidityToRemove } else { 0 };
        if (newGross == 0) {
          ranges := RBTree.delete(ranges, Nat.compare, sqrtUpper);
        } else {
          ranges := RBTree.put(ranges, Nat.compare, sqrtUpper, {
            d with
            liquidityNet = d.liquidityNet + actualLiquidityToRemove; // reverse of add
            liquidityGross = newGross;
          });
        };
      };
      case null {
        if (test) Debug.print("TICK_TREE_MISS removeConcentratedLiquidity upper posId=" # Nat.toText(position.positionId) # " sqrtUpper=" # Nat.toText(sqrtUpper) # " ratioUpper=" # Nat.toText(position.ratioUpper));
        logger.warn("AMM", "removeConcentratedLiquidity upper-tick miss posId=" # Nat.toText(position.positionId) # " ratioUpper=" # Nat.toText(position.ratioUpper), "removeConcentratedLiquidity");
      };
    };

    // Update active liquidity if current price is in range
    let currentRatio = if (sqrtCurrent > 0) { (sqrtCurrent * sqrtCurrent) / tenToPower60 } else { 0 };
    let newActiveLiquidity = if (currentRatio >= position.ratioLower and currentRatio < position.ratioUpper) {
      if (v3.activeLiquidity > actualLiquidityToRemove) { v3.activeLiquidity - actualLiquidityToRemove } else { 0 };
    } else { v3.activeLiquidity };

    // Update pool reserves — subtract total amounts (base + fees)
    let newReserve0 = if (pool.reserve0 > totalAmount0) { pool.reserve0 - totalAmount0 } else { 0 };
    let newReserve1 = if (pool.reserve1 > totalAmount1) { pool.reserve1 - totalAmount1 } else { 0 };
    let newTotalLiq = if (pool.totalLiquidity > actualLiquidityToRemove) { pool.totalLiquidity - actualLiquidityToRemove } else { 0 };

    Map.set(AMMpools, hashtt, poolKey, {
      pool with
      reserve0 = newReserve0; reserve1 = newReserve1;
      totalLiquidity = newTotalLiq;
      lastUpdateTime = nowVar;
    });

    // Update V3 data
    Map.set(poolV3Data, hashtt, poolKey, {
      v3 with
      activeLiquidity = newActiveLiquidity;
      totalFeesClaimed0 = v3.totalFeesClaimed0 + (actualFee0 * actualLiquidityToRemove / position.liquidity);
      totalFeesClaimed1 = v3.totalFeesClaimed1 + (actualFee1 * actualLiquidityToRemove / position.liquidity);
      ranges = ranges;
    });

    // Sync AMMPool from V3
    syncPoolFromV3(poolKey);

    // Clean up pool on full drain
    if (newActiveLiquidity == 0 and newReserve0 == 0 and newReserve1 == 0) {
      Map.delete(AMMpools, hashtt, poolKey);
      Map.delete(poolV3Data, hashtt, poolKey);
    };

    // Update or remove position
    let newLiquidity = position.liquidity - actualLiquidityToRemove;
    if (newLiquidity == 0) {
      // Remove position entirely
      let filtered = Array.filter<ConcentratedPosition>(positions, func(p) { p.positionId != positionId });
      if (filtered.size() == 0) {
        Map.delete(concentratedPositions, phash, caller);
      } else {
        Map.set(concentratedPositions, phash, caller, filtered);
      };
    } else {
      // Update with reduced liquidity; re-snapshot lastFeeGrowthInside to "now" so
      // remaining liquidity only earns on future in-range growth.
      let v3After = switch (Map.get(poolV3Data, hashtt, poolKey)) { case (?v) { v }; case null { v3 } };
      let updated = Array.map<ConcentratedPosition, ConcentratedPosition>(positions, func(p) {
        if (p.positionId == positionId) {
          let (insideNow0, insideNow1) = positionFeeGrowthInside(p, v3After);
          { p with liquidity = newLiquidity; lastFeeGrowth0 = insideNow0; lastFeeGrowth1 = insideNow1; lastUpdateTime = nowVar };
        } else { p };
      });
      Map.set(concentratedPositions, phash, caller, updated);
    };

    // Transfer tokens to user
    let Tfees0 = returnTfees(token0);
    let Tfees1 = returnTfees(token1);
    if (totalAmount0 > Tfees0) {
      Vector.add(tempTransferQueueLocal, (#principal(caller), totalAmount0 - Tfees0, token0, genTxId()));
    } else if (totalAmount0 > 0) {
      // DRIFT FIX: reserves + v3Resid already dropped by totalAmount0 above.
      // When the payout is smaller than the ledger fee, send the dust to
      // feescollectedDAO so the bookkeeping decrement is mirrored. Otherwise
      // the treasury keeps tokens that no bucket claims (positive drift).
      addFees(token0, totalAmount0, false, "", nowVar);
    };
    if (totalAmount1 > Tfees1) {
      Vector.add(tempTransferQueueLocal, (#principal(caller), totalAmount1 - Tfees1, token1, genTxId()));
    } else if (totalAmount1 > 0) {
      addFees(token1, totalAmount1, false, "", nowVar);
    };

    doInfoBeforeStep2();

    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };

    #Ok({
      amount0 = totalAmount0;
      amount1 = totalAmount1;
      fees0 = actualFee0;
      fees1 = actualFee1;
      liquidityRemoved = actualLiquidityToRemove;
      liquidityRemaining = if (position.liquidity > actualLiquidityToRemove) { position.liquidity - actualLiquidityToRemove } else { 0 };
    });
  };

  type DetailedLiquidityPosition = {
    token0 : Text;
    token1 : Text;
    liquidity : Nat;
    token0Amount : Nat;
    token1Amount : Nat;
    shareOfPool : Float;
    fee0 : Nat;
    fee1 : Nat;
    positionType : { #fullRange; #concentrated };
    positionId : ?Nat;
    ratioLower : ?Nat;
    ratioUpper : ?Nat;
  };

  public query ({ caller }) func getUserLiquidityDetailed() : async [DetailedLiquidityPosition] {
    if (isAllowedQuery(caller) != 1) { return [] };

    // V3 concentrated positions with computed fees and token amounts
    switch (Map.get(concentratedPositions, phash, caller)) {
      case null { [] };
      case (?cPositions) {
        Array.mapFilter<ConcentratedPosition, DetailedLiquidityPosition>(
          cPositions,
          func(pos) {
            let poolKey = (pos.token0, pos.token1);
            let pool = switch (Map.get(AMMpools, hashtt, poolKey)) { case (?p) { p }; case null { return null } };
            let v3 = switch (Map.get(poolV3Data, hashtt, poolKey)) { case (?v) { v }; case null { return null } };

            // Compute unclaimed fees via feeGrowthInside (same formula as removeConcentratedLiquidity).
            let (insideNow0, insideNow1) = positionFeeGrowthInside(pos, v3);
            let theoreticalFee0 = pos.liquidity * safeSub(insideNow0, pos.lastFeeGrowth0) / tenToPower60;
            let theoreticalFee1 = pos.liquidity * safeSub(insideNow1, pos.lastFeeGrowth1) / tenToPower60;
            let maxClaimable0 = safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0);
            let maxClaimable1 = safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1);
            let fee0 = Nat.min(theoreticalFee0, maxClaimable0);
            let fee1 = Nat.min(theoreticalFee1, maxClaimable1);

            // Compute token amounts from liquidity + price range + current price
            let sqrtLower = ratioToSqrtRatio(pos.ratioLower);
            let sqrtUpper = ratioToSqrtRatio(pos.ratioUpper);
            let (amount0, amount1) = amountsFromLiquidity(pos.liquidity, sqrtLower, sqrtUpper, v3.currentSqrtRatio);

            // Share of pool: only in-range positions actively earn fees
            let isInRange = v3.currentSqrtRatio > sqrtLower and v3.currentSqrtRatio < sqrtUpper;
            let shareOfPool = if (isInRange and pool.totalLiquidity > 0) {
              Float.fromInt(pos.liquidity) / Float.fromInt(pool.totalLiquidity);
            } else { 0.0 };

            ?{
              token0 = pos.token0; token1 = pos.token1;
              liquidity = pos.liquidity;
              token0Amount = amount0; token1Amount = amount1;
              shareOfPool; fee0; fee1;
              positionType = if (pos.ratioLower == FULL_RANGE_LOWER and pos.ratioUpper == FULL_RANGE_UPPER) { #fullRange } else { #concentrated };
              positionId = ?pos.positionId;
              ratioLower = ?pos.ratioLower;
              ratioUpper = ?pos.ratioUpper;
            };
          },
        );
      };
    };
  };

  public shared ({ caller }) func claimLPFees(token0i : Text, token1i : Text) : async ExTypes.ClaimFeesResult {
    if (isAllowed(caller) != 1) {
      return #Err(#NotAuthorized);
    };
    if (Text.size(token0i) > 150 or Text.size(token1i) > 150) {
      return #Err(#InvalidInput("Invalid token identifier"));
    };

    let pool2 = getPool(token0i, token1i);
    let token0 = pool2.0;
    let token1 = pool2.1;
    let poolKey = (token0, token1);

    // V3 path: claim fees using feeGrowthGlobal model (primary)
    switch (Map.get(concentratedPositions, phash, caller)) {
      case (?cPositions) {
        // Find all full-range positions for this pool and claim fees
        for (cp in cPositions.vals()) {
          if (cp.token0 == token0 and cp.token1 == token1 and cp.ratioLower == FULL_RANGE_LOWER and cp.ratioUpper == FULL_RANGE_UPPER and cp.liquidity > 0) {
            switch (Map.get(poolV3Data, hashtt, poolKey)) {
              case (?v3) {
                let nowVar = Time.now();
                // Use feeGrowthInside (which equals feeGrowthGlobal for full-range) —
                // wrapper makes math future-proof if this function is ever widened to concentrated.
                let (insideNow0, insideNow1) = positionFeeGrowthInside(cp, v3);
                let theoreticalFee0 = cp.liquidity * safeSub(insideNow0, cp.lastFeeGrowth0) / tenToPower60;
                let theoreticalFee1 = cp.liquidity * safeSub(insideNow1, cp.lastFeeGrowth1) / tenToPower60;
                let maxClaim0 = safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0);
                let maxClaim1 = safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1);
                let accumulatedFees0 = Nat.min(theoreticalFee0, maxClaim0);
                let accumulatedFees1 = Nat.min(theoreticalFee1, maxClaim1);

                if (accumulatedFees0 == 0 and accumulatedFees1 == 0) {
                  return #Err(#InvalidInput("No fees to claim"));
                };

                // Update position's fee snapshot to current inside (settle-then-re-snapshot rule).
                let updated = Array.map<ConcentratedPosition, ConcentratedPosition>(cPositions, func(p) {
                  if (p.positionId == cp.positionId) {
                    { p with lastFeeGrowth0 = insideNow0; lastFeeGrowth1 = insideNow1; lastUpdateTime = nowVar }
                  } else { p }
                });
                Map.set(concentratedPositions, phash, caller, updated);

                // Update V3 claimed tracking
                Map.set(poolV3Data, hashtt, poolKey, {
                  v3 with
                  totalFeesClaimed0 = v3.totalFeesClaimed0 + accumulatedFees0;
                  totalFeesClaimed1 = v3.totalFeesClaimed1 + accumulatedFees1;
                });

                // Transfer fees
                let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
                let Tfees0 = returnTfees(token0);
                let Tfees1 = returnTfees(token1);

                if (accumulatedFees0 > Tfees0) {
                  Vector.add(tempTransferQueueLocal, (#principal(caller), accumulatedFees0 - Tfees0, token0, genTxId()));
                } else if (accumulatedFees0 > 0) {
                  addFees(token0, accumulatedFees0, false, "", nowVar);
                };
                if (accumulatedFees1 > Tfees1) {
                  Vector.add(tempTransferQueueLocal, (#principal(caller), accumulatedFees1 - Tfees1, token1, genTxId()));
                } else if (accumulatedFees1 > 0) {
                  addFees(token1, accumulatedFees1, false, "", nowVar);
                };

                if (Vector.size(tempTransferQueueLocal) > 0) {
                  if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
                    Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
                  };
                };

                return #Ok({
                  fees0 = accumulatedFees0;
                  fees1 = accumulatedFees1;
                  transferred0 = if (accumulatedFees0 > Tfees0) { accumulatedFees0 - Tfees0 } else { 0 };
                  transferred1 = if (accumulatedFees1 > Tfees1) { accumulatedFees1 - Tfees1 } else { 0 };
                  dust0ToDAO = if (accumulatedFees0 > 0 and accumulatedFees0 <= Tfees0) { accumulatedFees0 } else { 0 };
                  dust1ToDAO = if (accumulatedFees1 > 0 and accumulatedFees1 <= Tfees1) { accumulatedFees1 } else { 0 };
                });
              };
              case null {};
            };
          };
        };
      };
      case null {};
    };

    // No full-range V3 position for this pair. Concentrated positions must use
    // claimConcentratedFees(positionId) since they are scoped per-position.
    #Err(#InvalidInput("No full-range position for pair. Use claimConcentratedFees(positionId) for concentrated positions."));
  };

  // Per-position fee claim for concentrated liquidity. Pays out the position's
  // unclaimed fees and re-snapshots lastFeeGrowth0/1 to the current feeGrowthInside;
  // the position's liquidity is left intact. Math is bit-for-bit identical to the
  // UI-facing query getUserConcentratedPositions so a successful claim drains
  // exactly the amount the UI showed.
  public shared ({ caller }) func claimConcentratedFees(positionId : Nat) : async ExTypes.ClaimFeesResult {
    if (isAllowed(caller) != 1) { return #Err(#NotAuthorized) };

    switch (Map.get(concentratedPositions, phash, caller)) {
      case null { return #Err(#OrderNotFound("No positions for caller")) };
      case (?positions) {
        var target : ?ConcentratedPosition = null;
        for (p in positions.vals()) {
          if (p.positionId == positionId and p.liquidity > 0) { target := ?p };
        };
        switch (target) {
          case null { return #Err(#OrderNotFound("Position not found or zero liquidity")) };
          case (?pos) {
            let poolKey = (pos.token0, pos.token1);
            let v3 = switch (Map.get(poolV3Data, hashtt, poolKey)) {
              case null { return #Err(#PoolNotFound("V3 data not found")) };
              case (?v) { v };
            };
            let nowVar = Time.now();

            let (insideNow0, insideNow1) = positionFeeGrowthInside(pos, v3);
            let theoreticalFee0 = pos.liquidity * safeSub(insideNow0, pos.lastFeeGrowth0) / tenToPower60;
            let theoreticalFee1 = pos.liquidity * safeSub(insideNow1, pos.lastFeeGrowth1) / tenToPower60;
            let maxClaim0 = safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0);
            let maxClaim1 = safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1);
            let fee0 = Nat.min(theoreticalFee0, maxClaim0);
            let fee1 = Nat.min(theoreticalFee1, maxClaim1);

            if (fee0 == 0 and fee1 == 0) {
              return #Err(#InvalidInput("No fees to claim"));
            };

            let updated = Array.map<ConcentratedPosition, ConcentratedPosition>(positions, func(p) {
              if (p.positionId == positionId) {
                { p with lastFeeGrowth0 = insideNow0; lastFeeGrowth1 = insideNow1; lastUpdateTime = nowVar }
              } else { p }
            });
            Map.set(concentratedPositions, phash, caller, updated);

            Map.set(poolV3Data, hashtt, poolKey, {
              v3 with
              totalFeesClaimed0 = v3.totalFeesClaimed0 + fee0;
              totalFeesClaimed1 = v3.totalFeesClaimed1 + fee1;
            });

            let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
            let Tfees0 = returnTfees(pos.token0);
            let Tfees1 = returnTfees(pos.token1);

            if (fee0 > Tfees0) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), fee0 - Tfees0, pos.token0, genTxId()));
            } else if (fee0 > 0) {
              addFees(pos.token0, fee0, false, "", nowVar);
            };
            if (fee1 > Tfees1) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), fee1 - Tfees1, pos.token1, genTxId()));
            } else if (fee1 > 0) {
              addFees(pos.token1, fee1, false, "", nowVar);
            };

            if (Vector.size(tempTransferQueueLocal) > 0) {
              if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
                Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
              };
            };

            return #Ok({
              fees0 = fee0;
              fees1 = fee1;
              transferred0 = if (fee0 > Tfees0) { fee0 - Tfees0 } else { 0 };
              transferred1 = if (fee1 > Tfees1) { fee1 - Tfees1 } else { 0 };
              dust0ToDAO = if (fee0 > 0 and fee0 <= Tfees0) { fee0 } else { 0 };
              dust1ToDAO = if (fee1 > 0 and fee1 <= Tfees1) { fee1 } else { 0 };
            });
          };
        };
      };
    };
  };

  // ────────────────────────────────────────────────────────────────────────
  // claimAllLPFees — single-call batch claim across both LP storage maps
  //   - V3 store (concentratedPositions): full-range AND concentrated positions
  //   - V2 legacy store (userLiquidityPositions): pre-V3 positions still active
  //
  // Per-position math is identical to claimLPFees / claimConcentratedFees /
  // batchClaimAllFees — this function is purely an aggregator. Transfers are
  // consolidated by (recipient, token) before flush; saved ledger fees are
  // recovered to feescollectedDAO via addFees (drift-critical).
  // ────────────────────────────────────────────────────────────────────────
  public type LPFeeClaimEntry = {
    source : { #v3; #v2 };
    positionId : Nat;
    token0 : Text;
    token1 : Text;
    ratioLower : Nat;
    ratioUpper : Nat;
    fees0 : Nat;
    fees1 : Nat;
    transferred0 : Nat;
    transferred1 : Nat;
    dust0ToDAO : Nat;
    dust1ToDAO : Nat;
  };
  public type ClaimAllLPFeesOk = {
    positionsScanned : Nat;
    positionsClaimed : Nat;
    entries : [LPFeeClaimEntry];
    totalsTransferredByToken : [(Text, Nat)];
    consolidationSavings : [(Text, Nat)];
  };
  public type ClaimAllLPFeesResult = { #Ok : ClaimAllLPFeesOk; #Err : ExTypes.ExchangeError };

  public shared ({ caller }) func claimAllLPFees() : async ClaimAllLPFeesResult {
    if (isAllowed(caller) != 1) { return #Err(#NotAuthorized) };

    let nowVar = Time.now();
    let entries = Buffer.Buffer<LPFeeClaimEntry>(8);
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var positionsScanned : Nat = 0;
    var positionsClaimed : Nat = 0;

    // ── Phase 1 — V3 unified store ──
    switch (Map.get(concentratedPositions, phash, caller)) {
      case null {};
      case (?v3Positions) {
        let updatedV3 = Buffer.Buffer<ConcentratedPosition>(v3Positions.size());
        for (pos in v3Positions.vals()) {
          positionsScanned += 1;
          if (pos.liquidity == 0) {
            updatedV3.add(pos);
          } else {
            let poolKey = (pos.token0, pos.token1);
            switch (Map.get(poolV3Data, hashtt, poolKey)) {
              case null { updatedV3.add(pos) };
              case (?v3) {
                let (insideNow0, insideNow1) = positionFeeGrowthInside(pos, v3);
                let theoretical0 = pos.liquidity * safeSub(insideNow0, pos.lastFeeGrowth0) / tenToPower60;
                let theoretical1 = pos.liquidity * safeSub(insideNow1, pos.lastFeeGrowth1) / tenToPower60;
                let max0 = safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0);
                let max1 = safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1);
                let fee0 = Nat.min(theoretical0, max0);
                let fee1 = Nat.min(theoretical1, max1);

                // Mirror claimConcentratedFees:4438-4440 zero-fee early-skip.
                // Position preserved bit-for-bit; no totalFeesClaimed increment, no snapshot mutation.
                if (fee0 == 0 and fee1 == 0) {
                  updatedV3.add(pos);
                } else {
                  Map.set(poolV3Data, hashtt, poolKey, {
                    v3 with
                    totalFeesClaimed0 = v3.totalFeesClaimed0 + fee0;
                    totalFeesClaimed1 = v3.totalFeesClaimed1 + fee1;
                  });

                  updatedV3.add({
                    pos with
                    lastFeeGrowth0 = insideNow0;
                    lastFeeGrowth1 = insideNow1;
                    lastUpdateTime = nowVar;
                  });

                  let Tfees0 = returnTfees(pos.token0);
                  let Tfees1 = returnTfees(pos.token1);
                  var transferred0 : Nat = 0; var transferred1 : Nat = 0;
                  var dust0 : Nat = 0; var dust1 : Nat = 0;
                  if (fee0 > Tfees0) {
                    transferred0 := fee0 - Tfees0;
                    Vector.add(tempTransferQueueLocal, (#principal(caller), transferred0, pos.token0, genTxId()));
                  } else if (fee0 > 0) {
                    dust0 := fee0;
                    addFees(pos.token0, fee0, false, "", nowVar);
                  };
                  if (fee1 > Tfees1) {
                    transferred1 := fee1 - Tfees1;
                    Vector.add(tempTransferQueueLocal, (#principal(caller), transferred1, pos.token1, genTxId()));
                  } else if (fee1 > 0) {
                    dust1 := fee1;
                    addFees(pos.token1, fee1, false, "", nowVar);
                  };

                  positionsClaimed += 1;
                  entries.add({
                    source = #v3;
                    positionId = pos.positionId;
                    token0 = pos.token0; token1 = pos.token1;
                    ratioLower = pos.ratioLower; ratioUpper = pos.ratioUpper;
                    fees0 = fee0; fees1 = fee1;
                    transferred0; transferred1;
                    dust0ToDAO = dust0; dust1ToDAO = dust1;
                  });
                };
              };
            };
          };
        };
        Map.set(concentratedPositions, phash, caller, Buffer.toArray(updatedV3));
      };
    };

    // ── Phase 2 — V2 legacy store ──
    // V2 path intentionally does NOT apply safeSub cap (mirrors batchClaimAllFees:4815-4865 exactly).
    // See plan "Inherited limitations" for context.
    switch (Map.get(userLiquidityPositions, phash, caller)) {
      case null {};
      case (?v2Positions) {
        let updatedV2 = Array.map<LiquidityPosition, LiquidityPosition>(v2Positions, func(pos) {
          positionsScanned += 1;
          let accFee0 = pos.fee0 / tenToPower60;
          let accFee1 = pos.fee1 / tenToPower60;
          if (accFee0 == 0 and accFee1 == 0) return pos;

          let Tfees0 = returnTfees(pos.token0);
          let Tfees1 = returnTfees(pos.token1);
          var transferred0 : Nat = 0; var transferred1 : Nat = 0;
          var dust0 : Nat = 0; var dust1 : Nat = 0;

          if (accFee0 > Tfees0) {
            transferred0 := accFee0 - Tfees0;
            Vector.add(tempTransferQueueLocal, (#principal(caller), transferred0, pos.token0, genTxId()));
            recordOpDrift("v3Claim_xfer", pos.token0, -accFee0);
          } else if (accFee0 > 0) {
            dust0 := accFee0;
            addFees(pos.token0, accFee0, false, "", nowVar);
            recordOpDrift("v3Claim_dust", pos.token0, 0);
          };
          if (accFee1 > Tfees1) {
            transferred1 := accFee1 - Tfees1;
            Vector.add(tempTransferQueueLocal, (#principal(caller), transferred1, pos.token1, genTxId()));
            recordOpDrift("v3Claim_xfer", pos.token1, -accFee1);
          } else if (accFee1 > 0) {
            dust1 := accFee1;
            addFees(pos.token1, accFee1, false, "", nowVar);
            recordOpDrift("v3Claim_dust", pos.token1, 0);
          };

          let poolKey = (pos.token0, pos.token1);
          switch (Map.get(poolV3Data, hashtt, poolKey)) {
            case (?v3) {
              Map.set(poolV3Data, hashtt, poolKey, {
                v3 with
                totalFeesClaimed0 = v3.totalFeesClaimed0 + accFee0;
                totalFeesClaimed1 = v3.totalFeesClaimed1 + accFee1;
              });
            };
            case null {};
          };
          switch (Map.get(AMMpools, hashtt, poolKey)) {
            case (?pool) {
              Map.set(AMMpools, hashtt, poolKey, {
                pool with
                totalFee0 = if (pool.totalFee0 > accFee0) { pool.totalFee0 - accFee0 } else { 0 };
                totalFee1 = if (pool.totalFee1 > accFee1) { pool.totalFee1 - accFee1 } else { 0 };
              });
            };
            case null {};
          };

          positionsClaimed += 1;
          entries.add({
            source = #v2;
            positionId = 0;
            token0 = pos.token0; token1 = pos.token1;
            ratioLower = FULL_RANGE_LOWER; ratioUpper = FULL_RANGE_UPPER;
            fees0 = accFee0; fees1 = accFee1;
            transferred0; transferred1;
            dust0ToDAO = dust0; dust1ToDAO = dust1;
          });

          { pos with fee0 = 0; fee1 = 0; lastUpdateTime = nowVar };
        });
        Map.set(userLiquidityPositions, phash, caller, updatedV2);
      };
    };

    // ── Phase 3 — consolidate transfers by (recipient, token) ──
    // Mirrors adminExecuteRouteStrategy:15915-15945. Drift-critical: recover saved
    // Tfees back to feescollectedDAO whenever count > 1 for a (recipient, token) bucket.
    let preCountMap = Map.new<Text, Nat>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(preCountMap, thash, key)) {
        case (?n) { Map.set(preCountMap, thash, key, n + 1) };
        case null { Map.set(preCountMap, thash, key, 1) };
      };
    };
    let consolidatedMap = Map.new<Text, (TransferRecipient, Nat, Text, Text)>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(consolidatedMap, thash, key)) {
        case (?ex) { Map.set(consolidatedMap, thash, key, (tx.0, ex.1 + tx.1, tx.2, tx.3)) };
        case null { Map.set(consolidatedMap, thash, key, tx) };
      };
    };
    let consolidatedVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    for ((_, tx) in Map.entries(consolidatedMap)) { Vector.add(consolidatedVec, tx) };

    let consolidationSavings = Buffer.Buffer<(Text, Nat)>(Map.size(preCountMap));
    for ((key, count) in Map.entries(preCountMap)) {
      if (count > 1) {
        let tkn = switch (Map.get(consolidatedMap, thash, key)) { case (?tx) { tx.2 }; case null { "" } };
        if (tkn != "") {
          let savedFees = (count - 1) * returnTfees(tkn);
          if (savedFees > 0) {
            addFees(tkn, savedFees, false, "", nowVar);
            consolidationSavings.add((tkn, savedFees));
          };
        };
      };
    };

    // Build totalsTransferredByToken from the consolidated queue
    let totalsTransferredByToken = Buffer.Buffer<(Text, Nat)>(Vector.size(consolidatedVec));
    for (tx in Vector.vals(consolidatedVec)) {
      totalsTransferredByToken.add((tx.2, tx.1));
    };

    // ── Phase 4 — single flush ──
    if (Vector.size(consolidatedVec) > 0) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(consolidatedVec), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(consolidatedVec));
      };
    };

    #Ok({
      positionsScanned;
      positionsClaimed;
      entries = Buffer.toArray(entries);
      totalsTransferredByToken = Buffer.toArray(totalsTransferredByToken);
      consolidationSavings = Buffer.toArray(consolidationSavings);
    });
  };

  public shared ({ caller }) func removeLiquidity(token0i : Text, token1i : Text, liquidityAmount : Nat) : async ExTypes.RemoveLiquidityResult {
    if (isAllowed(caller) != 1) {
      return #Err(#NotAuthorized);
    };
    if (Text.size(token0i) > 150 or Text.size(token1i) > 150) {
      return #Err(#Banned);
    };
    let nowVar = Time.now();

    let (token0, token1) = getPool(token0i, token1i);
    let poolKey = (token0, token1);
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    // V3 path: if caller has a full-range V3 position, remove directly (no self-call)
    label v3Path switch (Map.get(concentratedPositions, phash, caller)) {
      case (?cPositions) {
        var foundPos : ?ConcentratedPosition = null;
        for (cp in cPositions.vals()) {
          if (cp.token0 == token0 and cp.token1 == token1 and cp.ratioLower == FULL_RANGE_LOWER and cp.ratioUpper == FULL_RANGE_UPPER and cp.liquidity > 0) {
            foundPos := ?cp;
          };
        };
        switch (foundPos) {
          case null { /* no full-range V3 position — fall through to V2 */ };
          case (?position) {
            let removeAmt = Nat.min(liquidityAmount, position.liquidity);
            if (removeAmt == 0) break v3Path;

            let pool = switch (Map.get(AMMpools, hashtt, poolKey)) {
              case null { return #Err(#PoolNotFound("Pool not found")) };
              case (?p) { p };
            };
            let v3 = switch (Map.get(poolV3Data, hashtt, poolKey)) {
              case null { return #Err(#PoolNotFound("V3 data not found")) };
              case (?v) { v };
            };

            // Calculate fees via feeGrowthInside (full-range → equals feeGrowthGlobal).
            let (insideNow0, insideNow1) = positionFeeGrowthInside(position, v3);
            let theoreticalFee0 = position.liquidity * safeSub(insideNow0, position.lastFeeGrowth0) / tenToPower60;
            let maxClaimable0 = safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0);
            let actualFee0 = Nat.min(theoreticalFee0, maxClaimable0);
            let theoreticalFee1 = position.liquidity * safeSub(insideNow1, position.lastFeeGrowth1) / tenToPower60;
            let maxClaimable1 = safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1);
            let actualFee1 = Nat.min(theoreticalFee1, maxClaimable1);

            // Calculate token amounts using proportional share of actual reserves.
            // addLiquidity adds exact deposit amounts to reserves, so we must remove
            // proportionally from reserves (not from V3 math, which rounds differently).
            // DRIFT FIX: see removeConcentratedLiquidity — same full-range key convention.
            let sqrtLower = if (position.ratioLower == FULL_RANGE_LOWER) { FULL_RANGE_LOWER } else { ratioToSqrtRatio(position.ratioLower) };
            let sqrtUpper = if (position.ratioUpper == FULL_RANGE_UPPER) { FULL_RANGE_UPPER } else { ratioToSqrtRatio(position.ratioUpper) };
            let baseAmount0 = if (pool.totalLiquidity > 0) { mulDiv(removeAmt, pool.reserve0, pool.totalLiquidity) } else { 0 };
            let baseAmount1 = if (pool.totalLiquidity > 0) { mulDiv(removeAmt, pool.reserve1, pool.totalLiquidity) } else { 0 };
            let totalAmount0 = baseAmount0 + (actualFee0 * removeAmt / position.liquidity);
            let totalAmount1 = baseAmount1 + (actualFee1 * removeAmt / position.liquidity);

            // Update range tree
            var ranges = v3.ranges;
            switch (RBTree.get(ranges, Nat.compare, sqrtLower)) {
              case (?d) {
                let newGross = if (d.liquidityGross > removeAmt) { d.liquidityGross - removeAmt } else { 0 };
                if (newGross == 0) { ranges := RBTree.delete(ranges, Nat.compare, sqrtLower) }
                else { ranges := RBTree.put(ranges, Nat.compare, sqrtLower, { d with liquidityNet = d.liquidityNet - removeAmt; liquidityGross = newGross }) };
              };
              case null {
                if (test) Debug.print("TICK_TREE_MISS removeLiquidity lower posId=" # Nat.toText(position.positionId) # " sqrtLower=" # Nat.toText(sqrtLower) # " ratioLower=" # Nat.toText(position.ratioLower));
                logger.warn("AMM", "removeLiquidity lower-tick miss posId=" # Nat.toText(position.positionId), "removeLiquidity");
              };
            };
            switch (RBTree.get(ranges, Nat.compare, sqrtUpper)) {
              case (?d) {
                let newGross = if (d.liquidityGross > removeAmt) { d.liquidityGross - removeAmt } else { 0 };
                if (newGross == 0) { ranges := RBTree.delete(ranges, Nat.compare, sqrtUpper) }
                else { ranges := RBTree.put(ranges, Nat.compare, sqrtUpper, { d with liquidityNet = d.liquidityNet + removeAmt; liquidityGross = newGross }) };
              };
              case null {
                if (test) Debug.print("TICK_TREE_MISS removeLiquidity upper posId=" # Nat.toText(position.positionId) # " sqrtUpper=" # Nat.toText(sqrtUpper) # " ratioUpper=" # Nat.toText(position.ratioUpper));
                logger.warn("AMM", "removeLiquidity upper-tick miss posId=" # Nat.toText(position.positionId), "removeLiquidity");
              };
            };

            // Update active liquidity
            let currentRatio = if (v3.currentSqrtRatio > 0) { (v3.currentSqrtRatio * v3.currentSqrtRatio) / tenToPower60 } else { 0 };
            let newActiveLiq = if (currentRatio >= position.ratioLower and currentRatio < position.ratioUpper) {
              if (v3.activeLiquidity > removeAmt) { v3.activeLiquidity - removeAmt } else { 0 };
            } else { v3.activeLiquidity };

            // Update pool — subtract total amounts (base + fees) from reserves
            Map.set(AMMpools, hashtt, poolKey, {
              pool with
              reserve0 = if (pool.reserve0 > totalAmount0) { pool.reserve0 - totalAmount0 } else { 0 };
              reserve1 = if (pool.reserve1 > totalAmount1) { pool.reserve1 - totalAmount1 } else { 0 };
              totalLiquidity = if (pool.totalLiquidity > removeAmt) { pool.totalLiquidity - removeAmt } else { 0 };
              lastUpdateTime = nowVar;
            });
            Map.set(poolV3Data, hashtt, poolKey, {
              v3 with activeLiquidity = newActiveLiq;
              totalFeesClaimed0 = v3.totalFeesClaimed0 + (actualFee0 * removeAmt / position.liquidity);
              totalFeesClaimed1 = v3.totalFeesClaimed1 + (actualFee1 * removeAmt / position.liquidity);
              ranges = ranges;
            });

            // Update V3 position
            let newLiq = position.liquidity - removeAmt;
            if (newLiq == 0) {
              let filtered = Array.filter<ConcentratedPosition>(cPositions, func(p) { p.positionId != position.positionId });
              if (filtered.size() == 0) { Map.delete(concentratedPositions, phash, caller) }
              else { Map.set(concentratedPositions, phash, caller, filtered) };
            } else {
              let v3After = switch (Map.get(poolV3Data, hashtt, poolKey)) { case (?v) { v }; case null { v3 } };
              let updated = Array.map<ConcentratedPosition, ConcentratedPosition>(cPositions, func(p) {
                if (p.positionId == position.positionId) {
                  let (insideNow0, insideNow1) = positionFeeGrowthInside(p, v3After);
                  { p with liquidity = newLiq; lastFeeGrowth0 = insideNow0; lastFeeGrowth1 = insideNow1; lastUpdateTime = nowVar };
                } else { p };
              });
              Map.set(concentratedPositions, phash, caller, updated);
            };

            // Sync AMMPool from V3
            syncPoolFromV3(poolKey);

            // Transfer tokens
            let Tfees0 = returnTfees(token0);
            let Tfees1 = returnTfees(token1);
            if (totalAmount0 > Tfees0) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), totalAmount0 - Tfees0, token0, genTxId()));
            } else if (totalAmount0 > 0) {
              // DRIFT FIX: see removeConcentratedLiquidity — reserves + v3Resid
              // were already decremented; mirror that by pushing dust to DAO.
              addFees(token0, totalAmount0, false, "", nowVar);
            };
            if (totalAmount1 > Tfees1) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), totalAmount1 - Tfees1, token1, genTxId()));
            } else if (totalAmount1 > 0) {
              addFees(token1, totalAmount1, false, "", nowVar);
            };

            doInfoBeforeStep2();
            if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
              Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
            };

            return #Ok({ amount0 = totalAmount0; amount1 = totalAmount1; fees0 = actualFee0; fees1 = actualFee1; liquidityBurned = removeAmt });
          };
        };
      };
      case null {};
    };

    // No V3 position found
    #Err(#OrderNotFound("No liquidity position found"));
  };

  public query ({ caller }) func getAMMPoolInfo(token0 : Text, token1 : Text) : async ?{
    token0 : Text;
    token1 : Text;
    reserve0 : Nat;
    reserve1 : Nat;
    price0 : Float;
    price1 : Float;
  } {
    if (isAllowedQuery(caller) != 1) {
      return null;
    };
    let poolKey = getPool(token0, token1);

    switch (Map.get(AMMpools, hashtt, poolKey)) {
      case (null) {
        null;
      };
      case (?pool) {
        let (p0, p1) = getPoolPriceV3((pool.token0, pool.token1));
        ?{
          token0 = pool.token0;
          token1 = pool.token1;
          reserve0 = pool.reserve0;
          reserve1 = pool.reserve1;
          price0 = p0;
          price1 = p1;
        };
      };
    };
  };

  public query ({ caller }) func getAllAMMPools() : async [{
    token0 : Text;
    token1 : Text;
    reserve0 : Nat;
    reserve1 : Nat;
    price0 : Float;
    price1 : Float;
    totalLiquidity : Nat;
  }] {
    if (isAllowedQuery(caller) != 1) {
      return [];
    };
    let result = Vector.new<{
      token0 : Text;
      token1 : Text;
      reserve0 : Nat;
      reserve1 : Nat;
      price0 : Float;
      price1 : Float;
      totalLiquidity : Nat;
    }>();
    for ((_, pool) in Map.entries(AMMpools)) {
      if (pool.reserve0 > 0 and pool.reserve1 > 0) {
        let (p0, p1) = getPoolPriceV3((pool.token0, pool.token1));
        Vector.add(
          result,
          {
            token0 = pool.token0;
            token1 = pool.token1;
            reserve0 = pool.reserve0;
            reserve1 = pool.reserve1;
            price0 = p0;
            price1 = p1;
            totalLiquidity = pool.totalLiquidity;
          },
        );
      };
    };
    Vector.toArray(result);
  };

  // ═══════════════════════════════════════════════════════════════
  // DAO LP HELPER FUNCTIONS
  // Combined queries and batch operations for treasury LP management
  // ═══════════════════════════════════════════════════════════════

  // Step 22: Combined LP positions + pool data in one call (saves 1 inter-canister call per cycle)
  public query ({ caller }) func getDAOLiquiditySnapshot() : async {
    positions : [{
      token0 : Text; token1 : Text; liquidity : Nat;
      token0Amount : Nat; token1Amount : Nat; shareOfPool : Float;
      fee0 : Nat; fee1 : Nat;
    }];
    pools : [{
      token0 : Text; token1 : Text;
      reserve0 : Nat; reserve1 : Nat;
      totalLiquidity : Nat;
      price0 : Float; price1 : Float;
    }];
  } {
    if (isAllowedQuery(caller) != 1) return { positions = []; pools = [] };

    // Get caller's V2 LP positions
    let posVec = Vector.new<{
      token0 : Text; token1 : Text; liquidity : Nat;
      token0Amount : Nat; token1Amount : Nat; shareOfPool : Float;
      fee0 : Nat; fee1 : Nat;
    }>();
    switch (Map.get(userLiquidityPositions, phash, caller)) {
      case (?positions) {
        for (pos in positions.vals()) {
          let poolKey = (pos.token0, pos.token1);
          switch (Map.get(AMMpools, hashtt, poolKey)) {
            case (?pool) {
              if (pool.totalLiquidity > 0) {
                let t0Amount = (pos.liquidity * pool.reserve0) / pool.totalLiquidity;
                let t1Amount = (pos.liquidity * pool.reserve1) / pool.totalLiquidity;
                let share = Float.fromInt(pos.liquidity) / Float.fromInt(pool.totalLiquidity);
                Vector.add(posVec, {
                  token0 = pos.token0; token1 = pos.token1;
                  liquidity = pos.liquidity;
                  token0Amount = t0Amount; token1Amount = t1Amount;
                  shareOfPool = share;
                  fee0 = pos.fee0 / tenToPower60;
                  fee1 = pos.fee1 / tenToPower60;
                });
              };
            };
            case null {};
          };
        };
      };
      case null {};
    };

    // Get all pool states
    let poolVec = Vector.new<{
      token0 : Text; token1 : Text;
      reserve0 : Nat; reserve1 : Nat;
      totalLiquidity : Nat;
      price0 : Float; price1 : Float;
    }>();
    for ((_, pool) in Map.entries(AMMpools)) {
      if (pool.reserve0 > 0 and pool.reserve1 > 0) {
        let (p0, p1) = getPoolPriceV3((pool.token0, pool.token1));
        Vector.add(poolVec, {
          token0 = pool.token0; token1 = pool.token1;
          reserve0 = pool.reserve0; reserve1 = pool.reserve1;
          totalLiquidity = pool.totalLiquidity;
          price0 = p0;
          price1 = p1;
        });
      };
    };

    { positions = Vector.toArray(posVec); pools = Vector.toArray(poolVec) };
  };

  // Step 23: Batch claim fees from ALL caller's LP positions in one call
  public shared ({ caller }) func batchClaimAllFees() : async [{
    token0 : Text; token1 : Text;
    fees0 : Nat; fees1 : Nat;
    transferred0 : Nat; transferred1 : Nat;
  }] {
    if (isAllowed(caller) != 1) return [];
    let nowVar = Time.now();

    let results = Vector.new<{
      token0 : Text; token1 : Text;
      fees0 : Nat; fees1 : Nat;
      transferred0 : Nat; transferred1 : Nat;
    }>();
    let transferBatch = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    switch (Map.get(userLiquidityPositions, phash, caller)) {
      case (?positions) {
        let updatedPositions = Array.map<LiquidityPosition, LiquidityPosition>(
          positions,
          func(pos) {
            let accFee0 = pos.fee0 / tenToPower60;
            let accFee1 = pos.fee1 / tenToPower60;
            if (accFee0 == 0 and accFee1 == 0) return pos;

            let Tfees0 = returnTfees(pos.token0);
            let Tfees1 = returnTfees(pos.token1);

            var transferred0 : Nat = 0;
            var transferred1 : Nat = 0;

            if (accFee0 > Tfees0) {
              Vector.add(transferBatch, (#principal(caller), accFee0 - Tfees0, pos.token0, genTxId()));
              transferred0 := accFee0 - Tfees0;
              recordOpDrift("v3Claim_xfer", pos.token0, -accFee0);
            } else if (accFee0 > 0) {
              addFees(pos.token0, accFee0, false, "", nowVar);
              recordOpDrift("v3Claim_dust", pos.token0, 0);
            };
            if (accFee1 > Tfees1) {
              Vector.add(transferBatch, (#principal(caller), accFee1 - Tfees1, pos.token1, genTxId()));
              transferred1 := accFee1 - Tfees1;
              recordOpDrift("v3Claim_xfer", pos.token1, -accFee1);
            } else if (accFee1 > 0) {
              addFees(pos.token1, accFee1, false, "", nowVar);
              recordOpDrift("v3Claim_dust", pos.token1, 0);
            };

            // Update V3 totalFeesClaimed
            let poolKey = (pos.token0, pos.token1);
            switch (Map.get(poolV3Data, hashtt, poolKey)) {
              case (?v3) {
                Map.set(poolV3Data, hashtt, poolKey, {
                  v3 with
                  totalFeesClaimed0 = v3.totalFeesClaimed0 + accFee0;
                  totalFeesClaimed1 = v3.totalFeesClaimed1 + accFee1;
                });
              };
              case null {};
            };

            // Deduct from pool total fees
            switch (Map.get(AMMpools, hashtt, poolKey)) {
              case (?pool) {
                Map.set(AMMpools, hashtt, poolKey, {
                  pool with
                  totalFee0 = if (pool.totalFee0 > accFee0) { pool.totalFee0 - accFee0 } else { 0 };
                  totalFee1 = if (pool.totalFee1 > accFee1) { pool.totalFee1 - accFee1 } else { 0 };
                });
              };
              case null {};
            };

            Vector.add(results, {
              token0 = pos.token0; token1 = pos.token1;
              fees0 = accFee0; fees1 = accFee1;
              transferred0 = transferred0; transferred1 = transferred1;
            });

            // Zero out fees on position
            { pos with fee0 = 0; fee1 = 0; lastUpdateTime = nowVar };
          },
        );
        Map.set(userLiquidityPositions, phash, caller, updatedPositions);
      };
      case null {};
    };

    // Execute all transfers in one batch
    if (Vector.size(transferBatch) > 0) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray(transferBatch), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(transferBatch));
      };
    };

    Vector.toArray(results);
  };

  // Step 24: Batch adjust liquidity across multiple pools in one call
  public shared ({ caller }) func batchAdjustLiquidity(adjustments : [{
    token0 : Text; token1 : Text;
    action : { #Remove : { liquidityAmount : Nat } };
    // Note: #Add requires prior transfers with block numbers — handled individually
  }]) : async [{
    token0 : Text; token1 : Text;
    success : Bool; result : Text;
  }] {
    if (isAllowed(caller) != 1) return [];
    if (adjustments.size() > 10) return []; // Cap at 10 per call

    let results = Vector.new<{ token0 : Text; token1 : Text; success : Bool; result : Text }>();
    let transferBatch = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    let nowVar = Time.now();

    for (adj in adjustments.vals()) {
      let (token0, token1) = getPool(adj.token0, adj.token1);
      switch (adj.action) {
        case (#Remove({ liquidityAmount })) {
          // Find user's position
          switch (Map.get(userLiquidityPositions, phash, caller)) {
            case (?positions) {
              var found = false;
              for (pos in positions.vals()) {
                if ((pos.token0 == token0 and pos.token1 == token1) or (pos.token0 == token1 and pos.token1 == token0)) {
                  found := true;
                };
              };
              if (not found) {
                Vector.add(results, { token0 = adj.token0; token1 = adj.token1; success = false; result = "No position found" });
              } else {
                // Use the existing removeLiquidity logic inline
                try {
                  let removeResult = await removeLiquidity(adj.token0, adj.token1, liquidityAmount);
                  switch (removeResult) {
                    case (#Ok(ok)) {
                      Vector.add(results, { token0 = adj.token0; token1 = adj.token1; success = true; result = "Removed " # Nat.toText(ok.liquidityBurned) # " liq, got " # Nat.toText(ok.amount0) # "/" # Nat.toText(ok.amount1) });
                    };
                    case (#Err(e)) {
                      Vector.add(results, { token0 = adj.token0; token1 = adj.token1; success = false; result = debug_show(e) });
                    };
                  };
                } catch (e) {
                  Vector.add(results, { token0 = adj.token0; token1 = adj.token1; success = false; result = Error.message(e) });
                };
              };
            };
            case null {
              Vector.add(results, { token0 = adj.token0; token1 = adj.token1; success = false; result = "No positions" });
            };
          };
        };
      };
    };

    Vector.toArray(results);
  };

  // Step 25: Trusted DAO caller LP addition — skips spam protection and revoke fees
  public shared ({ caller }) func addLiquidityDAO(
    token0i : Text, token1i : Text,
    amount0i : Nat, amount1i : Nat,
    block0i : Nat, block1i : Nat,
    isInitial : ?Bool,
  ) : async ExTypes.AddLiquidityResult {
    // Only admin/DAO can call this
    if (not test and not isAdmin(caller)) {
      return #Err(#NotAuthorized);
    };
    // Delegate to regular addLiquidity — the admin check above replaces spam protection
    // Regular addLiquidity handles pool creation, reserves, refunds, V3 sync
    await addLiquidity(token0i, token1i, amount0i, amount1i, block0i, block1i, isInitial);
  };

  // Step 26: LP performance data for monitoring
  public query ({ caller }) func getDAOLPPerformance() : async [{
    token0 : Text; token1 : Text;
    currentValue0 : Nat; currentValue1 : Nat;
    totalFeesEarned0 : Nat; totalFeesEarned1 : Nat;
    shareOfPool : Float;
    poolVolume24h : Nat;
  }] {
    if (isAllowedQuery(caller) != 1) return [];

    let results = Vector.new<{
      token0 : Text; token1 : Text;
      currentValue0 : Nat; currentValue1 : Nat;
      totalFeesEarned0 : Nat; totalFeesEarned1 : Nat;
      shareOfPool : Float;
      poolVolume24h : Nat;
    }>();

    switch (Map.get(userLiquidityPositions, phash, caller)) {
      case (?positions) {
        for (pos in positions.vals()) {
          let poolKey = (pos.token0, pos.token1);
          switch (Map.get(AMMpools, hashtt, poolKey)) {
            case (?pool) {
              if (pool.totalLiquidity > 0) {
                let t0Amount = (pos.liquidity * pool.reserve0) / pool.totalLiquidity;
                let t1Amount = (pos.liquidity * pool.reserve1) / pool.totalLiquidity;
                let share = Float.fromInt(pos.liquidity) / Float.fromInt(pool.totalLiquidity);
                let volume = update24hVolume(poolKey);
                Vector.add(results, {
                  token0 = pos.token0; token1 = pos.token1;
                  currentValue0 = t0Amount; currentValue1 = t1Amount;
                  totalFeesEarned0 = pos.fee0 / tenToPower60;
                  totalFeesEarned1 = pos.fee1 / tenToPower60;
                  shareOfPool = share;
                  poolVolume24h = volume;
                });
              };
            };
            case null {};
          };
        };
      };
      case null {};
    };

    Vector.toArray(results);
  };

  // Function to update kline data with a new trade
  func updateKlineData(token1 : Text, token2 : Text, price : Float, volume : Nat) {
    if (price <= 0.0) return; // skip zero/negative prices

    let pool = getPool(token1, token2);
    let nowVar = Time.now();
    let currentTime = nowVar;
    let klineKey : KlineKey = (pool.0, pool.1, #fivemin);
    let (_, fiveMinDuration) = getTimeFrameDetails(#fivemin, currentTime);

    // First update current data
    let timeFrames : [TimeFrame] = [#fivemin, #hour, #fourHours, #day, #week];
    for (timeFrame in timeFrames.vals()) {
      let klineKey : KlineKey = (pool.0, pool.1, timeFrame);
      let (timeFrameStart, timeFrameDuration) = getTimeFrameDetails(timeFrame, currentTime);
      let tree = switch (Map.get(klineDataStorage, hashkl, klineKey)) {
        case null { RBTree.init<Int, KlineData>() };
        case (?existing) { existing };
      };
      let alignedTimestamp = alignTimestamp(currentTime, timeFrameDuration / 1_000_000_000);
      let currentKline = switch (RBTree.get(tree, compareTime, alignedTimestamp)) {
        case null {
          let lastClose = switch (getLastKline(klineKey)) {
            case null { price };
            case (?lastKline) { lastKline.close };
          };
          {
            timestamp = alignedTimestamp;
            open = lastClose;
            high = price;
            low = price;
            close = price;
            volume = volume;
          };
        };
        case (?existingKline) {
          {
            timestamp = existingKline.timestamp;
            open = if (existingKline.open == 0.0) { price } else { existingKline.open };
            high = Float.max(existingKline.high, price);
            low = if (existingKline.low == 0.0 or existingKline.low > price) { price } else { existingKline.low };
            close = price;
            volume = existingKline.volume + volume;
          };
        };
      };
      Map.set(klineDataStorage, hashkl, klineKey, RBTree.put(tree, compareTime, alignedTimestamp, currentKline));
    };

    // Check for gaps AFTER updating current data
    let lastKline = getLastKline(klineKey);
    switch (lastKline) {
      case null {
        catchUpPoolKlineData(pool.0, pool.1);
      };
      case (?kline) {
        if (kline.timestamp < nowVar - fiveMinDuration) {
          // If more than one interval old, fill gaps
          // We don't need to create klines for the current period as we just did that above
          let endTime = alignTimestamp(currentTime - fiveMinDuration, 300);
          let startTime = kline.timestamp + fiveMinDuration;
          let updatedKlines = createOrUpdateKlines(klineKey, startTime, endTime, ?kline);
        };
      };
    };
  };

  func catchUpPoolKlineData(token1 : Text, token2 : Text) {
    let nowVar = Time.now();
    let currentTime = nowVar;
    let timeFrames : [TimeFrame] = [#fivemin, #hour, #fourHours, #day, #week];

    for (timeFrame in timeFrames.vals()) {
      let klineKey : KlineKey = (token1, token2, timeFrame);
      let (_, timeFrameDuration) = getTimeFrameDetails(timeFrame, currentTime);

      // Find the last existing kline
      let lastKline = getLastKline(klineKey);

      let startTime = switch (lastKline) {
        case null {
          // If no klines exist, start from a reasonable past time, e.g., 30 days ago
          currentTime - (30 * 24 * 3600 * 1_000_000_000);
        };
        case (?kline) {
          // Start from the timestamp after the last kline
          kline.timestamp + timeFrameDuration;
        };
      };

      let updatedKlines = createOrUpdateKlines(klineKey, startTime, currentTime, lastKline);

    };
  };

  // funtion that fills KLine data if there havent been new trades for some time
  func catchUpKlineData() {
    let nowVar = Time.now();
    let currentTime = nowVar;
    let timeFrames : [TimeFrame] = [#fivemin, #hour, #fourHours, #day, #week];

    label pools for (poolKey in Vector.vals(pool_canister)) {
      let (token1, token2) = poolKey;


      for (timeFrame in timeFrames.vals()) {
        let klineKey : KlineKey = (token1, token2, timeFrame);

        // Find the last existing kline
        let lastKline = getLastKline(klineKey);

        let startTime = switch (lastKline) {
          case null {
            // If no klines exist, start from a reasonable past time, e.g., 30 days ago
            currentTime - (30 * 24 * 3600 * 1_000_000_000);
          };
          case (?kline) {
            // Start from the timestamp after the last kline
            kline.timestamp + 1;
          };
        };

        let updatedKlines = createOrUpdateKlines(klineKey, startTime, currentTime, lastKline);

      };
    };
  };

  func initializeKlines(token1 : Text, token2 : Text, initialPrice : Float, initialVolume : Nat) {
    let pool = getPool(token1, token2);
    let nowVar = Time.now();
    let currentTime = nowVar;
    let timeFrames : [TimeFrame] = [#fivemin, #hour, #fourHours, #day, #week];

    for (timeFrame in timeFrames.vals()) {
      let klineKey : KlineKey = (pool.0, pool.1, timeFrame);
      let (alignedTimestamp, _) = getTimeFrameDetails(timeFrame, currentTime);

      let initialKline : KlineData = {
        timestamp = alignedTimestamp;
        open = initialPrice;
        high = initialPrice;
        low = initialPrice;
        close = initialPrice;
        volume = initialVolume;
      };

      var tree = RBTree.init<Int, KlineData>();
      tree := RBTree.put(tree, compareTime, alignedTimestamp, initialKline);
      Map.set(klineDataStorage, hashkl, klineKey, tree);
    };
  };

  // function to check and aggregate all KLine data. aggregating= aggregating for example 12* 5 minute lines to a 1 hour Kline
  func checkAndAggregateAllPools() {
    let nowVar = Time.now();
    let currentTime = nowVar;
    let timeFrames : [TimeFrame] = [#fivemin, #hour, #fourHours, #day, #week];

    // First, check if we need to catch up
    label pools for (poolKey in Vector.vals(pool_canister)) {
      let klineKey : KlineKey = (poolKey.0, poolKey.1, #fivemin);
      let (timeFrameStart, timeFrameDuration) = getTimeFrameDetails(#fivemin, currentTime);

      // Find the last existing kline
      let lastKline = switch (getLastKline(klineKey)) {
        case null { continue pools; createEmptyKline(1, 0.0) };
        case (?a) { a };
      };
      if (lastKline.timestamp < nowVar - (300000000000)) {

        catchUpKlineData();
        break pools;
      };
    };

    // Now proceed with regular updates
    for (poolKey in Vector.vals(pool_canister)) {
      let (token1, token2) = poolKey;

      ignore updatePriceDayBefore(poolKey, currentTime);

      for (timeFrame in timeFrames.vals()) {
        let klineKey : KlineKey = (token1, token2, timeFrame);
        let (timeFrameStart, _) = getTimeFrameDetails(timeFrame, currentTime);



        // Find the last existing kline
        let lastKline = getLastKline(klineKey);


        // Calculate the start time for the first kline we need to add or update
        let startTime = switch (lastKline) {
          case null { timeFrameStart };
          case (?kline) { kline.timestamp + 1 };
        };

        let updatedKlines = createOrUpdateKlines(klineKey, startTime, currentTime, lastKline);

      };
    };
  };

  func getLastKline(klineKey : KlineKey) : ?KlineData {
    let nowVar = Time.now();

    switch (Map.get(klineDataStorage, hashkl, klineKey)) {
      case null { null };
      case (?tree) {
        switch (RBTree.scanLimit(tree, compareTime, 0, nowVar, #bwd, 1).results) {
          case (a) { if (a.size() == 0) { null } else { ?a[0].1 } };
        };
      };
    };
  };

  private func createOrUpdateKlines(klineKey : KlineKey, startTime : Int, endTime : Int, lastKline : ?KlineData) : [KlineData] {
    let (_, timeFrameDuration) = getTimeFrameDetails(klineKey.2, endTime);

    var tree = switch (Map.get(klineDataStorage, hashkl, klineKey)) {
      case null { RBTree.init<Int, KlineData>() };
      case (?existing) { existing };
    };

    // Align start and end times
    let alignedStartTime = alignTimestamp(startTime, timeFrameDuration / 1_000_000_000);
    let alignedEndTime = alignTimestamp(endTime, timeFrameDuration / 1_000_000_000);

    // Calculate number of intervals
    let numKlines = Int.max(((alignedEndTime - alignedStartTime) / timeFrameDuration) + 1, 1);

    func generateKline(index : Nat) : KlineData {
      // Properly align each timestamp instead of direct addition
      let currentTime = alignTimestamp(
        alignedStartTime + (index * timeFrameDuration),
        timeFrameDuration / 1_000_000_000,
      );

      switch (RBTree.get(tree, compareTime, currentTime)) {
        case null {
          let previousClose = if (index == 0) {
            switch (lastKline) {
              case null {
                // No previous kline — use current pool price instead of 0.0
                let poolKey2 = (klineKey.0, klineKey.1);
                switch (Map.get(AMMpools, hashtt, poolKey2)) {
                  case (?pool) {
                    if (pool.reserve0 > 0 and pool.reserve1 > 0) {
                      let d0 = switch (Map.get(tokenInfo, thash, pool.token0)) { case (?i) { i.Decimals }; case null { 8 } };
                      let d1 = switch (Map.get(tokenInfo, thash, pool.token1)) { case (?i) { i.Decimals }; case null { 8 } };
                      (Float.fromInt(pool.reserve1) * Float.fromInt(10 ** d0)) / (Float.fromInt(pool.reserve0) * Float.fromInt(10 ** d1));
                    } else { 0.0 };
                  };
                  case null { 0.0 };
                };
              };
              case (?kline) { kline.close };
            };
          } else {
            (generateKline(index - 1)).close;
          };
          createEmptyKline(currentTime, previousClose);
        };
        case (?existingKline) {
          // Don't overwrite existing data
          existingKline;
        };
      };
    };

    let klines = Array.tabulate<KlineData>(
      Int.abs(numKlines),
      func(i) {
        let kline = generateKline(i);
        // Only store if timestamp is valid (not in future)
        if (kline.timestamp <= Time.now()) {
          tree := RBTree.put(tree, compareTime, kline.timestamp, kline);
        };
        kline;
      },
    );

    Map.set(klineDataStorage, hashkl, klineKey, tree);
    klines;
  };

  public query ({ caller }) func getKlineData(token1 : Text, token2 : Text, timeFrame : TimeFrame, initialGet : Bool) : async [KlineData] {
    if (isAllowedQuery(caller) != 1) {
      return [];
    };
    let nowVar = Time.now();
    let pool = getPool(token1, token2);
    let klineKey : KlineKey = (pool.0, pool.1, timeFrame);

    switch (Map.get(klineDataStorage, hashkl, klineKey)) {
      case null {

        [];
      };
      case (?tree) {
        let scanResult = RBTree.scanLimit(
          tree,
          compareTime,
          0,
          nowVar,
          #bwd,
          if initialGet {
            13000 // Limit to 13000 entries (worst case max)
          } else { 2 },
        );

        let result = Array.map(scanResult.results, func((_, kline) : (Int, KlineData)) : KlineData { kline });

        result;
      };
    };
  };

  // Paginated/range variant of getKlineData.
  // - Returns ASCENDING order (oldest first) — matches lightweight-charts expectations.
  // - `before = null` → latest candles (timestamp ≤ now).
  // - `before = ?T` → candles with timestamp strictly < T (for scroll-left pagination).
  // - Skips zero-volume placeholder candles so the client doesn't have to dedup them.
  // - `limit` capped internally at 2000 (requests above or at 0 are clamped).
  public query ({ caller }) func getKlineDataRange(
    token1 : Text,
    token2 : Text,
    timeFrame : TimeFrame,
    before : ?Int,
    limit : Nat,
  ) : async [KlineData] {
    if (isAllowedQuery(caller) != 1) { return [] };
    let INTERNAL_CAP : Nat = 2000;
    let effectiveLimit : Nat = if (limit == 0 or limit > INTERNAL_CAP) { INTERNAL_CAP } else { limit };

    let pool = getPool(token1, token2);
    let klineKey : KlineKey = (pool.0, pool.1, timeFrame);

    switch (Map.get(klineDataStorage, hashkl, klineKey)) {
      case null { [] };
      case (?tree) {
        let upperBound : Int = switch (before) {
          case null { Time.now() };
          case (?t) {
            // scanLimit upperBound is inclusive; we want strictly < t, so subtract 1.
            if (t > 0) { t - 1 } else { 0 };
          };
        };

        let scanResult = RBTree.scanLimit(
          tree,
          compareTime,
          0,
          upperBound,
          #bwd, // descending from upperBound
          effectiveLimit,
        );

        // Filter out zero-volume placeholders (createEmptyKline fill-candles)
        // and extract KlineData values. Results are currently newest-first.
        let filteredDesc = Array.mapFilter<(Int, KlineData), KlineData>(
          scanResult.results,
          func((_, k) : (Int, KlineData)) : ?KlineData {
            if (k.volume > 0) { ?k } else { null };
          },
        );

        // Reverse to ascending (oldest first) for the frontend chart.
        Array.reverse(filteredDesc);
      };
    };
  };

  // Batched 7-day price trend for the Portfolio view.
  // For each requested token, returns 28 samples (oldest-first, one per 6h across ~7 days),
  // current USD spot, and 24h/7d percent change — computed server-side so the frontend and
  // other canisters can't disagree on "what's 24h change?". USD is derived from the token/ICP
  // and ICP/ckUSDC 4h klines (ckUSDC ≈ $1). Missing samples carry the last known price forward.
  // A token with no history at all returns an empty `points` array.
  public query ({ caller }) func get_token_trends_7d(tokens : [Principal]) : async {
    #ok : [{
      token : Principal;
      points : [Float];
      price_now : Float;
      change_pct_24h : Float;
      change_pct_7d : Float;
    }];
    #err : Text;
  } {
    if (isAllowedQuery(caller) != 1) { return #err("Not authorized") };

    let nowVar = Time.now();
    let SIX_HOURS_NS : Int = 21_600_000_000_000;
    let ICP_TEXT : Text = "ryjl3-tyaaa-aaaaa-aaaba-cai";
    let CKUSDC_TEXT : Text = "xevnm-gaaaa-aaaar-qafnq-cai";

    // 28 target timestamps, oldest-first. points[0] ≈ now - 162h, points[27] = now.
    // (24h-ago = points[27 - 4] = points[23] — matches change_pct_24h intent.)
    let targets : [Int] = Array.tabulate<Int>(28, func(i) { nowVar - (27 - i) * SIX_HOURS_NS });

    // Return the latest 4h-kline close at-or-before `ts` for the given pool, or null.
    func klineCloseAtOrBefore(t0 : Text, t1 : Text, ts : Int) : ?Float {
      let (pt0, pt1) = getPool(t0, t1);
      let key : KlineKey = (pt0, pt1, #fourHours);
      switch (Map.get(klineDataStorage, hashkl, key)) {
        case null { null };
        case (?tree) {
          let r = RBTree.scanLimit(tree, compareTime, 0, ts, #bwd, 1);
          if (r.results.size() > 0) { ?r.results[0].1.close } else { null };
        };
      };
    };

    // Price of `token` denominated in ICP at `ts`. Handles pool orientation and ICP itself.
    func tokenInICPAt(token : Text, ts : Int) : ?Float {
      if (token == ICP_TEXT) { return ?1.0 };
      let (pt0, pt1) = getPool(token, ICP_TEXT);
      switch (klineCloseAtOrBefore(token, ICP_TEXT, ts)) {
        case null { null };
        case (?close) {
          if (close <= 0.0) { null }
          else if (pt1 == ICP_TEXT) { ?close }         // close = ICP per token
          else { ?(1.0 / close) };                     // close = token per ICP → invert
        };
      };
    };

    // ICP price in USD at `ts`, via ICP/ckUSDC 4h kline (ckUSDC assumed $1).
    func icpInUSDAt(ts : Int) : ?Float {
      let (_pt0, pt1) = getPool(ICP_TEXT, CKUSDC_TEXT);
      switch (klineCloseAtOrBefore(ICP_TEXT, CKUSDC_TEXT, ts)) {
        case null { null };
        case (?close) {
          if (close <= 0.0) { null }
          else if (pt1 == CKUSDC_TEXT) { ?close }      // close = ckUSDC per ICP
          else { ?(1.0 / close) };                     // close = ICP per ckUSDC → invert
        };
      };
    };

    // Token price in USD at `ts`.
    func tokenInUSDAt(token : Text, ts : Int) : ?Float {
      if (token == CKUSDC_TEXT) { return ?1.0 };
      switch (icpInUSDAt(ts)) {
        case null { null };
        case (?icpUSD) {
          if (token == ICP_TEXT) { ?icpUSD }
          else switch (tokenInICPAt(token, ts)) {
            case null { null };
            case (?tokenICP) { ?(tokenICP * icpUSD) };
          };
        };
      };
    };

    let results = Array.map<Principal, {
      token : Principal;
      points : [Float];
      price_now : Float;
      change_pct_24h : Float;
      change_pct_7d : Float;
    }>(tokens, func(tokenPrin) {
      let tokenText = Principal.toText(tokenPrin);

      // Sample 28 points with carry-forward. anyData tracks whether we ever saw a real quote.
      var lastKnown : Float = 0.0;
      var anyData : Bool = false;
      let pointsBuf = Buffer.Buffer<Float>(28);
      for (ts in targets.vals()) {
        switch (tokenInUSDAt(tokenText, ts)) {
          case (?p) { lastKnown := p; anyData := true; pointsBuf.add(p) };
          case null { pointsBuf.add(lastKnown) };
        };
      };
      let points : [Float] = if (anyData) { Buffer.toArray(pointsBuf) } else { [] };

      // price_now: sample at now (the 4h kline close ≤ now). Falls back to last carried value.
      let priceNow : Float = switch (tokenInUSDAt(tokenText, nowVar)) {
        case (?p) { p };
        case null { lastKnown };
      };

      // Percentage changes. points[0] = ~7d ago, points[23] = 24h ago. Guarded against zero.
      let change24h : Float = if (points.size() > 23 and points[23] > 0.0 and priceNow > 0.0) {
        ((priceNow - points[23]) / points[23]) * 100.0
      } else { 0.0 };
      let change7d : Float = if (points.size() > 0 and points[0] > 0.0 and priceNow > 0.0) {
        ((priceNow - points[0]) / points[0]) * 100.0
      } else { 0.0 };

      {
        token = tokenPrin;
        points = points;
        price_now = priceNow;
        change_pct_24h = change24h;
        change_pct_7d = change7d;
      };
    });

    #ok(results);
  };

  // I could also use a for-loop to remove per-entry. However I think this is more efficient. It keeps between 13000  and 18000 of the newest entries and only starts if size is above 20000
  func trimKlineData() {
    let timeFrames : [TimeFrame] = [#fivemin, #hour, #fourHours, #day, #week];
    for (token1 in acceptedTokens.vals()) {
      for (token2 in acceptedTokens.vals()) {
        if (token1 != token2) {
          let pool = getPool(token1, token2);
          for (timeFrame in timeFrames.vals()) {
            let klineKey : KlineKey = (pool.0, pool.1, timeFrame);
            switch (Map.get(klineDataStorage, hashkl, klineKey)) {
              case null {};
              case (?tree) {
                let originalSize = RBTree.size(tree);
                if (originalSize > 20000) {
                  var trimmedTree = tree;
                  var entriesToRemove = originalSize - 15500; // Aim for the middle of our desired range
                  var iterationCount = 0;
                  let maxIterations = 10;

                  label a while (entriesToRemove > 0 and RBTree.size(trimmedTree) > 13000 and iterationCount < maxIterations) {
                    switch (RBTree.split(trimmedTree, compareTime)) {
                      case (?(leftTree, rightTree)) {
                        let leftSize = RBTree.size(leftTree);
                        if (RBTree.size(rightTree) >= 13000 and leftSize <= entriesToRemove) {
                          trimmedTree := rightTree;
                          entriesToRemove -= leftSize;
                        } else {
                          break a;
                        };
                      };
                      case null {

                        break a;
                      };
                    };
                    iterationCount += 1;
                  };

                  if (iterationCount == maxIterations) {

                  };

                  // Update the tree in the map
                  Map.set(klineDataStorage, hashkl, klineKey, trimmedTree);
                };
              };
            };
          };
        };
      };
    };
  };

  func updatePriceDayBefore(poolKey : (Text, Text), currentTime : Int) : Float {
    let klineKey : KlineKey = (poolKey.0, poolKey.1, #fivemin);
    let twentyFourHoursAgo = currentTime - 24 * 3600 * 1_000_000_000;

    switch (Map.get(klineDataStorage, hashkl, klineKey)) {
      case null {

        return 0.000000000001;
      };
      case (?tree) {
        // First try to get exactly 24h ago or the next available price
        let result24h = (
          RBTree.scanLimit(
            tree,
            compareTime,
            twentyFourHoursAgo,
            currentTime,
            #fwd, // Changed to forward scan
            288,
          )
        ).results;

        if (result24h.size() != 0 and result24h[0].1.close != 0.000000000001) {
          // If we found a valid price, use it
          updatePoolPriceDayBefore(poolKey, result24h[0].1.close);
          return result24h[0].1.close;
        } else {
          // If no valid price found, scan from beginning to find first valid price

          for (entry in result24h.vals()) {
            if (entry.1.close != 0.000000000001) {
              updatePoolPriceDayBefore(poolKey, entry.1.close);
              return entry.1.close;
            };
          };
          // If no valid price found at all, keep the default

          return 0.000000000001;
        };
      };
    };
  };

  func updatePoolPriceDayBefore(poolKey : (Text, Text), price : Float) {
    var index = 0;
    for (pair in Vector.vals(pool_canister)) {
      if (pair == poolKey) {
        Vector.put(price_day_before, index, price);
        AllExchangeInfo := {
          AllExchangeInfo with
          price_day_before = Vector.toArray(price_day_before);
        };
        return;
      };
      index += 1;
    };
  };
  public query func isExchangeFrozen() : async Bool { exchangeState == #Frozen };

  // Freeze all exchange activities, only the admin accounts can use this function
  public shared ({ caller }) func Freeze() : async () {
    if (not ownercheck(caller)) {
      return;
    };
    if (exchangeState == #Active) {
      exchangeState := #Frozen;
      logger.info("ADMIN", "Exchange FROZEN by " # Principal.toText(caller), "Freeze");
    } else {
      exchangeState := #Active;
      logger.info("ADMIN", "Exchange UNFROZEN by " # Principal.toText(caller), "Freeze");
    };
  };

  //0=not allowed 1=allowed 2=warning 3=day-ban 4=all-time ban
  //We are allowing X (allowedCalls) calls within 90 seconds, if an entity goes over that,
  //they get a warning and their 90 second spamCount is divided by 2.
  //If they go over the rate within a day while having a warning, they get a day-ban.
  //If the entity has gotten a day-ban before that occasion it gets an allTimeBan.
  //There is also a silent warning. If an user gets X (allowedSilentWarnings) of them
  // in 1 day, they also get a day-ban
  // *** To afat: 1. Ownercheck indeed adds principals to the Dayban if someone tries to perform a functions thats not allowed.
  // *** This is done to directly discourage people who are sniffing around. As I would also go for admin functions as the first thing to try
  // *** This should not give problems considering these addresses will be different from the principals that use the exchange as they should.
  private func ownercheck(caller : Principal) : Bool {
    if (not test and not isAdmin(caller)) {
      if (not (TrieSet.contains(dayBan, caller, Principal.hash(caller), Principal.equal))) {
        dayBan := TrieSet.put(dayBan, caller, Principal.hash(caller), Principal.equal);
      };
      logger.warn("ADMIN", "Unauthorized admin attempt by " # Principal.toText(caller), "ownercheck");
      return false;
    };
    return true;
  };
  private func isFeeCollector(caller : Principal) : Bool {
    if (caller == deployer.caller) return true;
    for (p in feeCollectors.vals()) { if (p == caller) return true };
    false;
  };
  private func DAOcheck(caller : Principal) : Bool {
    if (not test and not isAdmin(caller)) {
      dayBan := TrieSet.put(dayBan, caller, Principal.hash(caller), Principal.equal);
      return false;
    };
    return true;
  };
  // Strict membership check — true ONLY if caller is in the trusted
  // allowedCanisters list. Distinct from `isAllowed`, which also returns 1
  // for clean (non-banned, non-frozen) regular users. Used to gate sync-
  // transfer mode at every receiveTransferTasks call site: only trusted
  // callers (treasury, DAO entries, owners, registered bot principals) get
  // immediate transfers; regular users continue with the async setTimer
  // queue.
  private func isInAllowedCanisters(caller : Principal) : Bool {
    Array.find<Principal>(allowedCanisters, func(t) { t == caller }) != null;
  };

  private func isAllowed(caller : Principal) : Nat {
    let callerText = Principal.toText(caller);
    let allowed = Array.find<Principal>(allowedCanisters, func(t) { t == caller });
    if (exchangeState == #Frozen and allowed == null) {
      return 0;
    };
    if (allowed != null) { return 1 };
    let nowVar = Time.now();
    if (nowVar > timeStartSpamCheck + timeWindowSpamCheck) {
      timeStartSpamCheck := nowVar;
      Map.clear(spamCheck);
      over10 := TrieSet.empty();
    } else if (nowVar > timeStartSpamDayCheck + 86400000000000) {
      warnings := TrieSet.empty();
      Map.clear(spamCheckOver10);
      dayBan := TrieSet.empty();
      timeStartSpamDayCheck := nowVar;
    };
    if (callerText.size() < 29 and allowed == null) {
      return 0;
    } else if (allowed != null) {
      return 1;
    };
    let temp = Map.get(spamCheck, phash, caller);
    let num = (if (temp == null) { 0 } else { switch (temp) { case (?t) { t }; case (_) { 0 } } }) + 1;
    Map.set(spamCheck, phash, caller, num);
    if (num < allowedCalls) {
      if (num < allowedCalls / 2) {
        return 1;
      } else if (not TrieSet.contains(over10, caller, Principal.hash(caller), Principal.equal)) {
        over10 := TrieSet.put(over10, caller, Principal.hash(caller), Principal.equal);
        let temp = Map.get(spamCheckOver10, phash, caller);
        let num = switch (temp) { case (?val) val +1; case (null) 1 };
        if (num > allowedSilentWarnings) {
          if (not TrieSet.contains(dayBanRegister, caller, Principal.hash(caller), Principal.equal)) {
            dayBan := TrieSet.put(dayBan, caller, Principal.hash(caller), Principal.equal);
            dayBanRegister := TrieSet.put(dayBanRegister, caller, Principal.hash(caller), Principal.equal);
            return 3;
          } else {
            allTimeBan := TrieSet.put(allTimeBan, caller, Principal.hash(caller), Principal.equal);
            return 4;
          };
        } else {
          Map.set(spamCheckOver10, phash, caller, num);
          return 1;
        };
      } else {
        return 1;
      };
    } else {
      if (not TrieSet.contains(warnings, caller, Principal.hash(caller), Principal.equal)) {
        warnings := TrieSet.put(warnings, caller, Principal.hash(caller), Principal.equal);
        Map.set(spamCheck, phash, caller, num / 2);
        return 2;
      } else {
        if (not TrieSet.contains(dayBanRegister, caller, Principal.hash(caller), Principal.equal)) {
          dayBan := TrieSet.put(dayBan, caller, Principal.hash(caller), Principal.equal);
          dayBanRegister := TrieSet.put(dayBanRegister, caller, Principal.hash(caller), Principal.equal);
          return 3;
        } else {
          allTimeBan := TrieSet.put(allTimeBan, caller, Principal.hash(caller), Principal.equal);
          return 4;
        };
      };
    };
  };

  public shared ({ caller }) func clearAllBans() : async () {
    if (not ownercheck(caller)) { return };
    dayBan := TrieSet.empty();
    dayBanRegister := TrieSet.empty();
    allTimeBan := TrieSet.empty();
    warnings := TrieSet.empty();
    logger.info("ADMIN", "clearAllBans called by " # Principal.toText(caller), "clearAllBans");
  };

  // Admin diagnostic: returns the ban code for `p` per isAllowedQuery semantics.
  //   0 = anonymous / frozen (rejected)
  //   1 = allowed
  //   2 = warning (rate exceeded once, count halved)
  //   3 = day-banned
  //   4 = all-time-banned
  //   999 = caller is not admin (sentinel; never overlaps real codes)
  // Owner-only: ban-list contents are sensitive.
  public shared query ({ caller }) func adminCheckBan(p : Principal) : async Nat {
    if (not ownercheck(caller)) { return 999 };
    isAllowedQuery(p);
  };

  // Diagnostic: expose allowedCanisters so operators can verify DAO treasury /
  // DAO backend are allowlisted without guessing.
  public query func getAllowedCanisters() : async [Text] {
    Array.map<Principal, Text>(allowedCanisters, func(p) { Principal.toText(p) });
  };

  // Legacy stub — kept for Candid compat. Historical one-shot stuck-funds recovery;
  // superseded by adminDrainExchange. No-op to satisfy clients that still reference it.
  public shared ({ caller }) func refundStuckFunds() : async Text {
    if (not ownercheck(caller)) { return "Not authorized" };
    ignore refundStuckFundsCalled;
    "refundStuckFunds is deprecated; use adminDrainExchange";
  };

  // Admin safety net: unstick a lock zone entry if a trap or unforeseen error left an
  // accesscode in tradesBeingWorkedOn or a BlocksDone entry set without proper cleanup.
  // This is the fallback for cases where the automatic cleanup in try/catch blocks
  // didn't run (e.g., an untrappable error after a successful await).
  public shared ({ caller }) func clearStuckLocks(
    accesscode : ?Text,
    blocksDoneKey : ?Text,
  ) : async Bool {
    if (not ownercheck(caller)) { return false };
    switch (accesscode) {
      case (?ac) {
        tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, ac, Text.hash(ac), Text.equal);
        logger.info("ADMIN", "clearStuckLocks: cleared tradesBeingWorkedOn for " # ac # " by " # Principal.toText(caller), "clearStuckLocks");
      };
      case null {};
    };
    switch (blocksDoneKey) {
      case (?k) {
        Map.delete(BlocksDone, thash, k);
        logger.info("ADMIN", "clearStuckLocks: cleared BlocksDone key " # k # " by " # Principal.toText(caller), "clearStuckLocks");
      };
      case null {};
    };
    return true;
  };

  // Admin escape hatch: resets adminRecoveryRunning if a sync-region trap inside
  // adminRecoverWronglysent left it stuck at true. Only touches the serialization
  // flag — no funds move, no BlocksDone or BlocksAdminRecovered changes.
  public shared ({ caller }) func adminForceUnlockRecovery() : async Bool {
    if (not ownercheck(caller)) { return false };
    adminRecoveryRunning := false;
    logger.warn(
      "ADMIN",
      "adminForceUnlockRecovery: flag reset by " # Principal.toText(caller),
      "adminForceUnlockRecovery"
    );
    true;
  };

  // Admin: update the per-token minimum amount in place. Preserves all pools,
  // positions, prices, AMM reserves, and orderbook state — unlike addAcceptedToken
  // which requires a destructive remove+re-add. Floor of 1000 (inclusive).
  public shared ({ caller }) func setMinimumAmount(token : Text, newMinimum : Nat) : async ExTypes.ActionResult {
    if (not ownercheck(caller)) { return #Err(#NotAuthorized) };
    if (newMinimum < 100) { return #Err(#InvalidInput("Minimum must be >= 100")) };

    let idx = switch (Array.indexOf<Text>(token, acceptedTokens, Text.equal)) {
      case (?i) { i };
      case null { return #Err(#InvalidInput("Token not in acceptedTokens: " # token)) };
    };
    let oldMinimum = minimumAmount[idx];

    let newMinArr = Array.tabulate<Nat>(
      minimumAmount.size(),
      func(k : Nat) : Nat { if (k == idx) newMinimum else minimumAmount[k] },
    );
    minimumAmount := newMinArr;

    let newAssetMin = Vector.new<(Nat, Nat)>();
    for (p in Iter.range(0, Vector.size(pool_canister) - 1)) {
      let (t0, t1) = Vector.get(pool_canister, p);
      let (m0, m1) = Vector.get(asset_minimum_amount, p);
      let nm0 = if (t0 == token) newMinimum else m0;
      let nm1 = if (t1 == token) newMinimum else m1;
      Vector.add(newAssetMin, (nm0, nm1));
    };
    asset_minimum_amount := newAssetMin;

    updateTokenInfo<system>(false, true, []);
    updateStaticInfo();

    logger.info(
      "ADMIN",
      "setMinimumAmount: " # token # " " # Nat.toText(oldMinimum) # " -> " # Nat.toText(newMinimum) # " by " # Principal.toText(caller),
      "setMinimumAmount",
    );
    return #Ok("setMinimumAmount updated");
  };

  // Admin one-shot: repair `last_traded_price` and kline entries that were stored
  // as the reciprocal of the true price by an earlier reader/writer-convention bug
  // in orderPairing's updateLastTradedPriceVector canonicalisation. Overwrites
  // `last_traded_price[idx]` with the current AMM spot. For each kline bucket whose
  // stored close deviates from the current spot by more than 10× (or less than 1/10),
  // resets close/high/low to the current spot and zeroes the volume (original
  // denomination was mixed and cannot be reconstructed). Leaves clean buckets alone.
  public shared ({ caller }) func adminRepairLastTradedPriceAndKlines(
    affectedPoolIndexes : [Nat],
    alsoRepairVolume24h : Bool,
  ) : async Text {
    if (not ownercheck(caller)) { return "NotAuthorized" };
    let repaired = Vector.new<Text>();
    for (idx in affectedPoolIndexes.vals()) {
      if (idx >= Vector.size(pool_canister)) {
        Vector.add(repaired, "idx=" # Nat.toText(idx) # ":out-of-range");
      } else {
        let poolKey = Vector.get(pool_canister, idx);
        switch (Map.get(AMMpools, hashtt, poolKey)) {
          case null {
            Vector.add(repaired, "idx=" # Nat.toText(idx) # ":no-pool");
          };
          case (?pool) {
            if (pool.reserve0 == 0 or pool.reserve1 == 0) {
              Vector.add(repaired, "idx=" # Nat.toText(idx) # ":empty-reserves");
            } else if (idx >= AllExchangeInfo.asset_decimals.size()) {
              Vector.add(repaired, "idx=" # Nat.toText(idx) # ":no-decimals");
            } else {
              let dec0 = nat8ToNat(AllExchangeInfo.asset_decimals[idx].0);
              let dec1 = nat8ToNat(AllExchangeInfo.asset_decimals[idx].1);
              let spot = (Float.fromInt(pool.reserve1) * Float.fromInt(10 ** dec0))
                       / (Float.fromInt(pool.reserve0) * Float.fromInt(10 ** dec1));

              if (idx < Vector.size(last_traded_price)) {
                Vector.put(last_traded_price, idx, spot);
              };

              var bucketsTouched : Nat = 0;
              let timeFrames : [TimeFrame] = [#fivemin, #hour, #fourHours, #day, #week];
              for (tf in timeFrames.vals()) {
                let klineKey : KlineKey = (poolKey.0, poolKey.1, tf);
                switch (Map.get(klineDataStorage, hashkl, klineKey)) {
                  case null {};
                  case (?tree) {
                    var updated = tree;
                    for ((ts, k) in RBTree.entries(tree)) {
                      let closeBad = if (k.close == 0.0 or spot == 0.0) {
                        false;
                      } else {
                        let ratio = k.close / spot;
                        ratio > 10.0 or ratio < 0.1;
                      };
                      if (closeBad) {
                        updated := RBTree.put(updated, compareTime, ts, {
                          timestamp = k.timestamp;
                          open = if (k.open > 10.0 * spot or (k.open > 0.0 and k.open < 0.1 * spot)) { spot } else { k.open };
                          high = spot;
                          low = spot;
                          close = spot;
                          volume = 0;
                        });
                        bucketsTouched += 1;
                      };
                    };
                    Map.set(klineDataStorage, hashkl, klineKey, updated);
                  };
                };
              };

              if (alsoRepairVolume24h) { ignore update24hVolume(poolKey) };
              // price_day_before intentionally left alone — it's already sourced
              // from a pre-corruption kline close and will self-refresh once the
              // fixed reader/writer convention stops inverting new entries.

              Vector.add(repaired, "idx=" # Nat.toText(idx) # ":spot=" # Float.toText(spot) # ":buckets=" # Nat.toText(bucketsTouched));
            };
          };
        };
      };
    };
    doInfoBeforeStep2();
    let summary = Text.join(",", Vector.vals(repaired));
    logger.info("ADMIN", "adminRepairLastTradedPriceAndKlines by " # Principal.toText(caller) # " [" # summary # "]", "adminRepairLastTradedPriceAndKlines");
    summary;
  };

  // Admin one-shot: delete every kline entry with timestamp < cutoffNs across all
  // (token, token, timeframe) buckets in klineDataStorage. Per-bucket deletes are
  // capped (`maxDeletesPerBucket`, default 5000 when 0 is passed) so a single
  // message stays under the IC instruction limit; if the returned summary lists
  // capped > 0, call again with the same cutoff to continue draining.
  public shared ({ caller }) func adminDeleteKlinesBefore(
    cutoffNs : Int,
    maxDeletesPerBucket : Nat,
  ) : async Text {
    if (not ownercheck(caller)) { return "NotAuthorized" };
    let cap = if (maxDeletesPerBucket == 0) { 5000 } else { maxDeletesPerBucket };
    var totalDeleted : Nat = 0;
    var bucketsTouched : Nat = 0;
    var bucketsCapped : Nat = 0;
    for ((klineKey, tree) in Map.entries(klineDataStorage)) {
      let scan = RBTree.scanLimit(tree, compareTime, 0, cutoffNs - 1, #fwd, cap);
      if (scan.results.size() > 0) {
        var trimmed = tree;
        for ((ts, _) in scan.results.vals()) {
          trimmed := RBTree.delete(trimmed, compareTime, ts);
        };
        Map.set(klineDataStorage, hashkl, klineKey, trimmed);
        totalDeleted += scan.results.size();
        bucketsTouched += 1;
        if (Option.isSome(scan.nextKey)) { bucketsCapped += 1 };
      };
    };
    let more = bucketsCapped > 0;
    let summary =
      "deleted=" # Nat.toText(totalDeleted)
      # " buckets=" # Nat.toText(bucketsTouched)
      # " capped=" # Nat.toText(bucketsCapped)
      # " more=" # (if (more) "true" else "false");
    logger.info(
      "ADMIN",
      "adminDeleteKlinesBefore by " # Principal.toText(caller)
        # " cutoff=" # Int.toText(cutoffNs) # " [" # summary # "]",
      "adminDeleteKlinesBefore",
    );
    summary;
  };

  // function that allows admin to change certain variables
  public shared ({ caller }) func parameterManagement(
    parameters : {
      deleteFromDayBan : ?[Text];
      deleteFromAllTimeBan : ?[Text];
      addToAllTimeBan : ?[Text];
      changeAllowedCalls : ?Nat;
      changeallowedSilentWarnings : ?Nat;
      addAllowedCanisters : ?[Text];
      deleteAllowedCanisters : ?[Text];
      treasury_principal : ?Text;
      daoTreasuryPrincipalsText : ?[Text];
    }
  ) : async () {
    if (not ownercheck(caller)) {
      return;
    };
    logger.info("ADMIN", "parameterManagement called by " # Principal.toText(caller), "parameterManagement");

    if (parameters.deleteFromDayBan != null) {
      let deleteFromDayBan2 = switch (parameters.deleteFromDayBan) {
        case (?a) { a };
        case (null) { [] };
      };
      for (bannedUser in deleteFromDayBan2.vals()) {
        dayBan := TrieSet.delete(dayBan, Principal.fromText(bannedUser), Principal.hash(Principal.fromText(bannedUser)), Principal.equal);
      };
    };

    if (parameters.deleteFromAllTimeBan != null) {
      let deleteFromAllTimeBan2 = switch (parameters.deleteFromAllTimeBan) {
        case (?a) { a };
        case (null) { [] };
      };
      for (bannedUser in deleteFromAllTimeBan2.vals()) {
        allTimeBan := TrieSet.delete(allTimeBan, Principal.fromText(bannedUser), Principal.hash(Principal.fromText(bannedUser)), Principal.equal);
      };
    };

    if (parameters.changeAllowedCalls != null) {
      let changeAllowedCalls2 = switch (parameters.changeAllowedCalls) {
        case (?a) { a };
      };
      if (changeAllowedCalls2 >= 1 and changeAllowedCalls2 <= 100) {
        allowedCalls := changeAllowedCalls2;
      };
    };

    if (parameters.changeallowedSilentWarnings != null) {
      let changeallowedSilentWarnings2 = switch (parameters.changeallowedSilentWarnings) {
        case (?a) { a };
      };
      if (changeallowedSilentWarnings2 >= 1 and changeallowedSilentWarnings2 <= 100) {
        allowedSilentWarnings := changeallowedSilentWarnings2;
      };
    };

    if (parameters.addAllowedCanisters != null) {
      let addAllowedCanisters2 = switch (parameters.addAllowedCanisters) {
        case (?a) { a };
      };
      let allowedCanistersVec = Vector.fromArray<Principal>(allowedCanisters);
      for (canister in addAllowedCanisters2.vals()) {
        Vector.add(allowedCanistersVec, Principal.fromText(canister));
      };
      allowedCanisters := Vector.toArray(allowedCanistersVec);
    };

    if (parameters.deleteAllowedCanisters != null) {
      let deleteAllowedCanisters2 = switch (parameters.deleteAllowedCanisters) {
        case (?a) { a };
      };
      for (canister in deleteAllowedCanisters2.vals()) {
        allowedCanisters := Array.filter<Principal>(allowedCanisters, func(c) { c != Principal.fromText(canister) });
      };
    };

    if (parameters.addToAllTimeBan != null) {
      let addToAllTimeBan2 = switch (parameters.addToAllTimeBan) {
        case (?a) { a };
        case (null) { [] };
      };
      for (bannedUser in addToAllTimeBan2.vals()) {
        allTimeBan := TrieSet.put(allTimeBan, Principal.fromText(bannedUser), Principal.hash(Principal.fromText(bannedUser)), Principal.equal);
      };
    };

    if (parameters.treasury_principal != null) {
      let treasury_principal2 = switch (parameters.treasury_principal) {
        case (?a) { a };
      };
      treasury_text := treasury_principal2;
      treasury_principal := Principal.fromText(treasury_principal2);
      // Configure treasury with this canister's ID (inter-canister call bypasses inspect)
      let treasuryActor = actor (treasury_principal2) : treasuryType.Treasury;
      try {
        await treasuryActor.setOTCCanister(Principal.toText(Principal.fromActor(this)));
      } catch (_) {};
    };

    // Override list of DAO treasury principals eligible for the orderPairing
    // private-order bypass. Replaces the entire list so operators can prune /
    // re-order; pre-validates each entry via Principal.fromText (traps on bad input).
    if (parameters.daoTreasuryPrincipalsText != null) {
      let newList = switch (parameters.daoTreasuryPrincipalsText) {
        case (?a) { a };
        case (null) { [] };
      };
      // Validate each principal text by attempting conversion (traps on malformed input)
      for (p in newList.vals()) { ignore Principal.fromText(p) };
      daoTreasuryPrincipalsText := newList;
    };
  };

  private func isAllowedQuery(caller : Principal) : Nat {
    let callerText = Principal.toText(caller);
    // check if the caller is in the blacklist (dayBan or allTimeBan)
    if (
      (
        TrieSet.contains(dayBan, caller, Principal.hash(caller), Principal.equal) or
        TrieSet.contains(allTimeBan, caller, Principal.hash(caller), Principal.equal)
      ) and not Principal.isAnonymous(caller) and not test
    ) {


      return 0; // not allowed
    };

    // check for minimum principal length (to prevent certain types of attacks)
    if (callerText.size() < 29 and Array.indexOf<Principal>(caller, allowedCanisters, Principal.equal) == null and not Principal.isAnonymous(caller) and not test) {

      return 0; // not allowed
    };

    return 1; // allowed
  };

  let seconds = 500; //Run timer every X seconds
  var fastTimer = false;
  var trimNumer = 0;

  //Timer to update the metadata of all the assets and periodicaly trim data
  private func timerA<system>(tempInfo : [(Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text })]) : () {

    let timersize = Vector.size(timerIDs);
    if (timersize > 0) {
      for (i in Vector.vals(timerIDs)) {
        cancelTimer(i);
      };
    };
    timerIDs := Vector.new<Nat>();
    trimNumer += 1;
    if (trimNumer == 20) {
      trimNumer := 0;
      ignore setTimer<system>(
        #seconds(fuzz.nat.randomRange(50, 999)),
        func() : async () {
          trimPoolHistory();
        },
      );
      ignore setTimer<system>(
        #seconds(fuzz.nat.randomRange(50, 999)),
        func() : async () {
          trimKlineData();
        },
      );
      ignore setTimer<system>(
        #seconds(fuzz.nat.randomRange(50, 999)),
        func() : async () {
          trimSwapHistory();
        },
      );
      // Take daily pool snapshot (idempotent — checks if today's already exists)
      takePoolDailySnapshots();

    };

    // Every 20th cycle (~2.8 hours), do a full metadata rebuild including acceptedTokensInfo
    let doFullUpdate = trimNumer == 0;
    updateTokenInfo<system>(true, doFullUpdate, tempInfo);
    if (doFullUpdate) { updateStaticInfo() };

    Vector.add(
      timerIDs,
      setTimer<system>(
        #seconds(500),
        func() : async () {
          try {
            timerA<system>(await treasury.getTokenInfo());
          } catch (err) {


            retryFunc<system>(
              func() : async () {

                timerA<system>(await treasury.getTokenInfo());
              },
              5,
              10,
              10,
            );
          };
        },
      ),
    );

  };

  //Getting the metadata of each token and storing it
  private func updateTokenInfo<system>(requestUpdate : Bool, updateAll : Bool, tempInfo : [(Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text })]) : () {
    if requestUpdate {
      for (i in tempInfo.vals()) {
        Map.set(tokenInfo, thash, i.0, i.1);
      };
    };

    // Check for stuck transactions
    if (Vector.size(tempTransferQueue) > 0) {
      ignore setTimer<system>(
        #seconds(1),
        func() : async () {
          try { ignore await FixStuckTX("partial") } catch (err) {};
        },
      );
    };

    if updateAll {
      let asset_names2 = Vector.new<(Text, Text)>();
      let asset_symbols2 = Vector.new<(Text, Text)>();
      let asset_decimals2 = Vector.new<(Nat8, Nat8)>();
      let asset_transferfees2 = Vector.new<(Nat, Nat)>();
      var tkInfo = Vector.new<TokenInfo>();

      // Helper function to get token info
      func getTokenInfo(token : Text) : {
        TransferFee : Nat;
        Decimals : Nat;
        Name : Text;
        Symbol : Text;
      } {
        switch (Map.get(tokenInfo, thash, token)) {
          case (?info) { info };
          case null {
            { TransferFee = 0; Decimals = 0; Name = ""; Symbol = "" };
          };
        };
      };

      // Populate tkInfo first
      var i = 0;
      for (token in acceptedTokens.vals()) {

        let info = getTokenInfo(token);
        Vector.add(
          tkInfo,
          {
            address = token;
            name = info.Name;
            symbol = info.Symbol;
            transfer_fee = info.TransferFee;
            decimals = info.Decimals;
            minimum_amount = minimumAmount[i];
            asset_type = tokenType[i];
          },
        );
        i += 1;
      };

      // Populate other vectors based on pool_canister order
      for ((token1, token2) in Vector.vals(pool_canister)) {
        let info1 = getTokenInfo(token1);
        let info2 = getTokenInfo(token2);

        Vector.add(asset_names2, (info1.Name, info2.Name));
        Vector.add(asset_symbols2, (info1.Symbol, info2.Symbol));
        Vector.add(asset_decimals2, (natToNat8(info1.Decimals), natToNat8(info2.Decimals)));
        Vector.add(asset_transferfees2, (info1.TransferFee, info2.TransferFee));
      };

      asset_names := asset_names2;
      asset_symbols := asset_symbols2;
      asset_decimals := asset_decimals2;
      asset_transferfees := asset_transferfees2;
      acceptedTokensInfo := Vector.toArray(tkInfo);
    };

    tokenInfoARR := Map.toArray<Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text }>(tokenInfo);
  };

  // Update data used by FE — updates last_traded_price, volume counters, and kline data.
  //
  // CONTRACT:
  // - `tokenPair` is CANONICAL pool order, i.e. (pair.0, pair.1) = (pool_canister[idx].0, pool_canister[idx].1).
  //   Callers MUST compute this via `getPool(...)` before calling, not pass raw trade direction.
  // - `amount0` is the amount of canonical token0 traded (base asset).
  // - `amount1` is the amount of canonical token1 traded (quote asset).
  // - Result: price is `token1 per token0` (human units); volume is always in token1 (quote) units.
  //
  // Previously this function took (tokenPair, amountInit, amountSell) and had two branches that
  // were mathematically correct but relied on callers passing a consistent convention — callers
  // disagreed (orderPairing swapped args; FinishSell assumed trade direction = canonical), so the
  // stored price oscillated (inverted for reverse-direction trades) and volume accumulated in
  // mixed units. This signature forces a single canonical convention.
  private func updateLastTradedPrice(tokenPair : (Text, Text), amount0 : Nat, amount1 : Nat) {
    var price : Float = 0;
    var poolKey = ("", "");
    if (amount0 < 1000 or amount1 < 1000) {
      return;
    };

    switch (Map.get(poolIndexMap, hashtt, tokenPair)) {
      case null {};
      case (?index) {
        let pair = Vector.get(pool_canister, index);
        poolKey := pair;
        if (index < Vector.size(last_traded_price) and index < AllExchangeInfo.asset_decimals.size()) {
          let dec0 = nat8ToNat(AllExchangeInfo.asset_decimals[index].0);
          let dec1 = nat8ToNat(AllExchangeInfo.asset_decimals[index].1);
          // price = (amount1_human) / (amount0_human) = price of token0 in token1
          price := (Float.fromInt(amount1) * Float.fromInt(10 ** dec0)) / (Float.fromInt(amount0) * Float.fromInt(10 ** dec1));
          Vector.put(last_traded_price, index, price);
        };
      };
    };

    // volume is always in token1 (quote) units
    if (price > 0 and poolKey.0 != "" and poolKey.1 != "") {
      updateKlineData(poolKey.0, poolKey.1, price, amount1);
      ignore update24hVolume(poolKey);
    };
  };
  // New function to update 24h volume
  private func update24hVolume(poolKey : (Text, Text)) : Nat {
    let klineKey : KlineKey = (poolKey.0, poolKey.1, #fourHours);
    let currentTime = Time.now();
    let twentyFourHoursAgo = currentTime - 24 * 3600 * 1_000_000_000;

    switch (Map.get(klineDataStorage, hashkl, klineKey)) {
      case null {

        return 0;
      };
      case (?tree) {
        let result = RBTree.scanLimit(
          tree,
          compareTime,
          twentyFourHoursAgo,
          currentTime,
          #bwd,
          6,
        ).results;

        let totalVolume = Array.foldLeft<(Int, KlineData), Nat>(
          result,
          0,
          func(acc, kline) {
            acc + kline.1.volume;
          },
        );

        // Update AllExchangeInfo with new volume
        updateExchangeInfoVolume(poolKey, totalVolume);
        return totalVolume;
      };
    };
  };

  // New function to update volume in AllExchangeInfo
  private func updateExchangeInfoVolume(poolKey : (Text, Text), volume : Nat) {
    switch (Map.get(poolIndexMap, hashtt, poolKey)) {
      case null {};
      case (?index) {
        if (index < AllExchangeInfo.volume_24h.size()) {
          let updatedVolumes = Array.thaw<Nat>(AllExchangeInfo.volume_24h);
          updatedVolumes[index] := volume;
          let frozen = Array.freeze(updatedVolumes);
          AllExchangeInfo := { AllExchangeInfo with volume_24h = frozen };
          volume_24hArray := frozen;
        };
      };
    };
  };

  // sends all accepted tokens including their metadata
  public query ({ caller }) func getAcceptedTokensInfo() : async ?[TokenInfo] {
    if (isAllowedQuery(caller) != 1) {
      return null;
    };
    return ?acceptedTokensInfo;
  };

  public query func canTradeTokens(tokenIn : Text, tokenOut : Text) : async Bool {
    containsToken(tokenIn) and containsToken(tokenOut) and
    Array.find<Text>(pausedTokens, func(t) { t == tokenIn }) == null and
    Array.find<Text>(pausedTokens, func(t) { t == tokenOut }) == null;
  };

  // Get the tokens that currently can be traded with each other within the exchange.
  public query ({ caller }) func getAcceptedTokens() : async ?[Text] {
    if (isAllowedQuery(caller) != 1) {
      return null;
    };
    return ?acceptedTokens;
  };

  public query ({ caller }) func getPausedTokens() : async ?[Text] {
    if (isAllowedQuery(caller) != 1) {
      return null;
    };
    return ?pausedTokens;
  };

  // function that can be called by the DAO to add or remove tokens from the acceptedtokens list. If the token sent to the function already exists in the list, it gets deleted instead.
  // To add a token it has to be the ICP ledger, ICRC1,2 or 3. In terms of ICRC1 or 2 it needs to have the following fuunctions: get_transactions, icrc1_balance_of,icrc1_transfer
  // If not all transaction get saved on the token canister, there needs to be an archive canister, given  in get_transactions
  // There are base tokens and the other tokens. Base tokens are assets like ICP and USDC. Other tokens pair with these assets.

  var currentRunIdaddAcceptedToken = 0;
  var loggingMapaddAcceptedToken = Map.new<Nat, Text>();
  public shared ({ caller }) func addAcceptedToken(action : { #Add; #Remove; #Opposite }, added2 : Text, minimum : Nat, tType : { #ICP; #ICRC12; #ICRC3 }) : async ExTypes.ActionResult {
    // Sanitize token ID: trim whitespace and tab characters
    let added = Text.trim(added2, #predicate(func(c : Char) : Bool { c == ' ' or c == '\t' or c == '\n' or c == '\r' }));

    let logEntries = Vector.new<Text>();
    let runId = currentRunIdaddAcceptedToken;

    currentRunIdaddAcceptedToken += 1;

    // Function to log with RunId
    func logWithRunId(message : Text) {
      Vector.add(logEntries, message);
    };

    if (not ownercheck(caller)) {
      logWithRunId("Caller is not authorized to perform this action");
      return #Err(#NotAuthorized);
    };
    if (Array.indexOf<Text>(added, baseTokens, Text.equal) != null) {
      logWithRunId("Token is a base token: " # added);
      return #Err(#InvalidInput("Token is a base token: " # added));
    };
    //Minimum should be at least 1000
    assert (minimum > 1000 or action == #Remove);
    logWithRunId("Action: " # debug_show (action) # ", Token: " # added # ", Minimum: " # Nat.toText(minimum) # ", Type: " # debug_show (tType));

    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    // Per-structure refund sum trackers for drift diagnostics
    var dbgObRefund0 : Nat = 0;  // orderbook (liqMapSort public) for token0 (added token)
    var dbgOb1 : Nat = 0;
    var dbgForeignRefund : Nat = 0;
    var dbgPrivateRefund : Nat = 0;
    var dbgV2LpRefund : Nat = 0;
    var dbgObCount : Nat = 0;
    var dbgForeignCount : Nat = 0;
    var dbgPrivateCount : Nat = 0;
    var dbgV2LpCount : Nat = 0;
    if ((action == #Remove and containsToken(added)) or (containsToken(added) and action == #Opposite)) {
      logWithRunId("Removing token: " # added);

      var pool : Text = "";
      let pools_to_delete2 = Vector.new<Nat>();
      let old_new = Vector.new<(Nat, Nat)>();
      var indexdel = 0;
      var newindex = 0;

      for (index in Iter.range(0, Vector.size(pool_canister) - 1)) {
        let (token1, token2) = Vector.get(pool_canister, index);

        let isToken1Base = Array.find(baseTokens, func(b : Text) : Bool { b == token1 }) != null;
        let isToken2Base = Array.find(baseTokens, func(b : Text) : Bool { b == token2 }) != null;

        logWithRunId("Checking pool: " # token1 # "-" # token2);

        if (token1 == added or token2 == added) {
          Vector.add(pools_to_delete2, index);
          logWithRunId("Pool marked for deletion: " # Nat.toText(index));
        } else {
          Vector.add(old_new, (index, newindex));
          newindex += 1;
          logWithRunId("Pool kept: " # Nat.toText(index) # " -> " # Nat.toText(newindex));
        };
        indexdel += 1;
      };

      let pools_to_delete = Vector.toArray(pools_to_delete2);
      logWithRunId("Pools to delete: " # debug_show (pools_to_delete));

      // DRIFT FIX: Iterate liqMapSort DIRECTLY (not via pool_canister index) so we catch
      // orphan entries whose poolKey is no longer registered in pool_canister.
      // Orphans arise from concurrent #Opposite toggles: one call removes a pool from
      // pool_canister while another places orders into liqMapSort under a key that
      // gets de-registered shortly after. The old per-index iteration would miss those.
      let orphanKeysPublic = Vector.new<(Text, Text)>();
      for ((poolKey, _) in Map.entries(liqMapSort)) {
        if (poolKey.0 == added or poolKey.1 == added) {
          Vector.add(orphanKeysPublic, poolKey);
        };
      };
      for (poolKey in Vector.vals(orphanKeysPublic)) {
        logWithRunId("Processing liqMapSort entry: " # debug_show(poolKey));
        switch (Map.get(liqMapSort, hashtt, poolKey)) {
          case (null) {};
          case (?poolLiquidity) {
            for ((ratio, trades) in RBTree.entries(poolLiquidity)) {
              for (liquidityToDelete in trades.vals()) {
                logWithRunId("Processing liquidity: " # debug_show (liquidityToDelete));

                let accesscode = liquidityToDelete.accesscode;
                removeTrade(accesscode, liquidityToDelete.initPrincipal, poolKey);
                logWithRunId("Removed trade: " # accesscode);

                let amount_init = liquidityToDelete.amount_init;
                let amount_sell = liquidityToDelete.amount_sell;
                let RevokeFee = liquidityToDelete.RevokeFee;
                let token_init_identifier = liquidityToDelete.token_init_identifier;
                let init_principal = liquidityToDelete.initPrincipal;
                let Fee = liquidityToDelete.Fee;

                let totalFee = (amount_init) * Fee;
                let revoke_Fee = (totalFee - (totalFee / RevokeFee)) / 10000;
                let toBeSent = amount_init + revoke_Fee;
                Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(init_principal)), toBeSent, token_init_identifier, genTxId()));
                dbgObCount += 1;
                if (token_init_identifier == added) { dbgObRefund0 += toBeSent } else { dbgOb1 += toBeSent };
                logWithRunId("Added refund: " # debug_show ((init_principal, toBeSent, token_init_identifier)));

                let tokenbuy = poolKey.0;
                let tokensell = poolKey.1;
                let whichCoin = if (tokenbuy == token_init_identifier) {
                  tokensell;
                } else {
                  tokenbuy;
                };
                if (not Text.endsWith(accesscode, #text "excl")) {
                  replaceLiqMap(
                    true,
                    false,
                    token_init_identifier,
                    whichCoin,
                    accesscode,
                    (amount_init, amount_sell, 0, 0, "", liquidityToDelete.OCname, liquidityToDelete.time, liquidityToDelete.token_init_identifier, liquidityToDelete.token_sell_identifier, liquidityToDelete.strictlyOTC, liquidityToDelete.allOrNothing),
                    #Zero,
                    null,
                    ?{
                      amount_init = amount_init;
                      amount_sell = amount_sell;
                      init_principal = init_principal;
                      sell_principal = "";
                      accesscode = accesscode;
                      token_init_identifier = token_init_identifier;
                      filledInit = 0;
                      filledSell = 0;
                      strictlyOTC = false;
                      allOrNothing = false;
                    },
                  );
                  logWithRunId("Updated liquidity map for: " # accesscode);
                };
              };
            };
          };
        };
        ignore Map.remove(liqMapSort, hashtt, poolKey);
        logWithRunId("Removed pool from liqMapSort: " # debug_show (poolKey));
      };

      // DRIFT FIX: Iterate liqMapSortForeign DIRECTLY (not via foreignPools map).
      // foreignPools counter can fall out of sync with actual liqMapSortForeign entries
      // during concurrent operations, leaving orphaned orders. Direct iteration is authoritative.
      let orphanKeysForeign = Vector.new<(Text, Text)>();
      for ((poolKey, _) in Map.entries(liqMapSortForeign)) {
        if (poolKey.0 == added or poolKey.1 == added) {
          Vector.add(orphanKeysForeign, poolKey);
        };
      };
      for (poolKey in Vector.vals(orphanKeysForeign)) {
        logWithRunId("Processing liqMapSortForeign entry: " # debug_show(poolKey));
        switch (Map.get(liqMapSortForeign, hashtt, poolKey)) {
          case (null) {};
          case (?poolLiquidity) {
            for ((ratio, trades) in RBTree.entries(poolLiquidity)) {
              for (liquidityToDelete in trades.vals()) {
                let accesscode = liquidityToDelete.accesscode;
                removeTrade(accesscode, liquidityToDelete.initPrincipal, poolKey);

                let amount_init = liquidityToDelete.amount_init;
                let amount_sell = liquidityToDelete.amount_sell;
                let RevokeFee = liquidityToDelete.RevokeFee;
                let token_init_identifier = liquidityToDelete.token_init_identifier;
                let init_principal = liquidityToDelete.initPrincipal;
                let Fee = liquidityToDelete.Fee;

                let totalFee = (amount_init) * Fee;
                let revoke_Fee = (totalFee - (totalFee / RevokeFee)) / 10000;
                let toBeSent = amount_init + revoke_Fee;
                Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(init_principal)), toBeSent, token_init_identifier, genTxId()));
                dbgForeignCount += 1;
                if (token_init_identifier == added) { dbgForeignRefund += toBeSent };

                let tokenbuy = poolKey.0;
                let tokensell = poolKey.1;
                let whichCoin = if (tokenbuy == token_init_identifier) {
                  tokensell;
                } else {
                  tokenbuy;
                };
                if (not Text.endsWith(accesscode, #text "excl")) {
                  replaceLiqMap(
                    true,
                    false,
                    token_init_identifier,
                    whichCoin,
                    accesscode,
                    (amount_init, amount_sell, 0, 0, "", liquidityToDelete.OCname, liquidityToDelete.time, liquidityToDelete.token_init_identifier, liquidityToDelete.token_sell_identifier, liquidityToDelete.strictlyOTC, liquidityToDelete.allOrNothing),
                    #Zero,
                    null,
                    ?{
                      amount_init = amount_init;
                      amount_sell = amount_sell;
                      init_principal = init_principal;
                      sell_principal = "";
                      accesscode = accesscode;
                      token_init_identifier = token_init_identifier;
                      filledInit = 0;
                      filledSell = 0;
                      strictlyOTC = false;
                      allOrNothing = false;
                    },
                  );
                };
              };
            };
          };
        };
        ignore Map.remove(liqMapSortForeign, hashtt, poolKey);
        logWithRunId("Removed foreign pool from liqMapSortForeign: " # debug_show (poolKey));
      };

      // DRIFT FIX: Iterate tradeStorePrivate DIRECTLY (not via foreignPrivatePools+privateAccessCodes
      // indirection). Those indexes can fall out of sync, orphaning trades. Direct iteration is
      // authoritative for finding every private order that mentions the removed token.
      let orphanPrivateCodes = Vector.new<Text>();
      for ((accesscode, trade) in Map.entries(tradeStorePrivate)) {
        if (trade.token_init_identifier == added or trade.token_sell_identifier == added) {
          Vector.add(orphanPrivateCodes, accesscode);
        };
      };
      label privateOrders for (accesscode in Vector.vals(orphanPrivateCodes)) {
        let liquidityToDelete = switch (Map.get(tradeStorePrivate, thash, accesscode)) {
          case null { continue privateOrders };
          case (?a) { a };
        };
        if (liquidityToDelete.trade_done == 1) {
          Vector.addFromIter(tempTransferQueueLocal, (syncFixStuckTX(accesscode, liquidityToDelete.initPrincipal)).vals());
          logWithRunId("Fixed stuck transaction for: " # accesscode);
          continue privateOrders;
        };

        logWithRunId("Removing private order: " # accesscode);
        let amount_init = liquidityToDelete.amount_init;
        let amount_sell = liquidityToDelete.amount_sell;
        let RevokeFee = liquidityToDelete.RevokeFee;
        let token_init_identifier = liquidityToDelete.token_init_identifier;
        let init_principal = liquidityToDelete.initPrincipal;
        let Fee = liquidityToDelete.Fee;

        let totalFee = (amount_init) * Fee;
        let revoke_Fee = (totalFee - (totalFee / RevokeFee)) / 10000;
        let toBeSent = amount_init + revoke_Fee;

        Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(init_principal)), toBeSent, token_init_identifier, genTxId()));
        dbgPrivateCount += 1;
        if (token_init_identifier == added) { dbgPrivateRefund += toBeSent };

        removeTrade(accesscode, liquidityToDelete.initPrincipal, (liquidityToDelete.token_init_identifier, liquidityToDelete.token_sell_identifier));
        if (not Text.endsWith(accesscode, #text "excl")) {
          replaceLiqMap(
            true,
            false,
            token_init_identifier,
            liquidityToDelete.token_sell_identifier,
            accesscode,
            (amount_init, amount_sell, 0, 0, "", liquidityToDelete.OCname, liquidityToDelete.time, liquidityToDelete.token_init_identifier, liquidityToDelete.token_sell_identifier, liquidityToDelete.strictlyOTC, liquidityToDelete.allOrNothing),
            #Zero,
            null,
            ?{
              amount_init = amount_init;
              amount_sell = amount_sell;
              init_principal = init_principal;
              sell_principal = "";
              accesscode = accesscode;
              token_init_identifier = token_init_identifier;
              filledInit = 0;
              filledSell = 0;
              strictlyOTC = false;
              allOrNothing = false;
            },
          );
        };
      };
      // Handle AMM pools
      for (poolKey in Map.keys(AMMpools)) {
        if (poolKey.0 == added or poolKey.1 == added) {
          logWithRunId("Processing AMM pool: " # debug_show (poolKey));
          switch (Map.get(AMMpools, hashtt, poolKey)) {
            case (null) {
              logWithRunId("AMM pool not found: " # debug_show (poolKey));
            };
            case (?pool) {
              // Iterate through all users who have liquidity in this pool
              label a for (user in (TrieSet.toArray(pool.providers)).vals()) {
                let positions = switch (Map.get(userLiquidityPositions, phash, user)) {
                  case (?a) { a };
                  case null {
                    logWithRunId("No liquidity positions for user: " # debug_show (user));
                    continue a;
                    [{
                      token0 = "";
                      token1 = "";
                      liquidity = 0;
                      fee0 = 0;
                      fee1 = 0;
                      lastUpdateTime = 0;
                    }];
                  };
                };
                var updatedPositions = positions;
                var nowVar = Time.now();
                for (position in positions.vals()) {
                  if (position.token0 == poolKey.0 and position.token1 == poolKey.1) {
                    let amount0 = ((position.liquidity * pool.reserve0) / pool.totalLiquidity) +(position.fee0 / (tenToPower60));
                    let amount1 = ((position.liquidity * pool.reserve1) / pool.totalLiquidity) +(position.fee1 / (tenToPower60));
                    // Queue transfers to return liquidity to the user
                    let tFees0 = returnTfees(poolKey.0);
                    if (amount0 > tFees0) {
                      Vector.add(tempTransferQueueLocal, (#principal(user), amount0 - tFees0, poolKey.0, genTxId()));
                      dbgV2LpCount += 1;
                      if (poolKey.0 == added) { dbgV2LpRefund += (amount0 - tFees0) };
                      logWithRunId("Queued liquidity return for user: " # debug_show (user) # ", amount: " # Nat.toText(amount0 - tFees0) # " of " # poolKey.0);
                    } else {
                      addFees(poolKey.0, amount0, false, "", nowVar);
                      logWithRunId("Added fees for small amount: " # Nat.toText(amount0) # " of " # poolKey.0);
                    };
                    let tFees1 = returnTfees(poolKey.1);
                    if (amount1 > tFees1) {
                      Vector.add(tempTransferQueueLocal, (#principal(user), amount1 - tFees1, poolKey.1, genTxId()));
                      dbgV2LpCount += 1;
                      if (poolKey.1 == added) { dbgV2LpRefund += (amount1 - tFees1) };
                      logWithRunId("Queued liquidity return for user: " # debug_show (user) # ", amount: " # Nat.toText(amount1 - tFees1) # " of " # poolKey.1);
                    } else {
                      addFees(poolKey.1, amount1, false, "", nowVar);
                      logWithRunId("Added fees for small amount: " # Nat.toText(amount1) # " of " # poolKey.1);
                    };

                    // Remove this position from the user's positions
                    updatedPositions := Array.filter(
                      updatedPositions,
                      func(p : LiquidityPosition) : Bool {
                        p.token0 != poolKey.0 or p.token1 != poolKey.1;
                      },
                    );
                    logWithRunId("Removed liquidity position for user: " # debug_show (user));
                  };
                };
                if (updatedPositions.size() == 0) {
                  Map.delete(userLiquidityPositions, phash, user);
                  logWithRunId("Removed all liquidity positions for user: " # debug_show (user));
                } else {
                  Map.set(userLiquidityPositions, phash, user, updatedPositions);
                  logWithRunId("Updated liquidity positions for user: " # debug_show (user));
                };
              };

              // Refund concentrated (V3) positions before deleting pool.
              // Strategy: proportional-to-reserves for ALL positions (full-range + concentrated).
              // Rationale: pool reserves are a single shared resource on deletion; any V3
              // virtual-liquidity math (amountsFromLiquidity) can disagree with proportional
              // math and cause Σbase != pool.reserve, leaking tokens into drift.
              let v3Opt = Map.get(poolV3Data, hashtt, poolKey);
              let sqrtCurrent = switch (v3Opt) { case (?v3) v3.currentSqrtRatio; case null 0 };
              // Track cumulative fee claims to prevent over-claiming across positions
              // and to compute exact pool surplus after all refunds.
              var cumulativeFee0Claimed : Nat = 0;
              var cumulativeFee1Claimed : Nat = 0;
              var totalRefunded0 : Nat = 0;
              var totalRefunded1 : Nat = 0;
              var positionsIterated : Nat = 0;
              var sumPosLiquidity : Nat = 0;
              var dbgCTransferSum0 : Nat = 0;
              var dbgCDustToFees0 : Nat = 0;
              var dbgCTransferSum1 : Nat = 0;
              var dbgCDustToFees1 : Nat = 0;
              var dbgCTransferCount : Nat = 0;
              var dbgCDustCount : Nat = 0;
              let nowVar = Time.now();
              // pre-pass to compute ACTUAL sum of position liquidities
              // for this pool. Using pool.totalLiquidity as denominator is unsafe — if stale
              // tick-tree state inflated v3.activeLiquidity (which syncPoolFromV3 wrote to
              // pool.totalLiquidity), each position gets under-paid and the shortfall orphans
              // to feescollectedDAO. sumPosLiquidityPreCalc reflects reality: refunds sum to
              // exactly pool.reserve (modulo rounding).
              var sumPosLiquidityPreCalc : Nat = 0;
              for ((_, cPositions) in Map.entries(concentratedPositions)) {
                for (pos in cPositions.vals()) {
                  if ((pos.token0 == poolKey.0 and pos.token1 == poolKey.1) or (pos.token0 == poolKey.1 and pos.token1 == poolKey.0)) {
                    sumPosLiquidityPreCalc += pos.liquidity;
                  };
                };
              };
              if (test) {
                Debug.print("POOL_DELETE_START " # debug_show(poolKey)
                  # " pool.reserve0=" # Nat.toText(pool.reserve0)
                  # " pool.reserve1=" # Nat.toText(pool.reserve1)
                  # " pool.totalLiquidity=" # Nat.toText(pool.totalLiquidity)
                  # " sumPosLiquidityPreCalc=" # Nat.toText(sumPosLiquidityPreCalc));
              };
              for ((user, cPositions) in Map.entries(concentratedPositions)) {
                let remaining = Vector.new<ConcentratedPosition>();
                for (pos in cPositions.vals()) {
                  if ((pos.token0 == poolKey.0 and pos.token1 == poolKey.1) or (pos.token0 == poolKey.1 and pos.token1 == poolKey.0)) {
                    // Drift-safe unified refund math: proportional to sumPosLiquidityPreCalc
                    // (NOT pool.totalLiquidity, which can be inflated by tick-tree staleness).
                    // When healthy, sumPosLiquidityPreCalc == pool.totalLiquidity and behavior
                    // is identical to before. When stale, this redirects would-be DAO orphans
                    // back to their rightful LP owners.
                    let (rawBase0, rawBase1) = if (sumPosLiquidityPreCalc > 0) {
                      (mulDiv(pos.liquidity, pool.reserve0, sumPosLiquidityPreCalc),
                       mulDiv(pos.liquidity, pool.reserve1, sumPosLiquidityPreCalc))
                    } else { (0, 0) };
                    let remainingReserve0 = safeSub(pool.reserve0, totalRefunded0);
                    let remainingReserve1 = safeSub(pool.reserve1, totalRefunded1);
                    let base0 = Nat.min(rawBase0, remainingReserve0);
                    let base1 = Nat.min(rawBase1, remainingReserve1);
                    // V3 fees via feeGrowthInside — reduce maxClaim by cumulative claims to prevent over-claiming
                    let (fee0, fee1) = switch (v3Opt) {
                      case (?v3) {
                        let (insideNow0, insideNow1) = positionFeeGrowthInside(pos, v3);
                        let theoretical0 = pos.liquidity * safeSub(insideNow0, pos.lastFeeGrowth0) / tenToPower60;
                        let theoretical1 = pos.liquidity * safeSub(insideNow1, pos.lastFeeGrowth1) / tenToPower60;
                        let maxClaim0 = safeSub(safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0), cumulativeFee0Claimed);
                        let maxClaim1 = safeSub(safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1), cumulativeFee1Claimed);
                        (Nat.min(theoretical0, maxClaim0), Nat.min(theoretical1, maxClaim1));
                      };
                      case null { (0, 0) };
                    };
                    cumulativeFee0Claimed += fee0;
                    cumulativeFee1Claimed += fee1;
                    let total0 = base0 + fee0;
                    let total1 = base1 + fee1;
                    totalRefunded0 += total0;
                    totalRefunded1 += total1;
                    positionsIterated += 1;
                    sumPosLiquidity += pos.liquidity;
                    let tf0 = returnTfees(poolKey.0);
                    let tf1 = returnTfees(poolKey.1);
                    // Dust handling: route small amounts to feescollectedDAO via direct Map
                    // update (NOT addFees — addFees subtracts 1 sat per call which would
                    // cause negative drift accumulation across hundreds of dust events).
                    if (total0 > tf0) {
                      Vector.add(tempTransferQueueLocal, (#principal(user), total0 - tf0, poolKey.0, genTxId()));
                      dbgCTransferCount += 1;
                      dbgCTransferSum0 += total0;
                    } else if (total0 > 0) {
                      let cur0 = switch (Map.get(feescollectedDAO, thash, poolKey.0)) { case (?v) v; case null 0 };
                      Map.set(feescollectedDAO, thash, poolKey.0, cur0 + total0);
                      dbgCDustCount += 1;
                      dbgCDustToFees0 += total0;
                    };
                    if (total1 > tf1) {
                      Vector.add(tempTransferQueueLocal, (#principal(user), total1 - tf1, poolKey.1, genTxId()));
                      dbgCTransferCount += 1;
                      dbgCTransferSum1 += total1;
                    } else if (total1 > 0) {
                      let cur1 = switch (Map.get(feescollectedDAO, thash, poolKey.1)) { case (?v) v; case null 0 };
                      Map.set(feescollectedDAO, thash, poolKey.1, cur1 + total1);
                      dbgCDustCount += 1;
                      dbgCDustToFees1 += total1;
                    };
                    logWithRunId("Refunded V3 position for user: " # debug_show (user) # " liq=" # Nat.toText(pos.liquidity));
                  } else {
                    Vector.add(remaining, pos);
                  };
                };
                if (Vector.size(remaining) == 0) { Map.delete(concentratedPositions, phash, user) }
                else { Map.set(concentratedPositions, phash, user, Vector.toArray(remaining)) };
              };

              // After all LP refunds, remaining = protocol fees (30% of swap fees) + rounding dust.
              // Add to feescollectedDAO — this is legitimate DAO revenue (protocol fees).
              // Safe: the pool reserves only contain tokens from AMM operations, not user deposits.
              // When collectFees drains it, both wallet and fee decrease equally → drift unchanged.
              let poolTotal0 = pool.reserve0 + (switch (v3Opt) {
                case (?v3) { safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0) };
                case null 0;
              });
              let poolTotal1 = pool.reserve1 + (switch (v3Opt) {
                case (?v3) { safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1) };
                case null 0;
              });
              let poolRemainder0 = safeSub(poolTotal0, totalRefunded0);
              let poolRemainder1 = safeSub(poolTotal1, totalRefunded1);
              if (poolRemainder0 > 0) {
                let cur0 = switch (Map.get(feescollectedDAO, thash, poolKey.0)) { case (?v) v; case null 0 };
                Map.set(feescollectedDAO, thash, poolKey.0, cur0 + poolRemainder0);
              };
              if (poolRemainder1 > 0) {
                let cur1 = switch (Map.get(feescollectedDAO, thash, poolKey.1)) { case (?v) v; case null 0 };
                Map.set(feescollectedDAO, thash, poolKey.1, cur1 + poolRemainder1);
              };
              logWithRunId("Pool deletion remainder (protocol fees + dust) → feescollectedDAO: token0=" # Nat.toText(poolRemainder0) # " token1=" # Nat.toText(poolRemainder1));
              if (test) {
                Debug.print("POOL_DELETE_SUMMARY " # debug_show(poolKey)
                  # " positionsIterated=" # Nat.toText(positionsIterated)
                  # " sumPosLiquidity=" # Nat.toText(sumPosLiquidity)
                  # " pool.totalLiquidity=" # Nat.toText(pool.totalLiquidity)
                  # " pool.reserve0=" # Nat.toText(pool.reserve0)
                  # " pool.reserve1=" # Nat.toText(pool.reserve1)
                  # " totalRefunded0=" # Nat.toText(totalRefunded0)
                  # " totalRefunded1=" # Nat.toText(totalRefunded1)
                  # " cTransferCount=" # Nat.toText(dbgCTransferCount)
                  # " cTransferSum0=" # Nat.toText(dbgCTransferSum0)
                  # " cTransferSum1=" # Nat.toText(dbgCTransferSum1)
                  # " cDustCount=" # Nat.toText(dbgCDustCount)
                  # " cDustToFees0=" # Nat.toText(dbgCDustToFees0)
                  # " cDustToFees1=" # Nat.toText(dbgCDustToFees1)
                  # " v3Resid0=" # Nat.toText(switch (v3Opt) { case (?v3) safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0); case null 0 })
                  # " v3Resid1=" # Nat.toText(switch (v3Opt) { case (?v3) safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1); case null 0 })
                  # " poolRemainder0=" # Nat.toText(poolRemainder0)
                  # " poolRemainder1=" # Nat.toText(poolRemainder1));
              };

              Map.delete(poolV3Data, hashtt, poolKey);

              // Remove the pool
              Map.delete(AMMpools, hashtt, poolKey);
              logWithRunId("Removed AMM pool: " # debug_show (poolKey));
            };
          };
        };
      };

      logWithRunId("Updating last traded price and related data");
      let last_traded_price_vector2 = Vector.new<Float>();
      let price_day_before_vector2 = Vector.new<Float>();
      let volume_24h_vector2 = Vector.new<Nat>();
      let amm_reserve0_vector2 = Vector.new<Nat>();
      let amm_reserve1_vector2 = Vector.new<Nat>();
      var pool_canister_vector = Vector.new<(Text, Text)>();
      var asset_minimum_amount_vector = Vector.new<(Nat, Nat)>();

      for (i in Vector.vals(old_new)) {
        Vector.add(asset_minimum_amount_vector, Vector.get(asset_minimum_amount, i.0));
        Vector.add(pool_canister_vector, Vector.get(pool_canister, i.0));
        Vector.add(last_traded_price_vector2, Vector.get(last_traded_price, i.0));
        Vector.add(price_day_before_vector2, Vector.get(price_day_before, i.0));
        Vector.add(volume_24h_vector2, volume_24hArray[i.0]);

        // Add AMM data
        switch (Map.get(AMMpools, hashtt, Vector.get(pool_canister, i.0))) {
          case (?pool) {
            Vector.add(amm_reserve0_vector2, pool.reserve0);
            Vector.add(amm_reserve1_vector2, pool.reserve1);
          };
          case (null) {
            Vector.add(amm_reserve0_vector2, 0);
            Vector.add(amm_reserve1_vector2, 0);
          };
        };
      };

      last_traded_price := Vector.clone(last_traded_price_vector2);
      price_day_before := Vector.clone(price_day_before_vector2);
      volume_24hArray := Vector.toArray(volume_24h_vector2);
      pool_canister := pool_canister_vector;
      rebuildPoolIndex();
      asset_minimum_amount := asset_minimum_amount_vector;
      amm_reserve0Array := Vector.toArray(amm_reserve0_vector2);
      amm_reserve1Array := Vector.toArray(amm_reserve1_vector2);
      logWithRunId("Updated last traded price: " # debug_show (last_traded_price));

      for (index in Iter.range(0, acceptedTokens.size() - 1)) {
        for (index2 in Iter.range(index +1, acceptedTokens.size() - 1)) {
          if (acceptedTokens[index2] == added or acceptedTokens[index] == added) {

            ignore Map.remove(liqMapSort, hashtt, (acceptedTokens[index], acceptedTokens[index2]));
            logWithRunId("Removed from liqMapSort: " # acceptedTokens[index] # "-" # acceptedTokens[index2]);
            ignore Map.remove(liqMapSortForeign, hashtt, (acceptedTokens[index], acceptedTokens[index2]));
            logWithRunId("Removed from liqMapSortForeign: " # acceptedTokens[index] # "-" # acceptedTokens[index2]);
          };
        };
      };

      removeToken(added);
      logWithRunId("Removed token: " # added);

      if (test) {
        Debug.print("REMOVE_BREAKDOWN token=" # added
          # " orderbookCount=" # Nat.toText(dbgObCount)
          # " orderbookRefund(added-token)=" # Nat.toText(dbgObRefund0)
          # " foreignCount=" # Nat.toText(dbgForeignCount)
          # " foreignRefund(added-token)=" # Nat.toText(dbgForeignRefund)
          # " privateCount=" # Nat.toText(dbgPrivateCount)
          # " privateRefund(added-token)=" # Nat.toText(dbgPrivateRefund)
          # " v2LpCount=" # Nat.toText(dbgV2LpCount)
          # " v2LpRefund(added-token)=" # Nat.toText(dbgV2LpRefund)
          # " totalLocalTransfers=" # Nat.toText(Vector.size(tempTransferQueueLocal)));
      };
    } else if ((action == #Add and containsToken(added) == false) or (containsToken(added) == false and action == #Opposite)) {
      logWithRunId("Adding new token: " # added);
      let acceptedTokensVec = Vector.fromArray<Text>(acceptedTokens);
      Vector.add(acceptedTokensVec, added);
      acceptedTokens := Vector.toArray(acceptedTokensVec);
      let minimumAmountVec = Vector.fromArray<Nat>(minimumAmount);
      Vector.add(minimumAmountVec, minimum);
      minimumAmount := Vector.toArray(minimumAmountVec);
      let tokenTypeVec = Vector.fromArray<{ #ICP; #ICRC12; #ICRC3 }>(tokenType);
      Vector.add(tokenTypeVec, tType);
      tokenType := Vector.toArray(tokenTypeVec);

      var pool_canister_vector = Vector.new<(Text, Text)>();
      var asset_minimum_amount_vector = Vector.new<(Nat, Nat)>();
      let last_traded_price_vector2 = Vector.new<Float>();
      let price_day_before_vector2 = Vector.new<Float>();
      let volume_24h_vector2 = Vector.new<Nat>();
      let amm_reserve0_vector2 = Vector.new<Nat>();
      let amm_reserve1_vector2 = Vector.new<Nat>();

      // Add existing pools to the new vectors
      for (i in Iter.range(0, Vector.size(pool_canister) - 1)) {
        let poolKey = Vector.get(pool_canister, i);
        Vector.add(pool_canister_vector, poolKey);
        Vector.add(asset_minimum_amount_vector, Vector.get(asset_minimum_amount, i));

        // Get last traded price from most recent 5min kline
        let klineKey : KlineKey = (poolKey.0, poolKey.1, #fivemin);
        var lastPrice : Float = 0.000000000001;
        switch (Map.get(klineDataStorage, hashkl, klineKey)) {
          case (?tree) {
            let result = RBTree.scanLimit(tree, compareTime, 0, Time.now(), #bwd, 1).results;
            if (result.size() > 0) {
              lastPrice := result[0].1.close;
            };
          };
          case null {};
        };
        Vector.add(last_traded_price_vector2, lastPrice);

        // Add AMM data
        switch (Map.get(AMMpools, hashtt, poolKey)) {
          case (?pool) {
            Vector.add(amm_reserve0_vector2, pool.reserve0);
            Vector.add(amm_reserve1_vector2, pool.reserve1);
          };
          case (null) {
            Vector.add(amm_reserve0_vector2, 0);
            Vector.add(amm_reserve1_vector2, 0);
          };
        };

        // Get 24h volume using update24hVolume
        let volume = update24hVolume(poolKey);
        Vector.add(volume_24h_vector2, volume);

        // Get price day before using updatePriceDayBefore
        Vector.add(price_day_before_vector2, updatePriceDayBefore(poolKey, Time.now()));
      };

      // Add new pools only with base tokens
      for (baseToken in baseTokens.vals()) {
        if (added != baseToken) {
          Vector.add(pool_canister_vector, (added, baseToken));
          let baseTokenIndex = Array.indexOf<Text>(baseToken, acceptedTokens, Text.equal);
          switch (baseTokenIndex) {
            case (?index) {
              Vector.add(asset_minimum_amount_vector, (minimum, minimumAmount[index]));
            };
            case null {
              Vector.add(asset_minimum_amount_vector, (minimum, minimum));
            };
          };
          let poolKey = (added, baseToken);

          // Get last traded price
          let klineKey : KlineKey = (poolKey.0, poolKey.1, #fivemin);
          var lastPrice : Float = 0.000000000001;
          switch (Map.get(klineDataStorage, hashkl, klineKey)) {
            case (?tree) {
              let result = RBTree.scanLimit(tree, compareTime, 0, Time.now(), #bwd, 1).results;
              if (result.size() > 0) {
                lastPrice := result[0].1.close;
              };
            };
            case null {};
          };
          Vector.add(last_traded_price_vector2, lastPrice);

          // Add empty AMM data for new pool
          Vector.add(amm_reserve0_vector2, 0);
          Vector.add(amm_reserve1_vector2, 0);

          // Get volume and price day before
          let volume = update24hVolume(poolKey);
          Vector.add(volume_24h_vector2, volume);
          Vector.add(price_day_before_vector2, updatePriceDayBefore(poolKey, Time.now()));

          logWithRunId("Added new pool: " # added # "-" # baseToken);
        };
      };

      last_traded_price := Vector.clone(last_traded_price_vector2);
      price_day_before := Vector.clone(price_day_before_vector2);
      pool_canister := pool_canister_vector;
      rebuildPoolIndex();
      asset_minimum_amount := asset_minimum_amount_vector;
      volume_24hArray := Vector.toArray(volume_24h_vector2);
      amm_reserve0Array := Vector.toArray(amm_reserve0_vector2);
      amm_reserve1Array := Vector.toArray(amm_reserve1_vector2);
    } else {
      logWithRunId("No action taken: Token already exists or invalid action");
      return #Err(#InvalidInput("Token already exists or invalid action"));
    };


    doInfoBeforeStep2();
    logWithRunId("Updated exchange info");

    checkAndAggregateAllPools();
    logWithRunId("Checked and aggregated all pools");


    try {
      await treasury.getAcceptedtokens(acceptedTokens);
      updateTokenInfo<system>(true, true, await treasury.getTokenInfo());
      updateStaticInfo();
      logWithRunId("Updated token info from treasury");
      doInfoBeforeStep2();
    } catch (err) {
      logWithRunId("Error updating token info: " # Error.message(err));
      retryFunc<system>(
        func() : async () {
          await treasury.getAcceptedtokens(acceptedTokens);
          updateTokenInfo<system>(true, true, await treasury.getTokenInfo());
          updateStaticInfo();
          doInfoBeforeStep2();
        },
        5,
        10,
        10,
      );
    };


    // Transferring the transactions that have to be made to the treasury,
    Debug.print("addAcceptedToken: queuing " # debug_show(Vector.size(tempTransferQueueLocal)) # " transfers");
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { Debug.print("addAcceptedToken transfer ERROR: " # Error.message(err)); false })) {
      Debug.print("addAcceptedToken: transfers sent to treasury OK");
    } else {
      Debug.print("addAcceptedToken: transfer FAILED, queuing to tempTransferQueue");
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };


    logWithRunId("Final asset_minimum_amount: " # debug_show (asset_minimum_amount));
    logWithRunId("Final pool_canister: " # debug_show (pool_canister));
    logWithRunId("addAcceptedToken completed");

    let loggingText = Text.join("\n", Vector.toArray(logEntries).vals());
    Map.set(loggingMapaddAcceptedToken, nhash, runId, loggingText);
    return #Ok("addAcceptedToken completed");
  };

  public shared ({ caller }) func updateTokenType(
    token : Text,
    newType : { #ICP; #ICRC12; #ICRC3 },
  ) : async ExTypes.ActionResult {
    if (not isAdmin(caller)) { return #Err(#NotAuthorized) };
    let index = Array.indexOf<Text>(token, acceptedTokens, Text.equal);
    switch (index) {
      case (?k) {
        let mutable = Array.thaw<{ #ICP; #ICRC12; #ICRC3 }>(tokenType);
        mutable[k] := newType;
        tokenType := Array.freeze(mutable);
        // Refresh the cached acceptedTokensInfo so queries reflect the change immediately
        let updated = Array.thaw<TokenInfo>(acceptedTokensInfo);
        updated[k] := { acceptedTokensInfo[k] with asset_type = newType };
        acceptedTokensInfo := Array.freeze(updated);
        #Ok("Token type updated to " # debug_show (newType));
      };
      case null { #Err(#InvalidInput("Token not found")) };
    };
  };

  private func removeToken(tokenToRemove : Text) {
    let index2 : ?Nat = Array.indexOf<Text>(tokenToRemove, acceptedTokens, Text.equal);
    var index = 0;
    switch (index2) {
      case (?k) { index := k };
      case null {};
    };
    var i = 0;
    acceptedTokens := Array.filter<Text>(acceptedTokens, func(t) { t != tokenToRemove });
    minimumAmount := Array.filter<Nat>(minimumAmount, func(t) { if (i != index) { i += 1; return true } else { i += 1; return false } });
    i := 0;
    tokenType := Array.filter<{ #ICP; #ICRC12; #ICRC3 }>(tokenType, func(t) { if (i != index) { i += 1; return true } else { i += 1; return false } });
  };

  //Function that is used to retry certain awaits in case the process queue is full
  private func retryFunc<system>(
    Func : () -> async (),
    maxRetries : Nat,
    initialDelay : Nat,
    backoffFactor : Nat,
  ) {
    let initialDelayEdited = if (initialDelay < 2) {
      5;
    } else { initialDelay };

    ignore setTimer<system>(
      #seconds(initialDelayEdited),
      func() : async () {
        await retryLoop<system>(Func, maxRetries, initialDelayEdited, backoffFactor, 0);
      },
    );
  };

  private func retryLoop<system>(
    Func : () -> async (),
    remainingRetries : Nat,
    currentDelay : Nat,
    backoffFactor : Nat,
    attemptCount : Nat,
  ) : async () {
    try {
      await Func();

    } catch (err) {

      if (remainingRetries > 0) {
        let nextDelay = Nat.max(1, currentDelay +backoffFactor);

        ignore setTimer<system>(
          #seconds(nextDelay),
          func() : async () {
            await retryLoop<system>(Func, remainingRetries - 1, nextDelay, backoffFactor, attemptCount + 1);
          },
        );
      } else {

      };
    };
  };

  //Pausing a token, for instance when a metadata change is expected or if the ledger times out.
  //Paused tokens cant be traded with, however, existing orders stay.
  public shared ({ caller }) func pauseToken(token : Text) : async () {
    if (not ownercheck(caller)) {
      return;
    };
    logger.info("ADMIN", "pauseToken called for " # token # " by " # Principal.toText(caller), "pauseToken");
    if (
      (
        switch (Array.find<Text>(pausedTokens, func(t) { t == token })) {
          case null { false };
          case (_) { true };
        }
      ) != false
    ) {
      var temBuf = Buffer.fromArray<Text>(pausedTokens);
      var index2 = Buffer.indexOf(token, temBuf, Text.equal);
      let index = switch (index2) {
        case (?kk) { kk };
        case null { 99999 };
      };
      assert (index != 99999);
      ignore temBuf.remove(index);
      pausedTokens := Buffer.toArray(temBuf);
    } else {
      let pausedTokensVec = Vector.fromArray<Text>(pausedTokens);
      Vector.add(pausedTokensVec, token);
      pausedTokens := Vector.toArray(pausedTokensVec);
    };
  };

  // Function for the frontend to check how much % fee is being accounted for trades. Its in Basispoints so 1 represents 0.01%
  public shared query func hmFee() : async Nat {
    return ICPfee;
  };

  // Function for the frontend to check how much revokeFee there is. The total fee can be divided by this number.
  // That would be the fee if someone revokes their order.
  public shared query func hmRevokeFee() : async Nat {
    return RevokeFeeNow;
  };

  public shared query func hmRefFee() : async Nat {
    return ReferralFees;
  };

  // Function to collect the fees that are to be collected. In production this will go to the DAO treasury.
  public shared ({ caller }) func collectFees() : async ExTypes.ActionResult {
    if (not isFeeCollector(caller)) {
      return #Err(#NotAuthorized);
    };
    logger.info("ADMIN", "collectFees called by " # Principal.toText(caller), "collectFees");
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var endmessage = "done";

    // RVVR-TACOX-19
    for ((key, value) in Map.entries(feescollectedDAO)) {
      let Tfees = returnTfees(key);
      if (value > (Tfees)) {
        Vector.add(tempTransferQueueLocal, (#principal(owner3), value -Tfees, key, genTxId()));
        Map.set(feescollectedDAO, thash, key, 0);
      };
    };
    // Transfering the transactions that have to be made to the treasury,
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {

    } else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };
    return #Ok(endmessage);
  };

  public shared ({ caller }) func addFeeCollector(p : Principal) : async ExTypes.ActionResult {
    if (not isFeeCollector(caller)) { return #Err(#NotAuthorized) };
    for (existing in feeCollectors.vals()) {
      if (existing == p) { return #Ok("Already in list") };
    };
    feeCollectors := Array.append(feeCollectors, [p]);
    #Ok("Added");
  };

  public shared ({ caller }) func removeFeeCollector() : async ExTypes.ActionResult {
    let filtered = Array.filter<Principal>(feeCollectors, func(p) { p != caller });
    if (filtered.size() == feeCollectors.size()) { return #Err(#InvalidInput("Not in list")) };
    feeCollectors := filtered;
    #Ok("Removed self");
  };

  public query ({ caller }) func getFeeCollectors() : async [Principal] {
    if (not isFeeCollector(caller)) { return [] };
    feeCollectors;
  };

  // Change the trading fees, maximum is 0.5% and minimum is 0.01%
  public shared ({ caller }) func ChangeTradingfees(ok : Nat) {
    if (not ownercheck(caller)) {
      return;
    };
    logger.info("ADMIN", "ChangeTradingfees to " # Nat.toText(ok) # " by " # Principal.toText(caller), "ChangeTradingfees");
    if (ok <= 50 and ok >= 1) {
      ICPfee := ok;
    };
  };
  // Change the revoke trading fees, minimum is 1/50th and maximum is 1/3rd or the total fees.
  public shared ({ caller }) func ChangeRevokefees(ok : Nat) {
    if (not ownercheck(caller)) {
      return;
    };

    if (ok <= 50 and ok >= 3) {
      RevokeFeeNow := ok;
    };
  };
  public shared ({ caller }) func ChangeReferralFees(newFeePercentage : Nat) : async () {
    if (not ownercheck(caller)) {
      return;
    };
    logger.info("ADMIN", "ChangeReferralFees to " # Nat.toText(newFeePercentage) # " by " # Principal.toText(caller), "ChangeReferralFees");
    if (newFeePercentage <= 50 and newFeePercentage >= 1) {
      // Limit to 50% max
      ReferralFees := newFeePercentage;
    };
  };

  //Create a hash for the orderid/accesscode.
  private func PrivateHash() : Text {
    return fuzz.text.randomAlphanumeric(32)

  };

  public query ({ caller }) func exchangeInfo() : async ?pool {
    if (isAllowedQuery(caller) != 1) {
      return null;
    };
    ?AllExchangeInfo;
  };

  //Function made for people that sent an token to the exchange that is not supported.
  type RecoveryInput = { identifier : Text; block : Nat; tType : { #ICP; #ICRC12; #ICRC3 } };
  type RecoveryResult = { identifier : Text; block : Nat; success : Bool; error : Text };

  // Batch recovery: recover up to 20 stuck transfers, 5 in parallel per batch
  public shared ({ caller }) func recoverBatch(
    recoveries : [RecoveryInput]
  ) : async [RecoveryResult] {
    if (isAllowed(caller) != 1) {
      return [{ identifier = ""; block = 0; success = false; error = "Not allowed" }];
    };

    let maxRecoveries = Nat.min(recoveries.size(), 20);
    let results = Vector.new<RecoveryResult>();

    // Pre-filter: skip blocks already in BlocksDone (no async needed)
    let toRecover = Vector.new<RecoveryInput>();
    for (i in Iter.range(0, maxRecoveries - 1)) {
      let r = recoveries[i];
      if (Map.has(BlocksDone, thash, r.identifier # ":" # Nat.toText(r.block))) {
        Vector.add(results, { identifier = r.identifier; block = r.block; success = false; error = "Block already used" });
      } else {
        Vector.add(toRecover, r);
      };
    };

    let pending = Vector.toArray(toRecover);
    var idx = 0;

    // Process in batches of 5 in parallel
    while (idx < pending.size()) {
      let batchEnd = Nat.min(idx + 5, pending.size());

      // Fire up to 5 futures
      let f0 = if (idx + 0 < batchEnd) { ?((with timeout = 65) recoverWronglysent(pending[idx + 0].identifier, pending[idx + 0].block, pending[idx + 0].tType)) } else { null };
      let f1 = if (idx + 1 < batchEnd) { ?((with timeout = 65) recoverWronglysent(pending[idx + 1].identifier, pending[idx + 1].block, pending[idx + 1].tType)) } else { null };
      let f2 = if (idx + 2 < batchEnd) { ?((with timeout = 65) recoverWronglysent(pending[idx + 2].identifier, pending[idx + 2].block, pending[idx + 2].tType)) } else { null };
      let f3 = if (idx + 3 < batchEnd) { ?((with timeout = 65) recoverWronglysent(pending[idx + 3].identifier, pending[idx + 3].block, pending[idx + 3].tType)) } else { null };
      let f4 = if (idx + 4 < batchEnd) { ?((with timeout = 65) recoverWronglysent(pending[idx + 4].identifier, pending[idx + 4].block, pending[idx + 4].tType)) } else { null };

      // Await all in this batch
      let r0 = switch (f0) { case (?f) { try { await f } catch (_) { false } }; case null { false } };
      let r1 = switch (f1) { case (?f) { try { await f } catch (_) { false } }; case null { false } };
      let r2 = switch (f2) { case (?f) { try { await f } catch (_) { false } }; case null { false } };
      let r3 = switch (f3) { case (?f) { try { await f } catch (_) { false } }; case null { false } };
      let r4 = switch (f4) { case (?f) { try { await f } catch (_) { false } }; case null { false } };

      let batchResults = [r0, r1, r2, r3, r4];
      for (j in Iter.range(0, batchEnd - idx - 1)) {
        Vector.add(results, {
          identifier = pending[idx + j].identifier;
          block = pending[idx + j].block;
          success = batchResults[j];
          error = if (batchResults[j]) { "" } else { "Recovery failed" };
        });
      };

      idx := batchEnd;
    };

    Vector.toArray(results);
  };

  // Walks blockData; returns the gross transfer amount iff the block contains
  // a Transfer FROM `recipient` TO treasury_principal. Mirrors the case
  // branches in recoverWronglysent (#ICP, #ICRC12, #ICRC3) but only EXTRACTS —
  // does not queue any transfers. Returns null if no matching transfer.
  private func extractRecipientToTreasuryAmount(
    blockData : BlockData,
    recipient : Principal,
  ) : ?Nat {
    switch (blockData) {
      case (#ICP(response)) {
        for ({ transaction = { operation } } in response.blocks.vals()) {
          switch (operation) {
            case (? #Transfer({ amount; from; to })) {
              let check_from = Utils.accountToText({ hash = from });
              let check_to = Utils.accountToText({ hash = to });
              let from2 = Utils.accountToText(Utils.principalToAccount(recipient));
              let to2 = Utils.accountToText(Utils.principalToAccount(treasury_principal));
              if (Text.endsWith(check_from, #text from2)
                  and Text.endsWith(check_to, #text to2)) {
                return ?(nat64ToNat(amount.e8s));
              };
            };
            case _ {};
          };
        };
        null
      };
      case (#ICRC12(transactions)) {
        for ({ transfer } in transactions.vals()) {
          switch (transfer) {
            case (?{ to; from; amount = received }) {
              if (to.owner == treasury_principal and from.owner == recipient) {
                return ?received;
              };
            };
            case null {};
          };
        };
        null
      };
      case (#ICRC3(result)) {
        for (block in result.blocks.vals()) {
          switch (block.block) {
            case (#Map(entries)) {
              var to : ?ICRC1.Account = null;
              var from : ?ICRC1.Account = null;
              var amount : ?Nat = null;
              for ((key, value) in entries.vals()) {
                switch (key) {
                  case "to" {
                    switch (value) {
                      case (#Array(arr)) {
                        if (arr.size() >= 1) {
                          switch (arr[0]) {
                            case (#Blob(owner)) {
                              to := ?{
                                owner = Principal.fromBlob(owner);
                                subaccount = if (arr.size() > 1) {
                                  switch (arr[1]) {
                                    case (#Blob(s)) ?s;
                                    case _ null;
                                  };
                                } else null;
                              };
                            };
                            case _ {};
                          };
                        };
                      };
                      case (#Blob(owner)) {
                        to := ?{ owner = Principal.fromBlob(owner); subaccount = null };
                      };
                      case _ {};
                    };
                  };
                  case "from" {
                    switch (value) {
                      case (#Array(arr)) {
                        if (arr.size() == 1) {
                          switch (arr[0]) {
                            case (#Blob(owner)) {
                              from := ?{ owner = Principal.fromBlob(owner); subaccount = null };
                            };
                            case _ {};
                          };
                        };
                      };
                      case _ {};
                    };
                  };
                  case "amt" {
                    switch (value) {
                      case (#Nat(a)) { amount := ?a };
                      case (#Int(a)) { amount := ?Int.abs(a) };
                      case _ {};
                    };
                  };
                  case _ {};
                };
              };
              switch (to, from, amount) {
                case (?toAcc, ?fromAcc, ?howMuchReceived) {
                  if (toAcc.owner == treasury_principal and fromAcc.owner == recipient) {
                    return ?howMuchReceived;
                  };
                };
                case _ {};
              };
            };
            case _ {};
          };
        };
        null
      };
    };
  };

  // Replicates checkDiffs (Phase B+C math) for ONE token only. Returns
  //   balance(treasury) − pending − orderbalance − ammbalance − feebalances
  // Sync work for buckets + one icrc1_balance_of(treasury) + one
  // treasury.getPendingTransfersByToken(). Used as a sanity gate inside
  // adminRecoverWronglysent to verify funds are actually orphaned before
  // dispatching a refund.
  private func computeDriftForToken(token : Text) : async Int {
    let Tfees = returnTfees(token);

    var feebalance : Nat = 0;
    switch (Map.get(feescollectedDAO, thash, token)) {
      case (?asi) { feebalance := asi };
      case _ {};
    };
    for ((_, optEntry) in Map.entries(referrerFeeMap)) {
      switch (optEntry) {
        case (?(fees, _)) {
          for ((tok, amt) in Vector.vals(fees)) {
            if (tok == token) { feebalance += amt };
          };
        };
        case _ {};
      };
    };

    var orderbalance : Nat = 0;
    for ((poolKey, poolValue) in Map.entries(liqMapSort)) {
      let (t1, t2) = poolKey;
      if (t1 == token or t2 == token) {
        for ((_, trades) in RBTree.entries(poolValue)) {
          for (trade in trades.vals()) {
            if (trade.token_init_identifier == token) {
              orderbalance += trade.amount_init
                + (((trade.amount_init * trade.Fee) / (10000 * trade.RevokeFee))
                   * (trade.RevokeFee - 1))
                + Tfees;
            };
          };
        };
      };
    };
    for ((poolKey, poolValue) in Map.entries(liqMapSortForeign)) {
      let (t1, t2) = poolKey;
      if (t1 == token or t2 == token) {
        for ((_, trades) in RBTree.entries(poolValue)) {
          for (trade in trades.vals()) {
            if (trade.token_init_identifier == token) {
              orderbalance += trade.amount_init
                + (((trade.amount_init * trade.Fee) / (10000 * trade.RevokeFee))
                   * (trade.RevokeFee - 1))
                + Tfees;
            };
          };
        };
      };
    };
    for ((_, trade) in Map.entries(tradeStorePrivate)) {
      if (trade.token_init_identifier == token) {
        orderbalance += trade.amount_init
          + (((trade.amount_init * trade.Fee) / (10000 * trade.RevokeFee))
             * (trade.RevokeFee - 1))
          + Tfees;
      };
    };

    var ammbalance : Nat = 0;
    for ((poolKey, pool) in Map.entries(AMMpools)) {
      if (poolKey.0 == token) {
        ammbalance += pool.reserve0 + (pool.totalFee0 / tenToPower60);
        switch (Map.get(poolV3Data, hashtt, poolKey)) {
          case (?v3) { ammbalance += safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0) };
          case null {};
        };
      } else if (poolKey.1 == token) {
        ammbalance += pool.reserve1 + (pool.totalFee1 / tenToPower60);
        switch (Map.get(poolV3Data, hashtt, poolKey)) {
          case (?v3) { ammbalance += safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1) };
          case null {};
        };
      };
    };

    let act = actor (token) : ICRC1.FullInterface;
    let rawFut = act.icrc1_balance_of({ owner = treasury_principal; subaccount = null });
    let pendingFut = treasury.getPendingTransfersByToken();

    let raw = await rawFut;
    let pendingArr = await pendingFut;

    var pending : Nat = 0;
    for ((tok, amt) in pendingArr.vals()) { if (tok == token) { pending := amt } };
    for (txn in Vector.vals(tempTransferQueue)) {
      if (txn.2 == token) { pending += txn.1 };
    };

    let adjustedBalance : Nat = if (raw >= pending) { raw - pending } else { 0 };
    let drift : Int = adjustedBalance - (orderbalance + ammbalance + feebalance);
    drift;
  };

  // Admin one-shot recovery for a stuck (trap-orphaned) deposit. Bypasses but
  // does NOT delete the BlocksDone gate so a trapped swap's stuck deposit can
  // be returned. Marks BlocksAdminRecovered to prevent any future admin call
  // from re-paying. Concurrent admin calls serialize via adminRecoveryRunning.
  // Refuses if drift in `identifier` < deposit amount (Bug 5 — drain guard).
  //
  // Safe operating procedure:
  //   1. checkDiffs to confirm positive drift in `identifier` ≥ deposit amount.
  //   2. Run this function with the depositor's principal.
  //   3. checkDiffs again to confirm drift dropped by the refund amount.
  public shared ({ caller }) func adminRecoverWronglysent(
    recipient : Principal,
    identifier : Text,
    Block : Nat,
    tType : { #ICP; #ICRC12; #ICRC3 }
  ) : async Bool {
    if (not ownercheck(caller)) { return false };
    if (adminRecoveryRunning) { return false };
    adminRecoveryRunning := true;

    let blockKey = identifier # ":" # Nat.toText(Block);

    if (Map.has(BlocksAdminRecovered, thash, blockKey)) {
      adminRecoveryRunning := false;
      return false;
    };

    let nowVar = Time.now();

    let blockData = try { await* getBlockData(identifier, Block, tType) }
                    catch (_) { adminRecoveryRunning := false; return false };
    let timestamp = getTimestamp(blockData);
    if (timestamp == 0) { adminRecoveryRunning := false; return false };
    let timeDiff : Int = Int.abs(Time.now()) - timestamp;
    if (timeDiff > 1814400000000000) { adminRecoveryRunning := false; return false };

    let depositAmount : Nat = switch (extractRecipientToTreasuryAmount(blockData, recipient)) {
      case (?n) n;
      case null { adminRecoveryRunning := false; return false };
    };
    if (depositAmount == 0) { adminRecoveryRunning := false; return false };

    let drift : Int = await computeDriftForToken(identifier);
    if (drift < (depositAmount : Int)) {
      adminRecoveryRunning := false;
      logger.warn(
        "ADMIN",
        "adminRecoverWronglysent: refund refused — drift "
          # debug_show(drift) # " < depositAmount " # Nat.toText(depositAmount)
          # " for " # identifier # " block " # Nat.toText(Block)
          # " (recipient " # Principal.toText(recipient) # ")",
        "adminRecoverWronglysent"
      );
      return false;
    };

    let Tfees = returnTfees(identifier);
    if (depositAmount <= Tfees) { adminRecoveryRunning := false; return false };
    let refundAmount = depositAmount - Tfees;

    Map.set(BlocksAdminRecovered, thash, blockKey, nowVar);

    let q = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    Vector.add(q, (#principal(recipient), refundAmount, identifier, genTxId()));
    let delivered = try {
      await treasury.receiveTransferTasks(
        Vector.toArray<(TransferRecipient, Nat, Text, Text)>(q),
        isInAllowedCanisters(recipient)
      )
    } catch (_) { false };

    adminRecoveryRunning := false;

    if (delivered) {
      logger.info(
        "ADMIN",
        "adminRecoverWronglysent: refunded " # Nat.toText(refundAmount) # " "
          # identifier # " to " # Principal.toText(recipient)
          # " for block " # Nat.toText(Block)
          # " by admin " # Principal.toText(caller),
        "adminRecoverWronglysent"
      );
      return true;
    };

    Vector.addFromIter(tempTransferQueue, Vector.vals(q));
    logger.warn(
      "ADMIN",
      "adminRecoverWronglysent: treasury rejected dispatch, queued refund of "
        # Nat.toText(refundAmount) # " " # identifier # " for "
        # Principal.toText(recipient) # " block " # Nat.toText(Block),
      "adminRecoverWronglysent"
    );
    return false;
  };

  public shared ({ caller }) func recoverWronglysent(identifier : Text, Block : Nat, tType : { #ICP; #ICRC12; #ICRC3 }) : async Bool {
    if (isAllowed(caller) != 1) {
      return false;
    };
    var nowVar = Time.now();
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    if (Map.has(BlocksDone, thash, identifier # ":" #Nat.toText(Block))) {
      return false;
    };
    Map.set(BlocksDone, thash, identifier # ":" #Nat.toText(Block), nowVar);
    let nowVar2 = nowVar;

    try {
      let blockData = await* getBlockData(identifier, Block, tType);
      nowVar := Time.now();
      // Check if the transaction is not older than 21 days
      let timestamp = getTimestamp(blockData);
      if (timestamp == 0) {

        return false;
      } else {
        let currentTime = Int.abs(nowVar2);
        let timeDiff : Int = currentTime - timestamp;
        if (timeDiff > 1814400000000000) {
          // 21 days in nanoseconds

          return false;
        };
      };

      switch (blockData) {
        case (#ICP(response)) {
          for ({ transaction = { operation } } in response.blocks.vals()) {
            switch (operation) {
              case (? #Transfer({ amount; fee; from; to })) {
                let check_from = Utils.accountToText({ hash = from });
                let check_to = Utils.accountToText({ hash = to });
                let from2 = Utils.accountToText(Utils.principalToAccount(caller));
                let to2 = Utils.accountToText(Utils.principalToAccount(treasury_principal));
                if (Text.endsWith(check_from, #text from2) and Text.endsWith(check_to, #text to2)) {
                  try {
                    if (amount.e8s > fee.e8s) {
                      Vector.add(tempTransferQueueLocal, (#principal(caller), nat64ToNat(amount.e8s) - nat64ToNat(fee.e8s), identifier, genTxId()));
                    } else {
                      addFees(identifier, nat64ToNat(amount.e8s), false, "", nowVar);
                    };
                  } catch (ERR) {
                    Map.delete(BlocksDone, thash, identifier # ":" #Nat.toText(Block));
                    return false;
                  };
                  if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
                    Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
                  };
                  return true;
                };
              };
              case _ {
                Map.delete(BlocksDone, thash, identifier # ":" #Nat.toText(Block));
                return false;
              };
            };
          };
        };
        case (#ICRC12(transactions)) {
          for ({ transfer = ?{ to; fee; from; amount = howMuchReceived } } in transactions.vals()) {
            var fees : Nat = 0;
            switch (fee) {
              case null {};
              case (?fees2) { fees := (fees2) };
            };
            if (to.owner == treasury_principal and from.owner == caller) {
              if (nat64ToInt64(natToNat64(howMuchReceived)) > nat64ToInt64(natToNat64(fees))) {
                Vector.add(tempTransferQueueLocal, (#principal(caller), howMuchReceived -(fees), identifier, genTxId()));
              } else {
                addFees(identifier, howMuchReceived, false, "", nowVar);
              };
              if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
                Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
              };
              return true;
            };
          };
        };
        case (#ICRC3(result)) {
          for (block in result.blocks.vals()) {
            switch (block.block) {
              case (#Map(entries)) {
                var to : ?ICRC1.Account = null;
                var fee : ?Nat = null;
                var from : ?ICRC1.Account = null;
                var amount : ?Nat = null;

                for ((key, value) in entries.vals()) {
                  switch (key) {
                    case "to" {
                      switch (value) {
                        // RVVR-TACOX-22: Handling default accounts with null subaccount when only principal is provided
                        case (#Array(toArray)) {
                          if (toArray.size() >= 1) {
                            switch (toArray[0]) {
                              case (#Blob(owner)) {
                                to := ?{
                                  owner = Principal.fromBlob(owner);

                                  subaccount = if (toArray.size() > 1) {
                                    switch (toArray[1]) {
                                      case (#Blob(subaccount)) { ?subaccount };
                                      case _ { null };
                                    };
                                  } else {
                                    null // Default subaccount when only principal is provided
                                  };
                                };

                              };
                              case _ {};
                            };
                          };
                        };
                        case (#Blob(owner)) {
                          to := ?{
                            owner = Principal.fromBlob(owner);
                            subaccount = null;
                          };
                        };
                        case _ {};
                      };
                    };
                    case "fee" {
                      switch (value) {
                        case (#Nat(f)) { fee := ?f };
                        case (#Int(f)) { fee := ?Int.abs(f) };
                        case _ {};
                      };
                    };
                    case "from" {
                      switch (value) {
                        case (#Array(fromArray)) {
                          if (fromArray.size() == 1) {
                            switch (fromArray[0]) {
                              case (#Blob(owner)) {
                                from := ?{
                                  owner = Principal.fromBlob(owner);
                                  subaccount = null;
                                };
                              };
                              case _ {};
                            };
                          };
                        };
                        case _ {};
                      };
                    };
                    case "amt" {
                      switch (value) {
                        case (#Nat(amt)) { amount := ?amt };
                        case (#Int(amt)) { amount := ?Int.abs(amt) };
                        case _ {};
                      };
                    };
                    case _ {};
                  };
                };

                switch (to, fee, from, amount) {
                  case (?to, ?fee, ?from, ?howMuchReceived) {
                    var fees : Nat = fee;
                    if (to.owner == treasury_principal and from.owner == caller) {
                      if (howMuchReceived > fees) {
                        Vector.add(tempTransferQueueLocal, (#principal(caller), howMuchReceived - fees, identifier, genTxId()));
                      } else {
                        addFees(identifier, howMuchReceived, false, "", nowVar);
                      };
                      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
                        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
                      };
                      return true;
                    };
                  };
                  case _ {
                    Map.delete(BlocksDone, thash, identifier # ":" #Nat.toText(Block));
                    return false;
                  };
                };
              };
              case _ {
                Map.delete(BlocksDone, thash, identifier # ":" #Nat.toText(Block));
                return false;
              };
            };
          };
        };
      };
    } catch (err) {
      Map.delete(BlocksDone, thash, identifier # ":" #Nat.toText(Block));
    };

    Map.delete(BlocksDone, thash, identifier # ":" #Nat.toText(Block));
    return false;
  };

  // Here most of the orders start. This function is called when someone creates a position. It checks whether the asset that is offered has been received and then creates an entry in the registry. It returns the accesscode of the trade.
  // It also calls the orderPairing function, which pairs the new order with existing orders, to see whether it can be (partially) fulfilled already.
  // If the accesscode starts with "Public", it means everyone can view that trade.
  // Block= the block the position maker has sent the asset (token_init_identifier) in.
  // amount_sell= the amount of the asset the position maker wants in return.
  // amount_init= The amount of the asset the positionmaker has sent
  // token_sell_identifier= the canister address of the asset the position maker wants in return
  // token_init_identifier= the canister address of the asset the position maker offers for the token_sell_identifier
  // pub= Bool that tells the function whether the position is private (for OTC trades) or public (will it be included in the orderbooks?)]
  // excludeDAO = if pub==false, and the position is private, the maker has the option to whether the DAO can access the order or not when it trades.
  // OC= openchat name of the position maker. This is especially handy for OTC trades or public nonorderbook trades (in foreign pools), as people will be able to negotiate.
  // referrer= the principal of the referrer of the position maker. Only gets added as referrer if its the first position the calles makes.
  // allOrNothing= if true, the position finisher will only be able to trade with this position if the amount of the asset he offers fulfills the whole position.
  // strictlyOTC= if true, the position gets added to the OTC interface, which means it will not be included in the orderbooks.
  public shared ({ caller }) func addPosition(
    Block : Nat,
    amount_sell : Nat,
    amount_init : Nat,
    token_sell_identifier : Text,
    token_init_identifier : Text,
    pub : Bool,
    excludeDAO : Bool,
    OC : ?Text,
    referrer : Text,
    allOrNothing : Bool,
    strictlyOTC : Bool,
  ) : async ExTypes.OrderResult {
    if (isAllowed(caller) != 1) {
      return #Err(#NotAuthorized);
    };

    // made it > 150 incase a manual trader accedentily double pastes (was 70 at first)
    if (Text.size(referrer) > 150 or Text.size(token_sell_identifier) > 150 or Text.size(token_init_identifier) > 150) {
      dayBan := TrieSet.put(dayBan, caller, Principal.hash(caller), Principal.equal);
      return #Err(#Banned);
    };
    var OCname = switch (OC) {
      //Open chat names are between 3 and 16 characters
      case (?T) {
        if (Text.size(T) < 24 or Text.size(T) > 30) {
          if (Text.size(T) > 150) {
            dayBan := TrieSet.put(dayBan, caller, Principal.hash(caller), Principal.equal);
            return #Err(#Banned);
          } else { "" };
        } else { T };
      };
      case _ { "" };
    };

    var nowVar = Time.now();

    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    // Same-token reject: makes no economic sense and can drain pools via cyclic
    // AMM routes. Burn canister exempt (adminFlashArb routes back to start token).
    if (token_sell_identifier == token_init_identifier and not isFlashArbCaller(caller)) {
      if (Map.has(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block))) {
        return #Err(#InvalidInput("Block already processed"));
      };
      Map.set(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block), nowVar);
      let nowVar2 = nowVar;
      let tType = returnType(token_init_identifier);
      try {
        let blockData = await* getBlockData(token_init_identifier, Block, tType);
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token_init_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
      } catch (err) {
        Map.delete(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block));
      };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InvalidInput("Sell and buy token must be different"));
    };

    // Check if tokens are accepted. If not, refund the deposit (the user may have
    // sent tokens before the token was removed — don't leave them untracked).
    let tokenNotAccepted = (token_sell_identifier != "ryjl3-tyaaa-aaaaa-aaaba-cai" and containsToken(token_sell_identifier) == false)
      or (token_init_identifier != "ryjl3-tyaaa-aaaaa-aaaba-cai" and containsToken(token_init_identifier) == false);
    if (tokenNotAccepted) {
      if (Map.has(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block))) { return #Err(#InvalidInput("Block already processed")) };
      Map.set(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block), nowVar);
      let nowVar2 = nowVar;
      let tType = returnType(token_init_identifier);
      try {
        let blockData = await* getBlockData(token_init_identifier, Block, tType);
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token_init_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
      } catch (err) {
        Map.delete(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block));
      };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#TokenNotAccepted(token_init_identifier));
    };
    let user = Principal.toText(caller);

    if (((switch (Array.find<Text>(pausedTokens, func(t) { t == token_sell_identifier })) { case null { false }; case (?_) { true } })) or ((switch (Array.find<Text>(pausedTokens, func(t) { t == token_init_identifier })) { case null { false }; case (?_) { true } }))) {
      if (Map.has(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block))) { return #Err(#InvalidInput("Block already processed")) };
      Map.set(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block), nowVar);

      let nowVar2 = nowVar;
      let tType = returnType(token_init_identifier);

      try {

        let blockData = await* getBlockData(token_init_identifier, Block, tType);

        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token_init_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
      } catch (err) {
        Map.delete(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block));

      };
      // Transfering the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {

      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#TokenPaused("Init or sell token is paused at the moment OR order is public and one of the tokens is not a a base token"));
    };

    let nonPoolOrder = (pub and not isKnownPool(token_sell_identifier, token_init_identifier)) or strictlyOTC or allOrNothing;

    // check if amounts are not too low
    let amount_sell2 = if (amount_sell < 1) {
      1;
    } else { amount_sell };
    if (not returnMinimum(token_init_identifier, amount_init, false)) {
      if (Map.has(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block))) { return #Err(#InvalidInput("Block already processed")) };
      Map.set(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block), nowVar);

      let nowVar2 = nowVar;

      let tType = returnType(token_init_identifier);
      try {
        //Doing it this way so checkReceive does not have to be awaited, effectively eliminating pressure on the process queue
        let blockData = await* getBlockData(token_init_identifier, Block, tType);
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token_init_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
      } catch (err) {
        Map.delete(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block));
      };
      // Transfering the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {

      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InvalidInput("Amount too low"));
    };
    if (Map.has(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block))) {
      return #Err(#InvalidInput("Block already processed"));
    };
    Map.set(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block), nowVar);

    let nowVar2 = nowVar;
    trade_number += 1;
    var trade : TradePrivate = {
      Fee = ICPfee;
      amount_sell = amount_sell2;
      amount_init = amount_init;
      token_sell_identifier = token_sell_identifier;
      token_init_identifier = token_init_identifier;
      trade_done = 0;
      seller_paid = 0;
      init_paid = 1;
      trade_number = trade_number;
      SellerPrincipal = "0";
      initPrincipal = Principal.toText(caller);
      seller_paid2 = 0;
      init_paid2 = 0;
      RevokeFee = RevokeFeeNow;
      OCname = OCname;
      time = nowVar;
      filledInit = 0;
      filledSell = 0;
      allOrNothing = allOrNothing;
      strictlyOTC = strictlyOTC;
    };


    let tType = returnType(token_init_identifier);
    if (Vector.size(tempTransferQueue) > 0) {
      if FixStuckTXRunning {} else {
        FixStuckTXRunning := true;
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue), isInAllowedCanisters(caller)) } catch (err) { Debug.print(Error.message(err)); false })) {
          Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
        };
        FixStuckTXRunning := false;
      };
    };

    let blockData = try {
      await* getBlockData(token_init_identifier, Block, tType);
    } catch (err) {
      Map.delete(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block));

      #ICRC12([]);
    };
    nowVar := Time.now();
    if (blockData != #ICRC12([])) {
      //Check whether the referrer var is valid and whether the user does not have a referrer yet
      switch (Map.get(userReferrerLink, thash, user)) {
        case (?_) {
          // User already has a referrer link, do nothing

        };
        case (null) {
          // User doesn't have a referrer link, let's set it
          if (referrer == "") {
            // If no referrer provided, set to null
            Map.set(userReferrerLink, thash, user, null);
          } else {
            // Check if the referrer is a valid principal AND not the caller
            // themselves (self-referral would let the user siphon ReferralFees%
            // of their OWN fees back via claimFeesReferrer, stealing from DAO).
            let a = PrincipalExt.fromText(referrer);
            if (a == null or referrer == user) {
              Map.set(userReferrerLink, thash, user, null);
            } else {
              Map.set(userReferrerLink, thash, user, ?referrer);

            };
          };
        };
      };
      if ((
        (containsToken(token_init_identifier) == false) or (containsToken(token_sell_identifier) == false) or ((switch (Array.find<Text>(pausedTokens, func(t) { t == token_sell_identifier })) { case null { false }; case (?_) { true } })) or ((switch (Array.find<Text>(pausedTokens, func(t) { t == token_init_identifier })) { case null { false }; case (?_) { true } }))
      )) {
        Map.set(BlocksDone, thash, token_init_identifier # ":" #Nat.toText(Block), nowVar);
        Vector.clear(tempTransferQueueLocal);
        let nowVar2 = nowVar;

        let tType = returnType(token_init_identifier);
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(Block, caller, 0, token_init_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
        // Transfering the transactions that have to be made to the treasury,
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
          Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
        };
        return #Err(#TokenPaused("Asset paused during execution"));

      };
    };
    // revokeFees are already added in checkreceive, thats why we initiate the referrer loop already
    let (receiveBool, receiveTransfers) = if (blockData != #ICRC12([])) {
      checkReceive(Block, caller, amount_init, token_init_identifier, ICPfee, RevokeFeeNow, false, true, blockData, tType, nowVar2);
    } else { (false, []) };
    Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());
    if (not receiveBool) {
      // Transfering the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { Debug.print(Error.message(err)); false })) {

      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };

      return #Err(#InsufficientFunds("Deposit not received"));
    };

    var PrivateAC : Text = PrivateHash();
    if pub {
      PrivateAC := "Public" #PrivateAC;
    };

    if (excludeDAO and not pub) { PrivateAC := PrivateAC # "excl" };
    var plsbreak = 0;

    var feesToAdd = (token_init_identifier, 0);

    if (pub and not strictlyOTC and not allOrNothing) {

      // checking whether there are existing orders that can fulfill this one (partly)
      let thePairing = orderPairing(trade);
      let leftAmountInit = thePairing.0;
      Vector.addFromIter(tempTransferQueueLocal, thePairing.3.vals());
      let tfees = returnTfees(token_init_identifier);
      if (test) {
        Debug.print("DRIFT_TRACE addPosition: token=" # token_init_identifier
          # " amt=" # Nat.toText(amount_init)
          # " left=" # Nat.toText(leftAmountInit)
          # " wasAMM=" # debug_show(thePairing.4)
          # " pFee=" # Nat.toText(thePairing.1)
          # " poolFee=" # Nat.toText(thePairing.2)
          # " transfers=" # Nat.toText(thePairing.3.size())
          # " consumed=" # Nat.toText(thePairing.5));
      };
      if (leftAmountInit != amount_init and leftAmountInit != 0 and tfees < leftAmountInit) {
        if (amount_sell2 > 1) {

          let add = (((((amount_init - leftAmountInit) * ICPfee)) - (((((amount_init - leftAmountInit) * ICPfee) * 100000) / RevokeFeeNow) / 100000)) / 10000);
          if (add > 0) {
            addFees(token_init_identifier, add, false, user, nowVar);
          };

          // Record the instantly-filled portion
          var partialBuyAmount : Nat = 0;
          for (transaction in thePairing.3.vals()) {
            if (transaction.0 == #principal(caller) and transaction.2 == token_sell_identifier) {
              partialBuyAmount += transaction.1;
            };
          };
          if (partialBuyAmount > 0) {
            nextSwapId += 1;
            recordSwap(caller, {
              swapId = nextSwapId;
              tokenIn = token_init_identifier; tokenOut = token_sell_identifier;
              amountIn = amount_init - leftAmountInit; amountOut = partialBuyAmount;
              route = [token_init_identifier, token_sell_identifier];
              fee = thePairing.1 + thePairing.2;
              swapType = if (pub) { #direct } else { #otc };
              timestamp = nowVar;
            });
          };

          trade := {
            trade with
            amount_sell = (((leftAmountInit * 100000000000) / amount_init) * amount_sell2) / 100000000000;
            amount_init = leftAmountInit -tfees;
            seller_paid = 0;
            init_paid = 1;
            seller_paid2 = 0;
            init_paid2 = 0;
            time = nowVar;
            filledInit = amount_init -leftAmountInit;
            filledSell = amount_sell2 -((((leftAmountInit * 100000000000) / amount_init) * amount_sell2) / 100000000000);
          };
        } else {
          if (leftAmountInit > returnTfees(token_init_identifier)) {
            Vector.add(tempTransferQueueLocal, (#principal(caller), leftAmountInit, token_init_identifier, genTxId()));
          } else {
            addFees(token_init_identifier, leftAmountInit, false, "", nowVar);
          };
          plsbreak := 1;

        };
      } else if (leftAmountInit == 0) {

        let add = (((((amount_init) * ICPfee)) - (((((amount_init) * ICPfee) * 100000) / RevokeFeeNow) / 100000)) / 10000);
        let posInputTfees = if (thePairing.4) { tfees } else { 0 };
        if (add + posInputTfees > 0) {
          addFees(token_init_identifier, add + posInputTfees, false, user, nowVar);
        };
        // Record instant fill in swap history
        var toBeBoughtForHistory : Nat = 0;
        for (transaction in Vector.vals(tempTransferQueueLocal)) {
          if (transaction.0 == #principal(caller) and transaction.2 == token_sell_identifier) {
            toBeBoughtForHistory += transaction.1;
          };
        };
        nextSwapId += 1;
        recordSwap(caller, {
          swapId = nextSwapId;
          tokenIn = token_init_identifier; tokenOut = token_sell_identifier;
          amountIn = amount_init; amountOut = toBeBoughtForHistory;
          route = [token_init_identifier, token_sell_identifier];
          fee = thePairing.1 + thePairing.2;
          swapType = if (pub) { #direct } else { #otc };
          timestamp = nowVar;
        });

        if (thePairing.4) {
          var toBeBought = 0;
          for (transaction in Vector.vals(tempTransferQueueLocal)) {
            if (transaction.0 == #principal(caller) and transaction.2 == token_sell_identifier) {
              toBeBought += transaction.1;
            }; // transaction.1 should be the amount received
          };
          var pool : (Text, Text) = ("", "");
          label getPool for (p in Vector.vals(pool_canister)) {
            if ((token_init_identifier, token_sell_identifier) == p or (token_sell_identifier, token_init_identifier) == p) {
              pool := p;
              break getPool;
            };
          };
          var history_pool = switch (Map.get(pool_history, hashtt, pool)) {
            case null {
              RBTree.init<Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }]>();
            };
            case (?a) { a };
          };
          let histEntry = { amount_init = trade.amount_init; amount_sell = toBeBought; init_principal = trade.initPrincipal; sell_principal = "AMM"; accesscode = PrivateAC; token_init_identifier = trade.token_init_identifier; filledInit = trade.amount_init; filledSell = toBeBought; strictlyOTC = trade.strictlyOTC; allOrNothing = trade.allOrNothing };
          Map.set(pool_history, hashtt, pool, switch (RBTree.get(history_pool, compareTime, nowVar)) { case null { RBTree.put(history_pool, compareTime, nowVar, [histEntry]) }; case (?a) { let hVec = Vector.fromArray<{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }>(a); Vector.add(hVec, histEntry); RBTree.put(history_pool, compareTime, nowVar, Vector.toArray(hVec)) } });
        };

        plsbreak := 1;
      } else if (tfees >= leftAmountInit and leftAmountInit != amount_init and leftAmountInit != 0) {

        // For AMM-only swaps, leftAmountInit is a phantom from buyTfees adjustment.
        // Use full amount_init for fee calculation. Track input Tfees for AMM-only
        // fills (same as full fill path) since the order's Tfees deposit must be
        // accounted for after the order is consumed.
        let matchedForFee = if (thePairing.4) { amount_init } else { amount_init - leftAmountInit };
        let posInputTfees = if (thePairing.4) { tfees } else { 0 };
        let add = ((((matchedForFee * ICPfee)) - ((((matchedForFee * ICPfee) * 100000) / RevokeFeeNow) / 100000)) / 10000) + (if (thePairing.4) { posInputTfees } else { leftAmountInit });
        if (add > 0) {
          addFees(token_init_identifier, add, false, user, nowVar);
        };

        // Record as near-full fill (remainder was dust)
        var dustFillBuyAmount : Nat = 0;
        for (transaction in thePairing.3.vals()) {
          if (transaction.0 == #principal(caller) and transaction.2 == token_sell_identifier) {
            dustFillBuyAmount += transaction.1;
          };
        };
        if (dustFillBuyAmount > 0) {
          nextSwapId += 1;
          recordSwap(caller, {
            swapId = nextSwapId;
            tokenIn = token_init_identifier; tokenOut = token_sell_identifier;
            amountIn = amount_init - leftAmountInit; amountOut = dustFillBuyAmount;
            route = [token_init_identifier, token_sell_identifier];
            fee = thePairing.1 + thePairing.2;
            swapType = if (pub) { #direct } else { #otc };
            timestamp = nowVar;
          });
        };

        plsbreak := 1;
      };

      // Auto multi-hop: if direct pairing left significant unfilled amount, try routing through intermediate pools
      if (plsbreak == 0 and leftAmountInit > tfees * 3 and leftAmountInit > 10000) {
        let routes = findRoutes(token_init_identifier, token_sell_identifier, leftAmountInit);
        label routeSearch for (r in routes.vals()) {
          if (r.hops.size() <= 1) continue routeSearch; // skip direct (already tried above)

          // Check if AMM estimate meets user's price ratio (user wants at least amount_sell2 for amount_init)
          let requiredOutput = (leftAmountInit * amount_sell2) / amount_init;
          if (r.estimatedOut < requiredOutput) continue routeSearch;

          // Execute multi-hop for the remaining amount
          var hopAmount = leftAmountInit;
          var hopFailed = false;
          var lastSuccessfulHopOutput : Nat = 0;
          var lastSuccessfulHopToken : Text = "";

          for (hop in r.hops.vals()) {
            let syntheticTrade : TradePrivate = {
              Fee = ICPfee;
              amount_sell = 1;
              amount_init = hopAmount;
              token_sell_identifier = hop.tokenOut;
              token_init_identifier = hop.tokenIn;
              trade_done = 0;
              seller_paid = 0;
              init_paid = 1;
              seller_paid2 = 0;
              init_paid2 = 0;
              trade_number = 0;
              SellerPrincipal = "0";
              initPrincipal = Principal.toText(caller);
              RevokeFee = RevokeFeeNow;
              OCname = "";
              time = nowVar;
              filledInit = 0;
              filledSell = 0;
              allOrNothing = false;
              strictlyOTC = false;
            };
            let (_, pFee, _, transfers, _, _, _) = orderPairing(syntheticTrade);

            var thisHopOut : Nat = 0;
            for (tx in transfers.vals()) {
              if (tx.0 == #principal(caller) and tx.2 == hop.tokenOut) {
                thisHopOut += tx.1;
              } else {
                // Counterparty transfers — queue them
                Vector.add(tempTransferQueueLocal, tx);
              };
            };
            if (thisHopOut > 0) {
              lastSuccessfulHopOutput := thisHopOut;
              lastSuccessfulHopToken := hop.tokenOut;
            };
            hopAmount := thisHopOut;
            if (hopAmount == 0) { hopFailed := true };
          };

          if (not hopFailed and hopAmount >= requiredOutput) {
            // Multi-hop succeeded — send final output to user
            if (hopAmount > returnTfees(token_sell_identifier)) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), hopAmount, token_sell_identifier, genTxId()));
            };
            // Fee accounting for the portion filled by multi-hop
            let add = (((((leftAmountInit) * ICPfee)) - (((((leftAmountInit) * ICPfee) * 100000) / RevokeFeeNow) / 100000)) / 10000);
            if (add > 0) {
              addFees(token_init_identifier, add, false, user, nowVar);
            };
            plsbreak := 1;
          } else if (hopFailed and lastSuccessfulHopOutput > 0) {
            // Auto-multihop failed AFTER hop 0 succeeded.
            // - Hop 0 consumed the input tokens via AMM (state already modified)
            // - Intermediate tokens are in the wallet (AMM released them, no transfer)
            // - Send the intermediate output to the USER (fair: they deposited input,
            //   hop 0 converted it — give them what was produced by the successful hop).
            // - This also fixes accounting: wallet sends intermediate out, matching AMM decrease.
            let intermediateTransferFee = returnTfees(lastSuccessfulHopToken);
            if (lastSuccessfulHopOutput > intermediateTransferFee) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), lastSuccessfulHopOutput - intermediateTransferFee, lastSuccessfulHopToken, genTxId()));
            };
            // Fee accounting for the consumed input
            let add = (((((leftAmountInit) * ICPfee)) - (((((leftAmountInit) * ICPfee) * 100000) / RevokeFeeNow) / 100000)) / 10000);
            if (add > 0) {
              addFees(token_init_identifier, add, false, user, nowVar);
            };
            plsbreak := 1; // Don't place order — input was consumed by AMM
          };
          break routeSearch; // only try best route
        };
      };
    };
    if (plsbreak == 0) {

      if (not excludeDAO) {
        replaceLiqMap(false, false, token_init_identifier, token_sell_identifier, PrivateAC, (trade.amount_init, trade.amount_sell, ICPfee, RevokeFeeNow, Principal.toText(caller), trade.OCname, trade.time, trade.token_init_identifier, trade.token_sell_identifier, trade.strictlyOTC, trade.allOrNothing), #Zero, null, null);
      };

      addTrade(PrivateAC, Principal.toText(caller), trade, (token_init_identifier, token_sell_identifier));


      doInfoBeforeStep2();
      let poolKey = getPool(token_init_identifier, token_sell_identifier);
      ignore updatePriceDayBefore(poolKey, nowVar);
      // Transfering the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { Debug.print("Check"); Debug.print(Error.message(err)); false })) {

      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      label a if nonPoolOrder {
        let pair1 = (token_init_identifier, token_sell_identifier);
        let pair2 = (token_sell_identifier, token_init_identifier);

        let existsInForeignPools = (Map.has(foreignPools, hashtt, pair1) or Map.has(foreignPools, hashtt, pair2));

        if (not existsInForeignPools) {
          Map.set(foreignPools, hashtt, getPool(token_init_identifier, token_sell_identifier), 1);
          break a;
        };

        let pairToAdd = if existsInForeignPools {
          if (Map.has(foreignPools, hashtt, pair1)) pair1 else pair2;
        } else { getPool(token_init_identifier, token_sell_identifier) };
        Map.set(foreignPools, hashtt, pairToAdd, switch (Map.get(foreignPools, hashtt, pairToAdd)) { case (?a) { a +1 }; case null { 1 } });
      };
      label a if (not pub) {
        let pair1 = (token_init_identifier, token_sell_identifier);
        let pair2 = (token_sell_identifier, token_init_identifier);

        let existsInForeignPools = (Map.has(foreignPrivatePools, hashtt, pair1) or Map.has(foreignPrivatePools, hashtt, pair2));

        let pairToAdd = if existsInForeignPools {
          if (Map.has(foreignPrivatePools, hashtt, pair1)) pair1 else pair2;
        } else { getPool(token_init_identifier, token_sell_identifier) };
        Map.set(foreignPrivatePools, hashtt, pairToAdd, switch (Map.get(foreignPrivatePools, hashtt, pairToAdd)) { case (?a) { a +1 }; case null { 1 } });
      };
      return #Ok({
        accessCode = PrivateAC;
        tokenIn = token_init_identifier;
        tokenOut = token_sell_identifier;
        amountIn = amount_init;
        filled = 0;
        remaining = amount_init;
        buyAmountReceived = 0;
        swapId = null;
        isPublic = pub;
      });
    } else {

      doInfoBeforeStep2();
      let poolKey = getPool(token_init_identifier, token_sell_identifier);
      ignore updatePriceDayBefore(poolKey, nowVar);
      // Transfering the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { Debug.print("Check"); Debug.print(Error.message(err)); false })) {

      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Ok({
        accessCode = "";
        tokenIn = token_init_identifier;
        tokenOut = token_sell_identifier;
        amountIn = amount_init;
        filled = amount_init;
        remaining = 0;
        buyAmountReceived = 0;
        swapId = null;
        isPublic = pub;
      });
    };
  };

  // Multi-hop swap: executes a pre-computed route (from getExpectedMultiHopAmount) to swap tokenIn→tokenOut via intermediate pools.
  // Each hop uses the full hybrid AMM+orderbook matching engine (orderPairing).
  // route: array of SwapHop from getExpectedMultiHopAmount query
  // minAmountOut: slippage protection — reverts if final output is less
  // Block: the block number where the user sent tokenIn to the treasury
  public shared ({ caller }) func swapMultiHop(
    tokenIn : Text,
    tokenOut : Text,
    amountIn : Nat,
    route : [SwapHop],
    minAmountOut : Nat,
    Block : Nat,
  ) : async ExTypes.SwapResult {
    // 1. Auth & validation
    if (isAllowed(caller) != 1) return #Err(#NotAuthorized);

    // Validate route structure (cheap checks before any block processing)
    var validationError : Text = "";
    if (tokenIn == tokenOut and not isFlashArbCaller(caller)) {
      validationError := "Same token (cyclic route not allowed)";
    };
    if (validationError != "") { /* same-token already rejected */ }
    else if (route.size() < 1 or route.size() > 3) { validationError := "Invalid route: 1-3 hops required" }
    else if (route[0].tokenIn != tokenIn) { validationError := "Route mismatch: first hop tokenIn != tokenIn" }
    else if (route[route.size() - 1].tokenOut != tokenOut) { validationError := "Route mismatch: last hop tokenOut != tokenOut" }
    else {
      var i = 0;
      while (i < route.size() - 1) {
        if (route[i].tokenOut != route[i + 1].tokenIn) { validationError := "Route broken at hop " # Nat.toText(i) };
        i += 1;
      };
      if (validationError == "") {
        for (hop in route.vals()) {
          if (not containsToken(hop.tokenIn) or not containsToken(hop.tokenOut)) { validationError := "Token not accepted" };
          if ((switch (Array.find<Text>(pausedTokens, func(t) { t == hop.tokenIn })) { case null { false }; case (?_) { true } }) or
              (switch (Array.find<Text>(pausedTokens, func(t) { t == hop.tokenOut })) { case null { false }; case (?_) { true } })) {
            validationError := "A token in the route is paused";
          };
        };
      };
      if (validationError == "") {
        for (hop in route.vals()) {
          if (not isKnownPool(hop.tokenIn, hop.tokenOut)) { validationError := "No pool exists for hop " # hop.tokenIn # " -> " # hop.tokenOut };
        };
      };
      if (validationError == "") {
        if (not returnMinimum(tokenIn, amountIn, false)) { validationError := "Amount too low" };
      };
    };

    // If validation failed, try to process the block and refund the deposit
    if (validationError != "") {
      let tType = returnType(tokenIn);
      let tempRefund = Vector.new<(TransferRecipient, Nat, Text, Text)>();
      try {
        if (not Map.has(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block))) {
          Map.set(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block), Time.now());
          let blockData = await* getBlockData(tokenIn, Block, tType);
          Vector.addFromIter(tempRefund, (checkReceive(Block, caller, 0, tokenIn, ICPfee, RevokeFeeNow, true, true, blockData, tType, Time.now())).1.vals());
        };
      } catch (_) {
        // getBlockData failed — delete BlocksDone so user can retry or recover
        Map.delete(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block));
      };
      if (Vector.size(tempRefund) > 0) {
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempRefund), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
          Vector.addFromIter(tempTransferQueue, Vector.vals(tempRefund));
        };
      };
      return #Err(#InvalidInput(validationError));
    };

    var nowVar = Time.now();
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    let user = Principal.toText(caller);

    // 2. Block validation & fund receipt (same pattern as addPosition)
    if (Map.has(BlocksDone, thash, tokenIn # ":" #Nat.toText(Block))) { return #Err(#InvalidInput("Block already processed")) };
    Map.set(BlocksDone, thash, tokenIn # ":" #Nat.toText(Block), nowVar);
    let nowVar2 = nowVar;
    let tType = returnType(tokenIn);

    // Flush stuck transfers if any
    if (Vector.size(tempTransferQueue) > 0) {
      if FixStuckTXRunning {} else {
        FixStuckTXRunning := true;
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue), isInAllowedCanisters(caller)) } catch (err) { Debug.print(Error.message(err)); false })) {
          Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
        };
        FixStuckTXRunning := false;
      };
    };

    let blockData = try {
      await* getBlockData(tokenIn, Block, tType);
    } catch (err) {
      Map.delete(BlocksDone, thash, tokenIn # ":" #Nat.toText(Block));
      #ICRC12([]);
    };
    nowVar := Time.now();

    // Verify token acceptance again after await
    if (blockData == #ICRC12([])) {
      Map.delete(BlocksDone, thash, tokenIn # ":" #Nat.toText(Block));
      return #Err(#SystemError("Failed to get block data"));
    };

    let (receiveBool, receiveTransfers) = checkReceive(Block, caller, amountIn, tokenIn, ICPfee, RevokeFeeNow, false, true, blockData, tType, nowVar2);
    Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());
    if (not receiveBool) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { Debug.print(Error.message(err)); false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InsufficientFunds("Funds not received"));
    };

    // 3. Pre-check: simulate all hops to estimate output. Reject BEFORE state modification
    // if the estimated output is clearly below minAmountOut. This is a safety net — simulateSwap
    // only simulates AMM (not orderbook), so it may underestimate. False positives (rejecting
    // a swap that would succeed) are acceptable; false negatives (executing a swap that clearly
    // fails slippage) are NOT — the user would lose their input tokens.
    var estimatedOut = amountIn;
    for (hop in route.vals()) {
      let pk = getPool(hop.tokenIn, hop.tokenOut);
      switch (Map.get(AMMpools, hashtt, pk)) {
        case (?pool) {
          let v3 = Map.get(poolV3Data, hashtt, pk);
          let (out, _, _) = simulateSwap(pool, v3, hop.tokenIn, estimatedOut, ICPfee);
          estimatedOut := out;
        };
        case null { estimatedOut := 0 };
      };
    };
    if (estimatedOut < minAmountOut) {
      // Refund — no state was modified yet by orderPairing.
      let tradingFeePortion = (amountIn * ICPfee) / 10000;
      let revokeFeePortion = (amountIn * ICPfee) / (10000 * RevokeFeeNow);
      let untrackedFees = tradingFeePortion - revokeFeePortion;
      if (untrackedFees > 0) {
        addFees(tokenIn, untrackedFees, false, user, nowVar);
      };
      Vector.add(tempTransferQueueLocal, (#principal(caller), amountIn, tokenIn, genTxId()));
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#SlippageExceeded({ expected = minAmountOut; got = estimatedOut }));
    };

    // 4. Execute hops via orderPairing (modifies state — real execution)
    var currentAmount = amountIn;
    var firstHopRemaining : Nat = 0;
    var firstHopPoolFee : Nat = 0;
    var firstHopProtocolFee : Nat = 0;
    var firstHopHadOrderbookMatch = false;
    var lastHopWasAMMOnly = false;

    for (hopIndex in Iter.range(0, route.size() - 1)) {
      let hop = route[hopIndex];
      let isLastHop : Bool = hopIndex + 1 == route.size();

      let syntheticTrade : TradePrivate = {
        // Charge trading fee on ALL hops so LP providers earn fees on
        // intermediate pools (e.g. ICP/TACO pool in a DKP→ICP→TACO route).
        // Hop 0 fee is covered by the user's deposit overpayment.
        // Hops 1+ fee is covered by collecting protocol fees from
        // the intermediate token via addFees below.
        Fee = ICPfee;
        amount_sell = 1;
        amount_init = currentAmount;
        token_sell_identifier = hop.tokenOut;
        token_init_identifier = hop.tokenIn;
        trade_done = 0;
        seller_paid = 0;
        init_paid = 1;
        seller_paid2 = 0;
        init_paid2 = 0;
        trade_number = 0;
        SellerPrincipal = "0";
        initPrincipal = user;
        RevokeFee = RevokeFeeNow;
        OCname = "";
        time = nowVar;
        filledInit = 0;
        filledSell = 0;
        allOrNothing = false;
        strictlyOTC = false;
      };

      let (remaining, protocolFee, poolFee, transfers, wasAMMOnly, consumedOrders, _) = orderPairing(syntheticTrade);
      lastHopWasAMMOnly := wasAMMOnly;
      if (hopIndex == 0) {
        firstHopRemaining := remaining;
        firstHopPoolFee := poolFee;
        firstHopProtocolFee := protocolFee;
      };

      // For hops 1+, V3 already tracks both pool and protocol fees internally
      // via totalFeesCollected. No additional fee collection needed for
      // intermediate tokens since no extra deposit backs them.

      var hopOutput : Nat = 0;
      for (tx in transfers.vals()) {
        if (tx.0 == #principal(caller) and tx.2 == hop.tokenOut) {
          hopOutput += tx.1;
          if (isLastHop) {
            // Last hop: transfer final output to caller
            Vector.add(tempTransferQueueLocal, tx);
          };
          // Intermediate hop: don't transfer — tokens stay for next hop
        } else {
          // Counterparty payments — always queue
          Vector.add(tempTransferQueueLocal, tx);
          // Track if hop 0 matched against orderbook (counterparty receives input token)
          if (hopIndex == 0 and tx.2 == tokenIn) {
            firstHopHadOrderbookMatch := true;
          };
        };
      };

      // Handle unfilled portion on first hop — only refund genuine partial fills
      if (hopIndex == 0 and remaining > returnTfees(hop.tokenIn) * 3) {
        Vector.add(tempTransferQueueLocal, (#principal(caller), remaining, hop.tokenIn, genTxId()));
      };

      let prevCurrentAmount = currentAmount; // Save before overwrite for error path
      currentAmount := hopOutput;
      if (currentAmount == 0) {
        // No output — collect hop 0 fees. User deposited amount * (1 + Fee%) upfront;
        // the non-revoke portion (calculateFee) is user's fee to DAO. Additionally, any
        // AMM protocol fee deducted during partial hop-0 execution (firstHopProtocolFee)
        // also goes to DAO. The 70% LP portion stays in v3 residual (claimable by LPs).
        // Sync v3 claim by protocolFee only, so residual retains exactly the LP portion.
        let tradingFee = calculateFee(amountIn, ICPfee, RevokeFeeNow) + firstHopProtocolFee;
        if (tradingFee > 0) { addFees(tokenIn, tradingFee, false, user, nowVar) };
        if (firstHopProtocolFee > 0) {
          claimProtocolFeeInV3(tokenIn, route[0].tokenOut, firstHopProtocolFee);
        };

        // For hop 1+ failures: the intermediate tokens from previous hops are in the
        // wallet (AMM reserves decreased but tokens weren't transferred out).
        // Send them to the USER (fair: their input was converted by hop 0, give them
        // the result). This also fixes accounting: wallet sends intermediate out,
        // matching the AMM reserve decrease.
        if (hopIndex > 0 and prevCurrentAmount > 0) {
          let intermediateToken = route[hopIndex].tokenIn;
          let intermediateTransferFee = returnTfees(intermediateToken);
          if (prevCurrentAmount > intermediateTransferFee) {
            Vector.add(tempTransferQueueLocal, (#principal(caller), prevCurrentAmount - intermediateTransferFee, intermediateToken, genTxId()));
          } else {
            // Dust too small to transfer — track in feescollectedDAO so nothing is lost
            let cur = switch (Map.get(feescollectedDAO, thash, intermediateToken)) { case (?v) v; case null 0 };
            Map.set(feescollectedDAO, thash, intermediateToken, cur + prevCurrentAmount);
          };
        };

        // No output from this hop, stop
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
          Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
        };
        return #Err(#RouteFailed({ hop = hopIndex; reason = "No output" }));
      };

      // For intermediate hops: orderPairing deducted one transfer fee (sellTfees)
      // from the payout assuming a real transfer to the user. But intermediate hops
      // don't actually transfer — the tokens stay for the next hop. Add back the
      // unused transfer fee ONLY when the hop was AMM-only (where sellTfees was
      // deducted). When orderbook orders matched, the transfer amount already
      // includes extraFees and no sellTfees was deducted, so adding back would
      // inflate the amount.
      if (not isLastHop and wasAMMOnly) {
        currentAmount += returnTfees(hop.tokenOut);
      };
      // For intermediate hops with orderbook matches: the order tracking decreased
      // by the full matched amount, but the pool received sellTfees less (from the
      // transfer fee deduction in orderPairing). Track the gap in feescollectedDAO.
      // For intermediate hops with orderbook matches: a sellTfees gap exists between
      // order tracking and pool tracking. Track it in feescollectedDAO.
      // Also record which orders' counterparty tokens were compensated, so
      // revokeTrade can deduct if the order is later canceled.
      if (not isLastHop and not wasAMMOnly) {
        let hopTfees = returnTfees(hop.tokenOut);
        addFees(hop.tokenOut, hopTfees, false, "", nowVar);
      };
    };

    // 5. Fee collection — before slippage check since hops already executed and modified state.
    // Use full amountIn: the "remaining" from orderPairing is phantom (totalbuyTfees accounting)
    // for AMM-only swaps (~10K). The AMM consumed the full amount. For real partial fills,
    // the remaining is refunded to the user and the fee on that portion was collected at checkReceive.
    // NOTE: This only collects hop 0 fees. Hops 1+ fees are collected inside the loop above.
    let firstHopMatched : Nat = amountIn;
    if (firstHopMatched > 0) {
      // tradingFee = user's upfront non-revoke (calculateFee) + AMM protocol 30% (firstHopProtocolFee).
      // LP 70% stays in v3 residual (claimable). Sync v3 claim by protocolFee only.
      let tradingFee = calculateFee(firstHopMatched, ICPfee, RevokeFeeNow) + firstHopProtocolFee;
      let inputTfees = if (firstHopHadOrderbookMatch) { 0 } else { returnTfees(tokenIn) };
      let feeToAdd = tradingFee + inputTfees;
      addFees(tokenIn, feeToAdd, false, user, nowVar);
      if (firstHopProtocolFee > 0) {
        claimProtocolFeeInV3(tokenIn, route[0].tokenOut, firstHopProtocolFee);
      };
    };

    // 6. Slippage check on actual result
    if (currentAmount < minAmountOut) {
      // Execution already modified state (pools/orderbook).
      // We can't roll back, so we send whatever was obtained to the user.
      // The pre-check above should prevent this in most cases.
      // Return error message but still send the transfers.
      let routeVecSlip = Vector.new<Text>();
      Vector.add(routeVecSlip, tokenIn);
      for (hop in route.vals()) { Vector.add(routeVecSlip, hop.tokenOut) };
      nextSwapId += 1;
      recordSwap(caller, {
        swapId = nextSwapId; tokenIn; tokenOut;
        amountIn; amountOut = currentAmount;
        route = Vector.toArray(routeVecSlip);
        fee = calculateFee(amountIn, ICPfee, RevokeFeeNow);
        swapType = #multihop;
        timestamp = Time.now();
      });
      doInfoBeforeStep2();
      // Consolidate transfers before sending
      let slipConsolidatedMap = Map.new<Text, (TransferRecipient, Nat, Text, Text)>();
      for (tx in Vector.vals(tempTransferQueueLocal)) {
        let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
        let key = rcpt # ":" # tx.2;
        switch (Map.get(slipConsolidatedMap, thash, key)) {
          case (?existing) { Map.set(slipConsolidatedMap, thash, key, (tx.0, existing.1 + tx.1, tx.2, tx.3)) };
          case null { Map.set(slipConsolidatedMap, thash, key, tx) };
        };
      };
      let slipConsolidatedVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();
      for ((_, tx) in Map.entries(slipConsolidatedMap)) { Vector.add(slipConsolidatedVec, tx) };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(slipConsolidatedVec), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(slipConsolidatedVec));
      };
      return #Err(#SlippageExceeded({ expected = minAmountOut; got = currentAmount }));
    };

    // 7. Record swap history
    let routeVec = Vector.new<Text>();
    Vector.add(routeVec, tokenIn);
    for (hop in route.vals()) { Vector.add(routeVec, hop.tokenOut) };
    nextSwapId += 1;
    recordSwap(caller, {
      swapId = nextSwapId; tokenIn; tokenOut;
      amountIn; amountOut = currentAmount;
      route = Vector.toArray(routeVec);
      fee = calculateFee(amountIn, ICPfee, RevokeFeeNow);
      swapType = #multihop;
      timestamp = Time.now();
    });

    // 8. Update exchange info
    doInfoBeforeStep2();

    // 9. Consolidate transfers (combine same recipient+token to save transfer fees)
    let consolidatedMap = Map.new<Text, (TransferRecipient, Nat, Text, Text)>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(consolidatedMap, thash, key)) {
        case (?existing) { Map.set(consolidatedMap, thash, key, (tx.0, existing.1 + tx.1, tx.2, tx.3)) };
        case null { Map.set(consolidatedMap, thash, key, tx) };
      };
    };
    let consolidatedVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    for ((_, tx) in Map.entries(consolidatedMap)) { Vector.add(consolidatedVec, tx) };

    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(consolidatedVec), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(consolidatedVec));
    };

    #Ok({
      amountIn = amountIn;
      amountOut = currentAmount;
      tokenIn = tokenIn;
      tokenOut = tokenOut;
      route = Vector.toArray(routeVec);
      fee = calculateFee(amountIn, ICPfee, RevokeFeeNow);
      swapId = nextSwapId;
      hops = route.size();
      firstHopOrderbookMatch = firstHopHadOrderbookMatch;
      lastHopAMMOnly = lastHopWasAMMOnly;
    });
  };

  // ═══════════════════════════════════════════════════════════════
  // SPLIT-ROUTE SWAP — one deposit split across up to 3 routes
  // ═══════════════════════════════════════════════════════════════
  // Safety invariants:
  //   1. No double entry: fees collected exactly once per leg, no overlap
  //   2. No entry without recovery: every state change has a recovery path
  //   3. No negative drift: all tokens accounted for, rounding favors system
  // Atomicity: ZERO awaits between simulation and execution —
  //   Motoko actor model guarantees no interleaving, pool state frozen.
  public shared ({ caller }) func swapSplitRoutes(
    tokenIn : Text,
    tokenOut : Text,
    splits : [SplitLeg],
    minAmountOut : Nat,
    Block : Nat,
  ) : async ExTypes.SwapResult {
    // ── 1. Auth & structural validation (no state modification) ──
    if (isAllowed(caller) != 1) return #Err(#NotAuthorized);

    var totalAmountIn : Nat = 0;
    var validationError : Text = "";
    if (tokenIn == tokenOut and not isFlashArbCaller(caller)) {
      validationError := "Same token (cyclic route not allowed)";
    };
    if (validationError == "" and (splits.size() < 1 or splits.size() > 3)) { validationError := "1-3 splits required" };

    for (leg in splits.vals()) {
      if (validationError == "") {
        if (leg.amountIn == 0) { validationError := "Leg amount must be > 0" };
        if (leg.route.size() < 1 or leg.route.size() > 3) { validationError := "Each leg: 1-3 hops required" };
        if (validationError == "") {
          if (leg.route[0].tokenIn != tokenIn) { validationError := "Leg route must start with tokenIn" };
          if (leg.route[leg.route.size() - 1].tokenOut != tokenOut) { validationError := "Leg route must end with tokenOut" };
          var i = 0;
          while (i + 1 < leg.route.size()) {
            if (leg.route[i].tokenOut != leg.route[i + 1].tokenIn) { validationError := "Route broken at hop " # Nat.toText(i) };
            i += 1;
          };
          for (hop in leg.route.vals()) {
            if (not containsToken(hop.tokenIn) or not containsToken(hop.tokenOut)) { validationError := "Token not accepted" };
            if ((switch (Array.find<Text>(pausedTokens, func(t) { t == hop.tokenIn })) { case null { false }; case (?_) { true } }) or
                (switch (Array.find<Text>(pausedTokens, func(t) { t == hop.tokenOut })) { case null { false }; case (?_) { true } })) {
              validationError := "A token in the route is paused";
            };
            if (not isKnownPool(hop.tokenIn, hop.tokenOut)) { validationError := "No pool for hop " # hop.tokenIn # " -> " # hop.tokenOut };
          };
        };
      };
      totalAmountIn += leg.amountIn;
    };

    if (validationError == "" and not returnMinimum(tokenIn, totalAmountIn, false)) { validationError := "Total amount too low" };

    // ── Validation failed → try to process block and refund deposit ──
    if (validationError != "") {
      let tType = returnType(tokenIn);
      let tempRefund = Vector.new<(TransferRecipient, Nat, Text, Text)>();
      try {
        if (not Map.has(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block))) {
          Map.set(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block), Time.now());
          let blockData = await* getBlockData(tokenIn, Block, tType);
          Vector.addFromIter(tempRefund, (checkReceive(Block, caller, 0, tokenIn, ICPfee, RevokeFeeNow, true, true, blockData, tType, Time.now())).1.vals());
        };
      } catch (_) {
        Map.delete(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block));
      };
      if (Vector.size(tempRefund) > 0) {
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempRefund), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
          Vector.addFromIter(tempTransferQueue, Vector.vals(tempRefund));
        };
      };
      return #Err(#InvalidInput(validationError));
    };

    // ── 2. Block validation & fund receipt ──
    var nowVar = Time.now();
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    let user = Principal.toText(caller);

    if (Map.has(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block))) { return #Err(#InvalidInput("Block already processed")) };
    Map.set(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block), nowVar);
    let nowVar2 = nowVar;
    let tType = returnType(tokenIn);

    // Flush stuck transfers if any
    if (Vector.size(tempTransferQueue) > 0) {
      if FixStuckTXRunning {} else {
        FixStuckTXRunning := true;
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue), isInAllowedCanisters(caller)) } catch (err) { Debug.print(Error.message(err)); false })) {
          Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
        };
        FixStuckTXRunning := false;
      };
    };

    let blockData = try {
      await* getBlockData(tokenIn, Block, tType);
    } catch (err) {
      Map.delete(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block));
      #ICRC12([]);
    };
    nowVar := Time.now();

    if (blockData == #ICRC12([])) {
      Map.delete(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block));
      return #Err(#SystemError("Failed to get block data"));
    };

    // checkReceive with TOTAL amount — single deposit covers all legs
    // Revoke fee collected here: (totalAmountIn * ICPfee) / (10000 * RevokeFeeNow)
    let (receiveBool, receiveTransfers) = checkReceive(Block, caller, totalAmountIn, tokenIn, ICPfee, RevokeFeeNow, false, true, blockData, tType, nowVar2);
    Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());
    if (not receiveBool) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { Debug.print(Error.message(err)); false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InsufficientFunds("Funds not received"));
    };

    // ── 3. Execute all legs sequentially (state IS modified — NO await until transfers) ──
    // Between simulation check above and execution below: ZERO awaits.
    // Motoko actor model guarantees no other message interleaves.
    // Pool state is identical to simulation → execution output ≥ simulated.
    var totalOutput : Nat = 0;
    // Track whether ANY leg's first hop had an orderbook match. If so, the
    // single inputTfees buffer (one Tfees deposited with the transaction) was
    // consumed by a counterparty transfer. Only book the buffer to feescollectedDAO
    // if NO leg had an orderbook match (matches swapMultiHop:7588 semantics).
    var anyLegHadOrderbookMatch = false;

    for (legIndex in Iter.range(0, splits.size() - 1)) {
      let leg = splits[legIndex];
      var currentAmount = leg.amountIn;
      var legFirstHopRemaining : Nat = 0;
      var legFirstHopPoolFee : Nat = 0;
      var legFirstHopProtocolFee : Nat = 0;
      var legFirstHopHadOrderbookMatch = false;

      // Execute hops — identical logic to swapMultiHop lines 6197-6288
      label hopExec for (hopIndex in Iter.range(0, leg.route.size() - 1)) {
        let hop = leg.route[hopIndex];
        let isLastHop : Bool = hopIndex + 1 == leg.route.size();

        let syntheticTrade : TradePrivate = {
          Fee = ICPfee;
          amount_sell = 1;
          amount_init = currentAmount;
          token_sell_identifier = hop.tokenOut;
          token_init_identifier = hop.tokenIn;
          trade_done = 0;
          seller_paid = 0;
          init_paid = 1;
          seller_paid2 = 0;
          init_paid2 = 0;
          trade_number = 0;
          SellerPrincipal = "0";
          initPrincipal = user;
          RevokeFee = RevokeFeeNow;
          OCname = "";
          time = nowVar;
          filledInit = 0;
          filledSell = 0;
          allOrNothing = false;
          strictlyOTC = false;
        };

        let (remaining, legProtocolFee, poolFee, transfers, wasAMMOnly, consumedOrders, _) = orderPairing(syntheticTrade);

        if (hopIndex == 0) {
          legFirstHopRemaining := remaining;
          legFirstHopPoolFee := poolFee;
          legFirstHopProtocolFee := legProtocolFee;
        };

        // For intermediate hops, V3 already tracks both pool and protocol fees
        // internally via totalFeesCollected. No additional fee collection needed.

        // Transfer routing (same as swapMultiHop lines 6245-6267)
        var hopOutput : Nat = 0;
        for (tx in transfers.vals()) {
          if (tx.0 == #principal(caller) and tx.2 == hop.tokenOut) {
            hopOutput += tx.1;
            if (isLastHop) {
              // Last hop: transfer final output to caller
              Vector.add(tempTransferQueueLocal, tx);
            };
            // Intermediate hop: tokens stay for next hop
          } else {
            // Counterparty payments — always queue
            Vector.add(tempTransferQueueLocal, tx);
            // Track if hop 0 matched against orderbook
            if (hopIndex == 0 and tx.2 == tokenIn) {
              legFirstHopHadOrderbookMatch := true;
            };
          };
        };

        // Handle unfilled portion on first hop
        // Small phantom remaining (≈ buyTfees from totalbuyTfees accounting) is already
        // in AMM reserves — don't track it separately. Only refund genuine partial fills.
        if (hopIndex == 0 and remaining > returnTfees(hop.tokenIn) * 3) {
          Vector.add(tempTransferQueueLocal, (#principal(caller), remaining, hop.tokenIn, genTxId()));
          // DRIFT FIX: this refund transfer consumes the user's inputTfees buffer
          // (ledger fee deducted on outflow). Flag so we don't double-book it later.
          legFirstHopHadOrderbookMatch := true;
          recordOpDrift("splitRoutes_firstHopRefund", hop.tokenIn, -remaining);
        };

        let prevCurrentAmount = currentAmount;
        currentAmount := hopOutput;
        if (currentAmount == 0) {
          // Hop failed. If hopIndex > 0, refund intermediate tokens to user
          // (same pattern as swapMultiHop: user's input was converted by prior hops,
          // give them the intermediate result).
          if (hopIndex > 0 and prevCurrentAmount > 0) {
            let intermediateToken = hop.tokenIn;
            let itf = returnTfees(intermediateToken);
            if (prevCurrentAmount > itf) {
              Vector.add(tempTransferQueueLocal, (#principal(caller), prevCurrentAmount - itf, intermediateToken, genTxId()));
              recordOpDrift("splitRoutes_intermediateRefund", intermediateToken, -prevCurrentAmount);
            } else {
              // Dust too small to transfer — track in feescollectedDAO so nothing is lost
              let cur = switch (Map.get(feescollectedDAO, thash, intermediateToken)) { case (?v) v; case null 0 };
              Map.set(feescollectedDAO, thash, intermediateToken, cur + prevCurrentAmount);
              recordOpDrift("splitRoutes_intermediateDust", intermediateToken, 0);
            };
          };
          // DRIFT FIX: book the leg's trading fee on failure (mirrors swapMultiHop:7530-7531).
          // Without this, the user's deposited fee for the partial route is unaccounted → positive drift.
          // Use direct Map update to avoid addFees' -1 sat loss (would cause negative drift).
          // Additive: calculateFee (user upfront) + legFirstHopProtocolFee (AMM protocol).
          // Sync v3 claim by protocolFee only. LP 70% stays in v3 residual.
          let failedLegTradingFee = calculateFee(leg.amountIn, ICPfee, RevokeFeeNow) + legFirstHopProtocolFee;
          if (failedLegTradingFee > 0) {
            let curFee = switch (Map.get(feescollectedDAO, thash, tokenIn)) { case (?v) v; case null 0 };
            Map.set(feescollectedDAO, thash, tokenIn, curFee + failedLegTradingFee);
            if (legFirstHopProtocolFee > 0) {
              claimProtocolFeeInV3(tokenIn, leg.route[0].tokenOut, legFirstHopProtocolFee);
            };
            recordOpDrift("splitRoutes_failedLegFee", tokenIn, 0);
          };
          break hopExec;
        };

        // Restore transfer fee for intermediate AMM-only hops
        // (orderPairing deducted sellTfees assuming real transfer, but intermediate
        // hops don't actually transfer — add it back for AMM-only)
        if (not isLastHop and wasAMMOnly) {
          currentAmount += returnTfees(hop.tokenOut);
          recordOpDrift("splitRoutes_ammOnlyRefund", hop.tokenOut, 0);
        };
        if (not isLastHop and not wasAMMOnly) {
          // DRIFT FIX: use direct Map update instead of addFees to avoid -1 sat loss.
          let hopTfees = returnTfees(hop.tokenOut);
          let curFee = switch (Map.get(feescollectedDAO, thash, hop.tokenOut)) { case (?v) v; case null 0 };
          Map.set(feescollectedDAO, thash, hop.tokenOut, curFee + hopTfees);
          recordOpDrift("splitRoutes_hopTfees", hop.tokenOut, 0);
        };
      };

      // Per-leg fee tracking — only charge if the leg produced output.
      // Don't charge fees for failed legs (user got nothing from them).
      if (currentAmount > 0) {
        // Additive: calculateFee (user upfront) + legFirstHopProtocolFee (AMM protocol).
        // Sync v3 claim by protocolFee only. LP 70% stays in v3 residual.
        let legTradingFee = calculateFee(leg.amountIn, ICPfee, RevokeFeeNow) + legFirstHopProtocolFee;
        if (legTradingFee > 0) {
          // Direct Map update to avoid addFees' -1 sat loss.
          let curFee = switch (Map.get(feescollectedDAO, thash, tokenIn)) { case (?v) v; case null 0 };
          Map.set(feescollectedDAO, thash, tokenIn, curFee + legTradingFee);
          if (legFirstHopProtocolFee > 0) {
            claimProtocolFeeInV3(tokenIn, leg.route[0].tokenOut, legFirstHopProtocolFee);
          };
          recordOpDrift("splitRoutes_legTradingFee", tokenIn, 0);
        };
      };

      // Aggregate orderbook-match detection across legs for inputTfees decision below.
      if (legFirstHopHadOrderbookMatch) { anyLegHadOrderbookMatch := true };

      totalOutput += currentAmount;
    };

    // DRIFT FIX: book the inputTfees buffer ONLY if no leg matched orderbook.
    // If any leg had an orderbook counterparty transfer, the Tfees buffer was
    // consumed by that transfer's ledger fee. Booking it would over-credit and
    // cause negative drift (observed -19,822 for ckBTC before this guard).
    // Matches swapMultiHop:7588 (`inputTfees = if (firstHopHadOrderbookMatch) 0 else Tfees`).
    if (not anyLegHadOrderbookMatch) {
      let inputTfees = returnTfees(tokenIn);
      if (inputTfees > 0) {
        let curFee = switch (Map.get(feescollectedDAO, thash, tokenIn)) { case (?v) v; case null 0 };
        Map.set(feescollectedDAO, thash, tokenIn, curFee + inputTfees);
        recordOpDrift("splitRoutes_inputTfees", tokenIn, 0);
      };
    } else {
      recordOpDrift("splitRoutes_inputTfees_SKIPPED", tokenIn, 0);
    };

    // ── 5. Record swap history (single entry for all legs) ──
    let routeVec = Vector.new<Text>();
    Vector.add(routeVec, tokenIn);
    var legNum : Nat = 0;
    for (leg in splits.vals()) {
      for (hop in leg.route.vals()) { Vector.add(routeVec, hop.tokenOut) };
      if (legNum + 1 < splits.size()) { Vector.add(routeVec, "|") }; // leg separator
      legNum += 1;
    };
    nextSwapId += 1;
    recordSwap(caller, {
      swapId = nextSwapId;
      tokenIn;
      tokenOut;
      amountIn = totalAmountIn;
      amountOut = totalOutput;
      route = Vector.toArray(routeVec);
      fee = calculateFee(totalAmountIn, ICPfee, RevokeFeeNow);
      swapType = #multihop;
      timestamp = Time.now();
    });

    // ── 6. Update exchange info ──
    doInfoBeforeStep2();

    // ── Post-execution global slippage check (ATOMIC ROLLBACK on failure) ──
    // We removed the pre-check (it duplicated quote-time work and this final check
    // catches the same failures anyway). On failure, Debug.trap
    // reverts ALL state mutations from this update message (Motoko/IC actor model
    // guarantees this) — including BlocksDone, AMM/V3 pool state, orderbook entries,
    // fee accumulators. Crucially the queued transfers in tempTransferQueueLocal are
    // ALSO reverted because they haven't been awaited yet. Net effect: nothing happened.
    // The user's deposit on the ledger remains in the exchange treasury and the
    // BlocksDone marker is reverted, so the same Block can be retried (or recovered
    // via checkDiffs later if treasury moves on).
    if (totalOutput < minAmountOut) {
      Debug.trap("SlippageExceeded: expected at least " # Nat.toText(minAmountOut) # " got " # Nat.toText(totalOutput));
    };

    // ── 8. Consolidate transfers (combine same recipient+token to save transfer fees) ──
    // Track per-token transfer counts before consolidation
    let preCountMap = Map.new<Text, Nat>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(preCountMap, thash, key)) {
        case (?n) { Map.set(preCountMap, thash, key, n + 1) };
        case null { Map.set(preCountMap, thash, key, 1) };
      };
    };
    let consolidatedMap = Map.new<Text, (TransferRecipient, Nat, Text, Text)>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(consolidatedMap, thash, key)) {
        case (?existing) { Map.set(consolidatedMap, thash, key, (tx.0, existing.1 + tx.1, tx.2, tx.3)) };
        case null { Map.set(consolidatedMap, thash, key, tx) };
      };
    };
    let consolidatedVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    for ((_, tx) in Map.entries(consolidatedMap)) { Vector.add(consolidatedVec, tx) };

    // Track saved ledger fees from consolidation for OUTPUT token only.
    // Consolidating taker output transfers (tokenOut) saves ledger fees that create
    // untracked surplus.
    for ((key, count) in Map.entries(preCountMap)) {
      if (count > 1) {
        let token = switch (Map.get(consolidatedMap, thash, key)) {
          case (?tx) { tx.2 };
          case null { "" };
        };
        if (token == tokenOut) {
          let savedFees = (count - 1) * returnTfees(token);
          addFees(token, savedFees, false, "", nowVar);
        };
      };
    };

    // Send consolidated transfers to treasury
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(consolidatedVec), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(consolidatedVec));
    };

    #Ok({
      amountIn = totalAmountIn;
      amountOut = totalOutput;
      tokenIn = tokenIn;
      tokenOut = tokenOut;
      route = Vector.toArray(routeVec);
      fee = calculateFee(totalAmountIn, ICPfee, RevokeFeeNow);
      swapId = nextSwapId;
      hops = splits.size();
      firstHopOrderbookMatch = false;
      lastHopAMMOnly = false;
    });
  };

  // Estimate how much `token_init` can be input to push the AMM exactly to `orderRatio`.
  // V3 path (pool has poolV3Data): simulates the real V3 tick-walk via swapWithAMMV3 with
  //   a sqrtPriceLimit at the user's target. This is the only honest answer when V3
  //   concentrated liquidity is in use — V2-style reserveOut/reserveIn diverges from V3
  //   spot whenever ranges concentrate liquidity, and that divergence is signed (it's
  //   under V3 spot in price units, over V3 spot in inverse units), which is why the old
  //   V2-only estimator silently zeroed-out only the SELL direction on every V3 pool.
  // V2 fallback: pools without poolV3Data are degenerate (swapWithAMM also returns 0 for
  //   them), so we keep the original constant-product solve unchanged for compatibility.
  func getAMMLiquidity(pool : AMMPool, orderRatio : Ratio, token_init_identifier : Text) : (Nat, Ratio) {
    let reserveIn = if (pool.token0 == token_init_identifier) pool.reserve0 else pool.reserve1;
    let reserveOut = if (pool.token0 == token_init_identifier) pool.reserve1 else pool.reserve0;
    if ((returnMinimum(token_init_identifier, reserveIn, true) and returnMinimum(if (pool.token0 == token_init_identifier) pool.token1 else pool.token0, reserveOut, true)) == false) {
      return (0, #Value(0));
    };
    let poolKey = (pool.token0, pool.token1);
    let tokenInIsToken0 = (pool.token0 == token_init_identifier);

    switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case (?v3Live) {
        // V3 path — recalc-pure first so a one-sided / freshly-drained pool gets a
        // synced sqrtRatio without writing to Maps (recalcV3Pure returns new structs).
        let (poolPure, v3) = recalcV3Pure(pool, v3Live);
        let v3Spot : Ratio = if (v3.currentSqrtRatio > 0) {
          let raw = (v3.currentSqrtRatio * v3.currentSqrtRatio) / tenToPower60;
          if (tokenInIsToken0) {
            #Value(raw);
          } else if (raw > 0) {
            #Value(tenToPower120 / raw);
          } else { #Max };
        } else { #Zero };

        switch (orderRatio, v3Spot) {
          case (#Zero, _) {
            // accept-any-price: hand back a budget that reserveIn-cap will dominate later
            (reserveIn * 1000, v3Spot);
          };
          case (#Value(t), #Value(s)) {
            if (t == s) return (0, v3Spot);
            // V3 spot is on the unfavourable side of the user's limit → no liquidity
            // (caller's outer gate at orderPairing line 9150 already checks this, but
            // belt+braces — keeps the simulation off the hot path when nothing is to do).
            if (tokenInIsToken0) {
              // SELL token0 (e.g. SELL TACO with TACO=token0): user wants AMM spot
              // expressed as out/in (= ICP/TACO) ≥ their limit. Favourable iff s > t.
              if (s <= t) return (0, v3Spot);
            } else {
              // BUY token0 (token_init is token1): user wants AMM spot expressed as
              // out/in (= TACO/ICP, which is 1e120/v3PoolRatio) ≥ their limit.
              // Same s > t after the inversion above.
              if (s <= t) return (0, v3Spot);
            };
            // Translate user's target ratio to a sqrtPriceLimit in V3's native
            // (token1/token0 price) sqrt space.
            let targetSqrtPrice = if (tokenInIsToken0) {
              ratioToSqrtRatio(t);
            } else {
              if (t > 0) ratioToSqrtRatio(tenToPower120 / t) else 0;
            };
            // Generous upper-bound budget; the sqrtPriceLimit governs termination.
            let bigBudget = if (reserveIn > 0) reserveIn * 1000 else 0;
            if (bigBudget == 0) return (0, v3Spot);
            let (totalIn, _, _, _, _, _) = swapWithAMMV3(
              poolPure, v3, tokenInIsToken0, bigBudget, ICPfee, "sim", ?targetSqrtPrice
            );
            (totalIn, #Value(t));
          };
          case _ { (0, v3Spot) };
        };
      };
      case null {
        // V2 fallback (pre-existing logic, untouched).
        let currentRatio = if (reserveIn == 0) #Max else if (reserveOut == 0) #Zero else #Value((reserveOut * tenToPower60) / reserveIn);
        switch (orderRatio, currentRatio) {
          case (#Value(targetRatio), #Value(poolRatio)) {
            if (targetRatio == poolRatio) {
              return (0, currentRatio);
            };
            let k = reserveIn * reserveOut;
            let newReserveIn = sqrt((k * tenToPower60) / targetRatio);
            let amountIn = if (newReserveIn >= reserveIn) {
              newReserveIn - reserveIn;
            } else { 0 };
            (amountIn, #Value(targetRatio));
          };
          case (#Zero, #Value(_)) {
            (reserveIn * 1000, currentRatio);
          };
          case (_, _) { (0, currentRatio) };
        };
      };
    };
  };

  func ratioToPrice(ratio : Ratio) : Nat {
    switch (ratio) {
      case (#Zero) { 0 };
      case (#Max) { twoToPower256MinusOne }; // Use a very large number to represent "infinity"
      case (#Value(v)) { v };
    };
  };

  func swapWithAMM(pool : AMMPool, tokenInIsToken0 : Bool, amountIn : Nat, orderRatio : Ratio, fee : Nat) : (Nat, Nat, Nat, Nat, Nat, Nat, AMMPool) {

    // V3 path: use concentrated liquidity engine
    let poolKey = (pool.token0, pool.token1);
    // DRIFT FIX: re-sync V3 currentSqrtRatio from LIVE reserves before the swap.
    // Upstream operations (orderbook fills, LP ops) can change reserves without updating
    // currentSqrtRatio, making V3 math think the pool has MORE tokens than it does →
    // output exceeds reserves → clamp fires → insolvency risk.
    // sqrtRatioFromReserves handles one-sided pools (reserve0=0 or reserve1=0) correctly.
    recalculateActiveLiquidity(poolKey);
    // Re-read pool and v3 after sync (pool passed as param is pre-sync; use fresh copy)
    let syncedPool = switch (Map.get(AMMpools, hashtt, poolKey)) { case (?p) { p }; case null { pool } };
    switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case (?v3) {
        let (totalIn, totalOut, protocolFee, poolFee, updatedPool, updatedV3) = swapWithAMMV3(syncedPool, v3, tokenInIsToken0, amountIn, fee, "real", null);
        Map.set(poolV3Data, hashtt, poolKey, updatedV3);
        syncPoolFromV3(poolKey);
        let reserveIn = if (tokenInIsToken0) updatedPool.reserve0 else updatedPool.reserve1;
        let reserveOut = if (tokenInIsToken0) updatedPool.reserve1 else updatedPool.reserve0;
        return (totalIn, totalOut, reserveIn, reserveOut, protocolFee, poolFee, updatedPool);
      };
      case null {};
    };

    // No V3 data: no swap possible
    let reserveIn = if (tokenInIsToken0) pool.reserve0 else pool.reserve1;
    let reserveOut = if (tokenInIsToken0) pool.reserve1 else pool.reserve0;
    (0, 0, reserveIn, reserveOut, 0, 0, pool);
  };
  // Pure constant-product AMM simulation — no state modification.
  // Used for ranking multi-hop routes and slippage pre-checks.
  private func simulateConstantProductSwap(
    pool : AMMPool, tokenIn : Text, amountIn : Nat, fee : Nat
  ) : Nat {
    let tokenInIsToken0 = (tokenIn == pool.token0);
    let reserveIn = if (tokenInIsToken0) pool.reserve0 else pool.reserve1;
    let reserveOut = if (tokenInIsToken0) pool.reserve1 else pool.reserve0;
    if (reserveIn == 0 or reserveOut == 0) return 0;
    let totalFee = (amountIn * fee) / 10000;
    let effectiveIn = if (amountIn > totalFee) { amountIn - totalFee } else { 0 };
    if (effectiveIn == 0) return 0;
    (reserveOut * effectiveIn) / (reserveIn + effectiveIn);
  };

  // V3-aware simulation — returns (amountOut, updatedPool, updatedV3OrNull).
  // No global state modification. Caller gets updated copies for cross-leg simulation.
  // swapWithAMMV3 is already pure (uses local vars, returns new state, never calls Map.set).
  private func simulateSwap(
    pool : AMMPool, v3 : ?PoolV3Data, tokenIn : Text, amountIn : Nat, fee : Nat,
  ) : (Nat, AMMPool, ?PoolV3Data) {
    let tokenInIsToken0 = (tokenIn == pool.token0);
    switch (v3) {
      case (?v3Data) {
        // V3 path: use real V3 engine (pure — returns new state without Map.set).
        // Pre-sync the input pool/v3 so that simulation chains with drained reserves
        // don't trigger the swapV3 clamp via stale currentSqrtRatio.
        let (syncedPool, syncedV3) = recalcV3Pure(pool, v3Data);
        let (_totalIn, totalOut, _protocolFee, _poolFee, updatedPool, updatedV3) = swapWithAMMV3(syncedPool, syncedV3, tokenInIsToken0, amountIn, fee, "sim", null);
        (totalOut, updatedPool, ?updatedV3);
      };
      case null {
        // V2 path: constant product formula
        let out = simulateConstantProductSwap(pool, tokenIn, amountIn, fee);
        let reserveIn = if (tokenInIsToken0) pool.reserve0 else pool.reserve1;
        let reserveOut = if (tokenInIsToken0) pool.reserve1 else pool.reserve0;
        let totalFee2 = (amountIn * fee) / 10000;
        let effectiveIn = if (amountIn > totalFee2) { amountIn - totalFee2 } else { 0 };
        let updatedPool = {
          pool with
          reserve0 = if (tokenInIsToken0) { reserveIn + effectiveIn } else { safeSub(reserveOut, out) };
          reserve1 = if (tokenInIsToken0) { safeSub(reserveOut, out) } else { reserveIn + effectiveIn };
        };
        (out, updatedPool, null);
      };
    };
  };

  // Simulate a multi-hop swap by chaining orderPairing calls.
  // In query context, state changes are discarded. In update context, state changes persist.
  // Uses amount_sell=1 to create a near-zero ratio (market order) at each hop.
  type HopDetail = {
    tokenIn : Text;
    tokenOut : Text;
    amountIn : Nat;
    amountOut : Nat;
    fee : Nat;
    priceImpact : Float;
  };

  private func simulateMultiHop(
    hops : [SwapHop], amountIn : Nat, caller : Principal
  ) : { amountOut : Nat; totalFees : Nat; hopDetails : [HopDetail] } {
    var currentAmount = amountIn;
    var totalFees : Nat = 0;
    let nowVar = Time.now();
    let hopDetailsVec = Vector.new<HopDetail>();

    for (hop in hops.vals()) {
      let hopAmountIn = currentAmount;

      // Snapshot pre-swap reserves AND V3 sqrtRatio BEFORE orderPairing mutates the pool
      let hopPoolKey = getPool(hop.tokenIn, hop.tokenOut);
      let preHopReserves : ?(Nat, Nat) = switch (Map.get(AMMpools, hashtt, hopPoolKey)) {
        case (?hopPool) {
          let (rIn, rOut) = if (hopPool.token0 == hop.tokenIn) { (hopPool.reserve0, hopPool.reserve1) } else { (hopPool.reserve1, hopPool.reserve0) };
          ?(rIn, rOut);
        };
        case null { null };
      };
      let preHopV3Ratio : Nat = getPoolRatioV3(hopPoolKey);

      let syntheticTrade : TradePrivate = {
        Fee = ICPfee;
        amount_sell = 1;
        amount_init = currentAmount;
        token_sell_identifier = hop.tokenOut;
        token_init_identifier = hop.tokenIn;
        trade_done = 0;
        seller_paid = 0;
        init_paid = 1;
        seller_paid2 = 0;
        init_paid2 = 0;
        trade_number = 0;
        SellerPrincipal = "0";
        initPrincipal = Principal.toText(caller);
        RevokeFee = RevokeFeeNow;
        OCname = "";
        time = nowVar;
        filledInit = 0;
        filledSell = 0;
        allOrNothing = false;
        strictlyOTC = false;
      };

      let (_, protocolFee, poolFee, transfers, _, _, hopAmmAmountIn) = orderPairing(syntheticTrade);

      var hopOutput : Nat = 0;
      for (tx in transfers.vals()) {
        if (tx.0 == #principal(caller) and tx.2 == hop.tokenOut) {
          hopOutput += tx.1;
        };
      };

      // Per-hop V3-aware overall price impact against full hop input (partial fills surface as impact)
      let hopPriceImpact : Float = if (hopOutput > 0 and hopAmountIn > 0) {
        computeBlendedAmmImpact(hopPoolKey, hop.tokenIn, hopOutput, hopAmountIn, preHopV3Ratio, preHopReserves);
      } else { 0.0 };

      totalFees += protocolFee + poolFee;

      Vector.add(hopDetailsVec, {
        tokenIn = hop.tokenIn;
        tokenOut = hop.tokenOut;
        amountIn = hopAmountIn;
        amountOut = hopOutput;
        fee = protocolFee + poolFee;
        priceImpact = hopPriceImpact;
      });

      currentAmount := hopOutput;

      if (currentAmount == 0) {
        return { amountOut = 0; totalFees; hopDetails = Vector.toArray(hopDetailsVec) };
      };
    };

    { amountOut = currentAmount; totalFees; hopDetails = Vector.toArray(hopDetailsVec) };
  };

  // Snapshot of all global state that orderPairing (and its sub-callees swapWithAMM,
  // swapWithAMMV3, syncPoolFromV3) mutate. Used by batch quote endpoints to isolate
  // each request from the prior one's mutations — without this, request N in a batch
  // sees the cumulative effects of requests 0..N-1 as if their swaps had executed,
  // because IC only reverts state at the END of a query call, not between iterations.
  type QuoteStateSnapshot = {
    amm : [((Text, Text), AMMPool)];
    v3 : [((Text, Text), PoolV3Data)];
    hist : [((Text, Text), RBTree.Tree<Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }]>)];
    tempQueueSize : Nat;
    // Extended state captured so the swapSplitRoutes pre-check can run real
    // orderPairing via simulateMultiHop and fully revert its mutations before
    // real execution proceeds. Also fixes batch quote isolation: prior to this,
    // request N in a batch quote saw orderbook/fee/kline mutations from request N-1.
    liqPub : [((Text, Text), liqmapsort)];
    liqFor : [((Text, Text), liqmapsort)];
    tspub : [(Text, TradePrivate)];
    tspriv : [(Text, TradePrivate)];
    fees : [(Text, Nat)];
    ltp : [Float];
    klines : [(KlineKey, RBTree.Tree<Int, KlineData>)];
    vol24h : [Nat];
  };

  private func snapshotQuoteState() : QuoteStateSnapshot {
    {
      amm = Iter.toArray(Map.entries(AMMpools));
      v3 = Iter.toArray(Map.entries(poolV3Data));
      hist = Iter.toArray(Map.entries(pool_history));
      tempQueueSize = Vector.size(tempTransferQueue);
      liqPub = Iter.toArray(Map.entries(liqMapSort));
      liqFor = Iter.toArray(Map.entries(liqMapSortForeign));
      tspub = Iter.toArray(Map.entries(tradeStorePublic));
      tspriv = Iter.toArray(Map.entries(tradeStorePrivate));
      fees = Iter.toArray(Map.entries(feescollectedDAO));
      ltp = Iter.toArray(Vector.vals(last_traded_price));
      klines = Iter.toArray(Map.entries(klineDataStorage));
      vol24h = volume_24hArray;
    };
  };

  private func restoreQuoteState(snap : QuoteStateSnapshot) {
    // AMMpools: clear entries that aren't in snapshot, then overwrite remaining with snapshot values
    let ammKeys = Iter.toArray(Map.keys(AMMpools));
    for (k in ammKeys.vals()) { ignore Map.remove(AMMpools, hashtt, k) };
    for ((k, v) in snap.amm.vals()) { Map.set(AMMpools, hashtt, k, v) };

    let v3Keys = Iter.toArray(Map.keys(poolV3Data));
    for (k in v3Keys.vals()) { ignore Map.remove(poolV3Data, hashtt, k) };
    for ((k, v) in snap.v3.vals()) { Map.set(poolV3Data, hashtt, k, v) };

    let histKeys = Iter.toArray(Map.keys(pool_history));
    for (k in histKeys.vals()) { ignore Map.remove(pool_history, hashtt, k) };
    for ((k, v) in snap.hist.vals()) { Map.set(pool_history, hashtt, k, v) };

    // tempTransferQueue: trim back to snapshot size (orderPairing only Vector.add's on error paths)
    while (Vector.size(tempTransferQueue) > snap.tempQueueSize) {
      ignore Vector.removeLast(tempTransferQueue);
    };

    // Public orderbook (limit orders, sorted by ratio)
    let liqPubKeys = Iter.toArray(Map.keys(liqMapSort));
    for (k in liqPubKeys.vals()) { ignore Map.remove(liqMapSort, hashtt, k) };
    for ((k, v) in snap.liqPub.vals()) { Map.set(liqMapSort, hashtt, k, v) };

    // Foreign orderbook
    let liqForKeys = Iter.toArray(Map.keys(liqMapSortForeign));
    for (k in liqForKeys.vals()) { ignore Map.remove(liqMapSortForeign, hashtt, k) };
    for ((k, v) in snap.liqFor.vals()) { Map.set(liqMapSortForeign, hashtt, k, v) };

    // Trade store (public): per-accesscode TradePrivate records
    let tspubKeys = Iter.toArray(Map.keys(tradeStorePublic));
    for (k in tspubKeys.vals()) { ignore Map.remove(tradeStorePublic, thash, k) };
    for ((k, v) in snap.tspub.vals()) { Map.set(tradeStorePublic, thash, k, v) };

    // Trade store (private/OTC)
    let tsprivKeys = Iter.toArray(Map.keys(tradeStorePrivate));
    for (k in tsprivKeys.vals()) { ignore Map.remove(tradeStorePrivate, thash, k) };
    for ((k, v) in snap.tspriv.vals()) { Map.set(tradeStorePrivate, thash, k, v) };

    // Fee accumulators (DAO + per-token)
    let feeKeys = Iter.toArray(Map.keys(feescollectedDAO));
    for (k in feeKeys.vals()) { ignore Map.remove(feescollectedDAO, thash, k) };
    for ((k, v) in snap.fees.vals()) { Map.set(feescollectedDAO, thash, k, v) };

    // last_traded_price Vector — clear and reload (Vector.put writes by index, so values map 1:1)
    Vector.clear(last_traded_price);
    for (f in snap.ltp.vals()) { Vector.add(last_traded_price, f) };

    // Kline (OHLCV) per (token0, token1, timeframe)
    let klineKeys = Iter.toArray(Map.keys(klineDataStorage));
    for (k in klineKeys.vals()) { ignore Map.remove(klineDataStorage, hashkl, k) };
    for ((k, v) in snap.klines.vals()) { Map.set(klineDataStorage, hashkl, k, v) };

    // 24h volume cache (mirrors AllExchangeInfo.volume_24h)
    volume_24hArray := snap.vol24h;
    AllExchangeInfo := { AllExchangeInfo with volume_24h = snap.vol24h };
  };

  // Compute the AMM's pool ratio in the same `out/in × 10^60` convention as orderRatio.
  // Reads V3 currentSqrtRatio when available (the source of truth for V3 pools); falls
  // back to V2 reserve ratio only when poolV3Data is absent. Used at orderPairing entry
  // AND after every AMM swap, where the previous code recomputed from raw reserves and
  // therefore drifted whenever V3 ranges concentrated liquidity (= every active pool).
  private func computePoolRatioFor(pool : AMMPool, token_init_identifier : Text) : Ratio {
    let v3PoolRatio = getPoolRatioV3((pool.token0, pool.token1));
    if (v3PoolRatio > 0) {
      if (pool.token0 == token_init_identifier) { #Value(v3PoolRatio) }
      else { #Value(tenToPower120 / v3PoolRatio) };
    } else if (pool.token0 == token_init_identifier) {
      if (pool.reserve0 == 0) { #Max } else if (pool.reserve1 == 0) { #Zero }
      else { #Value((pool.reserve1 * tenToPower60) / pool.reserve0) };
    } else {
      if (pool.reserve1 == 0) { #Max } else if (pool.reserve0 == 0) { #Zero }
      else { #Value((pool.reserve0 * tenToPower60) / pool.reserve1) };
    };
  };

  // When an order is made, this function checks whether it can paired with other orders first. This may result in the order getting fulfilled without being registered on the exchange.
  private func orderPairing(data : TradePrivate) : (Nat, Nat, Nat, [(TransferRecipient, Nat, Text, Text)], Bool, Nat, Nat) {


    if (data.strictlyOTC or data.allOrNothing) {
      return (data.amount_init, 0, 0, [], false, 0, 0);
    };

    // DAO bypass eligibility — computed once per call (invariant for the whole iteration).
    // Treasury callers may match private orders that have excludeDAO=false (no "excl" suffix);
    // every other caller sees the original public-only filter behavior unchanged.
    let callerIsDAO : Bool = switch (Array.find<Text>(daoTreasuryPrincipalsText, func(t) { t == data.initPrincipal })) {
      case (?_) { true };
      case null { false };
    };

    let nonPoolOrder = not isKnownPool(data.token_sell_identifier, data.token_init_identifier);

    // Helper to record AMM swaps in pool_history
    func recordAMMHistory(pool2 : (Text, Text), amountIn2 : Nat, amountOut2 : Nat) {
      let histEntry = {
        amount_init = amountIn2;
        amount_sell = amountOut2;
        init_principal = data.initPrincipal;
        sell_principal = "AMM";
        accesscode = "";
        token_init_identifier = data.token_init_identifier;
        filledInit = amountIn2;
        filledSell = amountOut2;
        strictlyOTC = data.strictlyOTC;
        allOrNothing = data.allOrNothing;
      };
      let nowEntry = Time.now();
      var history_pool2 = switch (Map.get(pool_history, hashtt, pool2)) {
        case null { RBTree.init<Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }]>() };
        case (?a) { a };
      };
      Map.set(pool_history, hashtt, pool2, switch (RBTree.get(history_pool2, compareTime, nowEntry)) {
        case null { RBTree.put(history_pool2, compareTime, nowEntry, [histEntry]) };
        case (?a) { let hVec = Vector.fromArray<{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }>(a); Vector.add(hVec, histEntry); RBTree.put(history_pool2, compareTime, nowEntry, Vector.toArray(hVec)) };
      });
    };

    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var liquidityInPool : liqmapsort = switch (Map.get(if nonPoolOrder { liqMapSortForeign } else { liqMapSort }, hashtt, (data.token_sell_identifier, data.token_init_identifier))) {
      case null {
        RBTree.init<Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]>();
      };
      case (?(foundTrades)) {
        foundTrades;
      };
    };
    let updateLastTradedPriceVector = Vector.new<{ token_init_identifier : Text; token_sell_identifier : Text; amount_sell : Nat; amount_init : Nat }>();
    var TradeEntryVector = Vector.new<{ InitPrincipal : Text; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat }>();

    let ratio : Ratio = if (data.amount_init == 0) {
      #Max;
    } else if (data.amount_sell == 0) {
      #Zero;
    } else {
      #Value((data.amount_sell * tenToPower60) / data.amount_init);
    };

    var currentRatioAmountSell : Nat = 0; // Aggregate selling amount for current ratio
    var currentRatioAmountBuy : Nat = 0; // Aggregate buying amount for current ratio
    var lastProcessedRatio : ?Ratio = null; // Track when ratio changes


    var amountCoveredSell = 0;
    var ammAmountCoveredSell : Nat = 0;
    var amountCoveredBuy = 0;
    var amountBuying = 0;
    var amountSelling = 0;
    var totalProtocolFeeAmount = 0;
    var totalPoolFeeAmount = 0;

    var plsbreak = 0;
    let sellTfees = returnTfees(data.token_sell_identifier);
    let buyTfees = returnTfees(data.token_init_identifier);


    // starting totalbuyTfees negative. This corresponds with order makers having to offer 1 time the transfer fee of the asset they are offering. However, if their order fulfills more orders, its more than 1* transferfee.
    // meaning this has to be accounted for. Later in the calculations youll see this amount will be added to the current positions amountInit.
    // this might seem disadvantageous for the order maker, however this is balanced by the timeTfees. As the orders that are being linked to this order have also accounted for the transferfees of the asset they are offering
    // and this order may be fulfilling multiple other orders, this means those extra transferfees that are accounted for, can be sent to the current order maker.
    var totalbuyTfees : Int = -buyTfees;
    var timesTfees = 0;
    var fullyConsumedOrders = 0;


    let bestOrderbookRatio = switch (RBTree.scanLimit(liquidityInPool, compareRatio, #Zero, #Max, #bwd, 1).results) {
      case (array) {
        if (array.size() > 0) {

          array[0].0;
        } else {

          #Zero;
        };
      };
      case _ {

        #Zero;
      };
    };

    let poolKey = getPool(data.token_init_identifier, data.token_sell_identifier);
    var pool = switch (Map.get(AMMpools, hashtt, poolKey)) {
      case (null) {

        {
          token0 = "";
          token1 = "";
          reserve0 = 0;
          reserve1 = 0;
          totalLiquidity = 0;
          lastUpdateTime = 0;
          totalFee0 = 0;
          totalFee1 = 0;
          providers = TrieSet.empty<Principal>();
        };
      };
      case (?p) {

        p;
      };
    };
    var nowVar = Time.now();
    let orderRatio : Ratio = ratio;
    var poolRatio : Ratio = computePoolRatioFor(pool, data.token_init_identifier);



    var lastRatio : Ratio = #Max;




    if ((RBTree.size(liquidityInPool) == 0 or compareRatio(bestOrderbookRatio, ratio) == #less) and compareRatio(poolRatio, orderRatio) != #greater) {


      return (data.amount_init, totalProtocolFeeAmount, totalPoolFeeAmount, Vector.toArray(tempTransferQueueLocal), false, fullyConsumedOrders, ammAmountCoveredSell);
    };
    var amm_exhausted = false;
    var amm_swap_done = false;




    // Check AMM first

    if (pool.reserve0 != 0 and pool.reserve1 != 0) {
      if (compareRatio(poolRatio, orderRatio) == #greater and compareRatio(poolRatio, bestOrderbookRatio) == #greater) {





        let (ammAmount, ammEffectiveRatio) = getAMMLiquidity(pool, if (compareRatio(bestOrderbookRatio, orderRatio) == #less or bestOrderbookRatio == #Zero) { orderRatio } else { bestOrderbookRatio }, data.token_init_identifier);


        if (ammAmount > 10000) {

          let tokenInIsToken0 = data.token_init_identifier == pool.token0;
          let amountToSwap = Nat.min(ammAmount, data.amount_init - amountCoveredSell);

          let (amountIn, amountOut, newReserveIn, newReserveOut, protocolFeeAmount, poolFeeAmount, updatedPool) = swapWithAMM(pool, tokenInIsToken0, amountToSwap, if (compareRatio(bestOrderbookRatio, orderRatio) == #less or bestOrderbookRatio == #Zero) { orderRatio } else { bestOrderbookRatio }, data.Fee);

          // Update the pool state
          Map.set(AMMpools, hashtt, poolKey, updatedPool);

          recordOpDrift("swapAMM_best", data.token_init_identifier, amountIn);
          recordOpDrift("swapAMM_best", data.token_sell_identifier, -amountOut);

          totalProtocolFeeAmount += protocolFeeAmount;
          totalPoolFeeAmount += poolFeeAmount;


          amm_swap_done := true;
          amountCoveredSell += amountIn;
          ammAmountCoveredSell += amountIn;
          amountCoveredBuy += amountOut;

          recordAMMHistory(poolKey, amountIn, amountOut);

          Vector.add(updateLastTradedPriceVector, { token_init_identifier = data.token_init_identifier; token_sell_identifier = data.token_sell_identifier; amount_sell = amountIn; amount_init = amountOut });

          // Update the pool with new reserves; refresh poolRatio from V3 (truth) not V2 reserves
          pool := updatedPool;
          poolRatio := computePoolRatioFor(pool, data.token_init_identifier);
          Map.set(AMMpools, hashtt, poolKey, pool);



        } else {

        };
      } else {




      };
    } else {
      amm_exhausted := true;
    };


    var notFirstLoop = false;



    label orderLinking for ((currentRatio, trades) in RBTree.entriesRev(liquidityInPool)) {

      if (plsbreak == 1) {
        break orderLinking;
      };

      if (isLessThanRatio(currentRatio, ratio)) {
        break orderLinking;
      };


      if (notFirstLoop and not amm_exhausted) {
        // Check AMM liquidity between last ratio and current ratio
        if (compareRatio(lastRatio, currentRatio) != #equal and compareRatio(poolRatio, currentRatio) == #greater) {
          let (ammAmount, _) = getAMMLiquidity(pool, currentRatio, data.token_init_identifier);


          if (ammAmount > 10000) {
            let amountToSwap = Nat.min(ammAmount, data.amount_init - amountCoveredSell);
            let tokenInIsToken0 = data.token_init_identifier == pool.token0;
            let (amountIn, amountOut, newReserveIn, newReserveOut, protocolFeeAmount, poolFeeAmount, updatedPool) = swapWithAMM(pool, tokenInIsToken0, amountToSwap, currentRatio, data.Fee);

            recordOpDrift("swapAMM_current", data.token_init_identifier, amountIn);
            recordOpDrift("swapAMM_current", data.token_sell_identifier, -amountOut);

            totalProtocolFeeAmount += protocolFeeAmount;
            totalPoolFeeAmount += poolFeeAmount;
            amountCoveredSell += amountIn;
            ammAmountCoveredSell += amountIn;
            amountCoveredBuy += amountOut;
            amm_swap_done := true;

            recordAMMHistory(poolKey, amountIn, amountOut);

            Vector.add(updateLastTradedPriceVector, { token_init_identifier = data.token_init_identifier; token_sell_identifier = data.token_sell_identifier; amount_sell = amountIn; amount_init = amountOut });

            // Update the pool with new reserves; refresh poolRatio from V3 (truth) not V2 reserves
            pool := updatedPool;
            poolRatio := computePoolRatioFor(pool, data.token_init_identifier);

            Map.set(AMMpools, hashtt, poolKey, pool);

          };
        };

      } else { notFirstLoop := true };

      label through for (trade in trades.vals()) {
        if (trade.strictlyOTC or trade.allOrNothing) { continue through };
        // check whether the price of this position is not too high. If it is, we break the hole loop as its ordered
        if (amountCoveredSell >= data.amount_init) {
          break orderLinking;
        };
        // Visibility filter: public orders match for everyone; private orders with
        // excludeDAO=false also match when the caller is a registered DAO treasury.
        // Short-circuited on the precomputed callerIsDAO so non-DAO callers do exactly
        // one Text.startsWith call per trade — identical cost to the original code.
        let canMatch : Bool = if (callerIsDAO) {
          Text.startsWith(trade.accesscode, #text "Public")
            or not Text.endsWith(trade.accesscode, #text "excl")
        } else {
          Text.startsWith(trade.accesscode, #text "Public")
        };
        if (canMatch) {






          // < 0 == first item in loop
          if (totalbuyTfees < 0) {
            // check if the amount of the position we are trying to pair is not too mch for the current order, in case it is, we fulfill it partly
            if (amountCoveredSell + trade.amount_sell > data.amount_init and (amountCoveredSell < data.amount_init)) {




              //check whether the liquidit in position is too much for the position that is being linked, if that the case, the position that is being linked is done partially
              if (amountCoveredSell < data.amount_init) {
                amountSelling := data.amount_init - amountCoveredSell;
              } else {
                amountSelling := 0;
              };
              amountBuying := (((amountSelling * tenToPower60) / trade.amount_sell) * trade.amount_init) / tenToPower60;

              plsbreak := 1;
            } else if (amountCoveredSell >= data.amount_init) {
              break orderLinking;
            } else {
              // the order we are trying to pair has lower amounts than what the current order has yet to be fulfilled. Will go on to the next order if there is one
              amountBuying := trade.amount_init;
              if (amountBuying > 0) {
                timesTfees += 1;
                amountSelling := trade.amount_sell;

              };
            };
          } else {
            // check if the amount of the position we are trying to pair is not too mch for the current order, in case it is, we fulfill it partly
            if (amountCoveredSell + trade.amount_sell + totalbuyTfees + buyTfees > data.amount_init and amountCoveredSell + totalbuyTfees + buyTfees < data.amount_init) {




              amountSelling := data.amount_init - (amountCoveredSell + Int.abs(totalbuyTfees) + buyTfees);
              amountBuying := (((amountSelling * tenToPower60) / trade.amount_sell) * trade.amount_init) / tenToPower60;
              plsbreak := 1;
            } else if (amountCoveredSell + totalbuyTfees + buyTfees >= data.amount_init) {
              break orderLinking;
            } else {

              amountBuying := trade.amount_init;
              if (amountBuying > 0) {

                timesTfees += 1;
                amountSelling := trade.amount_sell;

              };
            };
          };
          if (amountBuying > 0) {
            totalbuyTfees += buyTfees;
            amountCoveredBuy += amountBuying;
            amountCoveredSell += amountSelling;

            var tradeentry = {
              accesscode = trade.accesscode;
              amount_sell = amountSelling;
              amount_init = amountBuying;
              InitPrincipal = trade.initPrincipal;
              Fee = trade.Fee;
              RevokeFee = trade.RevokeFee;
            };
            Vector.add(TradeEntryVector, tradeentry);

            let pair1 = (data.token_init_identifier, data.token_sell_identifier);
            let pair2 = (data.token_sell_identifier, data.token_init_identifier);

            // Update aggregated amounts for current ratio
            currentRatioAmountSell += amountSelling;
            currentRatioAmountBuy += amountBuying;

            // If this is first trade or ratio changed, update lastProcessedRatio
            switch (lastProcessedRatio) {
              case null { lastProcessedRatio := ?currentRatio };
              case (?lastRatio) {
                if (compareRatio(lastRatio, currentRatio) != #equal) {
                  // Ratio changed, update price with aggregated amounts
                  if ((Map.has(foreignPools, hashtt, pair1) or Map.has(foreignPools, hashtt, pair2)) == false) {
                    if (currentRatioAmountSell > 0 and currentRatioAmountBuy > 0) {
                      Vector.add(updateLastTradedPriceVector, { token_init_identifier = data.token_init_identifier; token_sell_identifier = data.token_sell_identifier; amount_sell = currentRatioAmountSell; amount_init = currentRatioAmountBuy });
                    };
                  };
                  // Reset aggregated amounts for new ratio
                  currentRatioAmountSell := amountSelling;
                  currentRatioAmountBuy := amountBuying;
                  lastProcessedRatio := ?currentRatio;
                };
              };
            };
          };
          if (plsbreak == 1) {
            break orderLinking;
          };
        };
      };
      lastRatio := currentRatio;
    };

    if (notFirstLoop and not amm_exhausted and data.amount_init > amountCoveredSell) {
      // Check AMM liquidity between last ratio and current ratio
      if (compareRatio(lastRatio, orderRatio) != #equal and compareRatio(poolRatio, orderRatio) == #greater) {
        let (ammAmount, _) = getAMMLiquidity(pool, orderRatio, data.token_init_identifier);

        if (ammAmount > 10000) {
          let amountToSwap = Nat.min(ammAmount, data.amount_init - amountCoveredSell);
          let tokenInIsToken0 = data.token_init_identifier == pool.token0;
          let (amountIn, amountOut, newReserveIn, newReserveOut, protocolFeeAmount, poolFeeAmount, updatedPool) = swapWithAMM(pool, tokenInIsToken0, amountToSwap, orderRatio, data.Fee);

          recordOpDrift("swapAMM_orderRatio", data.token_init_identifier, amountIn);
          recordOpDrift("swapAMM_orderRatio", data.token_sell_identifier, -amountOut);

          totalProtocolFeeAmount += protocolFeeAmount;
          totalPoolFeeAmount += poolFeeAmount;
          amountCoveredSell += amountIn;
          ammAmountCoveredSell += amountIn;
          amountCoveredBuy += amountOut;
          amm_swap_done := true;

          recordAMMHistory(poolKey, amountIn, amountOut);

          Vector.add(updateLastTradedPriceVector, { token_init_identifier = data.token_init_identifier; token_sell_identifier = data.token_sell_identifier; amount_sell = amountIn; amount_init = amountOut });

          // Update the pool with new reserves; refresh poolRatio from V3 (truth) not V2 reserves
          pool := updatedPool;
          poolRatio := computePoolRatioFor(pool, data.token_init_identifier);

          Map.set(AMMpools, hashtt, poolKey, pool);

        };
      };

    } else { notFirstLoop := true };

    if (Vector.size(TradeEntryVector) == 0 and not amm_swap_done) {

      return (data.amount_init, totalProtocolFeeAmount, totalPoolFeeAmount, Vector.toArray(tempTransferQueueLocal), false, fullyConsumedOrders, ammAmountCoveredSell);
    } else if (amm_swap_done and Vector.size(TradeEntryVector) == 0) {
      timesTfees := 0;
      totalbuyTfees := -buyTfees;

    };

    let updates = Vector.toArray(updateLastTradedPriceVector);
    let sortedUpdates = Array.sort<{ token_init_identifier : Text; token_sell_identifier : Text; amount_sell : Nat; amount_init : Nat }>(
      updates,
      func(a, b) {
        // Guard: avoid division by zero if either amount_init is 0
        if (a.amount_init == 0 or b.amount_init == 0) { return #equal };
        let ratioA = (a.amount_sell * tenToPower60) / a.amount_init;
        let ratioB = (b.amount_sell * tenToPower60) / b.amount_init;
        if (ratioA < ratioB) { #less } else if (ratioA > ratioB) { #greater } else {
          #equal;
        };
      },
    );

    // Then process the sorted updates.
    //
    // Reader/writer convention for updateLastTradedPriceVector entries: the four
    // Vector.add sites above (AMM at ~8258/8325/8479, orderbook aggregation at ~8440)
    // store the caller's INPUT amount in `amount_sell` and the caller's OUTPUT amount
    // in `amount_init` — field names are swapped relative to the tokens they pair
    // with. Canonicalize by mapping each amount to its actual token, then onto canonical
    // (amt0, amt1) based on which side is canonical token0.
    for (update in sortedUpdates.vals()) {
      let cPair = getPool(update.token_init_identifier, update.token_sell_identifier);
      let (amt0, amt1) = if (cPair.0 == update.token_init_identifier) {
        // canonical token0 == caller's input token.
        // update.amount_sell (= caller's input) is amount0; update.amount_init (= output) is amount1.
        (update.amount_sell, update.amount_init)
      } else {
        // canonical token0 == caller's output token.
        // update.amount_init (= caller's output) is amount0; update.amount_sell (= input) is amount1.
        (update.amount_init, update.amount_sell)
      };
      updateLastTradedPrice(cPair, amt0, amt1);
    };



    if (totalbuyTfees >= 0) {
      if (amountCoveredSell > 0) {
        amountCoveredSell += Int.abs(totalbuyTfees);
      };
    } else {
      if (amountCoveredSell >= Int.abs(totalbuyTfees)) {
        amountCoveredSell -= Int.abs(totalbuyTfees);
      } else {

        return (data.amount_init, totalProtocolFeeAmount, totalPoolFeeAmount, Vector.toArray(tempTransferQueueLocal), false, fullyConsumedOrders, ammAmountCoveredSell);
      };
    };

    // Update user's position (you'll need to implement this function)
    let updatedPool = switch (Map.get(AMMpools, hashtt, poolKey)) {
      case (null) {

        {
          token0 = "";
          token1 = "";
          reserve0 = 0;
          reserve1 = 0;
          totalLiquidity = 0;
          lastUpdateTime = 0;
          totalFee0 = 0;
          totalFee1 = 0;
          providers = TrieSet.empty<Principal>();
        };
      };
      case (?p) {

        p;
      };
    };
    let tokenInIsToken0 = data.token_init_identifier == pool.token0;


    let TradeEntries = Vector.toArray(TradeEntryVector);
    //send funds;



    if (amountCoveredBuy > 0) {
      if (timesTfees > 0) {
        let extraFees = (timesTfees - 1) * sellTfees;
        Vector.add(
          tempTransferQueueLocal,
          (
            #principal(Principal.fromText(data.initPrincipal)),
            amountCoveredBuy + extraFees,
            data.token_sell_identifier,
            genTxId(),
          ),
        );
      } else {
        let feesToDeduct = sellTfees;
        if (amountCoveredBuy > feesToDeduct) {
          Vector.add(
            tempTransferQueueLocal,
            (
              #principal(Principal.fromText(data.initPrincipal)),
              amountCoveredBuy - feesToDeduct,
              data.token_sell_identifier,
              genTxId(),
            ),
          );
        } else {
          addFees(data.token_sell_identifier, amountCoveredBuy, false, "", nowVar);
        };
      };
    };

    //process are positions that are linked
    if (TradeEntries.size() > 0) {
      for (i in Iter.range(0, TradeEntries.size() -1)) {


        var currentTrades2 : TradePrivate = Faketrade;

        // Route lookup to the correct store based on accesscode prefix. With the
        // DAO bypass active, TradeEntries[i] may reference a private order whose
        // record lives in tradeStorePrivate; falling back to a single-store lookup
        // would return null and corrupt the downstream replaceLiqMap arithmetic.
        let isPubAccessCode = Text.startsWith(TradeEntries[i].accesscode, #text "Public");
        let currentTrades = Map.get(
          if (isPubAccessCode) tradeStorePublic else tradeStorePrivate,
          thash,
          TradeEntries[i].accesscode
        );
        switch (currentTrades) {
          case null {};
          case (?(foundTrades)) {
            currentTrades2 := foundTrades;
          };
        };
        var error = 0;
        var as = 0;

        addFees(data.token_sell_identifier, ((TradeEntries[i].amount_init * TradeEntries[i].Fee) - (((TradeEntries[i].amount_init * 100000) * TradeEntries[i].Fee / TradeEntries[i].RevokeFee) / 100000)) / 10000, false, TradeEntries[i].InitPrincipal, nowVar);




        Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(TradeEntries[i].InitPrincipal)), TradeEntries[i].amount_sell, data.token_init_identifier, genTxId()));

        error := 0;
        //partially fulfilled
        if (TradeEntries[i].amount_init < currentTrades2.amount_init) {






          currentTrades2 := {
            currentTrades2 with
            amount_sell = currentTrades2.amount_sell - TradeEntries[i].amount_sell;
            amount_init = currentTrades2.amount_init - TradeEntries[i].amount_init;
            filledInit = currentTrades2.filledInit +TradeEntries[i].amount_init;
          };



          addTrade(TradeEntries[i].accesscode, currentTrades2.initPrincipal, currentTrades2, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));

          replaceLiqMap(
            false,
            true,
            currentTrades2.token_init_identifier,
            currentTrades2.token_sell_identifier,
            TradeEntries[i].accesscode,
            (currentTrades2.amount_init, currentTrades2.amount_sell, 0, 0, currentTrades2.initPrincipal, currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing),
            #Value(((currentTrades2.amount_init +TradeEntries[i].amount_init) * tenToPower60) / (currentTrades2.amount_sell +TradeEntries[i].amount_sell)),
            ?{
              Fee = currentTrades2.Fee;
              RevokeFee = currentTrades2.RevokeFee;
            },
            ?{
              amount_init = TradeEntries[i].amount_init;
              amount_sell = TradeEntries[i].amount_sell;
              init_principal = currentTrades2.initPrincipal;
              sell_principal = data.initPrincipal;
              accesscode = TradeEntries[i].accesscode;
              token_init_identifier = currentTrades2.token_init_identifier;
              filledInit = TradeEntries[i].amount_init;
              filledSell = TradeEntries[i].amount_sell;
              strictlyOTC = currentTrades2.strictlyOTC;
              allOrNothing = currentTrades2.allOrNothing;
            },
          );



          if (error == 1) {

            Vector.add(tempTransferQueue, (#principal(Principal.fromText(currentTrades2.initPrincipal)), TradeEntries[i].amount_sell, currentTrades2.token_sell_identifier, genTxId()));
          };
        } else {
          if (error == 0) {

            fullyConsumedOrders += 1;
            removeTrade(TradeEntries[i].accesscode, currentTrades2.initPrincipal, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));


            replaceLiqMap(
              true,
              false,
              data.token_sell_identifier,
              data.token_init_identifier,
              TradeEntries[i].accesscode,
              (currentTrades2.amount_init, currentTrades2.amount_sell, 0, 0, "", currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing),
              #Zero,
              null,
              ?{
                amount_init = currentTrades2.amount_init;
                amount_sell = currentTrades2.amount_sell;
                init_principal = currentTrades2.initPrincipal;
                sell_principal = data.initPrincipal;
                accesscode = TradeEntries[i].accesscode;
                token_init_identifier = currentTrades2.token_init_identifier;
                filledInit = TradeEntries[i].amount_init;
                filledSell = TradeEntries[i].amount_sell;
                strictlyOTC = currentTrades2.strictlyOTC;
                allOrNothing = currentTrades2.allOrNothing;
              },
            );

          } else {

            currentTrades2 := {
              currentTrades2 with
              trade_done = 1;
              seller_paid = 1;
              init_paid = 1;
              SellerPrincipal = DAOTreasuryText;
              seller_paid2 = 1;
              init_paid2 = 0;
            };

            addTrade(TradeEntries[i].accesscode, currentTrades2.initPrincipal, currentTrades2, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));

            replaceLiqMap(
              true,
              false,
              data.token_sell_identifier,
              data.token_init_identifier,
              TradeEntries[i].accesscode,
              (currentTrades2.amount_init, currentTrades2.amount_sell, 0, 0, "", currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing),
              #Zero,
              null,
              ?{
                amount_init = currentTrades2.amount_init;
                amount_sell = currentTrades2.amount_sell;
                init_principal = currentTrades2.initPrincipal;
                sell_principal = data.initPrincipal;
                accesscode = TradeEntries[i].accesscode;
                token_init_identifier = currentTrades2.token_init_identifier;
                filledInit = TradeEntries[i].amount_init;
                filledSell = TradeEntries[i].amount_sell;
                strictlyOTC = currentTrades2.strictlyOTC;
                allOrNothing = currentTrades2.allOrNothing;
              },
            );
          };
        };

      };
    };


    let remainingAmount = if (data.amount_init > amountCoveredSell) {
      data.amount_init - amountCoveredSell;
    } else {
      0;
    };
    // At the end of orderPairing, before returning:
    if (currentRatioAmountSell > 0 and currentRatioAmountBuy > 0) {
      let pair1 = (data.token_init_identifier, data.token_sell_identifier);
      let pair2 = (data.token_sell_identifier, data.token_init_identifier);
      if ((Map.has(foreignPools, hashtt, pair1) or Map.has(foreignPools, hashtt, pair2)) == false) {
        // currentRatioAmountSell is in token_init units (what caller sold/input).
        // currentRatioAmountBuy is in token_sell units (what caller bought/output).
        // Map to canonical (amount0, amount1).
        let cPair = getPool(data.token_init_identifier, data.token_sell_identifier);
        let (amt0, amt1) = if (cPair.0 == data.token_init_identifier) {
          (currentRatioAmountSell, currentRatioAmountBuy)
        } else {
          (currentRatioAmountBuy, currentRatioAmountSell)
        };
        updateLastTradedPrice(cPair, amt0, amt1);
      };
    };
    return (remainingAmount, totalProtocolFeeAmount, totalPoolFeeAmount, Vector.toArray(tempTransferQueueLocal), TradeEntries.size() == 0, fullyConsumedOrders, ammAmountCoveredSell);
  };

  // This function manages changes in the Maps that store the current liquidity, for instance when an order is partially filled. When an order gets deleted, edited or added, it manages all the maps and arrays to be updated.
  // del means something has to be deleted. copyFee is true when an exisitng has to be edited due to it being parially fulfilled (copyFee= the fee of the original order has to be kept, even if it changed in the mean time)
  // liqMapSort is a map that is used for orderPairing and getAllTradesDAOFilter, as it saves orders in terms of the ratio of the init token/sell token.
  private func replaceLiqMap(del : Bool, copyFee : Bool, asseta : Text, assetb : Text, accesscode : Text, data : (Nat, Nat, Nat, Nat, Text, Text, Int, Text, Text, Bool, Bool), oldratio : Ratio, olddata2 : ?{ Fee : Nat; RevokeFee : Nat }, historyData2 : ?{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }) {
    let pub = Text.startsWith(accesscode, #text "Public");
    let nowVar = Time.now();



    let ratio : Ratio = if (data.1 == 0) {
      #Max;
    } else if (data.0 == 0) {
      #Zero;
    } else {
      #Value((data.0 * tenToPower60) / data.1);
    };


    let key1 = (asseta, assetb);
    var olddata = switch (olddata2) {
      case null { { Fee = 5; RevokeFee = 5 } };
      case (?(foundTrades)) {
        foundTrades;
      };
    };
    var historydata = switch (historyData2) {
      case null {
        {
          amount_init = 0;
          amount_sell = 0;
          init_principal = "";
          sell_principal = "";
          accesscode = "";
          token_init_identifier = "";
          filledInit = 0;
          filledSell = 0;
          strictlyOTC = false;
          allOrNothing = false;
        };
      };
      case (?(foundTrades)) {
        foundTrades;
      };
    };

    var pool : (Text, Text) = ("", "");

    switch (Map.get(poolIndexMap, hashtt, (asseta, assetb))) {
      case (?idx) { pool := Vector.get(pool_canister, idx) };
      case null {};
    };



    let nonPoolOrder = not isKnownPool(assetb, asseta) or data.9 or data.10;

    var currentTrades2sort : liqmapsort = switch (Map.get(if nonPoolOrder { liqMapSortForeign } else { liqMapSort }, hashtt, key1)) {
      case null {
        RBTree.init<Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]>();
      };
      case (?(foundTrades)) {
        foundTrades;
      };
    };

    if del {
      if pub {
        if (historydata.init_principal != "") {

          var history_pool = switch (Map.get(pool_history, hashtt, pool)) {
            case null {
              RBTree.init<Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }]>();
            };
            case (?a) { a };
          };
          Map.set(pool_history, hashtt, pool, switch (RBTree.get(history_pool, compareTime, nowVar)) { case null { RBTree.put(history_pool, compareTime, nowVar, [historydata]) }; case (?a) { let hVec = Vector.fromArray<{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }>(a); Vector.add(hVec, historydata); RBTree.put(history_pool, compareTime, nowVar, Vector.toArray(hVec)) } });
        };

      };
      //Accesscode, amount init, amount sell, fee,revokefee,principal init
      var currentTrades2sort2 = switch (RBTree.get(currentTrades2sort, compareRatio, ratio)) {
        case null { [] };
        case (?(foundTrades)) {
          foundTrades;
        };
      };

      let filtered = Array.filter<{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }>(currentTrades2sort2, func(o) { o.accesscode != accesscode });

      if (filtered.size() == 0) {
        Map.set(if nonPoolOrder { liqMapSortForeign } else { liqMapSort }, hashtt, key1, RBTree.delete(currentTrades2sort, compareRatio, ratio));
      } else {
        Map.set(if nonPoolOrder { liqMapSortForeign } else { liqMapSort }, hashtt, key1, RBTree.put(currentTrades2sort, compareRatio, ratio, filtered));
      };
      removeTrade(accesscode, data.4, (data.7, data.8));

      //trade added
    } else if (copyFee == false) {

      if (RBTree.size(currentTrades2sort) == 0) {
        var newliqMap = RBTree.init<Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]>();

        newliqMap := RBTree.put(newliqMap, compareRatio, ratio, [{ accesscode = accesscode; amount_init = data.0; amount_sell = data.1; Fee = data.2; RevokeFee = data.3; initPrincipal = data.4; OCname = data.5; time = data.6; token_init_identifier = data.7; token_sell_identifier = data.8; strictlyOTC = data.9; allOrNothing = data.10 }]);
        Map.set(if nonPoolOrder { liqMapSortForeign } else { liqMapSort }, hashtt, key1, newliqMap);
      } else {
        let currentTradessort2 = RBTree.get(currentTrades2sort, compareRatio, ratio);
        var currentTrades2sort2 : [{
          time : Int;
          accesscode : Text;
          amount_init : Nat;
          amount_sell : Nat;
          Fee : Nat;
          RevokeFee : Nat;
          initPrincipal : Text;
          OCname : Text;
          token_init_identifier : Text;
          token_sell_identifier : Text;
          strictlyOTC : Bool;
          allOrNothing : Bool;
        }] = switch (currentTradessort2) {
          case null { [] };
          case (?foundTrades) { foundTrades };
        };
        var tempBuffer = Buffer.fromArray<{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }>(currentTrades2sort2);

        tempBuffer.add({
          accesscode = accesscode;
          amount_init = data.0;
          amount_sell = data.1;
          Fee = data.2;
          RevokeFee = data.3;
          initPrincipal = data.4;
          OCname = data.5;
          time = data.6;
          token_init_identifier = data.7;
          token_sell_identifier = data.8;
          strictlyOTC = data.9;
          allOrNothing = data.10;
        });
        Map.set(if nonPoolOrder { liqMapSortForeign } else { liqMapSort }, hashtt, key1, RBTree.put(currentTrades2sort, compareRatio, ratio, Buffer.toArray(tempBuffer)));
      };

      //trade done partly
    } else {


      if pub {
        if (historydata.init_principal != "") {

          var history_pool = switch (Map.get(pool_history, hashtt, pool)) {
            case null {
              RBTree.init<Time, [{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }]>();
            };
            case (?a) { a };
          };
          Map.set(pool_history, hashtt, pool, switch (RBTree.get(history_pool, compareTime, nowVar)) { case null { RBTree.put(history_pool, compareTime, nowVar, [historydata]) }; case (?a) { let hVec = Vector.fromArray<{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; filledInit : Nat; filledSell : Nat; strictlyOTC : Bool; allOrNothing : Bool }>(a); Vector.add(hVec, historydata); RBTree.put(history_pool, compareTime, nowVar, Vector.toArray(hVec)) } });
        };
      };

      let currentTradessort2 = RBTree.get(currentTrades2sort, compareRatio, oldratio);
      var currentTrades2sort2 : [{
        time : Int;
        accesscode : Text;
        amount_init : Nat;
        amount_sell : Nat;
        Fee : Nat;
        RevokeFee : Nat;
        initPrincipal : Text;
        OCname : Text;
        token_init_identifier : Text;
        token_sell_identifier : Text;
        strictlyOTC : Bool;
        allOrNothing : Bool;
      }] = switch (currentTradessort2) {
        case null { Debug.print("Didnt find the oldratio " #accesscode); [] };
        case (?foundTrades) { foundTrades };
      };
      let filtered = Array.filter<{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }>(currentTrades2sort2, func(o) { o.accesscode != accesscode });

      if (filtered.size() == 0) {
        currentTrades2sort := RBTree.delete(currentTrades2sort, compareRatio, oldratio);
      } else {
        currentTrades2sort := RBTree.put(currentTrades2sort, compareRatio, oldratio, filtered);
      };

      let currentTradessort22 = RBTree.get(currentTrades2sort, compareRatio, ratio);
      var currentTrades2sort22 : [{
        time : Int;
        accesscode : Text;
        amount_init : Nat;
        amount_sell : Nat;
        Fee : Nat;
        RevokeFee : Nat;
        initPrincipal : Text;
        OCname : Text;
        token_init_identifier : Text;
        token_sell_identifier : Text;
        strictlyOTC : Bool;
        allOrNothing : Bool;
      }] = switch (currentTradessort22) {
        case null { [] };
        case (?foundTrades) { foundTrades };
      };
      let tempVec2 = Vector.fromArray<{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }>(currentTrades2sort22);

      if (data.0 > 1 and data.1 > 1) {
        Vector.add(tempVec2, {
          accesscode = accesscode;
          amount_init = data.0;
          amount_sell = data.1;
          Fee = olddata.Fee;
          RevokeFee = olddata.RevokeFee;
          initPrincipal = data.4;
          OCname = data.5;
          time = data.6;
          token_init_identifier = data.7;
          token_sell_identifier = data.8;
          strictlyOTC = data.9;
          allOrNothing = data.10;
        });
      } else {
        removeTrade(accesscode, data.4, (data.7, data.8));
      };

      Map.set(if nonPoolOrder { liqMapSortForeign } else { liqMapSort }, hashtt, key1, RBTree.put(currentTrades2sort, compareRatio, ratio, Vector.toArray(tempVec2)));
    };
  };

  // fnction to extract when an transaction was done. If older than X days we dont accept it.
  func getTimestamp(blockData : BlockData) : Int {
    let optTimestamp = switch blockData {
      case (#ICP(data)) {
        ?data.blocks[0].timestamp.timestamp_nanos;
      };
      case (#ICRC12(transactions)) {
        if (transactions.size() == 0) {
          // Empty result means the requested block was not found (out-of-range claim
          // or offset mismatch). Return ?0 so the 21-day check rejects cleanly with
          // (false, []) instead of trapping on transactions[0] access.
          ?(0 : Nat64);
        } else {
          switch (transactions[0].transfer) {
            case (?{ created_at_time }) {
              switch (created_at_time) {
                case (?t) { ?t };
                case null { ?transactions[0].timestamp }; // fallback to top-level timestamp
              };
            };
            case null { ?transactions[0].timestamp }; // not a transfer, use top-level timestamp
          };
        };
      };
      case (#ICRC3(result)) {
        switch (result.blocks[0].block) {
          case (#Map(entries)) {
            var foundTimestamp : ?Nat64 = null;
            label timestampLoop for ((key, value) in entries.vals()) {
              if (key == "timestamp") {
                foundTimestamp := switch value {
                  case (#Nat(timestamp)) { ?Nat64.fromNat(timestamp) };
                  case (#Int(timestamp)) { ?Nat64.fromNat(Int.abs(timestamp)) };
                  case _ { null };
                };
                break timestampLoop;
              };
            };
            // If no timestamp in map entries, use block id as a signal that data exists
            // but don't fail — treat as "now" so it passes the 21-day check
            switch (foundTimestamp) {
              case (?t) { ?t };
              case null { ?Nat64.fromNat(Int.abs(Time.now())) };
            };
          };
          case _ { ?Nat64.fromNat(Int.abs(Time.now())) }; // non-Map block format, assume recent
        };
      };
    };

    let timestamp = switch optTimestamp {
      case (?t) { Int.abs(Nat64.toNat(t)) };
      case null { Int.abs(Time.now()) }; // fallback: treat as recent so it passes 21-day check (BlocksDone prevents double-spend)
    };

    timestamp;
  };

  // Keep only transactions whose index matches the requested block.
  // Archives that don't populate the index field pass through unchanged (older ledgers).
  // This guards against position-offset archives (e.g., CLOWN) returning a record at
  // a different global index than requested, which would otherwise be processed as
  // if it were the user's claimed block.
  private func filterByIndex(txs : [ICRC2.Transaction], block : Nat) : [ICRC2.Transaction] {
    Array.filter<ICRC2.Transaction>(txs, func(tx) {
      switch (tx.index) {
        case (?i) { i == block };
        case null { true };
      };
    });
  };

  private func getBlockData(token_identifier : Text, block : Nat, tType : { #ICP; #ICRC12; #ICRC3 }) : async* BlockData {

    if (token_identifier == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
      let t = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : actor {
        query_blocks : shared query { start : Nat64; length : Nat64 } -> async (LedgerType.QueryBlocksResponse);
      };
      let response = await t.query_blocks({
        start = natToNat64(block);
        length = 1;
      });

      if (response.blocks.size() > 0) {
        #ICP(response);
      } else {
        // Handle archived blocks
        switch (response.archived_blocks) {
          case (archived_blocks) {
            for (archive in archived_blocks.vals()) {
              if (block >= nat64ToNat(archive.start) and block < nat64ToNat(archive.start + archive.length)) {
                let archivedResult = await archived_blocks[0].callback({
                  start = natToNat64(block);
                  length = 1;
                });
                switch (archivedResult) {
                  case (#Ok(blockRange)) {
                    return #ICP({
                      certificate = null;
                      blocks = blockRange.blocks;
                      chain_length = 0;
                      first_block_index = natToNat64(block);
                      archived_blocks = [];
                    });
                  };
                  case (#Err(err)) {
                    throw Error.reject("Error querying archive: " # debug_show (err));
                  };
                };
              };
            };
            throw Error.reject("Block not found");
            return #ICP({
              certificate = null;
              blocks = [];
              chain_length = 0;
              first_block_index = natToNat64(block);
              archived_blocks = [];
            });
          };
        };
      };
    } else if (tType == #ICRC12) {
      let t = actor (token_identifier) : actor {
        get_transactions : shared query (ICRC2.GetTransactionsRequest) -> async (ICRC2.GetTransactionsResponse);
      };
      var didFallback = false;
      let ab = try {
        await t.get_transactions({ length = 1; start = block });
      } catch (_) {
        didFallback := true;
        // Some ledgers (e.g., CLOWN iwv6l-...) trap on any start > first_index in the
        // live chunk. Fall back with length=block+1 so the request spans archive + live:
        //  - If block is in live chunk: response returns real first_index + live records
        //    up to block (ledger caps at log_length naturally; huge length is safe).
        //  - If block is in archive: response returns sentinel first_index and the
        //    archived_transactions path below handles it.
        await t.get_transactions({ length = block + 1; start = 0 });
      };
      if (not didFallback and block > (ab.first_index + ab.log_length)) {
        throw Error.reject("Block is in future");
      };
      if (block >= ab.first_index and block < (ab.first_index + ab.log_length)) {
        #ICRC12(filterByIndex(ab.transactions, block));
      } else if (ab.archived_transactions.size() > 0) {
        // Fast path: if we already discovered this archive's position offset, jump
        // straight to the adjusted callback (skips the always-wrong direct attempt
        // and the probe). One archive call instead of three.
        switch (Map.get(tokenArchiveOffset, thash, token_identifier)) {
          case (?cachedOffset) {
            if (block < cachedOffset) {
              #ICRC12([]);
            } else {
              let adjusted = (await ab.archived_transactions[0].callback({ length = 1; start = block - cachedOffset })).transactions;
              #ICRC12(filterByIndex(adjusted, block));
            };
          };
          case null {
            // Slow path: try direct then probe-and-adjust. On success, cache the offset.
            let archiveResult = (await ab.archived_transactions[0].callback({ length = 1; start = block })).transactions;
            let filtered = filterByIndex(archiveResult, block);
            if (filtered.size() > 0) {
              #ICRC12(filtered);
            } else {
              // Archive may use position-based indexing with an offset from global indices.
              // Probe position 0 to discover the offset, then adjust the query.
              let probe = (await ab.archived_transactions[0].callback({ length = 1; start = 0 })).transactions;
              if (probe.size() > 0) {
                switch (probe[0].index) {
                  case (?firstIndex) {
                    if (block >= firstIndex) {
                      let adjusted = (await ab.archived_transactions[0].callback({ length = 1; start = block - firstIndex })).transactions;
                      let filteredAdjusted = filterByIndex(adjusted, block);
                      // Only cache the offset after filterByIndex confirms the
                      // adjusted call returned the correct global index.
                      if (filteredAdjusted.size() > 0) {
                        Map.set(tokenArchiveOffset, thash, token_identifier, firstIndex);
                      };
                      #ICRC12(filteredAdjusted);
                    } else {
                      #ICRC12([]);
                    };
                  };
                  case null { #ICRC12([]) };
                };
              } else {
                #ICRC12([]);
              };
            };
          };
        };
      } else {
        throw Error.reject("Block not found in archive");
      };
    } else {
      // ICRC3
      let t = actor (token_identifier) : ICRC3.Service;
      let result = await t.icrc3_get_blocks([{ start = block; length = 1 }]);
      if (result.blocks.size() > 0) {
        #ICRC3(result);
      } else if (result.archived_blocks.size() > 0) {
        let archivedResult = await result.archived_blocks[0].callback([{
          start = block;
          length = 1;
        }]);
        #ICRC3(archivedResult);
      } else {
        throw Error.reject("Block not found");
      };
    };
  };

  // Function that checks whether funding for buy or sell orders are sent.
  // Comments on the calculations:
  // fee = total fee to exchange if order is fulfilled, revokefee = part of total fee that is kept by exchange if order is revoked by the user
  // dao = used primarily for DAO functions, if true the amount given within this function already has precalculated the fee, the function just has to check whether that amount is received.
  // sendback = if true and more is sent than initially was passed to the function calling this function, the part that is sent too much is sent back. False primarily with DAO functions, as the DAO sends as much as it can
  // as it does not know what the exchange can handle.
  // Further Notes: in some cases you see the function addFees, this is due to the amount that has to be sent back is lower than the transferFees, meaning it can't be sent, instead that small amount is given to the exchange.
  private func checkReceive(
    block : Nat,
    caller : Principal,
    amount : Nat,
    tkn : Text,
    fee : Nat,
    revokefee : Nat,
    dao : Bool,
    sendback : Bool,
    blockData : BlockData,
    tType : { #ICP; #ICRC12; #ICRC3 },
    nowVar2 : Time,
  ) : (Bool, [(TransferRecipient, Nat, Text, Text)]) {
    let Tfees = returnTfees(tkn);

    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    func processTransaction(howMuchReceived : Nat, transferFee : Nat, from : Text, to : Text, isICP : Bool, fromSubaccount : ?Subaccount) : (Bool, [(TransferRecipient, Nat, Text, Text)]) {
      let from2 = if (isICP) Utils.accountToText(Utils.principalToAccount(caller)) else Principal.toText(caller);
      let to2 = if (isICP) Utils.accountToText(Utils.principalToAccount(treasury_principal)) else Principal.toText(treasury_principal);
      let isDefaultSubaccount = fromSubaccount == null;

      if (dao) {
        if (((isICP and Text.endsWith(from, #text from2) and Text.endsWith(to, #text to2)) or (not isICP and from == from2 and to == to2)) and howMuchReceived >= amount and isDefaultSubaccount) {
          if (sendback and howMuchReceived > amount) {
            let diff = howMuchReceived - amount;
            if (diff > transferFee) {
              let recipient : TransferRecipient = if (isDefaultSubaccount) {
                #principal(caller);
              } else {
                #accountId({ owner = caller; subaccount = fromSubaccount });
              };
              Vector.add(tempTransferQueueLocal, (recipient, diff - transferFee, tkn, genTxId()));
            } else {
              addFees(tkn, diff, false, "", nowVar2);
            };
          };
          return (true, Vector.toArray(tempTransferQueueLocal));
        } else if ((isICP and Text.endsWith(from, #text from2) and Text.endsWith(to, #text to2)) or (not isICP and from == from2 and to == to2)) {
          let recipient : TransferRecipient = if (isDefaultSubaccount) {
            #principal(caller);
          } else {
            #accountId({ owner = caller; subaccount = fromSubaccount });
          };
          if (howMuchReceived > 3 * Tfees) {
            Vector.add(tempTransferQueueLocal, (recipient, howMuchReceived - (3 * Tfees), tkn, genTxId()));
          } else {
            addFees(tkn, howMuchReceived, false, "", nowVar2);
          };
          return (false, Vector.toArray(tempTransferQueueLocal));
        };
      } else {
        if (((isICP and Text.endsWith(from, #text from2) and Text.endsWith(to, #text to2)) or (not isICP and from == from2 and to == to2)) and howMuchReceived >= ((amount * (fee + 10000)) / 10000) + transferFee and isDefaultSubaccount) {
          addFees(tkn, ((amount * fee) / (10000 * revokefee)), false, Principal.toText(caller), nowVar2);
          // Debug: track cumulative deposits
          if (test) {
            let prevDep = switch (Map.get(debugFeeTracker, thash, "dep_" # tkn)) { case (?v) v; case null 0 };
            Map.set(debugFeeTracker, thash, "dep_" # tkn, prevDep + howMuchReceived);
          };

          if (howMuchReceived > (((amount * (fee + 10000)) / 10000) + transferFee)) {
            let diff = howMuchReceived - ((amount * (fee + 10000)) / 10000) - transferFee;
            if (diff > transferFee) {
              let recipient : TransferRecipient = if (isDefaultSubaccount) {
                #principal(caller);
              } else {
                #accountId({ owner = caller; subaccount = fromSubaccount });
              };
              Vector.add(tempTransferQueueLocal, (recipient, diff - transferFee, tkn, genTxId()));
            } else {
              addFees(tkn, diff, false, "", nowVar2);
            };
          };
          return (true, Vector.toArray(tempTransferQueueLocal));
        } else if ((isICP and Text.endsWith(from, #text from2) and Text.endsWith(to, #text to2)) or (not isICP and from == from2 and to == to2)) {
          let recipient : TransferRecipient = if (isDefaultSubaccount) {
            #principal(caller);
          } else {
            #accountId({ owner = caller; subaccount = fromSubaccount });
          };
          if (howMuchReceived > (3 * Tfees)) {
            Vector.add(tempTransferQueueLocal, (recipient, howMuchReceived - (3 * Tfees), tkn, genTxId()));
          } else {
            addFees(tkn, howMuchReceived, false, "", nowVar2);
          };

          return (false, Vector.toArray(tempTransferQueueLocal));
        };
      };
      Map.delete(BlocksDone, thash, tkn # ":" # Nat.toText(block));

      return (false, Vector.toArray(tempTransferQueueLocal));
    };



    // Check if the transaction is not older than 21 days
    let timestamp = getTimestamp(blockData);
    if (timestamp == 0) {

      return (false, []);
    } else {
      let currentTime = Int.abs(nowVar2);
      let timeDiff : Int = currentTime - timestamp;
      if (timeDiff > 1814400000000000) {
        // 21 days in nanoseconds

        return (false, []);
      };
    };

    switch (tType, blockData) {
      case (#ICP, #ICP(ac)) {
        if (tkn == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
          for ({ transaction = { operation } } in ac.blocks.vals()) {
            var howMuchReceived : Nat64 = 0;
            var check_fee : Nat64 = 0;
            var check_from : Text = "";
            var check_to : Text = "";
            var fromSubaccount : ?Subaccount = null;

            switch (operation) {
              case (? #Transfer({ amount = { e8s = amounte8s }; fee = { e8s = fee8s }; from; to })) {
                howMuchReceived := amounte8s;
                check_fee := fee8s;
                check_from := Utils.accountToText({ hash = from });
                check_to := Utils.accountToText({ hash = to });
              };
              case (_) {};
            };

            return processTransaction(nat64ToNat(howMuchReceived), nat64ToNat(check_fee), check_from, check_to, true, fromSubaccount);
          };
        };
      };
      case (#ICRC12, #ICRC12(transactions)) {
        for ({ transfer = ?{ to; fee; from; amount } } in transactions.vals()) {
          var fees : Nat = 0;
          var sub : ?Subaccount = if (from.subaccount == null) { null } else {
            ?Blob.fromArray(switch (from.subaccount) { case (?a) { a } });
          };
          switch (fee) {
            case null {};
            case (?fees2) { fees := fees2 };
          };

          return processTransaction(amount, fees, Principal.toText(from.owner), Principal.toText(to.owner), false, sub);
        };
      };
      case (#ICRC3, #ICRC3(result)) {
        for (blockie in result.blocks.vals()) {
          switch (blockie.block) {
            case (#Map(outerEntries)) {
              var to : ?ICRC1.Account = null;
              var fee : ?Nat = null;
              var from : ?ICRC1.Account = null;
              var howMuchReceived : ?Nat = null;

              // ICRC3 blocks may nest tx fields inside a "tx" sub-map; merge both levels
              let entries = Buffer.fromArray<(Text, ICRC3.Value)>(outerEntries);
              for ((k, v) in outerEntries.vals()) {
                if (k == "tx") {
                  switch (v) {
                    case (#Map(txEntries)) {
                      for (entry in txEntries.vals()) { entries.add(entry) };
                    };
                    case _ {};
                  };
                };
              };

              for ((key, value) in entries.vals()) {
                switch (key) {
                  case "to" {
                    switch (value) {
                      case (#Array(toArray)) {
                        if (toArray.size() >= 1) {
                          switch (toArray[0]) {
                            case (#Blob(owner)) {
                              to := ?{
                                owner = Principal.fromBlob(owner);
                                subaccount = if (toArray.size() > 1) {
                                  switch (toArray[1]) {
                                    case (#Blob(subaccount)) { ?subaccount };
                                    case _ { null };
                                  };
                                } else {
                                  null // Default subaccount when only principal is provided
                                };
                              };
                            };
                            case _ {};
                          };
                        };
                      };
                      case (#Blob(owner)) {
                        to := ?{
                          owner = Principal.fromBlob(owner);
                          subaccount = null;
                        };
                      };
                      case _ {};
                    };
                  };
                  case "fee" {
                    switch (value) {
                      case (#Nat(f)) { fee := ?f };
                      case (#Int(f)) { fee := ?Int.abs(f) };
                      case _ {};
                    };
                  };
                  case "from" {
                    switch (value) {
                      case (#Array(fromArray)) {
                        if (fromArray.size() >= 1) {
                          switch (fromArray[0]) {
                            case (#Blob(owner)) {
                              from := ?{
                                owner = Principal.fromBlob(owner);
                                subaccount = if (fromArray.size() > 1) {
                                  switch (fromArray[1]) {
                                    case (#Blob(subaccount)) { ?subaccount };
                                    case _ { null };
                                  };
                                } else {
                                  null;
                                };
                              };
                            };
                            case _ {};
                          };
                        };
                      };
                      case _ {};
                    };
                  };
                  case "amt" {
                    switch (value) {
                      case (#Nat(amt)) { howMuchReceived := ?amt };
                      case (#Int(amt)) { howMuchReceived := ?Int.abs(amt) };
                      case _ {};
                    };
                  };
                  case _ {};
                };
              };

              switch (to, fee, from, howMuchReceived) {
                case (?to, ?fee, ?from, ?howMuchReceived) {
                  return processTransaction(howMuchReceived, fee, Principal.toText(from.owner), Principal.toText(to.owner), false, from.subaccount);
                };
                case _ {
                  Map.delete(BlocksDone, thash, tkn # ":" # Nat.toText(block));
                  return (false, Vector.toArray(tempTransferQueueLocal));
                };
              };
            };
            case _ {
              Map.delete(BlocksDone, thash, tkn # ":" # Nat.toText(block));
              return (false, Vector.toArray(tempTransferQueueLocal));
            };
          };
        };
      };
      case _ {};
    };

    (false, Vector.toArray(tempTransferQueueLocal));
  };

  // Function that adds or deletes fees to the registry. Even before an order is fulfilled, fees get added. This is the revokeFee.
  // Debug: track cumulative fees added per token (test mode only)
  transient let debugFeeTracker = Map.new<Text, Nat>();

  // Test-mode drift instrumentation: per-op, per-token cumulative tracked-side delta.
  // Key: "opName:token" → signed sum of what we intended to add/remove from treasury balance.
  // Compared against actual balance deltas, the op with the largest mismatch is the leak.
  transient let driftOpTracker = Map.new<Text, Int>();

  private func recordOpDrift(op : Text, token : Text, trackedDelta : Int) {
    if (not test) return;
    let key = op # ":" # token;
    let cur = switch (Map.get(driftOpTracker, thash, key)) { case (?v) v; case null 0 : Int };
    Map.set(driftOpTracker, thash, key, cur + trackedDelta);
  };

  public query ({ caller }) func getDriftOpTracker() : async [(Text, Int)] {
    if (not ownercheck(caller)) return [];
    Iter.toArray(Map.entries(driftOpTracker));
  };

  public shared ({ caller }) func resetDriftOpTracker() : async () {
    if (not ownercheck(caller)) return;
    for (k in Iter.toArray(Map.keys(driftOpTracker)).vals()) {
      Map.delete(driftOpTracker, thash, k);
    };
  };

  private func addFees(
    token : Text,
    amount : Nat,
    delfees : Bool,
    user : Text,
    nowVar : Time,
  ) : () {
    let currentFee : Nat = switch (Map.get(feescollectedDAO, thash, token)) {
      case (?v) v;
      case null 0;
    };

    if delfees {
      let newFee = if (amount <= currentFee) { currentFee - amount } else { 0 };
      Map.set(feescollectedDAO, thash, token, newFee);
    } else {
      let feeAmount = if (amount > 0) { amount - 1 } else { 0 };

      // Debug tracking
      if (test) {
        let prev = switch (Map.get(debugFeeTracker, thash, token)) { case (?v) v; case null 0 };
        Map.set(debugFeeTracker, thash, token, prev + amount);
      };

      // Check if the user has a referrer
      switch (Map.get(userReferrerLink, thash, user)) {

        case (null) {
          // If no referrer or referrer link is null, all fees go to DAO
          Map.set(userReferrerLink, thash, user, null);
          Map.set(feescollectedDAO, thash, token, currentFee + feeAmount);
        };
        case (??referrer) {
          // Calculate referral fee. Note: `ReferralFees` is read LIVE from the global, NOT
          // snapshotted per order. This is intentional — referrer rates apply at time of fill,
          // not time of order creation. Admin changes to ReferralFees affect all in-flight fills
          // from the moment of the change. This is inconsistent with Fee/RevokeFee (which ARE
          // snapshotted on orders) but matches product intent: referrer agreements are not
          // contract-like per-order commitments.
          let referralAmount = (feeAmount * ReferralFees) / 100;
          let daoAmount = feeAmount - referralAmount;

          // Update DAO fees
          Map.set(feescollectedDAO, thash, token, currentFee + daoAmount);

          // Update referrer fees
          switch (Map.get(referrerFeeMap, thash, referrer)) {
            case (??(fees, oldTime)) {
              let updatedFees = Vector.new<(Text, Nat)>();
              var found = false;
              for ((t, a) in Vector.vals(fees)) {
                if (t == token) {
                  Vector.add(updatedFees, (t, a + referralAmount));
                  found := true;
                } else {
                  Vector.add(updatedFees, (t, a));
                };
              };
              if (not found) {
                Vector.add(updatedFees, (token, referralAmount));
              };
              Map.set(referrerFeeMap, thash, referrer, ?(updatedFees, nowVar));

              // Update lastFeeAdditionByTime
              lastFeeAdditionByTime := RBTree.put(
                RBTree.delete(lastFeeAdditionByTime, compareTextTime, (referrer, oldTime)),
                compareTextTime,
                (referrer, nowVar),
                null,
              );
            };
            case (?null) {
              let newFees = Vector.new<(Text, Nat)>();
              Vector.add(newFees, (token, referralAmount));
              Map.set(referrerFeeMap, thash, referrer, ?(newFees, nowVar));
              lastFeeAdditionByTime := RBTree.put(lastFeeAdditionByTime, compareTextTime, (referrer, nowVar), null);
            };
            case (null) {
              let newFees = Vector.new<(Text, Nat)>();
              Vector.add(newFees, (token, referralAmount));
              Map.set(referrerFeeMap, thash, referrer, ?(newFees, nowVar));
              lastFeeAdditionByTime := RBTree.put(lastFeeAdditionByTime, compareTextTime, (referrer, nowVar), null);
            };
          };
        };
        case (?null) {
          Map.set(userReferrerLink, thash, user, null);
          // If no referrer or referrer link is null, all fees go to DAO
          Map.set(feescollectedDAO, thash, token, currentFee + feeAmount);
        };
      };
    };
  };
  // This function is called every time transfers are being done to update data for the FE.
  private func doInfoBeforeStep2() {
    // Sync V3 state after trades — orderbook fills change reserves without updating V3
    recalculateAllActiveLiquidity();

    let ammReserves0 = Vector.new<Nat>();
    let ammReserves1 = Vector.new<Nat>();

    for (pair in Vector.vals(pool_canister)) {
      switch (Map.get(AMMpools, hashtt, pair)) {
        case (?pool) {
          Vector.add(ammReserves0, pool.reserve0);
          Vector.add(ammReserves1, pool.reserve1);
        };
        case (null) {
          Vector.add(ammReserves0, 0);
          Vector.add(ammReserves1, 0);
        };
      };
    };

    AllExchangeInfo := {
      AllExchangeInfo with
      last_traded_price = Vector.toArray(last_traded_price);
      price_day_before = Vector.toArray(price_day_before);
      volume_24h = volume_24hArray;
      amm_reserve0 = Vector.toArray(ammReserves0);
      amm_reserve1 = Vector.toArray(ammReserves1);
    };
  };

  // Called only when token metadata changes (addAcceptedToken, removeToken, upgrade)
  private func updateStaticInfo() {
    AllExchangeInfo := {
      AllExchangeInfo with
      pool_canister = Vector.toArray(pool_canister);
      asset_names = Vector.toArray(asset_names);
      asset_symbols = Vector.toArray(asset_symbols);
      asset_decimals = Vector.toArray(asset_decimals);
      asset_transferfees = Vector.toArray(asset_transferfees);
      asset_minimum_amount = Vector.toArray(asset_minimum_amount);
    };
  };

  // Function to handle trade revocation for DAO, Seller, and Initiator
  public shared ({ caller }) func revokeTrade(
    accesscode : Text,
    revokeType : { #DAO : [Text]; #Seller; #Initiator },
  ) : async ExTypes.RevokeResult {
    if (isAllowed(caller) != 1) {
      return #Err(#NotAuthorized);
    };
    if (Text.size(accesscode) > 150) {
      return #Err(#Banned);
    };
    let isDAO = switch (revokeType) { case (#DAO(_)) true; case _ false };
    if ((isDAO and not DAOcheck(caller)) or (not isDAO and isAllowed(caller) != 1)) {

      return #Err(#NotAuthorized);
    };

    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var endmessage = "";

    func processTrade(accesscode : Text) {
      let pub = Text.startsWith(accesscode, #text "Public");

      // excludeDAO = if pub==false, and the position is private, the maker has the option to whether the DAO can access the order or not when it trades.
      let excludeDAO = (Text.endsWith(accesscode, #text "excl") and not pub);
      var currentTrades2 = switch (Map.get(if (pub) tradeStorePublic else tradeStorePrivate, thash, accesscode)) {
        case null return;
        case (?(foundTrades)) foundTrades;
      };

      if (currentTrades2.trade_done != 0 or currentTrades2.token_init_identifier == "0") return;

      if (not isDAO) {
        assert (currentTrades2.trade_done == 0);
        assert (Principal.fromText(if (revokeType == #Seller) currentTrades2.SellerPrincipal else currentTrades2.initPrincipal) == caller);
      };
      tradesBeingWorkedOn := TrieSet.put(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);

      let RevokeFee = currentTrades2.RevokeFee;

      if (not excludeDAO) {
        replaceLiqMap(
          true,
          false,
          currentTrades2.token_init_identifier,
          currentTrades2.token_sell_identifier,
          accesscode,
          (currentTrades2.amount_init, currentTrades2.amount_sell, 0, 0, "", currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing),
          #Zero,
          null,
          null,
        );
      };

      // Process seller and init payments, if paid they get sent back
      for (
        (paid, amount, token, principal) in [
          (currentTrades2.seller_paid, currentTrades2.amount_sell, currentTrades2.token_sell_identifier, currentTrades2.SellerPrincipal),
          (currentTrades2.init_paid, currentTrades2.amount_init, currentTrades2.token_init_identifier, currentTrades2.initPrincipal),
        ].vals()
      ) {
        if (paid == 1) {
          let refundAmount = amount + (((amount * currentTrades2.Fee) / (10000 * RevokeFee)) * (RevokeFee - 1));
          Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(principal)), refundAmount, token, genTxId()));
        };
      };

      // If this order was compensated during an intermediate multi-hop fill,
      // deduct the Tfees from feescollectedDAO since the cancel's refund consumes it.
      removeTrade(accesscode, currentTrades2.initPrincipal, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));
    };

    // SECURITY FIX: pre-check ownership at the outer level so an unauthorized
    // revoke returns a clean #Err instead of trapping via the asserts inside
    // processTrade. Funds were always safe (state rolls back on trap) but the
    // trap made debugging hard and surfaced as a generic canister error.
    switch (revokeType) {
      case (#Seller or #Initiator) {
        let pub = Text.startsWith(accesscode, #text "Public");
        switch (Map.get(if (pub) tradeStorePublic else tradeStorePrivate, thash, accesscode)) {
          case null { return #Err(#OrderNotFound(accesscode)) };
          case (?trade) {
            if (trade.trade_done != 0 or trade.token_init_identifier == "0") {
              return #Err(#OrderNotFound("Trade already done or invalid"));
            };
            let ownerText = if (revokeType == #Seller) trade.SellerPrincipal else trade.initPrincipal;
            switch (PrincipalExt.fromText(ownerText)) {
              case null { return #Err(#NotAuthorized) };
              case (?owner) {
                if (owner != caller) { return #Err(#NotAuthorized) };
              };
            };
          };
        };
      };
      case (#DAO(_)) {}; // already authorized via DAOcheck above
    };

    switch (revokeType) {
      case (#DAO(accesscodeArray)) {
        for (accesscode in accesscodeArray.vals()) { processTrade(accesscode) };
      };
      case (#Seller or #Initiator) { processTrade(accesscode) };
    };

    doInfoBeforeStep2();


    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {
      // tempTransferQueue := Vector.new<(TransferRecipient , Nat, Text)>();
    } else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };
    tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);

    switch (revokeType) {
      case (#DAO(_)) #Ok({ accessCode = ""; revokeType = #DAO; refunds = [] });
      case (#Seller) #Ok({ accessCode = accesscode; revokeType = #Seller; refunds = [] });
      case (#Initiator) #Ok({ accessCode = accesscode; revokeType = #Initiator; refunds = [] });
    };
  };

  type PositionData = {
    accesscode : Text;
    ICPprice : (Nat, Nat);
    decimals : (Nat, Nat);
  };

  // Define a new type for the output
  type RecalibratedPosition = {
    poolId : (Text, Text);
    accesscode : Text;
    amountInit : Nat;
    amountSell : Nat;
    fee : Nat;
    revokeFee : Nat;
  };

  // This function is called by the DAO to periodically let it know how its stands with the orders made by it. It also changes the orders of the DAO considering the current pricing of the assets.
  public shared ({ caller }) func recalibrateDAOpositions(positions : [PositionData]) : async [RecalibratedPosition] {
    if (not DAOcheck(caller)) {

      return [];
    };





    let recalibratedPositions = Vector.new<RecalibratedPosition>();


    label a for (position in positions.vals()) {


      var currentTrades2 : TradePrivate = Faketrade;


      let kk = Map.get(tradeStorePublic, thash, position.accesscode);
      switch (kk) {
        case null {

          continue a;
        };
        case (?(foundTrades)) {
          currentTrades2 := foundTrades;


        };
      };

      if (currentTrades2 != Faketrade and currentTrades2.trade_done == 0) {


        // Validate ICPprices and Decimals
        if (position.ICPprice.0 == 0 or position.ICPprice.1 == 0 or position.decimals.0 == 0 or position.decimals.1 == 0) {



          continue a;
        };



        let currentTrades22 = {
          currentTrades2 with
          amount_sell = (((((currentTrades2.amount_init * (tenToPower30)) / (10 ** position.decimals.0)) * position.ICPprice.0) / position.ICPprice.1) * (10 ** position.decimals.1)) / (tenToPower30);
          trade_done = 0;
        };



        addTrade(
          position.accesscode,
          currentTrades22.initPrincipal,
          currentTrades22,
          (currentTrades22.token_init_identifier, currentTrades22.token_sell_identifier),
        );



        replaceLiqMap(
          false,
          true,
          currentTrades2.token_init_identifier,
          currentTrades2.token_sell_identifier,
          position.accesscode,
          (
            currentTrades22.amount_init,
            currentTrades22.amount_sell,
            currentTrades22.Fee,
            currentTrades22.RevokeFee,
            currentTrades22.initPrincipal,
            currentTrades22.OCname,
            currentTrades22.time,
            currentTrades22.token_init_identifier,
            currentTrades22.token_sell_identifier,
            currentTrades22.strictlyOTC,
            currentTrades22.allOrNothing,
          ),
          #Value(((currentTrades2.amount_init) * tenToPower60) / (currentTrades2.amount_sell)),
          ?{
            Fee = currentTrades2.Fee;
            RevokeFee = currentTrades2.RevokeFee;
          },
          null,
        );



        Vector.add(
          recalibratedPositions,
          {
            poolId = (currentTrades22.token_init_identifier, currentTrades22.token_sell_identifier);
            accesscode = position.accesscode;
            amountInit = currentTrades22.amount_init;
            amountSell = currentTrades22.amount_sell;
            fee = currentTrades22.Fee;
            revokeFee = currentTrades22.RevokeFee;
          },
        );

      } else {

      };
    };


    doInfoBeforeStep2();





    Vector.toArray(recalibratedPositions);
  };

  //Function that gives the DAO all the tokens metadata. This is done as its cheaper to scrape this data only from one canister, and the exchange always has to accept the tokens accepted in the DAO.
  public query ({ caller }) func sendDAOInfo() : async [(Text, { TransferFee : Nat; Decimals : Nat; Name : Text; Symbol : Text })] {
    if (not DAOcheck(caller)) {
      return [];
    };
    return tokenInfoARR;
  };

  // This function is very important for the DAO. The DAO calls this function with the funding it needs.
  // 1. First getAllTradesDAOFilter is called, which goes through all the trading pools the OTC canister has and checks which trades could fulfill the need of the DAO.
  // These trades are sent back, alongside the number of assets that couldnt be fulfilled by the exchange.

  // 2. It checks whether it has received all the funds needed. Atm the DAO sends all funds it wants to trade . The exchange sends the funds back that cant be traded (- transaction fees) or it reates orders with it if it is ordered to.

  // 3. If it sees some transactions are not received, it stops the hole function. This is because of the way the different assets are all intertwined with each other and each asset can be part of multiple liquidity pools.
  // If not enough is sent by the DAO everything is also  sent back to it.

  // 4. the OTC exchange sends all the funds the DAO asked for after checking it received the collateral. If something fails to be sent, it gets saved and the DAO will be able to retrieve it by calling a certain retrieveFundsDao().

  // 5. In this part the trade creators get the asset they were trying to buy. If something fails it can always be retrieved. A big difference in procedure is when an order is partially done or fully.
  // if partially the order keeps being there as there are more funcs to be sold. If done fully, the trade gets deleted from existance as its done.

  // 6. The function is done and it sends back data about the funds that could not be retrieved from the OTC and will have to be gotten from a third party exchange. If it ordered to make orders with the leftovers, it also does that.

  type TradeData = {
    identifier : Text;
    amountBuy : Nat;
    amountSell : Nat;
    ICPPrice : Nat;
    decimals : Nat;
    block : Nat64;
    transferFee : Nat;
  };

  type ProcessedTrade = {
    identifier : Text;
    amountBought : Nat;
    amountSold : Nat;
  };

  type BatchProcessResult = {
    execMessage : Text;
    processedTrades : [ProcessedTrade];
    accesscodes : [{
      poolId : (Text, Text);
      accesscode : Text;
      amountInit : Nat;
      amountSell : Nat;
      fee : Nat;
      revokeFee : Nat;
    }];
  };

  type FilteredTradeResult = {
    trades : [TradeEntry];
    amounts : [TradeAmount];
    logging : Text;
  };

  type TradeAmount = {
    identifier : Text;
    amountBought : Nat;
    amountSold : Nat;
    transferFee : Nat;
    feesSell : Nat;
    feesBuy : Nat;
    timesTFees : Nat;
    representationPositionMaker : [(Text, Nat)];
  };

  transient var currentRunIdFinishSellBatchDAO = 0;
  transient var loggingMapFinishSellBatchDAO = Map.new<Nat, Text>();

  // DAO treasury swap: auto-routes, no separate quote call needed, no trading fee for DAO
  public shared ({ caller }) func treasurySwap(
    tokenIn : Text, tokenOut : Text,
    amountIn : Nat, minAmountOut : Nat,
    block : Nat,
  ) : async ExTypes.SwapResult {
    if (not DAOcheck(caller)) return #Err(#NotAuthorized);
    if (not containsToken(tokenIn) or not containsToken(tokenOut)) return #Err(#TokenNotAccepted("Token not accepted"));
    if (tokenIn == tokenOut) return #Err(#InvalidInput("Same token"));

    let nowVar = Time.now();
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    // Block validation
    if (Map.has(BlocksDone, thash, tokenIn # ":" # Nat.toText(block))) {
      return #Err(#InvalidInput("Block already used"));
    };
    Map.set(BlocksDone, thash, tokenIn # ":" # Nat.toText(block), nowVar);
    let tType = returnType(tokenIn);

    let blockData = try { await* getBlockData(tokenIn, block, tType) } catch (_) {
      Map.delete(BlocksDone, thash, tokenIn # ":" # Nat.toText(block));
      return #Err(#SystemError("Failed to get block data"));
    };

    // checkReceive with dao=true — simpler validation, no fee in deposit required
    let (receiveBool, receiveTransfers) = checkReceive(block, caller, amountIn, tokenIn, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar);
    Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());
    if (not receiveBool) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InsufficientFunds("Funds not received"));
    };

    // Find best route internally (FREE — no inter-canister call)
    let routes = findRoutes(tokenIn, tokenOut, amountIn);
    let bestRoute = if (routes.size() > 0 and routes[0].hops.size() >= 1) {
      routes[0].hops;
    } else {
      [{ tokenIn = tokenIn; tokenOut = tokenOut }];
    };

    // Execute hops via orderPairing
    // Pre-check removed: simulateMultiHop mutates AMMpools in update context,
    // causing actual execution to run against wrong reserves. Post-execution
    // slippage check at line ~9715 handles failures safely.
    var currentAmount = amountIn;
    label hopLoop for (hopIndex in Iter.range(0, bestRoute.size() - 1)) {
      let hop = bestRoute[hopIndex];
      let isLastHop = hopIndex + 1 == bestRoute.size();

      let syntheticTrade : TradePrivate = {
        Fee = ICPfee; // DAO pays LP fees (70% to LPs via AMM), no exchange trading fee
        amount_sell = 1; amount_init = currentAmount;
        token_sell_identifier = hop.tokenOut;
        token_init_identifier = hop.tokenIn;
        trade_done = 0; seller_paid = 0; init_paid = 1;
        seller_paid2 = 0; init_paid2 = 0; trade_number = 0;
        SellerPrincipal = "0";
        initPrincipal = Principal.toText(caller);
        RevokeFee = RevokeFeeNow; OCname = ""; time = nowVar;
        filledInit = 0; filledSell = 0;
        allOrNothing = false; strictlyOTC = false;
      };

      let (_, _, _, transfers, _, _, _) = orderPairing(syntheticTrade);
      var hopOutput : Nat = 0;
      for (tx in transfers.vals()) {
        if (tx.0 == #principal(caller) and tx.2 == hop.tokenOut) {
          hopOutput += tx.1;
          if (isLastHop) {
            Vector.add(tempTransferQueueLocal, tx);
          };
        } else {
          Vector.add(tempTransferQueueLocal, tx);
        };
      };

      // Handle unfilled portion on first hop
      let remaining = safeSub(currentAmount, hopOutput);
      if (remaining > returnTfees(hop.tokenIn) and hopIndex == 0) {
        Vector.add(tempTransferQueueLocal, (#principal(caller), remaining, hop.tokenIn, genTxId()));
      };

      currentAmount := hopOutput;
      if (currentAmount == 0) break hopLoop;

      // Add back transfer fee for intermediate hops
      if (not isLastHop) {
        currentAmount += returnTfees(hop.tokenOut);
      };
    };

    // Record in swap history
    let routeVec = Vector.new<Text>();
    Vector.add(routeVec, tokenIn);
    for (hop in bestRoute.vals()) { Vector.add(routeVec, hop.tokenOut) };
    nextSwapId += 1;
    recordSwap(caller, {
      swapId = nextSwapId; tokenIn; tokenOut;
      amountIn; amountOut = currentAmount;
      route = Vector.toArray(routeVec);
      fee = 0;
      swapType = #direct;
      timestamp = nowVar;
    });

    // Send all transfers
    doInfoBeforeStep2();
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };

    if (currentAmount < minAmountOut) {
      return #Err(#SlippageExceeded({ expected = minAmountOut; got = currentAmount }));
    };

    #Ok({
      amountIn = amountIn;
      amountOut = currentAmount;
      tokenIn = tokenIn;
      tokenOut = tokenOut;
      route = Vector.toArray(routeVec);
      fee = 0;
      swapId = nextSwapId;
      hops = bestRoute.size();
      firstHopOrderbookMatch = false;
      lastHopAMMOnly = false;
    });
  };

  public shared (msg) func FinishSellBatchDAO(
    trades : [TradeData],
    createOrdersIfNotDone : Bool,
    special : [Nat],
  ) : async ?BatchProcessResult {
    let logEntries = Vector.new<Text>();
    let runId = currentRunIdFinishSellBatchDAO;
    currentRunIdFinishSellBatchDAO += 1;

    func logWithRunId(message : Text) {
      Vector.add(logEntries, "FinishSellBatchDAO- " # message);
    };

    // Original authorization check
    if (not DAOcheck(msg.caller)) {
      logWithRunId("Unauthorized caller");
      return null;
    };

    // Original makeOrders logic
    var makeOrders = true;
    if createOrdersIfNotDone { makeOrders := true };

    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    // Debug logging of input parameters
    if verboseLogging {
      logWithRunId("-------");
      for (trade in trades.vals()) {
        logWithRunId("Trade: " # debug_show (trade));
      };
      logWithRunId("-------");
    };

    // Original block processing logic
    let nowVar = Time.now();
    for (i in Iter.range(0, trades.size() -1)) {
      if (trades[i].amountSell > 0) {
        if (Map.has(BlocksDone, thash, trades[i].identifier # ":" # Nat64.toText(trades[i].block))) { throw Error.reject("Block already processed") };
        Map.set(BlocksDone, thash, trades[i].identifier # ":" # Nat64.toText(trades[i].block), nowVar);
      };
    };

    // Original failure tracking
    var failReceiving = false;
    var fundsToSendBackIfFailVector = Vector.new<(Text, Nat)>();
    var whenError = 0;

    try {
      for (i in Iter.range(0, trades.size() -1)) {
        if (trades[i].amountSell != 0) {
          var failReceiving2 = false;
          let tType = returnType(trades[i].identifier);

          let blockData = try {
            await* getBlockData(trades[i].identifier, nat64ToNat(trades[i].block), tType);
          } catch (err) {
            logWithRunId("Block data error: " # Error.message(err));
            #ICRC12([]);
          };

          let (receiveBool, receiveTransfers) = if (blockData != #ICRC12([])) {
            checkReceive(
              nat64ToNat(trades[i].block),
              DAOTreasury,
              if failReceiving { tenToPower200 } else {
                trades[i].amountSell + trades[i].transferFee;
              },
              trades[i].identifier,
              ICPfee,
              RevokeFeeNow,
              true,
              (makeOrders == false),
              blockData,
              tType,
              nowVar,
            );
          } else { (false, []) };

          Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());

          if (not receiveBool) {
            failReceiving := true;
            failReceiving2 := true;
          };

          if (failReceiving == false) {
            Vector.add(fundsToSendBackIfFailVector, (trades[i].identifier, trades[i].amountSell));
          };
          whenError += 1;
        };
      };
    } catch (err) {
      logWithRunId("Error in receiving process: " # Error.message(err));
      let fundsToSendBackIfFail = Vector.toArray(fundsToSendBackIfFailVector);
      for (i in Array.vals(fundsToSendBackIfFail)) {
        Vector.add(tempTransferQueueLocal, (#principal(DAOTreasury), i.1, i.0, genTxId()));
      };

      for (i in Iter.range(whenError, trades.size() -1)) {
        if (trades[i].amountSell > 0) {
          Vector.add(tempTransferQueueLocal, (#principal(DAOTreasury), trades[i].amountSell, trades[i].identifier, genTxId()));
        };
      };

      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {
        logWithRunId("Successfully transferred funds back to treasury");
      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return ?{
        execMessage = "Fail receiving";
        processedTrades = [];
        accesscodes = [];
      };
    };

    if (failReceiving) {
      let fundsToSendBackIfFail = Vector.toArray(fundsToSendBackIfFailVector);
      for (i in Array.vals(fundsToSendBackIfFail)) {
        Vector.add(tempTransferQueueLocal, (#principal(DAOTreasury), i.1, i.0, genTxId()));
      };

      for (i in Iter.range(whenError, trades.size() -1)) {
        if (trades[i].amountSell > 0) {
          Vector.add(tempTransferQueueLocal, (#principal(DAOTreasury), trades[i].amountSell, trades[i].identifier, genTxId()));
        };
      };

      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {
        logWithRunId("Successfully transferred funds back to treasury");
      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return ?{
        execMessage = "Fail receiving";
        processedTrades = [];
        accesscodes = [];
      };
    };
    // Start trade processing
    let tradeResult = getAllTradesDAOFilter(trades);
    logWithRunId("Finished getAllTradesDAOFilter");
    Vector.add(logEntries, tradeResult.logging);

    for (t in tradeResult.trades.vals()) {
      tradesBeingWorkedOn := TrieSet.put(tradesBeingWorkedOn, t.accesscode, Text.hash(t.accesscode), Text.equal);
    };

    logWithRunId("Processing fees");
    for (i in Iter.range(0, tradeResult.amounts.size() -1)) {
      if (tradeResult.amounts[i].representationPositionMaker.size() > 0) {
        if (failReceiving == false) {
          for ((positionmaker, amount) in tradeResult.amounts[i].representationPositionMaker.vals()) {
            addFees(tradeResult.amounts[i].identifier, amount, false, positionmaker, nowVar);
          };
        };
      };
    };
    logWithRunId("Fees collected: " # debug_show (feescollectedDAO));
    logWithRunId("Entering step 4: managing entries");

    label r for (i in Iter.range(0, tradeResult.amounts.size() -1)) {
      var error = 0;

      if (tradeResult.amounts[i].amountBought > 0) {
        if (tradeResult.amounts[i].timesTFees != 0) {
          logWithRunId("2740" # debug_show (tradeResult.amounts[i].amountBought + ((tradeResult.amounts[i].timesTFees - 1) * tradeResult.amounts[i].transferFee)));
          Vector.add(tempTransferQueueLocal, (#principal(DAOTreasury), tradeResult.amounts[i].amountBought + ((tradeResult.amounts[i].timesTFees - 1) * tradeResult.amounts[i].transferFee), tradeResult.amounts[i].identifier, genTxId()));
        } else if (tradeResult.amounts[i].amountBought > (tradeResult.amounts[i].transferFee)) {
          logWithRunId("2743" # debug_show (DAOTreasury, tradeResult.amounts[i].amountBought - (tradeResult.amounts[i].transferFee), tradeResult.amounts[i].identifier));
          Vector.add(tempTransferQueueLocal, (#principal(DAOTreasury), tradeResult.amounts[i].amountBought - (tradeResult.amounts[i].transferFee), tradeResult.amounts[i].identifier, genTxId()));
        } else {
          addFees(tradeResult.amounts[i].identifier, tradeResult.amounts[i].amountBought, false, "", nowVar);
        };
      };
    };

    logWithRunId("Entering step 5: managing trade entries:\n" #debug_show (tradeResult.trades));
    for (i in Iter.range(0, tradeResult.trades.size() -1)) {
      let pub = Text.startsWith(tradeResult.trades[i].accesscode, #text "Public");
      var currentTrades2 : TradePrivate = Faketrade;
      if (pub) {
        let currentTrades = Map.get(tradeStorePublic, thash, tradeResult.trades[i].accesscode);
        switch (currentTrades) {
          case null {};
          case (?(foundTrades)) {
            currentTrades2 := foundTrades;
          };
        };
      } else {
        let currentTrades = Map.get(tradeStorePrivate, thash, tradeResult.trades[i].accesscode);
        switch (currentTrades) {
          case null {};
          case (?(foundTrades)) {
            currentTrades2 := foundTrades;
          };
        };
      };
      // Calculate the ratio before the trade was partially filled
      let oldRatio = #Value((currentTrades2.amount_init * tenToPower60) / currentTrades2.amount_sell);

      if (tradeResult.trades[i].amount_init < currentTrades2.amount_init -1) {
        Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(tradeResult.trades[i].InitPrincipal)), tradeResult.trades[i].amount_sell, tradeResult.trades[i].token_sell_identifier, genTxId()));

        // Update the trade
        currentTrades2 := {
          currentTrades2 with
          amount_sell = currentTrades2.amount_sell - tradeResult.trades[i].amount_sell;
          amount_init = currentTrades2.amount_init - tradeResult.trades[i].amount_init;
          trade_done = 0;
          seller_paid = 0;
          init_paid = 1;
          seller_paid2 = 0;
          init_paid2 = 0;
          filledInit = currentTrades2.filledInit + tradeResult.trades[i].amount_init;
          filledSell = currentTrades2.filledSell + tradeResult.trades[i].amount_sell;
        };

        // First, update the trade in storage
        addTrade(tradeResult.trades[i].accesscode, currentTrades2.initPrincipal, currentTrades2, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));

        // Then update the liquidity map with the correct ratio
        replaceLiqMap(
          false,
          true,
          currentTrades2.token_init_identifier,
          currentTrades2.token_sell_identifier,
          tradeResult.trades[i].accesscode,
          (currentTrades2.amount_init, currentTrades2.amount_sell, 0, 0, currentTrades2.initPrincipal, currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing),
          oldRatio, // Use the original ratio from before the partial fill
          ?{
            Fee = currentTrades2.Fee;
            RevokeFee = currentTrades2.RevokeFee;
          },
          ?{
            amount_init = tradeResult.trades[i].amount_init;
            amount_sell = tradeResult.trades[i].amount_sell;
            init_principal = currentTrades2.initPrincipal;
            sell_principal = DAOTreasuryText;
            accesscode = tradeResult.trades[i].accesscode;
            token_init_identifier = currentTrades2.token_init_identifier;
            filledInit = tradeResult.trades[i].amount_init;
            filledSell = tradeResult.trades[i].amount_sell;
            strictlyOTC = currentTrades2.strictlyOTC;
            allOrNothing = currentTrades2.allOrNothing;
          },
        );

      } else {
        Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(tradeResult.trades[i].InitPrincipal)), tradeResult.trades[i].amount_sell, tradeResult.trades[i].token_sell_identifier, genTxId()));

        removeTrade(tradeResult.trades[i].accesscode, currentTrades2.initPrincipal, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));

        replaceLiqMap(
          true,
          false,
          currentTrades2.token_init_identifier,
          currentTrades2.token_sell_identifier,
          tradeResult.trades[i].accesscode,
          (currentTrades2.amount_init, currentTrades2.amount_sell, 0, 0, "", currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing),
          #Zero,
          null,
          ?{
            amount_init = currentTrades2.amount_init;
            amount_sell = currentTrades2.amount_sell;
            init_principal = currentTrades2.initPrincipal;
            sell_principal = DAOTreasuryText;
            accesscode = tradeResult.trades[i].accesscode;
            token_init_identifier = currentTrades2.token_init_identifier;
            filledInit = tradeResult.trades[i].amount_init;
            filledSell = tradeResult.trades[i].amount_sell;
            strictlyOTC = currentTrades2.strictlyOTC;
            allOrNothing = currentTrades2.allOrNothing;
          },
        );

      };

      if (pub and tradeResult.trades[i].amount_init > 1000 and tradeResult.trades[i].amount_sell > 1000) {
        let pair1 = (tradeResult.trades[i].token_init_identifier, tradeResult.trades[i].token_sell_identifier);
        let pair2 = (tradeResult.trades[i].token_sell_identifier, tradeResult.trades[i].token_init_identifier);
        if ((Map.has(foreignPools, hashtt, pair1) or Map.has(foreignPools, hashtt, pair2)) == false) {
          // Canonicalize amounts for updateLastTradedPrice.
          let cPair = getPool(tradeResult.trades[i].token_init_identifier, tradeResult.trades[i].token_sell_identifier);
          let (amt0, amt1) = if (cPair.0 == tradeResult.trades[i].token_init_identifier) {
            (tradeResult.trades[i].amount_init, tradeResult.trades[i].amount_sell)
          } else {
            (tradeResult.trades[i].amount_sell, tradeResult.trades[i].amount_init)
          };
          updateLastTradedPrice(
            cPair,
            amt0,
            amt1,
          );
        };
      };

    };


    logWithRunId("Success");
    let Accesscodes = Vector.new<{ poolId : (Text, Text); accesscode : Text; amountInit : Nat; amountSell : Nat; fee : Nat; revokeFee : Nat }>();

    let amountSell2Vec = Vector.fromArray<Nat>(
      Array.mapEntries<TradeAmount, Nat>(
        tradeResult.amounts,
        func(amount, index) : Nat {


          let sellDiff = if (trades[index].amountSell >= amount.amountSold) {
            let diff = trades[index].amountSell - amount.amountSold;

            diff;
          } else {

            0;
          };
          let transferFee = if (amount.amountSold > 0) {

            amount.transferFee;
          } else {

            0;
          };
          let timesFees = if (amount.amountSold > 0) {
            let fees = amount.timesTFees * amount.transferFee;

            fees;
          } else {

            0;
          };
          let total = if (sellDiff + transferFee >= timesFees) {
            let t = sellDiff + transferFee - timesFees;

            t;
          } else {

            0;
          };
          total;
        },
      )
    );

    let amountBuy2Vec = Vector.fromArray<Nat>(
      Array.mapEntries<TradeAmount, Nat>(
        tradeResult.amounts,
        func(amount, index) : Nat {
          if (trades[index].amountBuy >= amount.amountBought) {
            trades[index].amountBuy - amount.amountBought;
          } else { 0 };
        },
      )
    );

    logWithRunId(
      "amountSell2Vec: \n" # debug_show amountSell2Vec # "\n"
    );

    logWithRunId(
      "amountBuy2Vec: \n" # debug_show amountBuy2Vec
    );
    // Before final return, add order creation logic
    // Create arrays of REMAINING amounts to trade
    if (createOrdersIfNotDone) {
      logWithRunId("Creating orders for remaining amounts:");

      label a for (i in Iter.range(0, tradeResult.amounts.size() - 1)) {
        let totalAvailableSell = Vector.get(amountSell2Vec, i);
        var remainingToDistribute = totalAvailableSell;

        logWithRunId("=== Starting distribution for token index " # debug_show (i) # " ===");
        logWithRunId("Total available to sell: " # debug_show (totalAvailableSell));

        if (remainingToDistribute == 0) {
          logWithRunId("Skipping - no available sell amount");
          continue a;
        };

        let trade1 = trades[i];
        logWithRunId("Processing sell token: " # trade1.identifier);
        logWithRunId("Token details - ICPPrice: " # debug_show (trade1.ICPPrice) # " decimals: " # debug_show (trade1.decimals));

        var possiblesell = if (remainingToDistribute != 0 and ((remainingToDistribute * 10000) / (10000 + ICPfee)) > trade1.transferFee) {
          ((remainingToDistribute * 10000) / (10000 + ICPfee));
        } else { 0 };

        logWithRunId("Possible sell after fees: " # debug_show (possiblesell));

        if (possiblesell == 0) {
          logWithRunId("Skipping - no possible sell amount after fees");
          continue a;
        };

        // First pass: Calculate total buy value
        var totalBuyValueICP = 0;
        var validBuyPairs = 0;
        let buyValueMap = Vector.new<(Nat, Nat)>(); // (index, value)

        logWithRunId("\n=== First Pass: Calculating total buy values ===");

        for (i2 in Iter.range(0, tradeResult.amounts.size() - 1)) {
          if (i != i2) {
            let remainingBuy = Vector.get(amountBuy2Vec, i2);
            let trade2 = trades[i2];

            logWithRunId("\nEvaluating buy token: " # trade2.identifier);
            logWithRunId("Remaining buy amount: " # debug_show (remainingBuy));

            if (remainingBuy > 0) {
              let buyValueICP = (remainingBuy * trade2.ICPPrice) / (10 ** trade2.decimals);
              Vector.add(buyValueMap, (i2, buyValueICP));
              totalBuyValueICP += buyValueICP;
              validBuyPairs += 1;

              logWithRunId("Buy value in ICP: " # debug_show (buyValueICP));
              logWithRunId("Running total buy value: " # debug_show (totalBuyValueICP));
            };
          };
        };

        logWithRunId("\nFirst pass summary:");
        logWithRunId("Total valid pairs: " # debug_show (validBuyPairs));
        logWithRunId("Total buy value in ICP: " # debug_show (totalBuyValueICP));

        if (validBuyPairs == 0) {
          logWithRunId("No valid buy pairs - skipping token");
          continue a;
        };

        // Second pass: Create proportional trades
        var totalUsed = 0;
        var totalFees = 0;

        logWithRunId("\n=== Second Pass: Creating proportional trades ===");
        var tdone = tradeResult.amounts[i].amountSold > 0;

        label b for ((i2, buyValueICP) in Vector.vals(buyValueMap)) {
          let trade2 = trades[i2];
          let remainingBuy = Vector.get(amountBuy2Vec, i2);

          logWithRunId("\nProcessing buy token: " # trade2.identifier);
          logWithRunId("Available for this pair: " # debug_show (remainingToDistribute));

          if (remainingToDistribute == 0) {
            logWithRunId("No remaining amount to distribute");
            continue b;
          };

          // Calculate proportion based on ICP value
          let proportion = (buyValueICP * tenToPower60) / totalBuyValueICP;
          var targetSellAmount = (possiblesell * proportion) / tenToPower30;

          targetSellAmount := if (targetSellAmount < remainingToDistribute) {
            targetSellAmount - trade1.transferFee;
          } else {
            remainingToDistribute - trade1.transferFee;
          };

          logWithRunId("Buy value proportion: " # debug_show (proportion) # "/" # debug_show (tenToPower30));
          logWithRunId("Target sell amount: " # debug_show (targetSellAmount));

          if (targetSellAmount == 0) continue b;

          if (
            returnMinimum(trade1.identifier, targetSellAmount, false) and
            returnMinimum(trade2.identifier, remainingBuy, false)
          ) {
            var tobuy = 0;
            var tosell = 0;

            let condition = (
              (((10000 * remainingBuy) * trade2.ICPPrice) / (10 ** trade2.decimals)) < (((10000 * targetSellAmount) * trade1.ICPPrice) / (10 ** trade1.decimals))
            );

            if (condition) {
              let proportionalBuy = remainingBuy * targetSellAmount / possiblesell;
              tobuy := proportionalBuy;
              tosell := (
                (
                  (((1000000000000 * proportionalBuy) * trade2.ICPPrice) / (10 ** trade2.decimals)) / ((targetSellAmount * trade1.ICPPrice) / (10 ** trade1.decimals))
                ) * targetSellAmount
              ) / 1000000000000;
            } else {
              tosell := targetSellAmount;
              tobuy := (
                (
                  (((targetSellAmount * 1000000000000) * trade1.ICPPrice) / (10 ** trade1.decimals)) / ((remainingBuy * trade2.ICPPrice) / (10 ** trade2.decimals))
                ) * remainingBuy
              ) / 1000000000000;
            };

            logWithRunId("Final amounts - toBuy: " # debug_show (tobuy) # " toSell: " # debug_show (tosell));

            let accesscode = addPositionDAO(tobuy, tosell, trade2.identifier, trade1.identifier);
            logWithRunId("Created position with accesscode: " # debug_show (accesscode));

            if (tosell > 0) {
              let feeAmount = ((tosell * (ICPfee)) / (10000 * RevokeFeeNow));
              logWithRunId("Adding fees: " # debug_show (feeAmount));
              addFees(trade1.identifier, feeAmount, false, "", nowVar);
              totalFees += feeAmount;
            };

            totalUsed += tosell;

            remainingToDistribute := if (remainingToDistribute > ((tosell * (10000 + ICPfee)) / 10000) + trade1.transferFee +50) {
              remainingToDistribute - ((tosell * (10000 + ICPfee)) / 10000) - (if tdone { trade1.transferFee + 50 } else { 50 });
            } else { 0 };
            tdone := true;

            // Update buy amount
            let newRemainingBuy = if (remainingBuy < tobuy) {
              0;
            } else {
              remainingBuy - tobuy;
            };

            logWithRunId("Updated amounts:");
            logWithRunId("New remaining to distribute: " # debug_show (remainingToDistribute));
            logWithRunId("New remaining buy: " # debug_show (newRemainingBuy));

            Vector.put(amountBuy2Vec, i2, newRemainingBuy);

            Vector.add(
              Accesscodes,
              {
                poolId = (trade1.identifier, trade2.identifier);
                accesscode = accesscode;
                amountInit = tobuy;
                amountSell = tosell;
                fee = ICPfee;
                revokeFee = RevokeFeeNow;
              },
            );
          };
        };

        // Update final remaining sell amount
        Vector.put(amountSell2Vec, i, remainingToDistribute);

        logWithRunId("=== Final Summary for " # trade1.identifier # " ===");
        logWithRunId("Total amount used: " # debug_show (totalUsed));
        logWithRunId("Total fees: " # debug_show (totalFees));
        logWithRunId("Remaining to distribute: " # debug_show (remainingToDistribute));
      };

    };

    // Handle remaining sell amounts
    logWithRunId("\n=== Processing remaining sell amounts ===");
    var i2 = 0;
    for (i in Vector.vals(amountSell2Vec)) {
      if (i > trades[i2].transferFee) {
        logWithRunId("Returning " # debug_show (i - trades[i2].transferFee) # " of " # trades[i2].identifier # " to treasury");
        Vector.add(tempTransferQueueLocal, (#principal(DAOTreasury), i - trades[i2].transferFee, trades[i2].identifier, genTxId()));
      } else {
        logWithRunId("Adding " # debug_show (i) # " of " # trades[i2].identifier # " as fees");
        addFees(trades[i2].identifier, i, false, "", nowVar);
      };
      i2 += 1;
    };

    // Treasury transfer of remaining amounts
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {
      if verboseLogging { logWithRunId("Successfully transferred remaining funds to treasury: " #debug_show (Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal))) };
    } else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };

    for (t in tradeResult.trades.vals()) {
      tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, t.accesscode, Text.hash(t.accesscode), Text.equal);
    };

    ?{
      execMessage = "Success";
      processedTrades = Array.map<TradeAmount, ProcessedTrade>(
        tradeResult.amounts,
        func(amount) : ProcessedTrade = {
          identifier = amount.identifier;
          amountBought = amount.amountBought;
          amountSold = amount.amountSold;
        },
      );
      accesscodes = Vector.toArray(Accesscodes);
    };
  };

  // function that is called by FinishSellBatchDAO when it has to make orders with the leftovers. Its shorter than addPosition as it does not have to go through orderPairing.
  private func addPositionDAO(
    amount_sell : Nat,
    amount_init : Nat,
    token_sell_identifier : Text,
    token_init_identifier : Text,
  ) : Text {

    trade_number += 1;
    var trade : TradePrivate = {
      Fee = ICPfee;
      amount_sell = amount_sell;
      amount_init = amount_init;
      token_sell_identifier = token_sell_identifier;
      token_init_identifier = token_init_identifier;
      trade_done = 0;
      seller_paid = 0;
      init_paid = 1;
      trade_number = trade_number;
      SellerPrincipal = "0";
      initPrincipal = DAOTreasuryText;
      seller_paid2 = 0;
      init_paid2 = 0;
      RevokeFee = RevokeFeeNow;
      OCname = "/community/lizfz-ryaaa-aaaar-bagsa-cai";
      time = Time.now();
      allOrNothing = false;
      filledInit = 0;
      filledSell = 0;
      strictlyOTC = false;
    };
    var accesscode : Text = PrivateHash();
    accesscode := "Public" #accesscode;


    let nonPoolOrder = not isKnownPool(token_sell_identifier, token_init_identifier);

    replaceLiqMap(
      false,
      false,
      token_init_identifier,
      token_sell_identifier,
      accesscode,
      (trade.amount_init, trade.amount_sell, ICPfee, RevokeFeeNow, DAOTreasuryText, trade.OCname, trade.time, token_init_identifier, token_sell_identifier, trade.strictlyOTC, trade.allOrNothing),
      #Zero,
      null,
      null,
    );

    addTrade(accesscode, DAOTreasuryText, trade, (token_init_identifier, token_sell_identifier));



    label a if nonPoolOrder {
      let pair1 = (token_init_identifier, token_sell_identifier);
      let pair2 = (token_sell_identifier, token_init_identifier);

      let existsInForeignPools = (Map.has(foreignPools, hashtt, pair1) or Map.has(foreignPools, hashtt, pair2));

      if (not existsInForeignPools) {
        Map.set(foreignPools, hashtt, pair1, 1);
        break a;
      };

      let pairToAdd = if existsInForeignPools {
        if (Map.has(foreignPools, hashtt, pair1)) pair1 else pair2;
      } else { pair1 };
      Map.set(foreignPools, hashtt, pairToAdd, switch (Map.get(foreignPools, hashtt, pairToAdd)) { case (?a) { a +1 }; case null { 1 } });
    };

    return accesscode;
  };

  // Function that makes it able to finalize multiple orders. Contemplating if it has any se in production, addPosition does everything this one does, smarter, however at a higher cycle price
  public shared (msg) func FinishSellBatch(
    Block : Nat64,
    accesscode : [Text],
    amount_Sell_by_Reactor : [Nat],
    token_sell_identifier : Text,
    token_init_identifier : Text,
  ) : async ExTypes.ActionResult {
    if (Text.size(token_sell_identifier) > 150 or Text.size(token_init_identifier) > 150 or Text.size(accesscode[0]) > 150) {
      return #Err(#Banned);
    };
    if (isAllowed(msg.caller) != 1) return #Err(#NotAuthorized);
    assert (amount_Sell_by_Reactor.size() == accesscode.size());
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    let sellTfees = returnTfees(token_init_identifier);
    let initTfees = returnTfees(token_sell_identifier);
    var haveToReturn = false;
    var totalInit = 0;

    for (i in amount_Sell_by_Reactor.vals()) {
      if (initTfees >= i) {
        haveToReturn := true;
      };
      totalInit += i;
    };
    if (not returnMinimum(token_sell_identifier, totalInit, false)) {
      haveToReturn := true;
    };

    if (Map.has(BlocksDone, thash, token_init_identifier # ":" #Nat64.toText(Block))) { return #Err(#InvalidInput("Block already processed")) };
    assert (accesscode.size() != 0);
    let nowVar2 = Time.now();
    Map.set(BlocksDone, thash, token_init_identifier # ":" #Nat64.toText(Block), nowVar2);

    for (accesscode in accesscode.vals()) {
      tradesBeingWorkedOn := TrieSet.put(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
    };
    let tType : { #ICP; #ICRC12; #ICRC3 } = returnType(token_init_identifier);

    if (
      ((switch (Array.find<Text>(pausedTokens, func(t) { t == token_sell_identifier })) { case null false; case (?_) true })) or
      ((switch (Array.find<Text>(pausedTokens, func(t) { t == token_init_identifier })) { case null false; case (?_) true })) or haveToReturn
    ) {

      let blockData = try {
        await* getBlockData(token_init_identifier, nat64ToNat(Block), tType);
      } catch (err) {
        Map.delete(BlocksDone, thash, token_init_identifier # ":" #Nat64.toText(Block));
        #ICRC12([]);
      };
      if (blockData != #ICRC12([])) {
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(nat64ToNat(Block), msg.caller, 0, token_init_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
      };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      for (accesscode in accesscode.vals()) {
        tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
      };
      return #Err(#TokenPaused("Token paused"));
    };

    var amountInit = 0;
    var amountSell = 0;
    var amountFees = 0;
    let TradeEntryVector = Vector.new<{ initPrincipal : Text; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; partial : Bool }>();
    var initTfeesDone = false;

    label a for (i in Iter.range(0, accesscode.size() - 1)) {
      let currentTrades2 = switch (Map.get(tradeStorePublic, thash, accesscode[i])) {
        case (?(foundTrades)) foundTrades;
        case null continue a;
      };
      if (
        not Text.startsWith(accesscode[i], #text "Public") or currentTrades2.trade_number == 0 or
        currentTrades2.token_sell_identifier != token_init_identifier or currentTrades2.token_init_identifier != token_sell_identifier or
        currentTrades2.trade_done == 1 or currentTrades2.init_paid != 1
      ) continue a;

      let (amountInitInc, amountSellInc, amountFeesInc) = if (amount_Sell_by_Reactor[i] < currentTrades2.amount_init) {
        let amtInit = ((((((amount_Sell_by_Reactor[i] * 100000000) / currentTrades2.amount_init) * currentTrades2.amount_sell)) * (10000 + currentTrades2.Fee)) / 100000000) + (10000 * sellTfees);
        let amtSell = amount_Sell_by_Reactor[i] - initTfees;
        let amtFees = ((((amount_Sell_by_Reactor[i] * 100000000) / currentTrades2.amount_init) * currentTrades2.amount_sell) * currentTrades2.Fee) / 100000000;
        (amtInit, amtSell, amtFees);
      } else {
        let amtInit = (currentTrades2.amount_sell * (10000 + currentTrades2.Fee)) + (10000 * sellTfees);
        let amtSell = amount_Sell_by_Reactor[i] + (if (initTfeesDone) { initTfees } else { initTfeesDone := true; 0 });
        let amtFees = (currentTrades2.amount_sell * currentTrades2.Fee);
        (amtInit, amtSell, amtFees);
      };
      if (amount_Sell_by_Reactor[i] != currentTrades2.amount_init and currentTrades2.allOrNothing) {
        continue a;
      };
      amountInit += amountInitInc;
      amountSell += amountSellInc;
      amountFees += amountFeesInc;

      Vector.add(
        TradeEntryVector,
        {
          initPrincipal = currentTrades2.initPrincipal;
          accesscode = accesscode[i];
          amount_init = amount_Sell_by_Reactor[i];
          amount_sell = (((amount_Sell_by_Reactor[i] * 100000000) / currentTrades2.amount_init) * currentTrades2.amount_sell) / 100000000;
          Fee = currentTrades2.Fee;
          RevokeFee = currentTrades2.RevokeFee;
          partial = (amount_Sell_by_Reactor[i] != currentTrades2.amount_init);
        },
      );
    };

    var TradeEntries = Vector.toArray(TradeEntryVector);

    let blockData = try {
      await* getBlockData(token_init_identifier, nat64ToNat(Block), returnType(token_init_identifier));
    } catch (err) {
      Map.delete(BlocksDone, thash, token_init_identifier # ":" #Nat64.toText(Block));
      #ICRC12([]);
    };

    let (receiveBool, receiveTransfers) = if (blockData != #ICRC12([])) {
      checkReceive(nat64ToNat(Block), msg.caller, if (amountInit != 0) { amountInit / 10000 } else { 0 }, token_init_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2);
    } else { (false, []) };

    Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());
    if (not receiveBool) {
      if (Vector.size(tempTransferQueueLocal) > 0) {
        if (try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false }) {} else {
          Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
        };
      };
      for (accesscode in accesscode.vals()) {
        tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
      };
      return #Err(#InsufficientFunds("Deposit not received"));
    };

    // Re-check trade details after await

    var amountInit2 = 0;
    var amountSell2 = 0;
    var amountFees2 = 0;
    initTfeesDone := false;
    var TradeEntryVector2 = Vector.new<{ initPrincipal : Text; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; partial : Bool }>();
    label getTradeInfo for (i in Iter.range(0, accesscode.size() - 1)) {
      let pub = Text.startsWith(accesscode[i], #text "Public");
      var currentTrades2 : TradePrivate = switch (Map.get(tradeStorePublic, thash, accesscode[i])) {
        case (?(foundTrades)) foundTrades;
        case null continue getTradeInfo;
      };
      if (
        not pub or currentTrades2.trade_number == 0 or currentTrades2.token_sell_identifier != token_init_identifier or
        currentTrades2.token_init_identifier != token_sell_identifier or currentTrades2.trade_done == 1 or currentTrades2.init_paid != 1
      ) {
        continue getTradeInfo;
      };

      if (amount_Sell_by_Reactor[i] < currentTrades2.amount_init) {
        amountInit2 += ((((((amount_Sell_by_Reactor[i] * 100000000) / currentTrades2.amount_init) * currentTrades2.amount_sell)) * (10000 + currentTrades2.Fee)) / 100000000) + (10000 * sellTfees);
        amountSell2 += amount_Sell_by_Reactor[i] - initTfees;
        amountFees2 += ((((amount_Sell_by_Reactor[i] * 100000000) / currentTrades2.amount_init) * currentTrades2.amount_sell) * currentTrades2.Fee) / 100000000;
      } else {
        amountInit2 += (currentTrades2.amount_sell * (10000 + currentTrades2.Fee)) + (10000 * sellTfees);
        amountSell2 += amount_Sell_by_Reactor[i] + (if (initTfeesDone) { initTfees } else { initTfeesDone := true; 0 });
        amountFees2 += (currentTrades2.amount_sell * currentTrades2.Fee);
      };

      Vector.add(
        TradeEntryVector2,
        {
          initPrincipal = currentTrades2.initPrincipal;
          accesscode = accesscode[i];
          amount_init = amount_Sell_by_Reactor[i];
          amount_sell = (((amount_Sell_by_Reactor[i] * 100000000) / currentTrades2.amount_init) * currentTrades2.amount_sell) / 100000000;
          Fee = currentTrades2.Fee;
          RevokeFee = currentTrades2.RevokeFee;
          partial = (amount_Sell_by_Reactor[i] != currentTrades2.amount_init);
        },
      );
    };

    if (amountInit != amountInit2) {
      TradeEntries := Vector.toArray(TradeEntryVector2);
      if (amountInit > amountInit2) {
        let add = ((amountInit - amountInit2) / 10000);
        if (add > sellTfees) {
          Vector.add(tempTransferQueueLocal, (#principal(msg.caller), ((amountInit - amountInit2) / 10000) - sellTfees, token_init_identifier, genTxId()));
        } else {
          addFees(token_init_identifier, ((amountInit - amountInit2) / 10000), false, Principal.toText(msg.caller), nowVar2);
        };
      } else {
        Vector.add(tempTransferQueueLocal, (#principal(msg.caller), (amountInit / 10000), token_init_identifier, genTxId()));
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {} else {
          Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
        };
        for (accesscode in accesscode.vals()) {
          tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
        };
        return #Err(#SystemError("Order updated during await"));
      };
      amountFees := amountFees2;
      amountSell := amountSell2;
      amountInit := amountInit2;
    };

    if (TradeEntries.size() == 0) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      for (accesscode in accesscode.vals()) {
        tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
      };
      return #Err(#OrderNotFound("No orders left"));
    };

    Vector.add(tempTransferQueueLocal, (#principal(msg.caller), amountSell, token_sell_identifier, genTxId()));
    addFees(token_init_identifier, (amountFees / 10000), false, Principal.toText(msg.caller), nowVar2);

    var endmessage = "";
    label a for (i in TradeEntries.vals()) {

      var currentTrades2 = switch (Map.get(if (Text.startsWith(i.accesscode, #text "Public")) { tradeStorePublic } else { tradeStorePrivate }, thash, i.accesscode)) {
        case (?(foundTrades)) foundTrades;
        case null continue a;
      };

      Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(i.initPrincipal)), i.amount_sell, token_init_identifier, genTxId()));
      addFees(token_sell_identifier, ((((i.amount_init) * i.Fee)) - (((((i.amount_init) * i.Fee) * 100000) / i.RevokeFee) / 100000)) / 10000, false, i.initPrincipal, nowVar2);

      if (i.partial) {
        currentTrades2 := {
          currentTrades2 with
          amount_sell = currentTrades2.amount_sell - i.amount_sell;
          amount_init = currentTrades2.amount_init - i.amount_init;
          trade_done = 0;
          seller_paid = 0;
          init_paid = 1;
          SellerPrincipal = Principal.toText(msg.caller);
          seller_paid2 = 0;
          init_paid2 = 0;
          filledInit = currentTrades2.filledInit + i.amount_init;
          filledSell = currentTrades2.filledSell + i.amount_sell;
        };
        addTrade(i.accesscode, currentTrades2.initPrincipal, currentTrades2, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));

        replaceLiqMap(false, true, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, i.accesscode, (currentTrades2.amount_init, currentTrades2.amount_sell, currentTrades2.Fee, currentTrades2.RevokeFee, currentTrades2.initPrincipal, currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing), #Value(((currentTrades2.amount_init + i.amount_init) * tenToPower60) / (currentTrades2.amount_sell + i.amount_sell)), ?{ Fee = currentTrades2.Fee; RevokeFee = currentTrades2.RevokeFee }, ?{ amount_init = i.amount_init; amount_sell = i.amount_sell; init_principal = currentTrades2.initPrincipal; sell_principal = Principal.toText(msg.caller); accesscode = i.accesscode; token_init_identifier = currentTrades2.token_init_identifier; filledInit = i.amount_init; filledSell = i.amount_sell; strictlyOTC = currentTrades2.strictlyOTC; allOrNothing = currentTrades2.allOrNothing });
      } else {
        removeTrade(i.accesscode, currentTrades2.initPrincipal, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));
        replaceLiqMap(true, false, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, i.accesscode, (currentTrades2.amount_init, currentTrades2.amount_sell, 0, 0, "", currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing), #Zero, null, ?{ amount_init = i.amount_init; amount_sell = i.amount_sell; init_principal = currentTrades2.initPrincipal; sell_principal = Principal.toText(msg.caller); accesscode = i.accesscode; token_init_identifier = currentTrades2.token_init_identifier; filledInit = i.amount_init; filledSell = i.amount_sell; strictlyOTC = currentTrades2.strictlyOTC; allOrNothing = currentTrades2.allOrNothing });
      };

      // Record swap for filler and order maker
      nextSwapId += 1;
      recordSwap(msg.caller, {
        swapId = nextSwapId;
        tokenIn = token_sell_identifier; tokenOut = token_init_identifier;
        amountIn = i.amount_sell; amountOut = i.amount_init;
        route = [token_sell_identifier, token_init_identifier];
        fee = (i.amount_sell * i.Fee) / 10000;
        swapType = #direct;
        timestamp = nowVar2;
      });
      nextSwapId += 1;
      recordSwap(Principal.fromText(i.initPrincipal), {
        swapId = nextSwapId;
        tokenIn = token_init_identifier; tokenOut = token_sell_identifier;
        amountIn = i.amount_init; amountOut = i.amount_sell;
        route = [token_init_identifier, token_sell_identifier];
        fee = (i.amount_init * i.Fee) / 10000;
        swapType = #limit;
        timestamp = nowVar2;
      });

      // Update kline data for this fill
      let fillPair = getPool(token_init_identifier, token_sell_identifier);
      let isForeignPool = Map.has(foreignPools, hashtt, (token_init_identifier, token_sell_identifier)) or
                          Map.has(foreignPools, hashtt, (token_sell_identifier, token_init_identifier));
      if (not isForeignPool) {
        // Map trade-direction amounts to canonical (amount0, amount1).
        // If the trade direction matches canonical order, amount_init = amount0.
        // Otherwise the amounts are swapped relative to canonical.
        let (amt0, amt1) = if (fillPair.0 == token_init_identifier) {
          (i.amount_init, i.amount_sell)
        } else {
          (i.amount_sell, i.amount_init)
        };
        updateLastTradedPrice(fillPair, amt0, amt1);
      };
    };

    doInfoBeforeStep2();
    let poolKey = getPool(token_init_identifier, token_sell_identifier);
    ignore updatePriceDayBefore(poolKey, nowVar2);
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };
    for (accesscode in accesscode.vals()) {
      tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
    };
    return #Ok("Trade done" # (if (endmessage != "") { ". Recoverable: " # endmessage } else { "" }));
  };

  // Function that  finishes a particular position. This is used primarily for private orders, as orderpairing does not work for those.
  public shared (msg) func FinishSell(
    Block : Nat64,
    accesscode : Text,
    amountSelling : Nat,
  ) : async ExTypes.ActionResult {
    if (isAllowed(msg.caller) != 1) {
      return #Err(#NotAuthorized);
    };
    if (Text.size(accesscode) > 150) {
      return #Err(#Banned);
    };

    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var currentTrades2 : TradePrivate = Faketrade;
    let pub = Text.startsWith(accesscode, #text "Public");
    let excludeDAO = (Text.endsWith(accesscode, #text "excl") and not pub);

    // Get current trade details
    if (pub) {
      switch (Map.get(tradeStorePublic, thash, accesscode)) {
        case (?(foundTrades)) { currentTrades2 := foundTrades };
        case null {};
      };
    } else {
      switch (Map.get(tradeStorePrivate, thash, accesscode)) {
        case (?(foundTrades)) { currentTrades2 := foundTrades };
        case null {};
      };
    };

    if (Map.has(BlocksDone, thash, currentTrades2.token_sell_identifier # ":" #Nat64.toText(Block))) { return #Err(#InvalidInput("Block already processed")) };
    let nowVar2 = Time.now();
    Map.set(BlocksDone, thash, currentTrades2.token_sell_identifier # ":" #Nat64.toText(Block), nowVar2);

    tradesBeingWorkedOn := TrieSet.put(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
    var tType : { #ICP; #ICRC12; #ICRC3 } = returnType(currentTrades2.token_sell_identifier);
    var blockData : BlockData = #ICRC12([]);
    if (
      returnMinimum(currentTrades2.token_sell_identifier, amountSelling, false) == false or
      ((switch (Array.find<Text>(pausedTokens, func(t) { t == currentTrades2.token_sell_identifier })) { case null { false }; case (?_) { true } })) or
      ((switch (Array.find<Text>(pausedTokens, func(t) { t == currentTrades2.token_init_identifier })) { case null { false }; case (?_) { true } })) or
      currentTrades2.trade_number == 0 or currentTrades2.trade_done == 1
    ) {
      // Handle minimum amount or paused token cases
      try {
        blockData := await* getBlockData(currentTrades2.token_sell_identifier, nat64ToNat(Block), tType);
        Vector.addFromIter(tempTransferQueueLocal, (checkReceive(nat64ToNat(Block), msg.caller, 0, currentTrades2.token_sell_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
      } catch (err) {
        Map.delete(BlocksDone, thash, currentTrades2.token_sell_identifier # ":" #Nat64.toText(Block));

      };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
      return #Err(#TokenPaused("Amount too low or token paused"));
    };

    let partial = (amountSelling < currentTrades2.amount_sell);

    blockData := try {
      await* getBlockData(currentTrades2.token_sell_identifier, nat64ToNat(Block), tType);
    } catch (err) {
      Map.delete(BlocksDone, thash, currentTrades2.token_sell_identifier # ":" #Nat64.toText(Block));
      #ICRC12([]);
    };
    let (receiveBool, receiveTransfers) = if (blockData != #ICRC12([])) {
      checkReceive(nat64ToNat(Block), msg.caller, amountSelling, currentTrades2.token_sell_identifier, currentTrades2.Fee, currentTrades2.RevokeFee, false, true, blockData, tType, nowVar2);
    } else { (false, []) };

    Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());
    if (not receiveBool) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
      return #Err(#InsufficientFunds("Deposit not received"));
    };

    // Re-check trade details after await
    var sendBack = false;
    switch (if pub { Map.get(tradeStorePublic, thash, accesscode) } else { Map.get(tradeStorePrivate, thash, accesscode) }) {
      case (?(foundTrades)) {
        if (foundTrades.amount_init == currentTrades2.amount_init and foundTrades.amount_sell == currentTrades2.amount_sell and foundTrades.trade_done == currentTrades2.trade_done and (not currentTrades2.allOrNothing or not partial)) {
          currentTrades2 := foundTrades;
        } else { sendBack := true };
      };
      case null { sendBack := true };
    };
    if sendBack {

      Vector.addFromIter(tempTransferQueueLocal, (checkReceive(nat64ToNat(Block), msg.caller, 0, currentTrades2.token_sell_identifier, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar2)).1.vals());
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
      return #Err(#OrderNotFound("Trade no longer exists"));
    };

    // Check if order details have changed
    var amountBuying = (currentTrades2.amount_init * ((amountSelling * tenToPower80) / currentTrades2.amount_sell)) / tenToPower80;

    // Proceed with the trade
    let init_paid2 = 1;
    let seller_paid2 = 1;

    // Handle transfers and fees
    Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(currentTrades2.initPrincipal)), amountSelling, currentTrades2.token_sell_identifier, genTxId()));
    Vector.add(tempTransferQueueLocal, (#principal(msg.caller), amountBuying, currentTrades2.token_init_identifier, genTxId()));
    if pub {
      let pair1 = (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier);
      let pair2 = (currentTrades2.token_sell_identifier, currentTrades2.token_init_identifier);
      if ((Map.has(foreignPools, hashtt, pair1) or Map.has(foreignPools, hashtt, pair2)) == false) {
        // amountBuying is in token_init units; amountSelling is in token_sell units.
        // Map to canonical (amount0, amount1).
        let cPair = getPool(currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier);
        let (amt0, amt1) = if (cPair.0 == currentTrades2.token_init_identifier) {
          (amountBuying, amountSelling)
        } else {
          (amountSelling, amountBuying)
        };
        updateLastTradedPrice(cPair, amt0, amt1);
      };
    };
    var nowVar = nowVar2;
    addFees(currentTrades2.token_sell_identifier, (((((amountSelling) * currentTrades2.Fee)) - (((((amountSelling) * currentTrades2.Fee) * 100000) / currentTrades2.RevokeFee) / 100000)) / 10000), false, Principal.toText(msg.caller), nowVar);
    addFees(currentTrades2.token_init_identifier, (((((amountBuying) * currentTrades2.Fee)) - (((((amountBuying) * currentTrades2.Fee) * 100000) / currentTrades2.RevokeFee) / 100000)) / 10000), false, currentTrades2.initPrincipal, nowVar);

    // Update trade record for partial fills (reduce amounts, track filled)
    if (partial) {
      currentTrades2 := {
        currentTrades2 with
        amount_sell = currentTrades2.amount_sell - amountSelling;
        amount_init = currentTrades2.amount_init - amountBuying;
        filledInit = currentTrades2.filledInit + amountBuying;
        filledSell = currentTrades2.filledSell + amountSelling;
      };
    };
    // Update liquidity map if necessary
    if (not excludeDAO) {
      replaceLiqMap(not partial, partial, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, accesscode, if (partial) (currentTrades2.amount_init, currentTrades2.amount_sell, currentTrades2.Fee, currentTrades2.RevokeFee, currentTrades2.initPrincipal, currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing) else (currentTrades2.amount_init, currentTrades2.amount_sell, 0, 0, "", currentTrades2.OCname, currentTrades2.time, currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier, currentTrades2.strictlyOTC, currentTrades2.allOrNothing), if (partial) #Value(((currentTrades2.amount_init + amountBuying) * tenToPower60) / (currentTrades2.amount_sell + amountSelling)) else #Zero, if (partial) ?{ Fee = currentTrades2.Fee; RevokeFee = currentTrades2.RevokeFee } else null, ?{ amount_init = amountBuying; amount_sell = amountSelling; init_principal = currentTrades2.initPrincipal; sell_principal = Principal.toText(msg.caller); accesscode = accesscode; token_init_identifier = currentTrades2.token_init_identifier; filledInit = amountBuying; filledSell = amountSelling; strictlyOTC = currentTrades2.strictlyOTC; allOrNothing = currentTrades2.allOrNothing });
    };
    if (partial) {
      addTrade(accesscode, currentTrades2.initPrincipal, currentTrades2, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));
    } else {
      removeTrade(accesscode, currentTrades2.initPrincipal, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));
    };

    // Record swap for the filler (seller)
    nextSwapId += 1;
    recordSwap(msg.caller, {
      swapId = nextSwapId;
      tokenIn = currentTrades2.token_sell_identifier; tokenOut = currentTrades2.token_init_identifier;
      amountIn = amountSelling; amountOut = amountBuying;
      route = [currentTrades2.token_sell_identifier, currentTrades2.token_init_identifier];
      fee = (amountSelling * currentTrades2.Fee) / 10000;
      swapType = #direct;
      timestamp = nowVar;
    });
    // Record swap for the order maker (initiator)
    nextSwapId += 1;
    recordSwap(Principal.fromText(currentTrades2.initPrincipal), {
      swapId = nextSwapId;
      tokenIn = currentTrades2.token_init_identifier; tokenOut = currentTrades2.token_sell_identifier;
      amountIn = amountBuying; amountOut = amountSelling;
      route = [currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier];
      fee = (amountBuying * currentTrades2.Fee) / 10000;
      swapType = #limit;
      timestamp = nowVar;
    });

    doInfoBeforeStep2();
    let poolKey = getPool(currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier);
    ignore updatePriceDayBefore(poolKey, nowVar);
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(msg.caller)) } catch (err) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };

    tradesBeingWorkedOn := TrieSet.delete(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal);
    return #Ok("Trade completed successfully");
  };

  public shared ({ caller }) func changeOwner2(pri : Principal) : async () {
    if (not test and caller != owner2) {
      if (not TrieSet.contains(dayBan, caller, Principal.hash(caller), Principal.equal)) {
        dayBan := TrieSet.put(dayBan, caller, Principal.hash(caller), Principal.equal);
      };
      return;
    };
    owner2 := pri;
  };

  // Admin escape hatch: drop a cached archive offset so the next block lookup
  // re-discovers it via probe. Only needed if a ledger ever rewinds/resets its
  // archive offset (extremely rare).
  public shared ({ caller }) func clearTokenArchiveOffset(token : Text) : async Bool {
    if (not test and caller != owner2) { return false };
    Map.delete(tokenArchiveOffset, thash, token);
    true;
  };

  public query func getTokenArchiveOffset(token : Text) : async ?Nat {
    Map.get(tokenArchiveOffset, thash, token);
  };

  public shared ({ caller }) func changeOwner3(pri : Principal) : async () {
    if (not test and caller != owner3) {
      if (not TrieSet.contains(dayBan, caller, Principal.hash(caller), Principal.equal)) {
        dayBan := TrieSet.put(dayBan, caller, Principal.hash(caller), Principal.equal);
      };
      return;
    };
    owner3 := pri;
  };

  // This function will be deleted in production, currently used in tests to delete all (remaining) positions.
  public query ({ caller }) func getAllTradesPrivateCostly() : async ?([Text], [TradePrivate]) {
    if (not ownercheck(caller)) {
      return null;
    };
    var bufferText : Buffer.Buffer<Text> = Buffer.Buffer<Text>(Map.size(tradeStorePrivate));
    var bufferTradeList : Buffer.Buffer<TradePrivate> = Buffer.Buffer<TradePrivate>(Map.size(tradeStorePrivate));

    for ((key, value) in Map.entries(tradeStorePrivate)) {
      bufferText.add(key);
      bufferTradeList.add(value);
    };
    let listAll = (Buffer.toArray(bufferText), Buffer.toArray(bufferTradeList));
    return ?listAll;
  };

  // This function returns all the public positions that are available, can only be called by the owners. Will also be  deleted in production as there are other functions out there.
  public query ({ caller }) func getAllTradesPublic() : async ?([Text], [TradePrivate]) {
    if (not ownercheck(caller)) {
      return null;
    };
    var bufferText : Buffer.Buffer<Text> = Buffer.Buffer<Text>(Map.size(tradeStorePublic));
    var bufferTradeList : Buffer.Buffer<TradePrivate> = Buffer.Buffer<TradePrivate>(Map.size(tradeStorePublic));

    for ((key, value) in Map.entries(tradeStorePublic)) {
      bufferText.add(key);
      bufferTradeList.add(value);
    };
    let listAll = (Buffer.toArray(bufferText), Buffer.toArray(bufferTradeList));
    return ?listAll;
  };



  // ═══════════════════════════════════════════════════════════════════
  // SECTION: Emergency Drain — removes ALL orders, liquidity, fees
  // and sweeps remaining balances to a target principal.
  // ═══════════════════════════════════════════════════════════════════

  private let DRAIN_BATCH_ORDERS : Nat = 500;
  private let DRAIN_BATCH_V2 : Nat = 200;
  private let DRAIN_BATCH_V3 : Nat = 150;

  // Consolidate transfer queue: merge entries with same (recipient, token) into one transfer.
  // Saves transfer fees when a user has multiple orders/positions in the same token.
  private func consolidateTransfers(queue : Vector.Vector<(TransferRecipient, Nat, Text, Text)>) : [(TransferRecipient, Nat, Text, Text)] {
    let merged = Map.new<Text, (TransferRecipient, Nat, Text, Text)>();
    for (tx in Vector.vals(queue)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(merged, thash, key)) {
        case (?existing) { Map.set(merged, thash, key, (tx.0, existing.1 + tx.1, tx.2, tx.3)) };
        case null { Map.set(merged, thash, key, tx) };
      };
    };
    let result = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    for ((_, tx) in Map.entries(merged)) { Vector.add(result, tx) };
    Vector.toArray(result);
  };

  // ── Entry point ──────────────────────────────────────────────────
  public shared ({ caller }) func adminDrainTestModeExchange(target : Principal) : async Text {
    // LOCKDOWN: drain is a one-time recovery function. After the production drain was
    // performed to resolve historical drift damage, this entry is permanently disabled
    // on mainnet. Only available when the exchange is in test mode (setTest(true) can
    // only be called by controller / canisterOTC principal — never on mainnet).
    if (not test) { return "Drain disabled — test mode only"; };
    if (caller != deployer.caller and caller != Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe")) {
      return "Not authorized — controller or odoge only";
    };
    if (drainState != #Idle and drainState != #Done) {
      return "Drain already in progress: " # drainStateText();
    };
    exchangeState := #Frozen;
    drainTarget := target;
    drainState := #DrainingOrders;
    ignore setTimer<system>(#seconds 1, drainStep);
    "Drain started. Exchange frozen. Target: " # Principal.toText(target);
  };

  public query ({ caller }) func adminDrainTestModeStatus() : async Text {
    if (not isAdmin(caller)) { return "Not authorized" };
    drainStateText();
  };

  private func drainStateText() : Text {
    switch (drainState) {
      case (#Idle) "Idle";
      case (#DrainingOrders) "Phase 1/5: Draining orders";
      case (#DrainingV2) "Phase 2/5: Draining V2 liquidity";
      case (#DrainingV3) "Phase 3/5: Draining V3 liquidity";
      case (#SweepingFees) "Phase 4/5: Sweeping fees";
      case (#SweepingRemainder) "Phase 5/5: Sweeping remaining balances";
      case (#Done) "Done";
    };
  };

  // ── Timer dispatch ───────────────────────────────────────────────
  private func drainStep<system>() : async () {
    switch (drainState) {
      case (#DrainingOrders) { await drainOrders() };
      case (#DrainingV2) { await drainV2Liquidity() };
      case (#DrainingV3) { await drainV3Liquidity() };
      case (#SweepingFees) { await drainFees() };
      case (#SweepingRemainder) { await sweepRemainder() };
      case (_) {};
    };
  };

  // ── Phase 1: Drain all orders ────────────────────────────────────
  private func drainOrders<system>() : async () {
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var processed : Nat = 0;
    var hasMore = false;

    // Collect a batch from tradeStorePublic
    let publicBatch = Vector.new<(Text, TradePrivate)>();
    for ((ac, trade) in Map.entries(tradeStorePublic)) {
      if (Vector.size(publicBatch) >= DRAIN_BATCH_ORDERS) {
        hasMore := true;
      } else {
        Vector.add(publicBatch, (ac, trade));
      };
    };

    // Process public batch
    for ((accesscode, t) in Vector.vals(publicBatch)) {
      if (not TrieSet.contains(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal)) {
        if (t.trade_done == 0) {
          // Full refund: amount + entire held fee (no revoke fee deduction)
          if (t.init_paid == 1) {
            let refund = t.amount_init + ((t.amount_init * t.Fee) / 10000);
            Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(t.initPrincipal)), refund, t.token_init_identifier, genTxId()));
          };
          if (t.seller_paid == 1) {
            let refund = t.amount_sell + ((t.amount_sell * t.Fee) / 10000);
            Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(t.SellerPrincipal)), refund, t.token_sell_identifier, genTxId()));
          };
        };
        replaceLiqMap(true, false, t.token_init_identifier, t.token_sell_identifier, accesscode, (t.amount_init, t.amount_sell, t.Fee, t.RevokeFee, t.initPrincipal, t.OCname, t.time, t.token_init_identifier, t.token_sell_identifier, t.strictlyOTC, t.allOrNothing), #Zero, null, null);
        processed += 1;
      };
    };

    // If public is done, collect from tradeStorePrivate
    if (not hasMore) {
      let privateBatch = Vector.new<(Text, TradePrivate)>();
      for ((ac, trade) in Map.entries(tradeStorePrivate)) {
        if (Vector.size(privateBatch) + processed >= DRAIN_BATCH_ORDERS) {
          hasMore := true;
        } else {
          Vector.add(privateBatch, (ac, trade));
        };
      };

      for ((accesscode, t) in Vector.vals(privateBatch)) {
        if (not TrieSet.contains(tradesBeingWorkedOn, accesscode, Text.hash(accesscode), Text.equal)) {
          if (t.trade_done == 0) {
            if (t.init_paid == 1) {
              let refund = t.amount_init + ((t.amount_init * t.Fee) / 10000);
              Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(t.initPrincipal)), refund, t.token_init_identifier, genTxId()));
            };
            if (t.seller_paid == 1) {
              let refund = t.amount_sell + ((t.amount_sell * t.Fee) / 10000);
              Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(t.SellerPrincipal)), refund, t.token_sell_identifier, genTxId()));
            };
          };
          replaceLiqMap(true, false, t.token_init_identifier, t.token_sell_identifier, accesscode, (t.amount_init, t.amount_sell, t.Fee, t.RevokeFee, t.initPrincipal, t.OCname, t.time, t.token_init_identifier, t.token_sell_identifier, t.strictlyOTC, t.allOrNothing), #Zero, null, null);
        };
      };
    };

    // Consolidate & send transfers (merge same user+token into single transfer)
    if (Vector.size(tempTransferQueueLocal) > 0) {
      let consolidated = consolidateTransfers(tempTransferQueueLocal);
      if ((try { await treasury.receiveTransferTasks(consolidated, false) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, consolidated.vals());
      };
    };

    // Check if done
    if (Map.size(tradeStorePublic) == 0 and Map.size(tradeStorePrivate) == 0) {
      // Safety-net clear all index structures
      Map.clear(userCurrentTradeStore);
      Map.clear(privateAccessCodes);
      Map.clear(foreignPools);
      Map.clear(foreignPrivatePools);
      Map.clear(liqMapSort);
      Map.clear(liqMapSortForeign);
      timeBasedTrades := RBTree.init<Time, [Text]>();
      doInfoBeforeStep2();
      drainState := #DrainingV2;
      ignore setTimer<system>(#seconds 1, drainStep);
    } else {
      ignore setTimer<system>(#seconds 2, drainStep);
    };
  };

  // ── Phase 2: Drain V2 liquidity ─────────────────────────────────
  private func drainV2Liquidity<system>() : async () {
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var processed : Nat = 0;
    var hasMore = false;

    let batch = Vector.new<(Principal, [LiquidityPosition])>();
    for ((principal, positions) in Map.entries(userLiquidityPositions)) {
      if (Vector.size(batch) >= DRAIN_BATCH_V2) {
        hasMore := true;
      } else {
        Vector.add(batch, (principal, positions));
      };
    };

    let nowVar = Time.now();

    for ((principal, positions) in Vector.vals(batch)) {
      for (position in positions.vals()) {
        let poolKey = (position.token0, position.token1);
        switch (Map.get(AMMpools, hashtt, poolKey)) {
          case null {};
          case (?pool) {
            if (pool.totalLiquidity > 0) {
              let amount0 = (position.liquidity * pool.reserve0) / pool.totalLiquidity;
              let amount1 = (position.liquidity * pool.reserve1) / pool.totalLiquidity;
              let fee0 = position.fee0 / tenToPower60;
              let fee1 = position.fee1 / tenToPower60;
              let total0 = amount0 + fee0;
              let total1 = amount1 + fee1;

              let Tfees0 = returnTfees(position.token0);
              let Tfees1 = returnTfees(position.token1);
              if (total0 > Tfees0) {
                Vector.add(tempTransferQueueLocal, (#principal(principal), total0 - Tfees0, position.token0, genTxId()));
              };
              if (total1 > Tfees1) {
                Vector.add(tempTransferQueueLocal, (#principal(principal), total1 - Tfees1, position.token1, genTxId()));
              };

              // Update pool
              Map.set(AMMpools, hashtt, poolKey, {
                pool with
                reserve0 = safeSub(pool.reserve0, amount0);
                reserve1 = safeSub(pool.reserve1, amount1);
                totalLiquidity = safeSub(pool.totalLiquidity, position.liquidity);
                totalFee0 = safeSub(pool.totalFee0, position.fee0);
                totalFee1 = safeSub(pool.totalFee1, position.fee1);
                lastUpdateTime = nowVar;
                providers = TrieSet.delete(pool.providers, principal, Principal.hash(principal), Principal.equal);
              });
            };
          };
        };
      };
      Map.delete(userLiquidityPositions, phash, principal);
      processed += 1;
    };

    // Consolidate & send transfers
    if (Vector.size(tempTransferQueueLocal) > 0) {
      let consolidated = consolidateTransfers(tempTransferQueueLocal);
      if ((try { await treasury.receiveTransferTasks(consolidated, false) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, consolidated.vals());
      };
    };

    if (Map.size(userLiquidityPositions) == 0) {
      drainState := #DrainingV3;
      ignore setTimer<system>(#seconds 1, drainStep);
    } else {
      ignore setTimer<system>(#seconds 2, drainStep);
    };
  };

  // ── Phase 3: Drain V3 concentrated liquidity ────────────────────
  private func drainV3Liquidity<system>() : async () {
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var processed : Nat = 0;
    var hasMore = false;

    let batch = Vector.new<(Principal, [ConcentratedPosition])>();
    for ((principal, positions) in Map.entries(concentratedPositions)) {
      if (Vector.size(batch) >= DRAIN_BATCH_V3) {
        hasMore := true;
      } else {
        Vector.add(batch, (principal, positions));
      };
    };

    let nowVar = Time.now();

    for ((principal, positions) in Vector.vals(batch)) {
      for (position in positions.vals()) {
        let poolKey = (position.token0, position.token1);

        switch (Map.get(poolV3Data, hashtt, poolKey)) {
          case null {};
          case (?v3) {
            // Calculate fees via feeGrowthInside (Uniswap V3 isolation).
            let (insideNow0, insideNow1) = positionFeeGrowthInside(position, v3);
            let theoreticalFee0 = position.liquidity * safeSub(insideNow0, position.lastFeeGrowth0) / tenToPower60;
            let maxClaimable0 = safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0);
            let actualFee0 = Nat.min(theoreticalFee0, maxClaimable0);

            let theoreticalFee1 = position.liquidity * safeSub(insideNow1, position.lastFeeGrowth1) / tenToPower60;
            let maxClaimable1 = safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1);
            let actualFee1 = Nat.min(theoreticalFee1, maxClaimable1);

            // Calculate base amounts from liquidity range
            // DRIFT FIX: see removeConcentratedLiquidity — same full-range key convention.
            let sqrtLower = if (position.ratioLower == FULL_RANGE_LOWER) { FULL_RANGE_LOWER } else { ratioToSqrtRatio(position.ratioLower) };
            let sqrtUpper = if (position.ratioUpper == FULL_RANGE_UPPER) { FULL_RANGE_UPPER } else { ratioToSqrtRatio(position.ratioUpper) };
            let sqrtCurrent = v3.currentSqrtRatio;
            let (baseAmount0, baseAmount1) = amountsFromLiquidity(position.liquidity, sqrtLower, sqrtUpper, sqrtCurrent);

            let totalAmount0 = baseAmount0 + actualFee0;
            let totalAmount1 = baseAmount1 + actualFee1;

            let Tfees0 = returnTfees(position.token0);
            let Tfees1 = returnTfees(position.token1);
            if (totalAmount0 > Tfees0) {
              Vector.add(tempTransferQueueLocal, (#principal(principal), totalAmount0 - Tfees0, position.token0, genTxId()));
            };
            if (totalAmount1 > Tfees1) {
              Vector.add(tempTransferQueueLocal, (#principal(principal), totalAmount1 - Tfees1, position.token1, genTxId()));
            };

            // Update V3 pool data
            // Update range tree boundaries
            var ranges = v3.ranges;
            let liq = position.liquidity;
            switch (RBTree.get(ranges, Nat.compare, sqrtLower)) {
              case (?d) {
                let newGross = safeSub(d.liquidityGross, liq);
                if (newGross == 0) {
                  ranges := RBTree.delete(ranges, Nat.compare, sqrtLower);
                } else {
                  ranges := RBTree.put(ranges, Nat.compare, sqrtLower, { d with liquidityNet = d.liquidityNet - liq; liquidityGross = newGross });
                };
              };
              case null {
                if (test) Debug.print("TICK_TREE_MISS drainV3 lower posId=" # Nat.toText(position.positionId) # " sqrtLower=" # Nat.toText(sqrtLower));
                logger.warn("AMM", "drainV3Liquidity lower-tick miss posId=" # Nat.toText(position.positionId), "drainV3Liquidity");
              };
            };
            switch (RBTree.get(ranges, Nat.compare, sqrtUpper)) {
              case (?d) {
                let newGross = safeSub(d.liquidityGross, liq);
                if (newGross == 0) {
                  ranges := RBTree.delete(ranges, Nat.compare, sqrtUpper);
                } else {
                  ranges := RBTree.put(ranges, Nat.compare, sqrtUpper, { d with liquidityNet = d.liquidityNet + liq; liquidityGross = newGross });
                };
              };
              case null {
                if (test) Debug.print("TICK_TREE_MISS drainV3 upper posId=" # Nat.toText(position.positionId) # " sqrtUpper=" # Nat.toText(sqrtUpper));
                logger.warn("AMM", "drainV3Liquidity upper-tick miss posId=" # Nat.toText(position.positionId), "drainV3Liquidity");
              };
            };

            // Update active liquidity
            let currentRatio = if (sqrtCurrent > 0) { (sqrtCurrent * sqrtCurrent) / tenToPower60 } else { 0 };
            let newActiveLiq = if (currentRatio >= position.ratioLower and currentRatio < position.ratioUpper) {
              safeSub(v3.activeLiquidity, liq);
            } else { v3.activeLiquidity };

            Map.set(poolV3Data, hashtt, poolKey, {
              v3 with
              activeLiquidity = newActiveLiq;
              totalFeesClaimed0 = v3.totalFeesClaimed0 + actualFee0;
              totalFeesClaimed1 = v3.totalFeesClaimed1 + actualFee1;
              ranges = ranges;
            });

            // Update AMMpools reserves
            switch (Map.get(AMMpools, hashtt, poolKey)) {
              case (?pool) {
                Map.set(AMMpools, hashtt, poolKey, {
                  pool with
                  reserve0 = safeSub(pool.reserve0, totalAmount0);
                  reserve1 = safeSub(pool.reserve1, totalAmount1);
                  totalLiquidity = safeSub(pool.totalLiquidity, liq);
                  lastUpdateTime = nowVar;
                });
              };
              case null {};
            };
          };
        };
      };
      Map.delete(concentratedPositions, phash, principal);
      processed += 1;
    };

    // Consolidate & send transfers
    if (Vector.size(tempTransferQueueLocal) > 0) {
      let consolidated = consolidateTransfers(tempTransferQueueLocal);
      if ((try { await treasury.receiveTransferTasks(consolidated, false) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, consolidated.vals());
      };
    };

    if (Map.size(concentratedPositions) == 0) {
      // Clear all pool data
      Map.clear(poolV3Data);
      Map.clear(AMMpools);
      doInfoBeforeStep2();
      drainState := #SweepingFees;
      ignore setTimer<system>(#seconds 1, drainStep);
    } else {
      ignore setTimer<system>(#seconds 2, drainStep);
    };
  };

  // ── Phase 4: Sweep fees ──────────────────────────────────────────
  private func drainFees<system>() : async () {
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    // DAO accumulated fees → drainTarget
    for ((token, amount) in Map.entries(feescollectedDAO)) {
      let Tfees = returnTfees(token);
      if (amount > Tfees) {
        Vector.add(tempTransferQueueLocal, (#principal(drainTarget), amount - Tfees, token, genTxId()));
      };
    };
    Map.clear(feescollectedDAO);

    // Referrer fees → referrer principals (these are user funds)
    for ((referrer, optEntry) in Map.entries(referrerFeeMap)) {
      switch (optEntry) {
        case (?(feeVec, _)) {
          for ((token, amount) in Vector.vals(feeVec)) {
            let Tfees = returnTfees(token);
            if (amount > Tfees) {
              Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(referrer)), amount - Tfees, token, genTxId()));
            };
          };
        };
        case null {};
      };
    };
    Map.clear(referrerFeeMap);

    // Consolidate & send transfers
    if (Vector.size(tempTransferQueueLocal) > 0) {
      let consolidated = consolidateTransfers(tempTransferQueueLocal);
      if ((try { await treasury.receiveTransferTasks(consolidated, false) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, consolidated.vals());
      };
    };

    doInfoBeforeStep2();
    drainState := #SweepingRemainder;
    ignore setTimer<system>(#seconds 2, drainStep);
  };

  // ── Phase 5: Sweep remaining balances to target ──────────────────
  private func sweepRemainder<system>() : async () {
    // Flush pending transfer queue first
    var settleRounds = 0;
    label settle loop {
      if (Vector.size(tempTransferQueue) > 0) {
        let snap = Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
        Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
        let ok = try { await treasury.receiveTransferTasks(snap, false) } catch (_) { false };
        if (not ok) { Vector.addFromIter<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue, snap.vals()) };
      };
      try { await treasury.drainTransferQueue() } catch (_) {};
      let pending = try { await treasury.getPendingTransferCount() } catch (_) { 0 };
      if (pending == 0 and Vector.size(tempTransferQueue) == 0) { break settle };
      settleRounds += 1;
      if (settleRounds >= 30) { break settle };
    };

    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    // Query on-chain balances and sweep to drainTarget
    for (token in acceptedTokens.vals()) {
      let Tfees = returnTfees(token);

      let balance : Nat = if (token == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
        let act = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : Ledger.Interface;
        nat64ToNat((await act.account_balance_dfx({ account = Utils.accountToText(Utils.principalToAccount(treasury_principal)) })).e8s);
      } else {
        let act = actor (token) : ICRC1.FullInterface;
        await act.icrc1_balance_of({ owner = treasury_principal; subaccount = null });
      };

      if (balance > Tfees + 1000) {
        Vector.add(tempTransferQueueLocal, (#principal(drainTarget), balance - Tfees, token, genTxId()));
      };
    };

    if (Vector.size(tempTransferQueueLocal) > 0) {
      let consolidated = consolidateTransfers(tempTransferQueueLocal);
      if ((try { await treasury.receiveTransferTasks(consolidated, false) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, consolidated.vals());
      };
    };

    // Final settle loop: ensure the sweep transfers we just queued actually drain out
    // of treasury BEFORE declaring Done. Without this, #Done could be reached with tokens
    // still sitting in treasury.transferQueue, never reaching drainTarget.
    var finalRounds = 0;
    label finalSettle loop {
      if (Vector.size(tempTransferQueue) > 0) {
        let snap2 = Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
        Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
        let ok2 = try { await treasury.receiveTransferTasks(snap2, false) } catch (_) { false };
        if (not ok2) { Vector.addFromIter<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue, snap2.vals()) };
      };
      try { await treasury.drainTransferQueue() } catch (_) {};
      let pending2 = try { await treasury.getPendingTransferCount() } catch (_) { 0 };
      if (pending2 == 0 and Vector.size(tempTransferQueue) == 0) { break finalSettle };
      finalRounds += 1;
      if (finalRounds >= 30) { break finalSettle };
    };

    drainState := #Done;
  };

  // Admin function to clean stray whitespace/tab characters from stored token IDs and force a full metadata refresh.
  public shared ({ caller }) func cleanTokenIds() : async ExTypes.ActionResult {
    if (not ownercheck(caller)) { return #Err(#NotAuthorized) };

    func sanitize(t : Text) : Text {
      Text.trim(t, #predicate(func(c : Char) : Bool { c == ' ' or c == '\t' or c == '\n' or c == '\r' }));
    };

    // Clean acceptedTokens
    acceptedTokens := Array.map<Text, Text>(acceptedTokens, sanitize);

    // Clean pool_canister entries
    let poolSize = Vector.size(pool_canister);
    let newPools = Vector.new<(Text, Text)>();
    for (i in Iter.range(0, if (poolSize == 0) { -1 } else { poolSize - 1 : Int })) {
      let (a, b) = Vector.get(pool_canister, i);
      Vector.add(newPools, (sanitize(a), sanitize(b)));
    };
    pool_canister := newPools;
    rebuildPoolIndex();

    // Clean baseTokens
    baseTokens := Array.map<Text, Text>(baseTokens, sanitize);

    // Force full metadata refresh
    try {
      await treasury.getAcceptedtokens(acceptedTokens);
      updateTokenInfo<system>(true, true, await treasury.getTokenInfo());
      updateStaticInfo();
      doInfoBeforeStep2();
      return #Ok("Cleaned " # Nat.toText(acceptedTokens.size()) # " tokens and rebuilt metadata");
    } catch (err) {
      return #Ok("Cleaned IDs but metadata refresh failed: " # Error.message(err));
    };
  };

  // This function is actually made for testing, to check whether the balances are still balanced. Also gives the ption to use this as collectFees, howver this cant be done in production
  // as it cant take into account transfers sent to the Exchange but not yet processed.
  public shared ({ caller }) func checkDiffs(returnFees : Bool, alwaysShow : Bool) : async ?(Bool, [(Int, Text)], [[{ accessCode : Text; identifier : Text; poolCanister : (Text, Text) }]]) {
    if (not ownercheck(caller)) {
      return null;
    };
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    // Settlement loop: only in test mode (state-modifying)
    if (test) {
      var settleRounds = 0;
      label settle loop {
        if (Vector.size(tempTransferQueue) > 0) {
          let snap = Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
          Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
          let ok = try { await treasury.receiveTransferTasks(snap, isInAllowedCanisters(caller)) } catch (_) { false };
          if (not ok) { Vector.addFromIter<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue, snap.vals()) };
        };
        try { await treasury.drainTransferQueue() } catch (_) {};
        let pending = try { await treasury.getPendingTransferCount() } catch (_) { 0 };
        if (pending == 0 and Vector.size(tempTransferQueue) == 0) {
          break settle;
        };
        settleRounds += 1;
        if (settleRounds >= 30) {
          Debug.print("DRIFT_SETTLE_EXHAUSTED: settleRounds=" # Nat.toText(settleRounds)
            # " treasuryPending=" # Nat.toText(pending)
            # " exchangeTempTransferQueue=" # Nat.toText(Vector.size(tempTransferQueue)));
          break settle
        };
      };
      let finalPending = try { await treasury.getPendingTransferCount() } catch (_) { 0 };
      Debug.print("DRIFT_FINAL_PENDING: treasuryPending=" # Nat.toText(finalPending)
        # " exchangeTempTransferQueue=" # Nat.toText(Vector.size(tempTransferQueue)));
    };

    let balancesVec = Vector.new<Int>();
    let feebalancesVec = Vector.new<Int>();
    let orderbalanceVec = Vector.new<Int>();
    let ammbalanceVec = Vector.new<Int>();
    let orderAccessCodesVec = Vector.new<[{
      accessCode : Text;
      identifier : Text;
      poolCanister : (Text, Text);
    }]>();
    let innie : Int = 0;

    // PHASE A — launch every ledger balance query in parallel (no awaits yet).
    // ICP ledger supports icrc1_balance_of since the SNS upgrade, so we can use a
    // single uniform interface across all accepted tokens. Plus one parallel call
    // to the treasury for pending-outgoing-transfer sums (so we can subtract them
    // from balance to avoid spurious positive-drift readings while transfers are
    // queued but not yet executed at the ledger).
    let balanceFuturesVec = Vector.new<async Nat>();
    for (i in acceptedTokens.vals()) {
      let act = actor (i) : ICRC1.FullInterface;
      Vector.add(balanceFuturesVec, act.icrc1_balance_of({ owner = treasury_principal; subaccount = null }));
    };
    let pendingFuture = treasury.getPendingTransfersByToken();

    // PHASE B — compute local accounting (orderbalance / ammbalance / fees) per
    // token. No awaits; runs while ledger queries are in flight.
    for (i in acceptedTokens.vals()) {
      let Tfees = returnTfees(i);

      var as : Nat = 0;
      switch (Map.get(feescollectedDAO, thash, i)) {
        case (?asi) { as := asi };
        case _ {};
      };
      // Also count referrer fees (held in referrerFeeMap, not in feescollectedDAO)
      for ((_, optEntry) in Map.entries(referrerFeeMap)) {
        switch (optEntry) {
          case (?(fees, _)) {
            for ((token, amount) in Vector.vals(fees)) {
              if (token == i) { as += amount };
            };
          };
          case _ {};
        };
      };
      Vector.add(feebalancesVec, innie + as);

      var openorders : Int = 0;
      var ammliquidity : Int = 0;
      let orderCodesVec = Vector.new<{
        accessCode : Text;
        identifier : Text;
        poolCanister : (Text, Text);
      }>();
      // Public orderbook (own + foreign pools)
      for ((poolKey, poolValue) in Map.entries(liqMapSort)) {
        let (token1, token2) = poolKey;
        if (token1 == i or token2 == i) {
          for ((_, trades) in RBTree.entries(poolValue)) {
            for (trade in trades.vals()) {
              if (trade.token_init_identifier == i) {
                openorders += trade.amount_init + (((trade.amount_init * trade.Fee) / (10000 * trade.RevokeFee)) * (trade.RevokeFee - 1)) + Tfees;
                Vector.add(orderCodesVec, { accessCode = trade.accesscode; identifier = i; poolCanister = poolKey });
              };
            };
          };
        };
      };
      for ((poolKey, poolValue) in Map.entries(liqMapSortForeign)) {
        let (token1, token2) = poolKey;
        if (token1 == i or token2 == i) {
          for ((_, trades) in RBTree.entries(poolValue)) {
            for (trade in trades.vals()) {
              if (trade.token_init_identifier == i) {
                openorders += trade.amount_init + (((trade.amount_init * trade.Fee) / (10000 * trade.RevokeFee)) * (trade.RevokeFee - 1)) + Tfees;
                Vector.add(orderCodesVec, { accessCode = trade.accesscode; identifier = i; poolCanister = poolKey });
              };
            };
          };
        };
      };
      // Private trades
      for ((key, value) in Map.entries(tradeStorePrivate)) {
        if (value.token_init_identifier == i) {
          openorders += value.amount_init + (((value.amount_init * value.Fee) / (10000 * value.RevokeFee)) * (value.RevokeFee - 1)) + Tfees;
          Vector.add(orderCodesVec, { accessCode = key; identifier = i; poolCanister = (value.token_init_identifier, value.token_sell_identifier) });
        };
      };
      // AMM pool reserves + V3 outstanding fees
      for ((poolKey, pool) in Map.entries(AMMpools)) {
        if (poolKey.0 == i) {
          ammliquidity += pool.reserve0 + (pool.totalFee0 / tenToPower60);
          switch (Map.get(poolV3Data, hashtt, poolKey)) {
            case (?v3) { ammliquidity += safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0) };
            case null {};
          };
        } else if (poolKey.1 == i) {
          ammliquidity += pool.reserve1 + (pool.totalFee1 / tenToPower60);
          switch (Map.get(poolV3Data, hashtt, poolKey)) {
            case (?v3) { ammliquidity += safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1) };
            case null {};
          };
        };
      };
      Vector.add(orderbalanceVec, innie + openorders);
      Vector.add(ammbalanceVec, innie + ammliquidity);
      Vector.add(orderAccessCodesVec, Vector.toArray(orderCodesVec));
    };

    // PHASE C — await all parallel queries. Build pending-by-token map from the
    // treasury's outgoing queue + the exchange's local tempTransferQueue, so a
    // queued payout shows up as "balance owed to leave" rather than positive drift.
    let pendingArr = await pendingFuture;
    let pendingByToken = Map.new<Text, Nat>();
    for ((tok, amt) in pendingArr.vals()) {
      Map.set(pendingByToken, thash, tok, amt);
    };
    for (txn in Vector.vals(tempTransferQueue)) {
      let tok = txn.2;
      let amt = txn.1;
      let cur = switch (Map.get(pendingByToken, thash, tok)) { case (?n) n; case null 0 };
      Map.set(pendingByToken, thash, tok, cur + amt);
    };

    let balanceFutures = Vector.toArray(balanceFuturesVec);
    var balIdx : Nat = 0;
    for (i in acceptedTokens.vals()) {
      let raw = await balanceFutures[balIdx];
      let pending : Nat = switch (Map.get(pendingByToken, thash, i)) { case (?n) n; case null 0 };
      let adjusted : Nat = if (raw >= pending) { raw - pending } else { 0 };
      Vector.add(balancesVec, innie + adjusted);
      balIdx += 1;
    };

    let balances = Vector.toArray(balancesVec);
    let feebalances = Vector.toArray(feebalancesVec);
    let orderbalance = Vector.toArray(orderbalanceVec);
    let ammbalance = Vector.toArray(ammbalanceVec);
    let orderAccessCodes = Vector.toArray(orderAccessCodesVec);

    var i2 = 0;
    var error = false;
    let differenceVec = Vector.new<(Int, Text)>();

    for (i in acceptedTokens.vals()) {
      let Tfees = returnTfees(i);
      let drift = balances[i2] - (orderbalance[i2] + ammbalance[i2] + feebalances[i2]);
      Vector.add(differenceVec, (drift, i));
      if (test) {
        // Always print in test mode so we can diff between timers.
        Debug.print("DRIFT_COMPONENTS " # i # ": bal=" # debug_show(balances[i2])
          # " ord=" # debug_show(orderbalance[i2])
          # " amm=" # debug_show(ammbalance[i2])
          # " fee=" # debug_show(feebalances[i2])
          # " drift=" # debug_show(drift));
        // Per-AMM-pool reserve breakdown for this token — makes it visible which
        // specific pool is overstating reserve vs. what treasury actually holds.
        for ((poolKey, pool) in Map.entries(AMMpools)) {
          if (poolKey.0 == i) {
            let v3Fee = switch (Map.get(poolV3Data, hashtt, poolKey)) {
              case (?v3) { safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0) };
              case null { 0 };
            };
            Debug.print("  POOL " # poolKey.0 # "/" # poolKey.1 # " [0]: reserve=" # Nat.toText(pool.reserve0) # " totalFee=" # Nat.toText(pool.totalFee0) # " v3Resid=" # Nat.toText(v3Fee) # " totalLiq=" # Nat.toText(pool.totalLiquidity));
          } else if (poolKey.1 == i) {
            let v3Fee = switch (Map.get(poolV3Data, hashtt, poolKey)) {
              case (?v3) { safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1) };
              case null { 0 };
            };
            Debug.print("  POOL " # poolKey.0 # "/" # poolKey.1 # " [1]: reserve=" # Nat.toText(pool.reserve1) # " totalFee=" # Nat.toText(pool.totalFee1) # " v3Resid=" # Nat.toText(v3Fee) # " totalLiq=" # Nat.toText(pool.totalLiquidity));
          };
        };
      };
      i2 += 1;
    };
    if (test) {
      // Dump the drift-op tracker — per-op cumulative expected tracked-side deltas.
      // Compare to DRIFT_COMPONENTS to see which op's cumulative delta correlates
      // with a token's drift magnitude.
      Debug.print("DRIFT_OP_TRACKER:");
      for ((key, delta) in Map.entries(driftOpTracker)) {
        Debug.print("  " # key # " = " # debug_show(delta));
      };
    };
    i2 := 0;
    if (test and returnFees) {
      for (i in acceptedTokens.vals()) {
        let Tfees = returnTfees(i);
        if (Int.abs(balances[i2]) > Int.abs(orderbalance[i2])) {
          if (Int.abs(balances[i2]) - Int.abs(orderbalance[i2]) > Tfees) {
            Vector.add(tempTransferQueueLocal, (#principal(owner3), (Int.abs(balances[i2]) -Int.abs(orderbalance[i2])) -Tfees, i, genTxId()));
            Map.set(feescollectedDAO, thash, i, 0);
          };
        };
        i2 += 1;

      };
      // Transfering the transactions that have to be made to the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {

      } else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
    };

    // Adjust the difference array to account for minimum liquidity
    var difference = Vector.toArray(differenceVec);
    let check = Map.new<Text, Null>();
    difference := Array.map<(Int, Text), (Int, Text)>(
      difference,
      func(diff : (Int, Text)) : (Int, Text) {
        let (amount, token) = diff;
        var adjustedAmount = amount;
        if (Map.has(check, thash, token)) {
          return (amount, token);
        };

        label a for (tokenMin in (TrieSet.toArray(AMMMinimumLiquidityDone)).vals()) {

          if (token == tokenMin) {
            adjustedAmount -= minimumLiquidity; // Subtract minimumLiquidity for each pool the token is in
            Map.set(check, thash, token, null);
            break a;
          };
        };

        (adjustedAmount, token);
      },
    );


    // Launch identity can reclaim positive drift (minus 1000 buffer) in non-test mode
    let launchPrincipal = Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe");
    if (caller == launchPrincipal and returnFees and not test) {
      let reclaimTransfers = Vector.new<(TransferRecipient, Nat, Text, Text)>();
      for ((drift, token) in difference.vals()) {
        if (drift > 1000) {
          let amount : Nat = Int.abs(drift) - 1000;
          let Tfees = returnTfees(token);
          if (amount > Tfees) {
            Vector.add(reclaimTransfers, (#principal(launchPrincipal), amount - Tfees, token, genTxId()));
          };
        };
      };
      if (Vector.size(reclaimTransfers) > 0) {
        if (not (try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(reclaimTransfers), isInAllowedCanisters(caller)) } catch (_) { false })) {
          Vector.addFromIter(tempTransferQueue, Vector.vals(reclaimTransfers));
        };
      };
    };

    label a for (i in difference.vals()) {
      if ((i.0 < 0 or i.0 > 1000) or alwaysShow) {
        error := true;
        break a;
      };
    };

    return ?(error, difference, orderAccessCodes);
  };

  // If some funds get stuck during the DAO transaction, this function helps the DAO retrieve it.
  public shared ({ caller }) func retrieveFundsDao(trades : [(Text, Nat64)]) : async () {
    if (not DAOcheck(caller)) {
      return;
    };
    let nowVar = Time.now();
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    label a for (i in Iter.range(0, trades.size())) {
      if (Map.has(BlocksDone, thash, trades[i].0 # ":" # Nat64.toText(trades[i].1))) {
        continue a;
      };
      Map.set(BlocksDone, thash, trades[i].0 # ":" # Nat64.toText(trades[i].1), nowVar);
      let tType = returnType(trades[i].0);
      //Doing it this way so checkReceive does not have to be awaited, effectively eliminating pressure on the process queue
      let blockData = try {
        await* getBlockData(trades[i].0, nat64ToNat(trades[i].1), tType);
      } catch (err) {
        Map.delete(BlocksDone, thash, trades[i].0 # ":" # Nat64.toText(trades[i].1));
        #ICRC12([]);
      };
      let (receiveBool, receiveTransfers) = if (blockData != #ICRC12([])) checkReceive(nat64ToNat(trades[i].1), DAOTreasury, 0, trades[i].0, ICPfee, RevokeFeeNow, true, true, blockData, tType, nowVar) else {
        Map.delete(BlocksDone, thash, trades[i].0 # ":" # Nat64.toText(trades[i].1));
        (false, []);
      };
      Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());

    };
    // Transfering the transactions that have to be made to the treasury,
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {

    } else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
    };
  };

  private func getAllTradesDAOFilter(trades : [TradeData]) : FilteredTradeResult {
    let logFilterEntries = Vector.new<Text>();

    func logFilter(message : Text) {
      Vector.add(logFilterEntries, "getAllTradesDAOFilter- " # message);
    };

    // Debug logging of input
    if verboseLogging {
      logFilter("/////////");
      logFilter(debug_show (trades));
      logFilter("////////");
    };

    // Create combination pairs — pre-filter buyers/sellers to reduce iterations
    let combinationbuffer = Vector.new<(Text, Text)>();
    let TradeEntryVector = Vector.new<TradeEntry>();

    let buyers = Vector.new<Nat>();
    let sellers = Vector.new<Nat>();
    for (i in Iter.range(0, trades.size() - 1)) {
      if (trades[i].amountBuy > 0) Vector.add(buyers, i);
      if (trades[i].amountSell > 0) Vector.add(sellers, i);
    };

    for (bi in Vector.vals(buyers)) {
      for (si in Vector.vals(sellers)) {
        if (trades[bi].identifier != trades[si].identifier) {
          Vector.add(combinationbuffer, (trades[bi].identifier, trades[si].identifier));
          if verboseLogging { logFilter(debug_show ((trades[bi].identifier, trades[si].identifier))) };
        };
      };
    };

    // Initialize tracking arrays
    // Persistent buffers — mutated in-place throughout the combination loop
    let bufAmountBuy2 = Buffer.Buffer<Nat>(trades.size());
    let bufAmountBuy = Buffer.Buffer<Nat>(trades.size());
    let bufAmountSell2 = Buffer.Buffer<Nat>(trades.size());
    let bufAmountSell3 = Buffer.Buffer<Nat>(trades.size());
    let bufAmountFeesSell = Buffer.Buffer<Nat>(trades.size());
    let bufAmountFeesBuy = Buffer.Buffer<Nat>(trades.size());
    let bufTimesTFees = Buffer.Buffer<Nat>(trades.size());
    let bufRepMaker = Buffer.Buffer<[(Text, Nat)]>(trades.size());

    for (t in trades.vals()) {
      bufAmountBuy2.add(t.amountBuy);
      bufAmountBuy.add(0);
      bufAmountSell2.add(t.amountSell * 10000);
      bufAmountSell3.add(0);
      bufAmountFeesSell.add(0);
      bufAmountFeesBuy.add(0);
      bufTimesTFees.add(0);
      bufRepMaker.add([]);
    };

    let identifierIndex = Map.new<Text, Nat>();
    for (i in Iter.range(0, trades.size() - 1)) {
      Map.set(identifierIndex, thash, trades[i].identifier, i);
    };

    for (index in Iter.range(0, Vector.size(combinationbuffer) - 1)) {

      let cbget = Vector.get(combinationbuffer, index);
      let nonPoolOrder = not isKnownPool(cbget.1, cbget.0);
      var liquidityInPool = switch (Map.get(if nonPoolOrder { liqMapSortForeign } else { liqMapSort }, hashtt, (cbget.0, cbget.1))) {
        case null {
          RBTree.init<Ratio, [{ time : Int; accesscode : Text; amount_init : Nat; amount_sell : Nat; Fee : Nat; RevokeFee : Nat; initPrincipal : Text; OCname : Text; token_init_identifier : Text; token_sell_identifier : Text; strictlyOTC : Bool; allOrNothing : Bool }]>();
        };
        case (?foundTrades) { foundTrades };
      };

      var amountCoveredSell : Nat = 0;
      var amountCoveredBuy : Nat = 0;
      var cumAmountSell : Nat = 0;
      var cumAmountOTCFeesSell : Nat = 0;
      var cumAmountOTCFeesBuy : Nat = 0;

      let tokenbuy = cbget.0;
      let tokensell = cbget.1;

      var tokenInIsToken0 = false;

      // Check AMM pool first
      let poolKey = getPool(tokenbuy, tokensell);
      var poolRatio : Ratio = #Zero;
      var pool = switch (Map.get(AMMpools, hashtt, poolKey)) {
        case (null) {
          if verboseLogging { logFilter("No AMM pool found for pair") };
          poolRatio := #Max;
          null;
        };
        case (?p) {
          tokenInIsToken0 := tokensell == p.token0;
          if verboseLogging { logFilter("Found AMM pool: " # debug_show (p)) };
          poolRatio := computePoolRatioFor(p, tokensell);
          ?p;
        };
      };

      var totalPoolFeeAmount = 0;
      var totalProtocolFeeAmount = 0;
      // Get best orderbook ratio first
      let bestOrderbookRatio = switch (RBTree.scanLimit(liquidityInPool, compareRatio, #Zero, #Max, #bwd, 1).results) {
        case (array) {
          if (array.size() > 0) {

            array[0].0;
          } else {

            #Zero;
          };
        };
        case _ {

          #Zero;
        };
      };

      // Get indices for arrays
      var buyindex = 99;
      var sellindex = 99;

      switch (Map.get(identifierIndex, thash, tokenbuy)) {
        case null {};
        case (?idx) { buyindex := idx };
      };
      switch (Map.get(identifierIndex, thash, tokensell)) {
        case null {};
        case (?idx) { sellindex := idx };
      };

      var toTradeBuy = bufAmountBuy2.get(buyindex);
      var toTradeSell = bufAmountSell2.get(sellindex);
      var ICPPricebuyindex = trades[buyindex].ICPPrice;
      var Decimalsbuyindex = trades[buyindex].decimals;
      var ICPPricesellindex = trades[sellindex].ICPPrice;
      var Decimalssellindex = trades[sellindex].decimals;
      var Transferfeessellindex = trades[sellindex].transferFee;
      var Transferfeesbuyindex = trades[buyindex].transferFee;
      var TimesTFeessellindex = bufTimesTFees.get(sellindex);
      var TimesTFeesbuyindex = bufTimesTFees.get(buyindex);
      let orderRatio : Ratio = #Value(((ICPPricebuyindex * tenToPower120) / (10 ** Decimalsbuyindex)) / ((ICPPricesellindex * tenToPower60) / (10 ** Decimalssellindex)));

      switch (pool) {
        case (?p) {
          if verboseLogging {
            logFilter("Initial pool state: " # debug_show (p));
            logFilter("Current amounts - toTradeBuy: " # debug_show (toTradeBuy) # " toTradeSell: " # debug_show (toTradeSell / 10000));
            logFilter("Current coverage - amountCoveredBuy: " # debug_show (amountCoveredBuy) # " amountCoveredSell: " # debug_show (amountCoveredSell));
          };
          if ((toTradeBuy * tenToPower64) != 0 and toTradeSell > (Transferfeessellindex * 10000)) {
            //let orderRatio:Ratio = #Value((toTradeBuy * tenToPower64) / (toTradeSell-(Transferfeessellindex*10000)));
            let remainingSell = if (toTradeSell >= amountCoveredSell + (Transferfeessellindex * 10000)) {
              ((toTradeSell - amountCoveredSell - (Transferfeessellindex * 10000)) * (10000000 - (7000000 * (ICPfee * 1000) / 10000000))) / 10000000;
            } else {
              0;
            };

            //let orderRatio:Ratio = #Value(((toTradeBuy-amountCoveredBuy)*tenToPower60) /((remainingSell)/10000));

            if (p.reserve0 != 0 and p.reserve1 != 0 and compareRatio(poolRatio, orderRatio) == #greater and orderRatio != #Zero and compareRatio(poolRatio, bestOrderbookRatio) == #greater) {

              if verboseLogging {
                logFilter("Calculated orderRatio: " # debug_show (orderRatio));
                logFilter("Best orderbook ratio: " # debug_show (bestOrderbookRatio));
                logFilter("Pool ratio: " # debug_show (poolRatio));
              };

              let targetRatio = if (compareRatio(bestOrderbookRatio, orderRatio) == #less or bestOrderbookRatio == #Zero) {
                if verboseLogging { logFilter("Using orderRatio as target") };
                orderRatio;
              } else {
                if verboseLogging { logFilter("Using bestOrderbookRatio as target") };
                bestOrderbookRatio;
              };

              let (ammAmount, ammEffectiveRatio) = getAMMLiquidity(p, targetRatio, tokensell);
              if verboseLogging { logFilter("AMM Liquidity check - amount: " # debug_show (ammAmount) # " effectiveRatio: " # debug_show (ammEffectiveRatio)) };

              let remainingBuy = if (toTradeBuy >= amountCoveredBuy) {
                toTradeBuy - amountCoveredBuy;
              } else { 0 };

              if verboseLogging { logFilter("Remaining amounts - buy: " # debug_show (remainingBuy) # " sell: " # debug_show (remainingSell / 10000)) };

              label a if (ammAmount > 10000 and remainingBuy > Transferfeesbuyindex and remainingSell > Transferfeessellindex * 10000) {
                let amountToSwap = Nat.min(
                  ammAmount,
                  remainingSell / 10000,
                );
                if verboseLogging { logFilter("Amount to swap: " # debug_show (amountToSwap)) };

                let (amountIn, amountOut, _, _, protocolFeeAmount, poolFeeAmount, updatedPool) = swapWithAMM(p, tokenInIsToken0, amountToSwap, targetRatio, ICPfee);
                if verboseLogging {
                  logFilter("Swap results - amountIn: " # debug_show (amountIn) # " amountOut: " # debug_show (amountOut));
                  logFilter("Fees - protocol: " # debug_show (protocolFeeAmount) # " pool: " # debug_show (poolFeeAmount));
                };

                let oldCoveredBuy = amountCoveredBuy;
                let oldCoveredSell = amountCoveredSell;
                let oldCumAmount = cumAmountSell;
                let oldCumFees = cumAmountOTCFeesSell;

                amountCoveredBuy += amountOut;
                amountCoveredSell += (amountIn +protocolFeeAmount +poolFeeAmount) * 10000;
                cumAmountSell += amountIn +poolFeeAmount;
                cumAmountOTCFeesSell += (protocolFeeAmount +poolFeeAmount) * 10000;
                totalPoolFeeAmount += poolFeeAmount;
                totalProtocolFeeAmount += protocolFeeAmount;

                if verboseLogging {
                  logFilter("Updated amounts - coveredBuy: " # debug_show (amountCoveredBuy) # " (delta: " # debug_show (amountCoveredBuy - oldCoveredBuy) # ")");
                  logFilter("Updated amounts - coveredSell: " # debug_show (amountCoveredSell) # " (delta: " # debug_show (amountCoveredSell - oldCoveredSell) # ")");
                  logFilter("Updated cumulative - amount: " # debug_show (cumAmountSell) # " (delta: " # debug_show (cumAmountSell - oldCumAmount) # ")");
                  logFilter("Updated cumulative - fees: " # debug_show (cumAmountOTCFeesSell) # " (delta: " # debug_show (cumAmountOTCFeesSell - oldCumFees) # ")");
                  logFilter("Total fees - pool: " # debug_show (totalPoolFeeAmount) # " protocol: " # debug_show (totalProtocolFeeAmount));
                };

                Map.set(AMMpools, hashtt, poolKey, updatedPool);
                poolRatio := computePoolRatioFor(updatedPool, tokensell);

                pool := ?updatedPool;
                if verboseLogging { logFilter("Updated pool state: " # debug_show (updatedPool)) };
              } else {
                if verboseLogging { logFilter("Skipping AMM swap - insufficient amounts or liquidity") };
              };
            };
          };
        };
        case null {
          if verboseLogging { logFilter("No pool found") };
        };
      };
      var notFirstLoop = false;
      let representVec = Vector.new<(Text, Nat)>();

      // Process orderbook entries
      label sortedMap for ((currentRatio, trades) in RBTree.entriesRev(liquidityInPool)) {

        let remainingSell = if (toTradeSell >= amountCoveredSell + (Transferfeessellindex * 10000)) {
          ((toTradeSell - amountCoveredSell - (Transferfeessellindex * 10000)) * (10000000 - (7000000 * (ICPfee * 1000) / 10000000))) / 10000000;
        } else {
          0;
        };

        // let orderRatio:Ratio = #Value(((toTradeBuy-amountCoveredBuy)*tenToPower60) /((remainingSell)/10000));
        //let orderRatio:Ratio = if ((toTradeBuy * tenToPower64) != 0 and toTradeSell > (Transferfeessellindex*10000)){#Value((toTradeBuy * tenToPower64) / (toTradeSell-(Transferfeessellindex*10000)));}else{#Max};
        if (isLessThanRatio(currentRatio, orderRatio)) {

          break sortedMap;
        };
        if (notFirstLoop) {
          // Between ratios AMM check:
          switch (pool) {
            case (?p) {
              let (ammAmount, _) = getAMMLiquidity(p, currentRatio, tokensell);

              if verboseLogging {
                logFilter("Calculated orderRatio: " # debug_show (orderRatio));
                logFilter("Current ratio: " # debug_show (currentRatio));
                logFilter("Pool ratio: " # debug_show (poolRatio));
              };

              if (p.reserve0 != 0 and p.reserve1 != 0 and compareRatio(poolRatio, currentRatio) == #greater and orderRatio != #Zero) {

                label a if (ammAmount > 10000 and (if (toTradeBuy >= amountCoveredBuy) { toTradeBuy - amountCoveredBuy } else { 0 }) > Transferfeesbuyindex and remainingSell > Transferfeessellindex * 10000) {
                  let amountToSwap = Nat.min(
                    ammAmount,
                    remainingSell / 10000,
                  );
                  let (amountIn, amountOut, _, _, protocolFeeAmount, poolFeeAmount, updatedPool) = swapWithAMM(p, tokenInIsToken0, amountToSwap, currentRatio, ICPfee);

                  amountCoveredBuy += amountOut;
                  amountCoveredSell += (amountIn +protocolFeeAmount +poolFeeAmount) * 10000;
                  cumAmountSell += amountIn +poolFeeAmount;
                  cumAmountOTCFeesSell += (protocolFeeAmount +poolFeeAmount) * 10000;
                  totalPoolFeeAmount += poolFeeAmount;
                  totalProtocolFeeAmount += protocolFeeAmount;

                  // Update pool and pool ratio (V3-aware refresh)
                  Map.set(AMMpools, hashtt, poolKey, updatedPool);
                  poolRatio := computePoolRatioFor(updatedPool, tokensell);
                  pool := ?updatedPool;
                };
              };
            };
            case null {};
          };
        } else {
          notFirstLoop := true;
        };

        // Process trades at current ratio
        var kk3 = trades;
        label createTrade for (data in kk3.vals()) {
          if (data.allOrNothing) {
            continue createTrade;
          };
          var breakpls = 0;
          if (
            (((data.amount_init * ICPPricebuyindex) * 30000000) / (10 ** Decimalsbuyindex)) > (((data.amount_sell * ICPPricesellindex) * 30000000) / (10 ** Decimalssellindex))
          ) {

            var newAmountSell = data.amount_sell * 10000;
            var newAmountBuy = data.amount_init;

            if (
              (
                toTradeSell <= (amountCoveredSell + newAmountSell) + (data.amount_sell * data.Fee) +
                (Transferfeessellindex * 10000)
              ) and ((toTradeBuy <= amountCoveredBuy + data.amount_init) == false)
            ) {

              newAmountSell := (((toTradeSell - amountCoveredSell) * (10000 - data.Fee)) / 10000) -
              (Transferfeessellindex * 10000);
              newAmountBuy := (
                ((newAmountSell * 10 ** 70) / (data.amount_sell * 10 ** 4)) * (data.amount_init)
              ) / 10 ** 70;
              breakpls := 1;
            } else if (toTradeBuy <= amountCoveredBuy + data.amount_init) {
              newAmountBuy := toTradeBuy - amountCoveredBuy;
              newAmountSell := (
                ((newAmountBuy * 10 ** 60) / data.amount_init) * (data.amount_sell * 10000)
              ) / 10 ** 60;
              breakpls := 1;
            } else {
              TimesTFeesbuyindex += 1;
            };

            amountCoveredSell += newAmountSell + ((newAmountSell * data.Fee) / 10000) +(Transferfeessellindex * 10000);
            amountCoveredBuy += newAmountBuy;
            cumAmountSell += newAmountSell / 10000;
            cumAmountOTCFeesSell += ((newAmountSell * data.Fee) / 10000);
            cumAmountOTCFeesBuy += (
              newAmountBuy * data.Fee * (((data.RevokeFee - 1) * 10 ** 20) / data.RevokeFee)
            ) / 10 ** 20;
            TimesTFeessellindex += 1;

            if verboseLogging { logFilter("Adding trade: " # debug_show ({ accesscode = data.accesscode; amount_sell = newAmountSell / 10000; amount_init = newAmountBuy; token_sell_identifier = tokensell; token_init_identifier = tokenbuy; Fee = data.Fee; InitPrincipal = data.initPrincipal })) };

            Vector.add(
              TradeEntryVector,
              {
                accesscode = data.accesscode;
                amount_sell = newAmountSell / 10000;
                amount_init = newAmountBuy;
                token_sell_identifier = tokensell;
                token_init_identifier = tokenbuy;
                Fee = data.Fee;
                InitPrincipal = data.initPrincipal;
              },
            );

            Vector.add(
              representVec,
              (
                data.initPrincipal,
                (
                  (newAmountBuy * data.Fee * (((data.RevokeFee - 1) * 10 ** 20) / data.RevokeFee)) / 10 ** 20
                ) / 10000,
              ),
            );

            if (breakpls == 1) {
              break sortedMap;
            };
          };
        };
      };

      // Final AMM check:
      switch (pool) {
        case (?p) {

          if (toTradeBuy > amountCoveredBuy and toTradeSell > amountCoveredSell) {
            if (((toTradeBuy -amountCoveredBuy) * tenToPower64) != 0 and (toTradeSell -amountCoveredSell) > (Transferfeessellindex * 10000)) {
              //let orderRatio:Ratio = #Value(((toTradeBuy - amountCoveredBuy)  * tenToPower64) / (toTradeSell-amountCoveredSell-(Transferfeessellindex*10000)));
              let remainingSell = if (toTradeSell >= amountCoveredSell + (Transferfeessellindex * 10000)) {
                ((toTradeSell - amountCoveredSell - (Transferfeessellindex * 10000)) * (10000000 - (7000000 * (ICPfee * 1000) / 10000000))) / 10000000;
              } else {
                0;
              };
              //let orderRatio:Ratio = #Value(((toTradeBuy-amountCoveredBuy)*tenToPower60) /((remainingSell)/10000));

              if (compareRatio(poolRatio, orderRatio) == #greater and orderRatio != #Zero) {
                let (ammAmount, _) = getAMMLiquidity(p, orderRatio, tokensell);

                label a if (ammAmount > 10000 and (if (toTradeBuy >= amountCoveredBuy) { toTradeBuy - amountCoveredBuy } else { 0 }) > Transferfeesbuyindex and remainingSell > Transferfeessellindex * 10000) {
                  let amountToSwap = Nat.min(
                    ammAmount,
                    remainingSell / 10000,
                  );

                  if verboseLogging {
                    logFilter("Calculated orderRatio: " # debug_show (orderRatio));
                    logFilter("Pool ratio: " # debug_show (poolRatio));
                  };

                  let (amountIn, amountOut, _, _, protocolFeeAmount, poolFeeAmount, updatedPool) = swapWithAMM(p, tokenInIsToken0, amountToSwap, orderRatio, ICPfee);

                  amountCoveredBuy += amountOut;
                  amountCoveredSell += (amountIn +protocolFeeAmount +poolFeeAmount) * 10000;
                  cumAmountSell += amountIn +poolFeeAmount;
                  cumAmountOTCFeesSell += (protocolFeeAmount +poolFeeAmount) * 10000;
                  totalPoolFeeAmount += poolFeeAmount;
                  totalProtocolFeeAmount += protocolFeeAmount;

                  // Update pool and pool ratio (V3-aware refresh)
                  Map.set(AMMpools, hashtt, poolKey, updatedPool);
                  poolRatio := computePoolRatioFor(updatedPool, tokensell);
                  pool := ?updatedPool;
                };
              };
            };
          };
        };
        case null {};
      };
      // V3 fee distribution handled via feeGrowthGlobal in swapWithAMMV3 — no per-swap distribution needed

      // Update amounts — direct mutation, no array copies
      bufAmountBuy2.put(
        buyindex,
        if (toTradeBuy > amountCoveredBuy) {
          toTradeBuy - amountCoveredBuy;
        } else {
          0;
        },
      );

      bufAmountSell2.put(
        sellindex,
        if (toTradeSell > amountCoveredSell) {
          toTradeSell - amountCoveredSell;
        } else {
          0;
        },
      );
      bufAmountSell3.put(sellindex, bufAmountSell3.get(sellindex) + cumAmountSell);
      bufAmountFeesSell.put(sellindex, bufAmountFeesSell.get(sellindex) + cumAmountOTCFeesSell);
      bufAmountFeesBuy.put(buyindex, bufAmountFeesBuy.get(buyindex) + cumAmountOTCFeesBuy);
      bufTimesTFees.put(sellindex, TimesTFeessellindex);
      bufTimesTFees.put(buyindex, TimesTFeesbuyindex);
      let repVec = Vector.fromArray<(Text, Nat)>(bufRepMaker.get(buyindex));
      Vector.addFromIter(repVec, Vector.vals(representVec));
      bufRepMaker.put(buyindex, Vector.toArray(repVec));
      bufAmountBuy.put(buyindex, bufAmountBuy.get(buyindex) + amountCoveredBuy);
      if verboseLogging {
        logFilter("Totalpoolfeeamoutn for token " #tokensell # ": " #debug_show (totalPoolFeeAmount));
        logFilter("totalProtocolFeeAmount for token " #tokensell # ": " #debug_show (totalProtocolFeeAmount));
        logFilter("cumAmountOTCFeesSell for token " #tokensell # ": " #debug_show (cumAmountOTCFeesSell));
      };
    };

    let amountBuffer = Vector.new<TradeAmount>();
    if (trades.size() > 0) {
      for (i in Iter.range(0, trades.size() - 1)) {
        Vector.add(
          amountBuffer,
          {
            identifier = trades[i].identifier;
            amountBought = bufAmountBuy.get(i);
            amountSold = bufAmountSell3.get(i);
            transferFee = trades[i].transferFee;
            feesSell = bufAmountFeesSell.get(i) / 10000;
            feesBuy = bufAmountFeesBuy.get(i) / 10000;
            timesTFees = bufTimesTFees.get(i);
            representationPositionMaker = bufRepMaker.get(i);
          },
        );
      };
    };

    logFilter("Returning from getAllTradesDAOFilter");
    if verboseLogging { logFilter(debug_show (Vector.toArray(amountBuffer))) };

    return {
      trades = Vector.toArray(TradeEntryVector);
      amounts = Vector.toArray(amountBuffer);
      logging = Text.join("\n", Vector.toArray(logFilterEntries).vals());
    };
  };

  // Function that gets the amounts to buy and sell from the DAO and links them to positions within the exchange. Dit alot of arithmetric tricks (**60) so the amounts dont
  // get truncated during rounding. Ver similar to orderpairing, howver it also goes through private trades that allowed the DAO to go through them

  //Get trade data using an accesscode
  public query ({ caller }) func getPrivateTrade(pass : Text) : async ?TradePosition {
    if (isAllowedQuery(caller) != 1) {
      return null;
    };
    var currentTrades2 : TradePrivate = Faketrade;
    if (Text.startsWith(pass, #text "Public")) {
      let currentTrades = Map.get(tradeStorePublic, thash, pass);
      switch (currentTrades) {
        case null {};
        case (?(foundTrades)) {
          currentTrades2 := foundTrades;
        };
      };
    } else {
      let currentTrades = Map.get(tradeStorePrivate, thash, pass);
      switch (currentTrades) {
        case null {};
        case (?(foundTrades)) {
          currentTrades2 := foundTrades;
        };
      };
    };

    let currentTrades3 = {
      amount_sell = currentTrades2.amount_sell;
      amount_init = currentTrades2.amount_init;
      token_sell_identifier = currentTrades2.token_sell_identifier;
      token_init_identifier = currentTrades2.token_init_identifier;
      trade_number = currentTrades2.trade_number;
      Fee = currentTrades2.Fee;
      trade_done = currentTrades2.trade_done;
      strictlyOTC = currentTrades2.strictlyOTC;
      allOrNothing = currentTrades2.allOrNothing;
      OCname = currentTrades2.OCname;
      time = currentTrades2.time;
      filledInit = currentTrades2.filledInit;
      filledSell = currentTrades2.filledSell;
      initPrincipal = currentTrades2.initPrincipal;
    };
    return ?currentTrades3;

  };
  // RVVR-TACOX-28 Fix: Implement RunIds and Refactor Long Running Processes
  // This implementation addresses the complexity management issue by:
  // 1. Introducing RunIds for long-running processes (FinishSellBatchDAO and addAcceptedToken).
  // 2. Implementing detailed logging throughout these processes.
  // 3. Storing logs in separate maps for each function type.
  // 4. Providing a query function to retrieve recent logs, enabling easier debugging and monitoring.
  // These changes allow for better traceability, easier diagnosis of issues, and improved
  // understanding of the system's behavior during complex operations.
  public query ({ caller }) func getLogging(functionType : { #FinishSellBatchDAO; #addAcceptedToken }, getLastXEntries : Nat) : async [(Nat, Text)] {
    if (isAllowedQuery(caller) != 1) {
      return [];
    };

    let entries = switch (functionType) {
      case (#FinishSellBatchDAO) {
        Map.toArrayDesc(loggingMapFinishSellBatchDAO);
      };
      case (#addAcceptedToken) {
        Map.toArrayDesc(loggingMapaddAcceptedToken);
      };
    };

    let lastXEntries = Nat.min(getLastXEntries, 50); // Limit to 50 entries maximum
    Array.subArray(entries, 0, lastXEntries);
  };

  //get all the open trades of caller
  public query ({ caller }) func getUserTrades() : async [TradePrivate2] {
    if (isAllowedQuery(caller) != 1) {
      return [];
    };

    let principal = Principal.toText(caller);
    let userTrades = Vector.new<TradePrivate2>();



    switch (Map.get(userCurrentTradeStore, thash, principal)) {
      case (null) {

        // User has no trades
        return [];
      };
      case (?accessCodes) {

        for (accessCode in (TrieSet.toArray(accessCodes)).vals()) {
          let trade = if (Text.startsWith(accessCode, #text "Public")) {
            switch (Map.get(tradeStorePublic, thash, accessCode)) {
              case (null) { null };
              case (?t) { ?{ t with accesscode = accessCode } };
            };
          } else {
            switch (Map.get(tradeStorePrivate, thash, accessCode)) {
              case (null) { null };
              case (?t) { ?{ t with accesscode = accessCode } };
            };

          };

          switch (trade) {
            case (null) {
              // Trade not found, which shouldn't happen

            };
            case (?t) {
              Vector.add(userTrades, t);
            };
          };
        };
      };
    };

    return Vector.toArray(userTrades);
  };

  //Previous trades in the current Pool. Can be replaced by using getUserTrades
  public query ({ caller }) func getUserPreviousTrades(token1 : Text, token2 : Text) : async [{
    amount_init : Nat;
    amount_sell : Nat;
    init_principal : Text;
    sell_principal : Text;
    accesscode : Text;
    token_init_identifier : Text;
    timestamp : Int;
    strictlyOTC : Bool;
    allOrNothing : Bool;
  }] {
    if (isAllowedQuery(caller) != 1) {
      return [];
    };



    let principal = Principal.toText(caller);
    let pool = getPool(token1, token2);

    switch (Map.get(pool_history, hashtt, pool)) {
      case (null) {
        // No history for this pool
        return [];
      };
      case (?historyTree) {
        let userTrades = Vector.new<{ amount_init : Nat; amount_sell : Nat; init_principal : Text; sell_principal : Text; accesscode : Text; token_init_identifier : Text; timestamp : Int; strictlyOTC : Bool; allOrNothing : Bool }>();

        for ((timestamp, trades) in RBTree.entries(historyTree)) {
          for (trade in trades.vals()) {
            if (trade.init_principal == principal or trade.sell_principal == principal) {
              Vector.add(
                userTrades,
                {
                  amount_init = trade.amount_init;
                  amount_sell = trade.amount_sell;
                  init_principal = trade.init_principal;
                  sell_principal = trade.sell_principal;
                  accesscode = trade.accesscode;
                  token_init_identifier = trade.token_init_identifier;
                  timestamp = timestamp;
                  strictlyOTC = trade.strictlyOTC;
                  allOrNothing = trade.allOrNothing;
                },
              );
            };
          };
        };


        return Vector.toArray(userTrades);
      };
    };
  };

  public query ({ caller }) func getUserTradeHistory(limit : Nat) : async [{
    amount_init : Nat;
    amount_sell : Nat;
    token_init_identifier : Text;
    token_sell_identifier : Text;
    timestamp : Int;
    accesscode : Text;
    counterparty : Text;
  }] {
    if (isAllowedQuery(caller) != 1) {
      return [];
    };
    let principal = Principal.toText(caller);
    let maxLimit = Nat.min(limit, 200);
    let result = Vector.new<{
      amount_init : Nat;
      amount_sell : Nat;
      token_init_identifier : Text;
      token_sell_identifier : Text;
      timestamp : Int;
      accesscode : Text;
      counterparty : Text;
    }>();

    label poolLoop for ((_, historyTree) in Map.entries(pool_history)) {
      for ((timestamp, trades) in RBTree.entriesRev(historyTree)) {
        for (trade in trades.vals()) {
          if (trade.init_principal == principal or trade.sell_principal == principal) {
            let counterparty = if (trade.init_principal == principal) { trade.sell_principal } else { trade.init_principal };
            Vector.add(
              result,
              {
                amount_init = trade.amount_init;
                amount_sell = trade.amount_sell;
                token_init_identifier = trade.token_init_identifier;
                token_sell_identifier = "";
                timestamp = timestamp;
                accesscode = trade.accesscode;
                counterparty = counterparty;
              },
            );
            if (Vector.size(result) >= maxLimit) {
              break poolLoop;
            };
          };
        };
      };
    };

    Vector.toArray(result);
  };

  // Unified per-user swap history — one entry per completed swap (including multi-hop as single entry)
  public query ({ caller }) func getUserSwapHistory(limit : Nat) : async [SwapRecord] {
    if (isAllowedQuery(caller) != 1) { return [] };
    let maxLimit = Nat.min(limit, 200);
    switch (Map.get(userSwapHistory, phash, caller)) {
      case null { [] };
      case (?tree) {
        let results = RBTree.scanLimit(tree, Int.compare, 0, 9_999_999_999_999_999_999_999, #bwd, maxLimit);
        Array.map<(Int, SwapRecord), SwapRecord>(results.results, func((_, r)) { r });
      };
    };
  };

  // Get concentrated liquidity ranges for a pool (for liquidity distribution chart)
  public query ({ caller }) func getPoolRanges(token0 : Text, token1 : Text) : async [{
    ratioLower : Nat;
    ratioUpper : Nat;
    liquidity : Nat;
    token0Locked : Nat;
    token1Locked : Nat;
  }] {
    if (isAllowedQuery(caller) != 1) { return [] };
    let poolKey = getPool(token0, token1);
    switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case null { [] };
      case (?v3) {
        let result = Vector.new<{ ratioLower : Nat; ratioUpper : Nat; liquidity : Nat; token0Locked : Nat; token1Locked : Nat }>();
        // Build ranges from user positions (exact bounds) rather than tick tree
        let seen = Map.new<Text, Bool>();
        for ((_, positions) in Map.entries(concentratedPositions)) {
          for (pos in positions.vals()) {
            if (pos.token0 == poolKey.0 and pos.token1 == poolKey.1 and pos.liquidity > 0) {
              let key = Nat.toText(pos.ratioLower) # ":" # Nat.toText(pos.ratioUpper);
              if (not Map.has(seen, thash, key)) {
                Map.set(seen, thash, key, true);
                let sqrtLower = ratioToSqrtRatio(pos.ratioLower);
                let sqrtUpper = ratioToSqrtRatio(pos.ratioUpper);
                let (amt0, amt1) = amountsFromLiquidity(pos.liquidity, sqrtLower, sqrtUpper, v3.currentSqrtRatio);
                Vector.add(result, {
                  ratioLower = pos.ratioLower;
                  ratioUpper = pos.ratioUpper;
                  liquidity = pos.liquidity;
                  token0Locked = amt0;
                  token1Locked = amt1;
                });
              };
            };
          };
        };
        // Sort by liquidity descending, limit to top 10
        let sorted = Array.sort<{ ratioLower : Nat; ratioUpper : Nat; liquidity : Nat; token0Locked : Nat; token1Locked : Nat }>(
          Vector.toArray(result), func(a, b) { Nat.compare(b.liquidity, a.liquidity) }
        );
        if (sorted.size() > 10) { Array.subArray(sorted, 0, 10) } else { sorted };
      };
    };
  };

  // TEMPORARY: Debug function to dump ALL raw V3 tick entries
  public query func debugV3Ticks(token0 : Text, token1 : Text) : async {
    currentSqrtRatio : Nat;
    activeLiquidity : Nat;
    reserveRatio : Nat;
    reserveSqrtRatio : Nat;
    ticks : [{ tick : Nat; liquidityNet : Int; liquidityGross : Nat }];
  } {
    let poolKey = getPool(token0, token1);
    switch (Map.get(poolV3Data, hashtt, poolKey), Map.get(AMMpools, hashtt, poolKey)) {
      case (?v3, ?pool) {
        let ratio = if (pool.reserve0 > 0) { (pool.reserve1 * tenToPower60) / pool.reserve0 } else { 0 };
        let sqrtR = ratioToSqrtRatio(ratio);
        let ticksResult = Vector.new<{ tick : Nat; liquidityNet : Int; liquidityGross : Nat }>();
        for ((tick, data) in RBTree.entries(v3.ranges)) {
          Vector.add(ticksResult, { tick; liquidityNet = data.liquidityNet; liquidityGross = data.liquidityGross });
        };
        { currentSqrtRatio = v3.currentSqrtRatio; activeLiquidity = v3.activeLiquidity; reserveRatio = ratio; reserveSqrtRatio = sqrtR; ticks = Vector.toArray(ticksResult) };
      };
      case _ { { currentSqrtRatio = 0; activeLiquidity = 0; reserveRatio = 0; reserveSqrtRatio = 0; ticks = [] } };
    };
  };

  // Get user's concentrated liquidity positions
  type ConcentratedPositionDetailed = {
    positionId : Nat;
    token0 : Text; token1 : Text;
    liquidity : Nat;
    ratioLower : Nat; ratioUpper : Nat;
    lastFeeGrowth0 : Nat; lastFeeGrowth1 : Nat;
    lastUpdateTime : Int;
    fee0 : Nat; fee1 : Nat;
    token0Amount : Nat; token1Amount : Nat;
  };

  public query ({ caller }) func getUserConcentratedPositions() : async [ConcentratedPositionDetailed] {
    if (isAllowedQuery(caller) != 1) { return [] };
    switch (Map.get(concentratedPositions, phash, caller)) {
      case null { [] };
      case (?positions) {
        Array.map<ConcentratedPosition, ConcentratedPositionDetailed>(
          positions,
          func(pos) {
            let poolKey = (pos.token0, pos.token1);
            let v3 = Map.get(poolV3Data, hashtt, poolKey);

            let (fee0, fee1) = switch (v3) {
              case (?v) {
                // Use feeGrowthInside — only in-range growth accrues. This is what the UI
                // reads for "unclaimed fees"; out-of-range positions correctly show 0.
                let (insideNow0, insideNow1) = positionFeeGrowthInside(pos, v);
                let tf0 = pos.liquidity * safeSub(insideNow0, pos.lastFeeGrowth0) / tenToPower60;
                let tf1 = pos.liquidity * safeSub(insideNow1, pos.lastFeeGrowth1) / tenToPower60;
                let mc0 = safeSub(v.totalFeesCollected0, v.totalFeesClaimed0);
                let mc1 = safeSub(v.totalFeesCollected1, v.totalFeesClaimed1);
                (Nat.min(tf0, mc0), Nat.min(tf1, mc1));
              };
              case null { (0, 0) };
            };

            let sqrtLower = ratioToSqrtRatio(pos.ratioLower);
            let sqrtUpper = ratioToSqrtRatio(pos.ratioUpper);
            let currentSqrt = switch (v3) { case (?v) { v.currentSqrtRatio }; case null { tenToPower60 } };
            let (amount0, amount1) = amountsFromLiquidity(pos.liquidity, sqrtLower, sqrtUpper, currentSqrt);

            {
              positionId = pos.positionId;
              token0 = pos.token0; token1 = pos.token1;
              liquidity = pos.liquidity;
              ratioLower = pos.ratioLower; ratioUpper = pos.ratioUpper;
              lastFeeGrowth0 = pos.lastFeeGrowth0; lastFeeGrowth1 = pos.lastFeeGrowth1;
              lastUpdateTime = pos.lastUpdateTime;
              fee0; fee1; token0Amount = amount0; token1Amount = amount1;
            };
          },
        );
      };
    };
  };

  // Per-pool statistics: reserves, volumes, fees, liquidity, history
  public query ({ caller }) func getPoolStats(token0 : Text, token1 : Text) : async ?{
    token0 : Text; token1 : Text;
    symbol0 : Text; symbol1 : Text;
    decimals0 : Nat; decimals1 : Nat;
    reserve0 : Nat; reserve1 : Nat;
    price0 : Float; price1 : Float;
    priceChange24hPct : Float;
    volume24h : Nat; volume7d : Nat;
    feeRateBps : Nat; lpFeeSharePct : Nat;
    feesLifetimeToken0 : Nat; feesLifetimeToken1 : Nat;
    totalLiquidity : Nat; activeLiquidity : Nat;
    history : [PoolDailySnapshot];
  } {
    if (isAllowedQuery(caller) != 1) return null;
    let poolKey = getPool(token0, token1);
    let pool = switch (Map.get(AMMpools, hashtt, poolKey)) { case null { return null }; case (?p) { p } };
    let poolIdx = switch (Map.get(poolIndexMap, hashtt, poolKey)) { case (?i) { i }; case null { return null } };

    let sym0 = switch (Map.get(tokenInfo, thash, pool.token0)) { case (?i) { i.Symbol }; case null { "" } };
    let sym1 = switch (Map.get(tokenInfo, thash, pool.token1)) { case (?i) { i.Symbol }; case null { "" } };
    let dec0 = switch (Map.get(tokenInfo, thash, pool.token0)) { case (?i) { i.Decimals }; case null { 8 } };
    let dec1 = switch (Map.get(tokenInfo, thash, pool.token1)) { case (?i) { i.Decimals }; case null { 8 } };

    let (price0, price1) = getPoolPriceV3(poolKey);

    let lastPrice = if (poolIdx < Vector.size(last_traded_price)) { Vector.get(last_traded_price, poolIdx) } else { 0.0 };
    let prevPrice = if (poolIdx < Vector.size(price_day_before)) { Vector.get(price_day_before, poolIdx) } else { 0.0 };
    let priceChange = if (prevPrice > 0.0) { ((lastPrice - prevPrice) / prevPrice) * 100.0 } else { 0.0 };

    let vol24h = if (poolIdx < volume_24hArray.size()) { volume_24hArray[poolIdx] } else { 0 };

    // 7D volume from daily K-lines
    let kKey : KlineKey = (pool.token0, pool.token1, #day);
    var vol7d : Nat = 0;
    switch (Map.get(klineDataStorage, hashkl, kKey)) {
      case (?tree) {
        let sevenDaysAgo = Time.now() - 7 * 24 * 3600 * 1_000_000_000;
        let scan = RBTree.scanLimit(tree, compareTime, sevenDaysAgo, Time.now(), #bwd, 7);
        for ((_, kline) in scan.results.vals()) { vol7d += kline.volume };
      };
      case null {};
    };

    // Lifetime fees
    let fees0 = pool.totalFee0 / tenToPower60;
    let fees1 = pool.totalFee1 / tenToPower60;
    let v3Fees = switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case (?v3) { (safeSub(v3.totalFeesCollected0, v3.totalFeesClaimed0), safeSub(v3.totalFeesCollected1, v3.totalFeesClaimed1)) };
      case null { (0, 0) };
    };

    let activeLiq = switch (Map.get(poolV3Data, hashtt, poolKey)) {
      case (?v3) { v3.activeLiquidity }; case null { pool.totalLiquidity };
    };

    // History from daily snapshots (up to 90 days)
    let histVec = Vector.new<PoolDailySnapshot>();
    switch (Map.get(poolDailySnapshots, hashtt, poolKey)) {
      case (?tree) {
        let ninetyDaysAgo = Time.now() - 90 * 24 * 3600 * 1_000_000_000;
        let scan = RBTree.scanLimit(tree, Int.compare, ninetyDaysAgo, Time.now(), #bwd, 90);
        for ((_, snap) in scan.results.vals()) { Vector.add(histVec, snap) };
      };
      case null {};
    };

    ?{
      token0 = pool.token0; token1 = pool.token1;
      symbol0 = sym0; symbol1 = sym1;
      decimals0 = dec0; decimals1 = dec1;
      reserve0 = pool.reserve0; reserve1 = pool.reserve1;
      price0; price1;
      priceChange24hPct = priceChange;
      volume24h = vol24h; volume7d = vol7d;
      feeRateBps = ICPfee; lpFeeSharePct = LP_FEE_SHARE_PERCENT;
      feesLifetimeToken0 = fees0 + v3Fees.0;
      feesLifetimeToken1 = fees1 + v3Fees.1;
      totalLiquidity = pool.totalLiquidity;
      activeLiquidity = activeLiq;
      history = Vector.toArray(histVec);
    };
  };

  // Compact pool stats for all pools (pool list page)
  public query ({ caller }) func getAllPoolStats() : async [{
    token0 : Text; token1 : Text;
    symbol0 : Text; symbol1 : Text;
    decimals0 : Nat; decimals1 : Nat;
    reserve0 : Nat; reserve1 : Nat;
    price0 : Float; price1 : Float;
    priceChange24hPct : Float;
    volume24h : Nat;
    feeRateBps : Nat;
    totalLiquidity : Nat;
    activeLiquidity : Nat;
  }] {
    if (isAllowedQuery(caller) != 1) return [];
    let result = Vector.new<{
      token0 : Text; token1 : Text; symbol0 : Text; symbol1 : Text;
      decimals0 : Nat; decimals1 : Nat; reserve0 : Nat; reserve1 : Nat;
      price0 : Float; price1 : Float; priceChange24hPct : Float;
      volume24h : Nat; feeRateBps : Nat;
      totalLiquidity : Nat; activeLiquidity : Nat;
    }>();

    for (i in Iter.range(0, Vector.size(pool_canister) - 1)) {
      let poolKey = Vector.get(pool_canister, i);
      switch (Map.get(AMMpools, hashtt, poolKey)) {
        case (?pool) {
          if (pool.reserve0 > 0 or pool.reserve1 > 0) {
            let sym0 = switch (Map.get(tokenInfo, thash, pool.token0)) { case (?inf) { inf.Symbol }; case null { "" } };
            let sym1 = switch (Map.get(tokenInfo, thash, pool.token1)) { case (?inf) { inf.Symbol }; case null { "" } };
            let dec0 = switch (Map.get(tokenInfo, thash, pool.token0)) { case (?inf) { inf.Decimals }; case null { 8 } };
            let dec1 = switch (Map.get(tokenInfo, thash, pool.token1)) { case (?inf) { inf.Decimals }; case null { 8 } };
            let (p0, p1) = getPoolPriceV3(poolKey);
            let lastP = if (i < Vector.size(last_traded_price)) { Vector.get(last_traded_price, i) } else { 0.0 };
            let prevP = if (i < Vector.size(price_day_before)) { Vector.get(price_day_before, i) } else { 0.0 };
            let pChange = if (prevP > 0.0) { ((lastP - prevP) / prevP) * 100.0 } else { 0.0 };
            let vol = if (i < volume_24hArray.size()) { volume_24hArray[i] } else { 0 };
            let actLiq = switch (Map.get(poolV3Data, hashtt, poolKey)) {
              case (?v3) { v3.activeLiquidity }; case null { pool.totalLiquidity };
            };

            Vector.add(result, {
              token0 = pool.token0; token1 = pool.token1;
              symbol0 = sym0; symbol1 = sym1;
              decimals0 = dec0; decimals1 = dec1;
              reserve0 = pool.reserve0; reserve1 = pool.reserve1;
              price0 = p0; price1 = p1;
              priceChange24hPct = pChange;
              volume24h = vol;
              feeRateBps = ICPfee;
              totalLiquidity = pool.totalLiquidity;
              activeLiquidity = actLiq;
            });
          };
        };
        case null {};
      };
    };

    Vector.toArray(result);
  };

  // Function that returns USD prices for tokens in the exchange.
  // Each token's USD price is calculated based on its trading activity
  // with either ICP or ckUSDC, whichever provides more reliable data.
  //
  // Requirements for valid price data:
  // 1. Uses price from most recent 5-minute period with >= 3 ICP or >= 50 ckUSDC volume
  // 2. Total volume in past 2 hours >= 30 ICP or >= 400 ckUSDC
  // 3. Valid KLine data must exist
  //
  // Parameters:
  // - ICPpriceUSD: Current USD price of ICP
  // - ckUSDCpriceUSD: Current USD price of ckUSDC (should be close to 1.0)
  //
  // Returns:
  // - error: true if data requirements not met for any token
  // - data: Array of token addresses, their USD prices, and timestamp of last valid update
  public query ({ caller }) func getTokenUSDPrices(ICPpriceUSD : Float, ckUSDCpriceUSD : Float) : async ?{
    error : Bool;
    data : [(Text, { address : Text; priceUSD : Float; timeLastValidUpdate : Int })];
  } {
    if (isAllowedQuery(caller) != 1) {
      return null;
    };

    let ICP_ADDRESS = "ryjl3-tyaaa-aaaaa-aaaba-cai";
    let CKUSDC_ADDRESS = "xevnm-gaaaa-aaaar-qafnq-cai";
    let nowVar = Time.now();
    let twoHoursAgo = nowVar - 2 * 3600 * 1_000_000_000;

    let result = Vector.new<(Text, { address : Text; priceUSD : Float; timeLastValidUpdate : Int })>();
    var hasError = false;

    // Process each token except base tokens
    label a for (tokenAddress in acceptedTokens.vals()) {
      if (Array.find<Text>(baseTokens, func(t) { t == tokenAddress }) != null) {
        continue a;
      };

      // Initialize variables for ICP and ckUSDC pools
      var validICPPrice = false;
      var validCKUSDCPrice = false;
      var selectedPrice : Float = 0;
      var selectedTimestamp : Int = 0;

      // Check ICP pool
      let ICPpoolKey : KlineKey = (tokenAddress, ICP_ADDRESS, #fivemin);
      var ICPvolumeLast2Hours : Nat = 0;
      var ICPlastHighVolumePrice : ?{ price : Float; timestamp : Int } = null;
      let nowie = Time.now();

      switch (Map.get(klineDataStorage, hashkl, ICPpoolKey)) {
        case (?tree) {
          let fiveMinKlines = RBTree.scanLimit(
            tree,
            compareTime,
            twoHoursAgo,
            nowVar,
            #bwd,
            24 // 2 hours worth of 5-min candles
          ).results;

          for ((_, kline) in fiveMinKlines.vals()) {
            // Convert volume to actual ICP amount (8 decimals)
            let volumeICP = kline.volume;
            ICPvolumeLast2Hours += volumeICP;

            // Check if this kline has high enough volume
            if (volumeICP >= 300000000) {
              // 3 ICP
              // Update only if we haven't found a more recent high volume kline
              if (ICPlastHighVolumePrice == null) {
                ICPlastHighVolumePrice := ?{
                  price = kline.close;
                  timestamp = if (kline.timestamp + 300_000_000_000 < nowie) {
                    kline.timestamp + 300_000_000_000;
                  } else { nowie }; // Add 5 minutes to get end of period
                };
              };
            };
          };

          if (ICPlastHighVolumePrice != null and ICPvolumeLast2Hours >= 3000000000) {
            // 30 ICP
            let temp = switch (ICPlastHighVolumePrice) {
              case null { { price = 0.0; timestamp = 0 } };
              case (?a) { a };
            };
            validICPPrice := true;
            selectedPrice := temp.price * ICPpriceUSD;
            selectedTimestamp := temp.timestamp;
          };
        };
        case (null) {};
      };

      // Check ckUSDC pool if ICP pool didn't provide valid data
      if (not validICPPrice) {
        let ckUSDCpoolKey : KlineKey = (tokenAddress, CKUSDC_ADDRESS, #fivemin);
        var ckUSDCvolumeLast2Hours : Nat = 0;
        var ckUSDClastHighVolumePrice : ?{ price : Float; timestamp : Int } = null;

        switch (Map.get(klineDataStorage, hashkl, ckUSDCpoolKey)) {
          case (?tree) {
            let fiveMinKlines = RBTree.scanLimit(
              tree,
              compareTime,
              twoHoursAgo,
              nowVar,
              #bwd,
              24 // 2 hours worth of 5-min candles
            ).results;

            for ((_, kline) in fiveMinKlines.vals()) {
              // Convert volume to actual USDC amount (6 decimals)
              let volumeUSDC = kline.volume;
              ckUSDCvolumeLast2Hours += volumeUSDC;

              // Check if this kline has high enough volume
              if (volumeUSDC >= 50000000) {
                // 50 USDC
                // Update only if we haven't found a more recent high volume kline
                if (ckUSDClastHighVolumePrice == null) {
                  ckUSDClastHighVolumePrice := ?{
                    price = kline.close;
                    timestamp = if (kline.timestamp + 300_000_000_000 < nowie) {
                      kline.timestamp + 300_000_000_000;
                    } else { nowie }; // Add 5 minutes to get end of period
                  };
                };
              };
            };

            if (ckUSDClastHighVolumePrice != null and ckUSDCvolumeLast2Hours >= 400000000) {
              // 400 USDC
              let temp = switch (ckUSDClastHighVolumePrice) {
                case null { { price = 0.0; timestamp = 0 } };
                case (?a) { a };
              };
              validCKUSDCPrice := true;
              selectedPrice := temp.price * ckUSDCpriceUSD;
              selectedTimestamp := temp.timestamp;
            };
          };
          case (null) {};
        };
      };

      // Add token price to results if valid data was found
      if (validICPPrice or validCKUSDCPrice) {
        Vector.add(
          result,
          (
            tokenAddress,
            {
              address = tokenAddress;
              priceUSD = selectedPrice;
              timeLastValidUpdate = selectedTimestamp;
            },
          ),
        );
      } else {
        hasError := true;
      };
    };

    ?{
      error = hasError;
      data = Vector.toArray(result);
    };
  };

  var FixStuckTXRunning = false;

  // Serializes adminRecoverWronglysent so two parallel admin calls cannot both
  // pass the BlocksAdminRecovered guard and double-dispatch a refund.
  var adminRecoveryRunning : Bool = false;

  // Retrieve funds that are stuck. If partials is used as text, it will go through the tempTransferQueue vector. If an accesscode is given, it will see what went wrong and send stuck assets back to the one its for within a position.
  public shared ({ caller }) func FixStuckTX(accesscode : Text) : async ExTypes.ActionResult {
    if (accesscode == "partial") {
      if (not ownercheck(caller)) {
        return #Err(#NotAuthorized);
      };
      if FixStuckTXRunning {
        return #Err(#NotAuthorized);
      };
      FixStuckTXRunning := true;
    } else {
      if (isAllowed(caller) != 1) {
        return #Err(#NotAuthorized);
      };
      if (Text.size(accesscode) > 150) {
        return #Err(#Banned);
      };
    };
    if (accesscode == "partial") {
      // Transfering the transactions that have to be made by the treasury,
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue), isInAllowedCanisters(caller)) } catch (err) { return #Err(#SystemError(Error.message(err))); FixStuckTXRunning := false; false })) {
        Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
      };
      FixStuckTXRunning := false;
      return #Ok("Stuck trades fixed");
    };
    let tempTransferQueueLocal = syncFixStuckTX(accesscode, Principal.toText(caller));

    // Transfering the transactions that have to be made to the treasury,
    if ((try { await treasury.receiveTransferTasks(tempTransferQueueLocal, isInAllowedCanisters(caller)) } catch (err) { false })) {

    } else {
      Vector.addFromIter(tempTransferQueue, tempTransferQueueLocal.vals());
    };
    #Ok("Done");
  };

  func syncFixStuckTX(accesscode : Text, caller : Text) : [(TransferRecipient, Nat, Text, Text)] {
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    var currentTrades2 : TradePrivate = Faketrade;
    let pub = Text.startsWith(accesscode, #text "Public");
    if pub {
      let currentTrades = Map.get(tradeStorePublic, thash, accesscode);
      switch (currentTrades) {
        case null {};
        case (?(foundTrades)) {
          currentTrades2 := foundTrades;
        };
      };
    } else {
      let currentTrades = Map.get(tradeStorePrivate, thash, accesscode);
      switch (currentTrades) {
        case null {};
        case (?(foundTrades)) {
          currentTrades2 := foundTrades;
        };
      };
    };

    assert (currentTrades2.init_paid2 == 0 or currentTrades2.seller_paid2 == 0);
    assert (currentTrades2.trade_done == 1);
    var init_paid2 = currentTrades2.init_paid2;
    var seller_paid2 = currentTrades2.seller_paid2;
    var endmessage = "";
    var therewaserror = 0;
    if (currentTrades2.init_paid2 == 0) {
      if (currentTrades2.init_paid == 1 and currentTrades2.seller_paid == 1) {
        Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(currentTrades2.initPrincipal)), currentTrades2.amount_sell, currentTrades2.token_sell_identifier, genTxId()));
        init_paid2 := 1;
      };
      let RevokeFee = currentTrades2.RevokeFee;
      if (currentTrades2.init_paid == 1 and currentTrades2.seller_paid == 0) {
        Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(currentTrades2.initPrincipal)), currentTrades2.amount_init +(((currentTrades2.amount_init * (currentTrades2.Fee)) / (10000 * RevokeFee)) * (RevokeFee -1)), currentTrades2.token_init_identifier, genTxId()));
        init_paid2 := 1;

      };
    };
    if (currentTrades2.seller_paid2 == 0) {
      if (currentTrades2.seller_paid == 1 and currentTrades2.init_paid == 1) {
        Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(currentTrades2.SellerPrincipal)), currentTrades2.amount_init, currentTrades2.token_init_identifier, genTxId()));
        seller_paid2 := 1;

      };
      let RevokeFee = currentTrades2.RevokeFee;
      if (currentTrades2.seller_paid == 1 and currentTrades2.init_paid == 0) {

        Vector.add(tempTransferQueueLocal, (#principal(Principal.fromText(currentTrades2.SellerPrincipal)), currentTrades2.amount_sell +(((currentTrades2.amount_sell * (currentTrades2.Fee)) / (10000 * RevokeFee)) * (RevokeFee -1)), currentTrades2.token_sell_identifier, genTxId()));
        seller_paid2 := 1;
      };
    };
    if (seller_paid2 == 1 and init_paid2 == 1) {
      removeTrade(accesscode, currentTrades2.initPrincipal, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));

      return Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal);
    } else {
      currentTrades2 := {
        currentTrades2 with
        trade_done = 1;
        seller_paid2 = seller_paid2;
        init_paid2 = init_paid2;

      };
      addTrade(accesscode, currentTrades2.initPrincipal, currentTrades2, (currentTrades2.token_init_identifier, currentTrades2.token_sell_identifier));

      return Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal);
    };
  };

  // Manually add a timer if it does not start automatically anymore somehow.
  // The vector named timerIDs saves all the calls to timerA, which in turn makes sure all the token info gets updated all the time.
  // It gets saved in a vector so there is no chance that the number of timers increase (exponentially) and can be cancelled when timerA gets called.
  // retryFunc is made so these functions get retried in the case the process queuue is full. This function you see here can be seen as a last resort.
  public shared ({ caller }) func addTimer() : async () {
    if (not ownercheck(caller)) {
      return;
    };
    Vector.add(
      timerIDs,
      setTimer(
        #seconds(1),
        func() : async () {

          first_time_running_after_upgrade := 0;
          try {
            updateTokenInfo(true, true, await treasury.getTokenInfo());
          } catch (Err) {};
          if (first_time_running == 1) {
            first_time_running := 0;
          };
          try {
            timerA(await treasury.getTokenInfo());
          } catch (err) {


            retryFunc<system>(
              func() : async () {

                timerA(await treasury.getTokenInfo());
              },
              5,
              10,
              10,
            );
          };
        },
      ),
    );
  };

  public query ({ caller }) func get_cycles() : async Nat {
    if (not ownercheck(caller)) {
      return 0;
    };
    return Cycles.balance();
  };

  public query func getLogs(count : Nat) : async [Logger.LogEntry] {
    logger.getLastLogs(count);
  };

  //coming 4 functions will be deleted in production
  public func p2athird(p : Text) : async Text {
    //private later
    Utils.accountToText(Utils.principalToAccount(Principal.fromText(p)));
  };

  public query (msg) func p2a() : async Text {
    //delete later
    Utils.accountToText(Utils.principalToAccount(msg.caller));
  };
  public query func p2acannister() : async Text {
    //delete later
    Utils.accountToText(Utils.principalToAccount(treasury_principal));
  };
  public query func returncontractprincipal() : async Text {
    //delete later
    Principal.toText(treasury_principal);
  };

  //Note to afat: in the notes you mentioned that you would make acceptedtokens a set. I decided to keep it as is as the Array will have a manageable number of entries and in some functions the order of the entries is also important (f.i. minimmamounts).
  private func containsToken(token : Text) : Bool {
    switch (Array.find<Text>(acceptedTokens, func(t) { t == token })) {
      case null { false };
      case (?_) { true };
    };
  };

  private func returnMinimum(token : Text, amount : Nat, x10 : Bool) : Bool {
    let index2 : ?Nat = Array.indexOf<Text>(token, acceptedTokens, Text.equal);
    var index = 0;
    switch (index2) {
      case (?k) { index := k };
      case null {};
    };
    (if x10 { amount > minimumAmount[index] * 10 } else { amount > minimumAmount[index] });
  };

  private func returnType(token : Text) : { #ICP; #ICRC12; #ICRC3 } {
    let index2 : ?Nat = Array.indexOf<Text>(token, acceptedTokens, Text.equal);
    switch (index2) {
      case (?k) { tokenType[k] };
      case null { #ICRC12 }; // Fallback: most tokens are ICRC12; callers validate token acceptance before reaching here
    };
  };

  private func returnTfees(token : Text) : Nat {
    var Tfees = switch (Map.get(tokenInfo, thash, token)) {
      case null { 10000 };
      case (?(foundTrades)) {
        foundTrades.TransferFee;
      };
    };
    Tfees;
  };

  private func returnDecimals(token : Text) : Nat {
    switch (Map.get(tokenInfo, thash, token)) {
      case null { 8 };
      case (?(foundTrades)) { foundTrades.Decimals };
    };
  };

  private func removeTrade(accesscode : Text, initPrincipal : Text, pool : (Text, Text)) {
    var removedTrade : ?TradePrivate = null;

    if (Text.startsWith(accesscode, #text "Public")) {
      removedTrade := Map.remove(tradeStorePublic, thash, accesscode);
      let pair1 = pool;
      let pair2 = (pool.1, pool.0);

      let pairToRemove = if (Map.has(foreignPools, hashtt, pair1) or isKnownPool(pair1.0, pair1.1)) pair1 else pair2;
      let secondpairToRemove = if (pairToRemove == pair1) pair2 else pair1;

      switch (Map.get(foreignPools, hashtt, pairToRemove)) {
        case (null) {
          switch (Map.get(foreignPools, hashtt, secondpairToRemove)) {
            case (null) {

            };
            case (?count) {
              if (count <= 1) {
                // If count is 1 or less, remove the entry completely
                ignore Map.remove(foreignPools, hashtt, secondpairToRemove);
              } else {
                // Decrement the count
                Map.set(foreignPools, hashtt, secondpairToRemove, count - 1);
              };
            };
          };
        };
        case (?count) {
          if (count <= 1) {
            // If count is 1 or less, remove the entry completely
            ignore Map.remove(foreignPools, hashtt, pairToRemove);
          } else {
            // Decrement the count
            Map.set(foreignPools, hashtt, pairToRemove, count - 1);
          };
        };
      };
    } else {
      removedTrade := Map.remove(tradeStorePrivate, thash, accesscode);
      switch (Map.get(privateAccessCodes, hashtt, pool)) {
        case null {};
        case (?V) {
          let a = TrieSet.delete(V, accesscode, Text.hash(accesscode), Text.equal);
          if (TrieSet.size(a) == 0) {
            ignore Map.remove(privateAccessCodes, hashtt, pool);
          } else {
            Map.set(privateAccessCodes, hashtt, pool, a);
          };
        };
      };
      let pair1 = pool;
      let pair2 = (pool.1, pool.0);

      let pairToRemove = if (Map.has(foreignPrivatePools, hashtt, pair1)) pair1 else pair2;
      let secondpairToRemove = if (pairToRemove == pair1) pair2 else pair1;

      //change foreignPools count
      switch (Map.get(foreignPrivatePools, hashtt, pairToRemove)) {
        case (null) {
          switch (Map.get(foreignPrivatePools, hashtt, secondpairToRemove)) {
            case (null) {};
            case (?count) {
              if (count <= 1) {
                // If count is 1 or less, remove the entry completely
                ignore Map.remove(foreignPrivatePools, hashtt, secondpairToRemove);
              } else {
                // Decrement the count
                Map.set(foreignPrivatePools, hashtt, secondpairToRemove, count - 1);
              };
            };
          };
        };
        case (?count) {
          if (count <= 1) {
            // If count is 1 or less, remove the entry completely
            ignore Map.remove(foreignPrivatePools, hashtt, pairToRemove);
          } else {
            // Decrement the count
            Map.set(foreignPrivatePools, hashtt, pairToRemove, count - 1);
          };
        };
      };

      //edit Map that saves trades per user
    };
    switch (Map.get(userCurrentTradeStore, thash, initPrincipal)) {
      case (?V) {
        let a = TrieSet.delete(V, accesscode, Text.hash(accesscode), Text.equal);
        if (TrieSet.size(a) == 0) {
          ignore Map.remove(userCurrentTradeStore, thash, initPrincipal);
        } else { Map.set(userCurrentTradeStore, thash, initPrincipal, a) };
      };
      case null {};
    };

    // remove from time-based tree
    switch (removedTrade) {
      case (?trade) {
        switch (RBTree.get(timeBasedTrades, compareTime, trade.time)) {
          case (null) {};
          case (?existingCodes) {
            let updatedCodes = Array.filter(existingCodes, func(code : Text) : Bool { code != accesscode });
            if (Array.size(updatedCodes) == 0) {
              // If no codes left, remove the entire entry
              timeBasedTrades := RBTree.delete(timeBasedTrades, compareTime, trade.time);
            } else {
              // Update with remaining codes
              timeBasedTrades := RBTree.put(timeBasedTrades, compareTime, trade.time, updatedCodes);
            };
          };
        };
      };
      case null {};
    };

  };

  private func addTrade(accesscode : Text, initPrincipal : Text, trade : TradePrivate, pool : (Text, Text)) {
    if (Text.startsWith(accesscode, #text "Public")) {
      ignore Map.set(tradeStorePublic, thash, accesscode, trade);
      switch (Map.get(userCurrentTradeStore, thash, initPrincipal)) {
        case (?V) {
          Map.set(userCurrentTradeStore, thash, initPrincipal, TrieSet.put(V, accesscode, Text.hash(accesscode), Text.equal));
        };
        case null {
          Map.set(userCurrentTradeStore, thash, initPrincipal, TrieSet.put(TrieSet.empty<Text>(), accesscode, Text.hash(accesscode), Text.equal));
        };
      };
    } else {
      ignore Map.set(tradeStorePrivate, thash, accesscode, trade);
      switch (Map.get(privateAccessCodes, hashtt, pool)) {
        case null {
          Map.set(privateAccessCodes, hashtt, pool, TrieSet.put(TrieSet.empty<Text>(), accesscode, Text.hash(accesscode), Text.equal));
        };
        case (?V) {
          Map.set(privateAccessCodes, hashtt, pool, TrieSet.put(V, accesscode, Text.hash(accesscode), Text.equal));
        };

      };
      switch (Map.get(userCurrentTradeStore, thash, initPrincipal)) {
        case (?V) {
          Map.set(userCurrentTradeStore, thash, initPrincipal, TrieSet.put(V, accesscode, Text.hash(accesscode), Text.equal));
        };
        case null {
          Map.set(userCurrentTradeStore, thash, initPrincipal, TrieSet.put(TrieSet.empty<Text>(), accesscode, Text.hash(accesscode), Text.equal));
        };
      };
    };

    // add to time-based tree
    switch (RBTree.get(timeBasedTrades, compareTime, trade.time)) {
      case (null) {
        // no entry for this timestamp, create a new array
        timeBasedTrades := RBTree.put(timeBasedTrades, compareTime, trade.time, [accesscode]);
      };
      case (?existingCodes) {
        // append to existing array
        let updatedCodes = if (Array.indexOf(accesscode, existingCodes, Text.equal) == null) {
          let codesVec = Vector.fromArray<Text>(existingCodes);
          Vector.add(codesVec, accesscode);
          Vector.toArray(codesVec);
        } else { existingCodes };
        timeBasedTrades := RBTree.put(timeBasedTrades, compareTime, trade.time, updatedCodes);
      };
    };
  };

  //after upgrade make sure all KLines are filled
  system func postupgrade() {
    // recalculate from tick tree for all pools
    recalculateAllActiveLiquidity();
    rebuildPoolIndex();
    checkAndAggregateAllPools();
    // Reset all bans on every upgrade
    dayBan := TrieSet.empty();
    dayBanRegister := TrieSet.empty();
    allTimeBan := TrieSet.empty();
    warnings := TrieSet.empty();
  };

  // Periodically process tempTransferQueue to avoid tokens getting stuck
  // when no users interact with the exchange for extended periods.
  private func startTempTransferQueueTimer<system>() {
    ignore setTimer<system>(
      #nanoseconds(300_000_000_000), // 5 minutes
      func() : async () {
        if (Vector.size(tempTransferQueue) > 0 and not FixStuckTXRunning) {
          FixStuckTXRunning := true;
          if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue), false) } catch (_) { false })) {
            Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
          };
          FixStuckTXRunning := false;
        };
        startTempTransferQueueTimer<system>();
      },
    );
  };
  startTempTransferQueueTimer<system>();



  if (first_time_running_after_upgrade == 1) {
    let timersize = Vector.size(timerIDs);
    if (timersize > 0) {
      for (i in Vector.vals(timerIDs)) {
        cancelTimer(i);
      };
    };
    ignore recurringTimer<system>(
      #seconds(24 * 60 * 60), // Run once a day
      func() : async () {

        trimOldReferralFees<system>();
      },
    );

    ignore recurringTimer(
      #seconds(fuzz.nat.randomRange(80400, 88400)),
      func() : async () {

        await cleanupOldTrades();
      },
    );

    ignore recurringTimer<system>(
      #seconds(3600), // Run once an hour
      func() : async () {
        for (poolKey in AllExchangeInfo.pool_canister.vals()) {
          ignore update24hVolume(poolKey);
        };
      },
    );

    Vector.add(
      timerIDs,
      setTimer(
        #seconds(0),
        func() : async () {

          first_time_running_after_upgrade := 0;
          try {
            updateTokenInfo(true, true, await treasury.getTokenInfo());

          } catch (Err) { Debug.print(debug_show ("Error at tokeninfosync")) };
          if (first_time_running == 1) {
            first_time_running := 0;
          };
          Vector.add(
            timerIDs,
            setTimer(
              #seconds(1),
              func() : async () {
                try { timerA(await treasury.getTokenInfo()) } catch (err) {
                  Vector.add(
                    timerIDs,
                    setTimer(
                      #seconds(1),
                      func() : async () {
                        try {
                          timerA(await treasury.getTokenInfo());
                        } catch (err) {};
                        updateStaticInfo();
                        AllExchangeInfo := {
                          AllExchangeInfo with
                          last_traded_price = Vector.toArray(last_traded_price);
                          price_day_before = Vector.toArray(price_day_before);
                        };
                      },
                    ),
                  );
                  return ();
                };
                updateStaticInfo();
                AllExchangeInfo := {
                  AllExchangeInfo with
                  last_traded_price = Vector.toArray(last_traded_price);
                  price_day_before = Vector.toArray(price_day_before);
                };
              },
            ),
          );
        },
      ),
    );
  };

  // ═══════════════════════════════════════════════════════════════
  // ADMIN ROUTE ANALYSIS — discover and execute multi-hop circular routes
  // ═══════════════════════════════════════════════════════════════

  public query func adminAnalyzeRouteEfficiency(
    token : Text,
    sampleSize : Nat,
    depth : Nat,
  ) : async [{
    route : [SwapHop];
    outputAmount : Nat;
    efficiency : Int;
    efficiencyBps : Int;
    hopDetails : [HopDetail];
  }] {
    if (depth < 2 or depth > 6 or sampleSize == 0) { return [] };

    var routesExplored : Nat = 0;
    let MAX_ROUTES : Nat = 5000;

    let results = Vector.new<{
      route : [SwapHop];
      outputAmount : Nat;
      efficiency : Int;
      efficiencyBps : Int;
      hopDetails : [HopDetail];
    }>();

    // Build list of possible intermediate tokens (exclude target token)
    let mids = Array.filter<Text>(acceptedTokens, func(t) { t != token });

    // Recursive route builder: enumerate all paths token→...→token
    func buildAndSimulate(current : Text, hopsLeft : Nat, visited : [Text], routeSoFar : [SwapHop]) {
      if (routesExplored >= MAX_ROUTES) { return };
      routesExplored += 1;
      if (hopsLeft == 0) {
        // Last hop: must connect back to target token
        if (isKnownPool(current, token)) {
          let fullRoute = Array.append(routeSoFar, [{ tokenIn = current; tokenOut = token }]);
          // Simulate the full route
          let simPools = Map.new<(Text, Text), AMMPool>();
          let simV3 = Map.new<(Text, Text), PoolV3Data>();
          var amount = sampleSize;
          let hopDetailsVec = Vector.new<HopDetail>();
          var failed = false;

          for (hop in fullRoute.vals()) {
            let pk = getPool(hop.tokenIn, hop.tokenOut);
            let poolOpt = switch (Map.get(simPools, hashtt, pk)) { case (?p) { ?p }; case null { Map.get(AMMpools, hashtt, pk) } };
            let v3Opt = switch (Map.get(simV3, hashtt, pk)) { case (?v) { ?v }; case null { Map.get(poolV3Data, hashtt, pk) } };
            switch (poolOpt) {
              case (?pool) {
                let (out, updatedPool, updatedV3) = simulateSwap(pool, v3Opt, hop.tokenIn, amount, ICPfee);
                if (out == 0) { failed := true };
                Map.set(simPools, hashtt, pk, updatedPool);
                switch (updatedV3) { case (?uv3) { Map.set(simV3, hashtt, pk, uv3) }; case null {} };
                let hopAmountIn = amount;
                Vector.add(hopDetailsVec, {
                  tokenIn = hop.tokenIn; tokenOut = hop.tokenOut;
                  amountIn = hopAmountIn; amountOut = out;
                  fee = (hopAmountIn * ICPfee) / 10000;
                  priceImpact = 0.0;
                });
                amount := out;
              };
              case null { failed := true };
            };
            if (failed) { return };
          };

          if (not failed and amount > 0) {
            let eff : Int = amount - sampleSize;
            let effBps : Int = if (sampleSize > 0) { (eff * 10000) / sampleSize } else { 0 };
            Vector.add(results, {
              route = fullRoute;
              outputAmount = amount;
              efficiency = eff;
              efficiencyBps = effBps;
              hopDetails = Vector.toArray(hopDetailsVec);
            });
          };
        };
        return;
      };

      // Try each intermediate token
      for (mid in mids.vals()) {
        // Skip if already visited (no repeated intermediates)
        let alreadyVisited = switch (Array.find<Text>(visited, func(v) { v == mid })) {
          case (?_) { true }; case null { false };
        };
        if (not alreadyVisited and isKnownPool(current, mid)) {
          let newRoute = Array.append(routeSoFar, [{ tokenIn = current; tokenOut = mid }]);
          let newVisited = Array.append(visited, [mid]);
          buildAndSimulate(mid, hopsLeft - 1, newVisited, newRoute);
        };
      };
    };

    // Start enumeration from target token
    for (d in Iter.range(1, depth - 1)) {
      buildAndSimulate(token, d, [token], []);
    };

    // Sort by efficiency descending, return top 20
    let allResults = Vector.toArray(results);
    let sorted = Array.sort<{
      route : [SwapHop]; outputAmount : Nat; efficiency : Int;
      efficiencyBps : Int; hopDetails : [HopDetail];
    }>(allResults, func(a, b) {
      if (a.efficiencyBps > b.efficiencyBps) { #less }
      else if (a.efficiencyBps < b.efficiencyBps) { #greater }
      else { #equal };
    });

    let maxResults = Nat.min(sorted.size(), 20);
    Array.tabulate(maxResults, func(i : Nat) : {
      route : [SwapHop]; outputAmount : Nat; efficiency : Int;
      efficiencyBps : Int; hopDetails : [HopDetail];
    } { sorted[i] });
  };

  // Single-call optimal arb finder. Runs ternary search across a capital range
  // and returns the (amount, route) with highest absolute profit (in token units).
  // Replaces N separate adminAnalyzeRouteEfficiency calls + external ternary —
  // saves inter-canister round trips, gives consistent snapshot semantics
  // (all probes hit the same pool state), and bounds work to one query budget.
  //
  // Tracks running-best across all probes — robust against bumpy AMM curves
  // (OTC orders create discrete profit jumps that violate strict unimodality).
  //
  // Returns null if:
  //   - validation fails
  //   - no positive-profit route found in the searched range
  public query func adminFindOptimalArb(
    token : Text,
    minSample : Nat,
    maxSample : Nat,
    depth : Nat,
    probes : Nat,
    tolerancePct : Nat,
  ) : async ?{
    amount : Nat;
    route : [SwapHop];
    outputAmount : Nat;
    efficiency : Int;
    efficiencyBps : Int;
    hopDetails : [HopDetail];
    probesRun : Nat;
  } {
    if (depth < 2 or depth > 6) { return null };
    if (probes < 1 or probes > 20) { return null };
    if (maxSample == 0 or minSample >= maxSample) { return null };

    let MAX_ROUTES : Nat = 5000;
    let mids = Array.filter<Text>(acceptedTokens, func(t) { t != token });

    // Helper: enumerate all routes at given sampleSize, return single best
    // positive-profit candidate (or null if none).
    // Identical simulation logic to adminAnalyzeRouteEfficiency's buildAndSimulate
    // but tracks just the best instead of accumulating all routes — saves allocation.
    let findBestAt = func(sampleSize : Nat) : ?{
      route : [SwapHop];
      outputAmount : Nat;
      efficiency : Int;
      efficiencyBps : Int;
      hopDetails : [HopDetail];
    } {
      if (sampleSize == 0) { return null };
      var routesExplored : Nat = 0;
      var best : ?{
        route : [SwapHop];
        outputAmount : Nat;
        efficiency : Int;
        efficiencyBps : Int;
        hopDetails : [HopDetail];
      } = null;

      func buildAndSimulate(current : Text, hopsLeft : Nat, visited : [Text], routeSoFar : [SwapHop]) {
        if (routesExplored >= MAX_ROUTES) { return };
        routesExplored += 1;
        if (hopsLeft == 0) {
          if (isKnownPool(current, token)) {
            let fullRoute = Array.append(routeSoFar, [{ tokenIn = current; tokenOut = token }]);
            let simPools = Map.new<(Text, Text), AMMPool>();
            let simV3 = Map.new<(Text, Text), PoolV3Data>();
            var amount = sampleSize;
            let hopDetailsVec = Vector.new<HopDetail>();
            var failed = false;
            for (hop in fullRoute.vals()) {
              let pk = getPool(hop.tokenIn, hop.tokenOut);
              let poolOpt = switch (Map.get(simPools, hashtt, pk)) { case (?p) { ?p }; case null { Map.get(AMMpools, hashtt, pk) } };
              let v3Opt = switch (Map.get(simV3, hashtt, pk)) { case (?v) { ?v }; case null { Map.get(poolV3Data, hashtt, pk) } };
              switch (poolOpt) {
                case (?pool) {
                  let (out, updatedPool, updatedV3) = simulateSwap(pool, v3Opt, hop.tokenIn, amount, ICPfee);
                  if (out == 0) { failed := true };
                  Map.set(simPools, hashtt, pk, updatedPool);
                  switch (updatedV3) { case (?uv3) { Map.set(simV3, hashtt, pk, uv3) }; case null {} };
                  let hopAmountIn = amount;
                  Vector.add(hopDetailsVec, {
                    tokenIn = hop.tokenIn; tokenOut = hop.tokenOut;
                    amountIn = hopAmountIn; amountOut = out;
                    fee = (hopAmountIn * ICPfee) / 10000;
                    priceImpact = 0.0;
                  });
                  amount := out;
                };
                case null { failed := true };
              };
              if (failed) { return };
            };
            if (not failed and amount > sampleSize) {
              let eff : Int = amount - sampleSize;
              let effBps : Int = if (sampleSize > 0) { (eff * 10000) / sampleSize } else { 0 };
              let candidate = {
                route = fullRoute;
                outputAmount = amount;
                efficiency = eff;
                efficiencyBps = effBps;
                hopDetails = Vector.toArray(hopDetailsVec);
              };
              switch (best) {
                case null { best := ?candidate };
                case (?b) { if (eff > b.efficiency) { best := ?candidate } };
              };
            };
          };
          return;
        };
        for (mid in mids.vals()) {
          let alreadyVisited = switch (Array.find<Text>(visited, func(v) { v == mid })) {
            case (?_) { true }; case null { false };
          };
          if (not alreadyVisited and isKnownPool(current, mid)) {
            let newRoute = Array.append(routeSoFar, [{ tokenIn = current; tokenOut = mid }]);
            let newVisited = Array.append(visited, [mid]);
            buildAndSimulate(mid, hopsLeft - 1, newVisited, newRoute);
          };
        };
      };

      for (d in Iter.range(1, depth - 1)) {
        buildAndSimulate(token, d, [token], []);
      };
      best;
    };

    // Ternary search over [minSample, maxSample]. Tracks running best across
    // probes so a bumpy curve doesn't lose a good candidate to a subsequent
    // worse probe.
    var low : Nat = minSample;
    var high : Nat = maxSample;
    var bestSeen : ?{
      amount : Nat;
      route : [SwapHop];
      outputAmount : Nat;
      efficiency : Int;
      efficiencyBps : Int;
      hopDetails : [HopDetail];
    } = null;
    var probesRun : Nat = 0;

    label probesLoop while (probesRun < probes) {
      if (high <= low) { break probesLoop };

      let m1 : Nat = low + (high - low) / 3;
      let m2 : Nat = if (high > (high - low) / 3) { high - (high - low) / 3 } else { high };

      let r1 = findBestAt(m1);
      let r2 = findBestAt(m2);

      let p1 : Int = switch (r1) { case (?b) { b.efficiency }; case null { 0 } };
      let p2 : Int = switch (r2) { case (?b) { b.efficiency }; case null { 0 } };

      // Track running best
      switch (r1) {
        case null {};
        case (?b) {
          let cand = { amount = m1; route = b.route; outputAmount = b.outputAmount; efficiency = b.efficiency; efficiencyBps = b.efficiencyBps; hopDetails = b.hopDetails };
          switch (bestSeen) {
            case null { bestSeen := ?cand };
            case (?s) { if (cand.efficiency > s.efficiency) { bestSeen := ?cand } };
          };
        };
      };
      switch (r2) {
        case null {};
        case (?b) {
          let cand = { amount = m2; route = b.route; outputAmount = b.outputAmount; efficiency = b.efficiency; efficiencyBps = b.efficiencyBps; hopDetails = b.hopDetails };
          switch (bestSeen) {
            case null { bestSeen := ?cand };
            case (?s) { if (cand.efficiency > s.efficiency) { bestSeen := ?cand } };
          };
        };
      };

      // Narrow the range
      if (p1 < p2) { low := m1 } else { high := m2 };

      // Early termination
      if (maxSample > 0 and tolerancePct > 0) {
        let rangePct = ((high - low) * 100) / maxSample;
        if (rangePct < tolerancePct) { probesRun += 1; break probesLoop };
      };

      probesRun += 1;
    };

    switch (bestSeen) {
      case null { null };
      case (?b) {
        ?{
          amount = b.amount;
          route = b.route;
          outputAmount = b.outputAmount;
          efficiency = b.efficiency;
          efficiencyBps = b.efficiencyBps;
          hopDetails = b.hopDetails;
          probesRun;
        };
      };
    };
  };

  public shared ({ caller }) func adminExecuteRouteStrategy(
    amount : Nat,
    route : [SwapHop],
    minOutput : Nat,
    Block : Nat,
  ) : async ExTypes.SwapResult {
    if (not ownercheck(caller)) { return #Err(#NotAuthorized) };
    if (route.size() < 2 or route.size() > 6) { return #Err(#InvalidInput("2-6 hops required")) };

    let tokenIn = route[0].tokenIn;
    let tokenOut = route[route.size() - 1].tokenOut;
    let user = Principal.toText(caller);

    // Validate route continuity
    var i = 0;
    while (i < route.size() - 1) {
      if (route[i].tokenOut != route[i + 1].tokenIn) {
        return #Err(#InvalidInput("Route broken at hop " # Nat.toText(i)));
      };
      i += 1;
    };

    // Validate all pools exist
    for (hop in route.vals()) {
      if (not isKnownPool(hop.tokenIn, hop.tokenOut)) {
        return #Err(#PoolNotFound(hop.tokenIn # " / " # hop.tokenOut));
      };
    };

    var nowVar = Time.now();
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();

    // Block check and checkReceive
    if (Map.has(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block))) { return #Err(#InvalidInput("Block already processed")) };
    Map.set(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block), nowVar);
    let nowVar2 = nowVar;
    let tType = returnType(tokenIn);

    // Flush stuck transfers
    if (Vector.size(tempTransferQueue) > 0) {
      if FixStuckTXRunning {} else {
        FixStuckTXRunning := true;
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue), isInAllowedCanisters(caller)) } catch (err) { Debug.print(Error.message(err)); false })) {
          Vector.clear<(TransferRecipient, Nat, Text, Text)>(tempTransferQueue);
        };
        FixStuckTXRunning := false;
      };
    };

    let blockData = try {
      await* getBlockData(tokenIn, Block, tType);
    } catch (err) {
      Map.delete(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block));
      #ICRC12([]);
    };
    nowVar := Time.now();

    if (blockData == #ICRC12([])) {
      Map.delete(BlocksDone, thash, tokenIn # ":" # Nat.toText(Block));
      return #Err(#SystemError("Failed to get block data"));
    };

    let (receiveBool, receiveTransfers) = checkReceive(Block, caller, amount, tokenIn, ICPfee, RevokeFeeNow, false, true, blockData, tType, nowVar2);
    Vector.addFromIter(tempTransferQueueLocal, receiveTransfers.vals());
    if (not receiveBool) {
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { Debug.print(Error.message(err)); false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
      };
      return #Err(#InsufficientFunds("Funds not received"));
    };

    // Execute hops
    var currentAmount = amount;
    var firstHopPoolFee : Nat = 0;
    // capture 30% DAO portion from hop 0 to avoid adding
    // 99.5% non-revoke (which includes the 70% already in feeGrowthGlobal).
    var firstHopProtocolFee : Nat = 0;
    var firstHopHadOrderbookMatch = false;
    var lastHopWasAMMOnly = false;

    for (hopIndex in Iter.range(0, route.size() - 1)) {
      let hop = route[hopIndex];
      let isLastHop : Bool = hopIndex + 1 == route.size();

      let syntheticTrade : TradePrivate = {
        Fee = ICPfee;
        amount_sell = 1;
        amount_init = currentAmount;
        token_sell_identifier = hop.tokenOut;
        token_init_identifier = hop.tokenIn;
        trade_done = 0; seller_paid = 0; init_paid = 1;
        seller_paid2 = 0; init_paid2 = 0; trade_number = 0;
        SellerPrincipal = "0"; initPrincipal = user;
        RevokeFee = RevokeFeeNow; OCname = "";
        time = nowVar; filledInit = 0; filledSell = 0;
        allOrNothing = false; strictlyOTC = false;
      };

      let (remaining, protocolFee, poolFee, transfers, wasAMMOnly, consumedOrders, _) = orderPairing(syntheticTrade);
      lastHopWasAMMOnly := wasAMMOnly;
      if (hopIndex == 0) {
        firstHopPoolFee := poolFee;
        firstHopProtocolFee := protocolFee;
        firstHopHadOrderbookMatch := not wasAMMOnly;
      };

      // For hops 1+, V3 handles fees internally
      // (same as swapMultiHop)

      var hopOutput : Nat = 0;
      for (tx in transfers.vals()) {
        if (tx.0 == #principal(caller) and tx.2 == hop.tokenOut) {
          hopOutput += tx.1;
          if (isLastHop) {
            Vector.add(tempTransferQueueLocal, tx);
          };
        } else {
          Vector.add(tempTransferQueueLocal, tx);
          if (hopIndex == 0 and tx.2 == tokenIn) {
            firstHopHadOrderbookMatch := true;
          };
        };
      };

      if (hopIndex == 0 and remaining > returnTfees(hop.tokenIn) * 3) {
        Vector.add(tempTransferQueueLocal, (#principal(caller), remaining, hop.tokenIn, genTxId()));
      };

      currentAmount := hopOutput;
      if (currentAmount == 0) {
        if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(tempTransferQueueLocal), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
          Vector.addFromIter(tempTransferQueue, Vector.vals(tempTransferQueueLocal));
        };
        return #Err(#RouteFailed({ hop = hopIndex; reason = "No output" }));
      };

      // Restore sellTfees for intermediate AMM-only hops
      if (not isLastHop and wasAMMOnly) {
        currentAmount += returnTfees(hop.tokenOut);
      };
      // Track sellTfees gap for intermediate hybrid hops
      if (not isLastHop and not wasAMMOnly) {
        addFees(hop.tokenOut, returnTfees(hop.tokenOut), false, "", nowVar);
      };
    };

    // Fee collection for hop 0
    // Additive: calculateFee (user upfront) + firstHopProtocolFee (AMM protocol).
    // Sync v3 claim by protocolFee only.
    let tradingFee = calculateFee(amount, ICPfee, RevokeFeeNow) + firstHopProtocolFee;
    let inputTfees = if (firstHopHadOrderbookMatch) { 0 } else { returnTfees(tokenIn) };
    let feeToAdd = tradingFee + inputTfees;
    addFees(tokenIn, feeToAdd, false, user, nowVar);
    if (firstHopProtocolFee > 0) {
      claimProtocolFeeInV3(tokenIn, route[0].tokenOut, firstHopProtocolFee);
    };

    // Slippage check
    if (currentAmount < minOutput) {
      let slipConsolidatedMap = Map.new<Text, (TransferRecipient, Nat, Text, Text)>();
      for (tx in Vector.vals(tempTransferQueueLocal)) {
        let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
        let key = rcpt # ":" # tx.2;
        switch (Map.get(slipConsolidatedMap, thash, key)) {
          case (?existing) { Map.set(slipConsolidatedMap, thash, key, (tx.0, existing.1 + tx.1, tx.2, tx.3)) };
          case null { Map.set(slipConsolidatedMap, thash, key, tx) };
        };
      };
      let slipVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();
      for ((_, tx) in Map.entries(slipConsolidatedMap)) { Vector.add(slipVec, tx) };
      if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(slipVec), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
        Vector.addFromIter(tempTransferQueue, Vector.vals(slipVec));
      };
      return #Err(#SlippageExceeded({ expected = minOutput; got = currentAmount }));
    };

    // Record swap
    let routeVec = Vector.new<Text>();
    Vector.add(routeVec, tokenIn);
    for (hop in route.vals()) { Vector.add(routeVec, hop.tokenOut) };
    nextSwapId += 1;
    recordSwap(caller, {
      swapId = nextSwapId; tokenIn; tokenOut;
      amountIn = amount; amountOut = currentAmount;
      route = Vector.toArray(routeVec);
      fee = calculateFee(amount, ICPfee, RevokeFeeNow);
      swapType = #multihop; timestamp = Time.now();
    });

    doInfoBeforeStep2();

    // Consolidate and send transfers
    let preCountMap = Map.new<Text, Nat>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(preCountMap, thash, key)) {
        case (?n) { Map.set(preCountMap, thash, key, n + 1) };
        case null { Map.set(preCountMap, thash, key, 1) };
      };
    };
    let consolidatedMap = Map.new<Text, (TransferRecipient, Nat, Text, Text)>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(consolidatedMap, thash, key)) {
        case (?existing) { Map.set(consolidatedMap, thash, key, (tx.0, existing.1 + tx.1, tx.2, tx.3)) };
        case null { Map.set(consolidatedMap, thash, key, tx) };
      };
    };
    let consolidatedVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    for ((_, tx) in Map.entries(consolidatedMap)) { Vector.add(consolidatedVec, tx) };

    // Track consolidation savings for output token
    for ((key, count) in Map.entries(preCountMap)) {
      if (count > 1) {
        let tkn = switch (Map.get(consolidatedMap, thash, key)) { case (?tx) { tx.2 }; case null { "" } };
        if (tkn == tokenOut) {
          let savedFees = (count - 1) * returnTfees(tkn);
          addFees(tkn, savedFees, false, "", nowVar);
        };
      };
    };

    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(consolidatedVec), isInAllowedCanisters(caller)) } catch (err) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(consolidatedVec));
    };

    #Ok({
      amountIn = amount;
      amountOut = currentAmount;
      tokenIn = tokenIn;
      tokenOut = tokenOut;
      route = Vector.toArray(routeVec);
      fee = calculateFee(amount, ICPfee, RevokeFeeNow);
      swapId = nextSwapId;
      hops = route.size();
      firstHopOrderbookMatch = firstHopHadOrderbookMatch;
      lastHopAMMOnly = lastHopWasAMMOnly;
    });
  };

  // ── Flash arbitrage (zero-capital, allowlist-gated) ──
  // Trusted callers hoisted to top of actor (see FLASH_ARB_CALLERS declaration
  // after DAOTreasury) so the membership check is also visible to the same-token
  // guard in addPosition / swapMultiHop / swapSplitRoutes.

  // adminFlashArb — execute a circular arbitrage route using exchange-internal
  // capital (lent from feescollectedDAO[startToken] when available, phantom otherwise).
  // The buyback canister provides NO ICP/ckUSDC capital; profit (or zero) is paid
  // out via a single ICRC-1 transfer of `netProfit` of startToken to the caller.
  //
  // Atomicity & safety: the entire synchronous segment (route execution + fee
  // booking + lend repay) commits atomically. If realized < notional + minProfit
  // + costs, the function traps and ALL state mutations roll back — no funds move,
  // no record is created, and the buyback's profit guarantee is preserved
  // (netProfit is always >= 0 on success, message reverts otherwise).
  //
  // Drift profile mirrors adminExecuteRouteStrategy (same fee booking, same
  // ±10K satoshi envelope per call). No new drift sources introduced.
  public shared ({ caller }) func adminFlashArb(
    notional : Nat,
    route : [SwapHop],
    minProfit : Nat,
  ) : async {
    #Ok : {
      notional : Nat;
      realized : Nat;
      grossProfit : Nat;     // realized − notional (before fees)
      netProfit : Nat;       // grossProfit − tradingFee − inputTfees − transferFee
      tradingFee : Nat;      // calculateFee(notional, ICPfee, RevokeFeeNow) + firstHopProtocolFee
      inputTfees : Nat;
      transferFee : Nat;
      hops : Nat;
      swapId : Nat;
      capitalSource : { #lent; #phantom };
    };
    #Err : Text;
  } {
    // ── Auth: ONLY the buyback canister may call ──
    if (not isFlashArbCaller(caller)) {
      return #Err("Not authorized: caller is not on the flash-arb allowlist");
    };

    // ── Input validation ──
    if (notional == 0) return #Err("notional must be > 0");
    if (route.size() < 2 or route.size() > 6) return #Err("2-6 hops required");
    let startToken = route[0].tokenIn;
    if (route[route.size() - 1].tokenOut != startToken) {
      return #Err("Route must be circular: last.tokenOut != first.tokenIn");
    };

    // Validate route continuity
    var rci = 0;
    while (rci < route.size() - 1) {
      if (route[rci].tokenOut != route[rci + 1].tokenIn) {
        return #Err("Route broken at hop " # Nat.toText(rci));
      };
      rci += 1;
    };

    // Validate all pools exist
    for (hop in route.vals()) {
      if (not isKnownPool(hop.tokenIn, hop.tokenOut)) {
        return #Err("Pool not found: " # hop.tokenIn # " / " # hop.tokenOut);
      };
    };

    // ── Capital decision: lend mode (preferred) vs phantom mode (fallback) ──
    let lendable = switch (Map.get(feescollectedDAO, thash, startToken)) {
      case (?v) v;
      case null 0;
    };
    let useLend : Bool = lendable >= notional;

    var nowVar = Time.now();
    // tempTransferQueueLocal is message-scoped; trap below this line reverts it cleanly
    // along with all other state mutations from this point onward.
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    let user = Principal.toText(caller);

    // ── LEND (accounting only — no physical token movement) ──
    if (useLend) {
      Map.set(feescollectedDAO, thash, startToken, (lendable - notional : Nat));
    };

    // ── EXECUTE HOPS (synchronous, no awaits) ──
    var currentAmount : Nat = notional;
    var firstHopProtocolFee : Nat = 0;
    var firstHopHadOrderbookMatch : Bool = false;

    for (hopIndex in Iter.range(0, route.size() - 1)) {
      let hop = route[hopIndex];
      let isLastHop : Bool = hopIndex + 1 == route.size();

      let syntheticTrade : TradePrivate = {
        Fee = ICPfee;
        amount_sell = 1;
        amount_init = currentAmount;
        token_sell_identifier = hop.tokenOut;
        token_init_identifier = hop.tokenIn;
        trade_done = 0; seller_paid = 0; init_paid = 1;
        seller_paid2 = 0; init_paid2 = 0; trade_number = 0;
        SellerPrincipal = "0"; initPrincipal = user;
        RevokeFee = RevokeFeeNow; OCname = "";
        time = nowVar; filledInit = 0; filledSell = 0;
        allOrNothing = false; strictlyOTC = false;
      };

      let (remaining, protocolFee, _poolFee, transfers, wasAMMOnly, _, _) = orderPairing(syntheticTrade);
      if (hopIndex == 0) {
        firstHopProtocolFee := protocolFee;
        firstHopHadOrderbookMatch := not wasAMMOnly;
      };

      var hopOutput : Nat = 0;
      for (tx in transfers.vals()) {
        if (tx.0 == #principal(caller) and tx.2 == hop.tokenOut) {
          hopOutput += tx.1;
          // Flash arb: do NOT queue the last hop output here — we split it into
          // (repay notional via accounting) + (netProfit via outgoing transfer)
          // after the loop. Intermediate hop outputs are consumed by the next hop.
        } else {
          // OTC seller payouts, AMM rebates, refunds — queue normally.
          Vector.add(tempTransferQueueLocal, tx);
          if (hopIndex == 0 and tx.2 == startToken) {
            firstHopHadOrderbookMatch := true;
          };
        };
      };

      // Trap on hop 0 partial fill (route diverged from snapshot — abort cleanly).
      if (hopIndex == 0 and remaining > returnTfees(hop.tokenIn) * 3) {
        Debug.trap("Flash arb hop 0 partial fill: remaining=" # Nat.toText(remaining));
      };

      currentAmount := hopOutput;
      if (currentAmount == 0) {
        Debug.trap("Flash arb hop " # Nat.toText(hopIndex) # " produced no output");
      };

      // Mirror adminExecuteRouteStrategy intermediate-hop handling
      if (not isLastHop and wasAMMOnly) {
        currentAmount += returnTfees(hop.tokenOut);
      };
      if (not isLastHop and not wasAMMOnly) {
        addFees(hop.tokenOut, returnTfees(hop.tokenOut), false, "", nowVar);
      };
    };

    let realized = currentAmount;
    let grossProfit : Nat = if (realized >= notional) { realized - notional } else { 0 };

    // ── FEE COLLECTION (EXACT mirror of adminExecuteRouteStrategy:15870-15879) ──
    let tradingFee = calculateFee(notional, ICPfee, RevokeFeeNow) + firstHopProtocolFee;
    let inputTfees = if (firstHopHadOrderbookMatch) { 0 } else { returnTfees(startToken) };
    let feeToAdd = tradingFee + inputTfees;
    let transferFee = returnTfees(startToken);

    // ── PROFIT GUARANTEE: trap if profit insufficient (reverts ALL state) ──
    // Costs to the buyback: feeToAdd (booked to feescollectedDAO) + transferFee
    // (paid by exchange_treasury on the outgoing netProfit transfer).
    if (grossProfit < feeToAdd + transferFee + minProfit) {
      Debug.trap(
        "Flash arb profit guard: realized=" # Nat.toText(realized) #
        " notional=" # Nat.toText(notional) #
        " grossProfit=" # Nat.toText(grossProfit) #
        " feeToAdd=" # Nat.toText(feeToAdd) #
        " transferFee=" # Nat.toText(transferFee) #
        " minProfit=" # Nat.toText(minProfit)
      );
    };
    let netProfit : Nat = grossProfit - feeToAdd - transferFee;

    // Book all fees to feescollectedDAO + sync V3 internal accounting.
    addFees(startToken, feeToAdd, false, user, nowVar);
    if (firstHopProtocolFee > 0) {
      claimProtocolFeeInV3(startToken, route[0].tokenOut, firstHopProtocolFee);
    };

    // ── REPAY LEND (accounting only) ──
    if (useLend) {
      let postLend = (lendable - notional : Nat);
      Map.set(feescollectedDAO, thash, startToken, postLend + notional);
    };

    // ── QUEUE PROFIT TRANSFER ──
    Vector.add(tempTransferQueueLocal, (#principal(caller), netProfit, startToken, genTxId()));

    // ── RECORD SWAP (swapType=#multihop, tokenIn==tokenOut indicates flash arb) ──
    nextSwapId += 1;
    let routeVec = Vector.new<Text>();
    Vector.add(routeVec, startToken);
    for (hop in route.vals()) { Vector.add(routeVec, hop.tokenOut) };
    recordSwap(caller, {
      swapId = nextSwapId;
      tokenIn = startToken;
      tokenOut = startToken;
      amountIn = notional;
      amountOut = realized;
      route = Vector.toArray(routeVec);
      fee = tradingFee;
      swapType = #multihop;
      timestamp = Time.now();
    });

    doInfoBeforeStep2();

    // ── CONSOLIDATE TRANSFERS (mirror adminExecuteRouteStrategy:15915-15945) ──
    let preCountMap = Map.new<Text, Nat>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(preCountMap, thash, key)) {
        case (?n) { Map.set(preCountMap, thash, key, n + 1) };
        case null { Map.set(preCountMap, thash, key, 1) };
      };
    };
    let consolidatedMap = Map.new<Text, (TransferRecipient, Nat, Text, Text)>();
    for (tx in Vector.vals(tempTransferQueueLocal)) {
      let rcpt = switch (tx.0) { case (#principal(p)) { Principal.toText(p) }; case (#accountId(a)) { Principal.toText(a.owner) } };
      let key = rcpt # ":" # tx.2;
      switch (Map.get(consolidatedMap, thash, key)) {
        case (?existing) { Map.set(consolidatedMap, thash, key, (tx.0, existing.1 + tx.1, tx.2, tx.3)) };
        case null { Map.set(consolidatedMap, thash, key, tx) };
      };
    };
    let consolidatedVec = Vector.new<(TransferRecipient, Nat, Text, Text)>();
    for ((_, tx) in Map.entries(consolidatedMap)) { Vector.add(consolidatedVec, tx) };

    // Track consolidation savings — drift-critical (line 15938-15945 logic).
    for ((key, count) in Map.entries(preCountMap)) {
      if (count > 1) {
        let tkn = switch (Map.get(consolidatedMap, thash, key)) { case (?tx) { tx.2 }; case null { "" } };
        if (tkn == startToken) {
          let savedFees = (count - 1) * returnTfees(tkn);
          addFees(tkn, savedFees, false, "", nowVar);
        };
      };
    };

    // ── FLUSH (single await — commits all synchronous state above) ──
    if ((try { await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Text, Text)>(consolidatedVec), isInAllowedCanisters(caller)) } catch (_) { false })) {} else {
      Vector.addFromIter(tempTransferQueue, Vector.vals(consolidatedVec));
    };

    #Ok({
      notional;
      realized;
      grossProfit;
      netProfit;
      tradingFee;
      inputTfees;
      transferFee;
      hops = route.size();
      swapId = nextSwapId;
      capitalSource = if (useLend) { #lent } else { #phantom };
    });
  };

  // certain rules that get applied before cyclespent so spamming is mitigated. Here also certain ruling is available considering who can access certain functions.
  system func inspect({
    caller : Principal;
    arg : Blob;
    msg : {
      #ChangeReferralFees : () -> (newFeePercentage : Nat);
        #ChangeRevokefees : () -> (ok : Nat);
        #ChangeTradingfees : () -> (ok : Nat);
        #FinishSell :
          () -> (Block : Nat64, accesscode : Text, amountSelling : Nat);
        #FinishSellBatch :
          () ->
            (Block : Nat64, accesscode : [Text],
             amount_Sell_by_Reactor : [Nat], token_sell_identifier : Text,
             token_init_identifier : Text);
        #FinishSellBatchDAO :
          () ->
            (trades : [TradeData], createOrdersIfNotDone : Bool,
             special : [Nat]);
        #FixStuckTX : () -> (accesscode : Text);
        #Freeze : () -> ();
        #addAcceptedToken :
          () ->
            (action : {#Add; #Opposite; #Remove}, added2 : Text,
             minimum : Nat, tType : {#ICP; #ICRC12; #ICRC3});
        #addConcentratedLiquidity :
          () ->
            (token0i : Text, token1i : Text, amount0i : Nat, amount1i : Nat,
             priceLower : Nat, priceUpper : Nat, block0i : Nat, block1i : Nat);
        #addLiquidity :
          () ->
            (token0i : Text, token1i : Text, amount0i : Nat, amount1i : Nat,
             block0i : Nat, block1i : Nat, isInitial : ?Bool);
        #removeConcentratedLiquidity :
          () ->
            (token0i : Text, token1i : Text, positionId : Nat, liquidityAmount : Nat);
        #addPosition :
          () ->
            (Block : Nat, amount_sell : Nat, amount_init : Nat,
             token_sell_identifier : Text, token_init_identifier : Text,
             pub : Bool, excludeDAO : Bool, OC : ?Text, referrer : Text,
             allOrNothing : Bool, strictlyOTC : Bool);
        #addTimer : () -> ();
        #changeOwner2 : () -> (pri : Principal);
        #changeOwner3 : () -> (pri : Principal);
        #clearTokenArchiveOffset : () -> (token : Text);
        #getTokenArchiveOffset : () -> (token : Text);
        #checkDiffs : () -> (returnFees : Bool, alwaysShow : Bool);
        #getDriftOpTracker : () -> ();
        #resetDriftOpTracker : () -> ();
        #clearAllBans : () -> ();
        #clearStuckLocks : () -> (accesscode : ?Text, blocksDoneKey : ?Text);
        #setMinimumAmount : () -> (token : Text, newMinimum : Nat);
        #adminRepairLastTradedPriceAndKlines : () -> (affectedPoolIndexes : [Nat], alsoRepairVolume24h : Bool);
        #adminDeleteKlinesBefore : () -> (cutoffNs : Int, maxDeletesPerBucket : Nat);
        #adminCheckBan : () -> (p : Principal);
        #adminForceUnlockRecovery : () -> ();
        #adminRecoverWronglysent :
          () ->
            (recipient : Principal, identifier : Text, Block : Nat,
             tType : {#ICP; #ICRC12; #ICRC3});
        #cleanTokenIds : () -> ();
        #isExchangeFrozen : () -> ();
        #resetAllState : () -> ();
        #getAllowedCanisters : () -> ();
        #refundStuckFunds : () -> ();
        #checkFeesReferrer : () -> ();
        #claimFeesReferrer : () -> ();
        #collectFees : () -> ();
        #addFeeCollector : () -> (p : Principal);
        #removeFeeCollector : () -> ();
        #getFeeCollectors : () -> ();
        #exchangeInfo : () -> ();
        #getAMMPoolInfo : () -> (token0 : Text, token1 : Text);
        #getAcceptedTokens : () -> ();
        #getAcceptedTokensInfo : () -> ();
        #getAllAMMPools : () -> ();
        #getAllTradesPrivateCostly : () -> ();
        #getAllTradesPublic : () -> ();
        #getCurrentLiquidity :
          () ->
            (token1 : Text, token2 : Text, direction : {#backward; #forward},
             limit : Nat, cursor : ?Ratio);
        #getCurrentLiquidityForeignPools :
          () ->
            (limit : Nat, poolQuery : ?[PoolQuery],
             onlySpecifiedPools : Bool);
        #getExpectedMultiHopAmount :
          () -> (tokenIn : Text, tokenOut : Text, amountIn : Nat);
        #getExpectedReceiveAmount :
          () -> (tokenSell : Text, tokenBuy : Text, amountSell : Nat);
        #getExpectedReceiveAmountBatch :
          () -> (requests : [{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }]);
        #getExpectedReceiveAmountBatchMulti :
          () -> (requests : [{ tokenSell : Text; tokenBuy : Text; amountSell : Nat }],
                 maxRoutesPerRequest : Nat);
        #getKlineData :
          () ->
            (token1 : Text, token2 : Text, timeFrame : TimeFrame,
             initialGet : Bool);
        #getKlineDataRange :
          () ->
            (token1 : Text, token2 : Text, timeFrame : TimeFrame,
             before : ?Int, limit : Nat);
        #get_token_trends_7d : () -> (tokens : [Principal]);
        #getLogs : () -> (count : Nat);
        #getLogging :
          () ->
            (functionType : {#FinishSellBatchDAO; #addAcceptedToken},
             getLastXEntries : Nat);
        #getOrderbookCombined :
          () ->
            (token0 : Text, token1 : Text, numLevels : Nat,
             stepBasisPoints : Nat);
        #getPausedTokens : () -> ();
        #getPoolHistory : () -> (token1 : Text, token2 : Text, limit : Nat);
        #recoverBatch : () -> (recoveries : [{ identifier : Text; block : Nat; tType : { #ICP; #ICRC12; #ICRC3 } }]);
        #getPoolStats : () -> (token0 : Text, token1 : Text);
        #getAllPoolStats : () -> ();
        #getPoolRanges : () -> (token0 : Text, token1 : Text);
        #getPrivateTrade : () -> (pass : Text);
        #getTokenUSDPrices :
          () -> (ICPpriceUSD : Float, ckUSDCpriceUSD : Float);
        #getUserLiquidityDetailed : () -> ();
        #getUserPreviousTrades : () -> (token1 : Text, token2 : Text);
        #getUserReferralInfo : () -> ();
        #getUserTradeHistory : () -> (limit : Nat);
        #getUserConcentratedPositions : () -> ();
        #getUserSwapHistory : () -> (limit : Nat);
        #getUserTrades : () -> ();
        #get_cycles : () -> ();
        #hmFee : () -> ();
        #hmRefFee : () -> ();
        #hmRevokeFee : () -> ();
        #p2a : () -> ();
        #p2acannister : () -> ();
        #p2athird : () -> (p : Text);
        #parameterManagement :
          () ->
            (parameters : {
                            addAllowedCanisters : ?[Text];
                            addToAllTimeBan : ?[Text];
                            changeAllowedCalls : ?Nat;
                            changeallowedSilentWarnings : ?Nat;
                            daoTreasuryPrincipalsText : ?[Text];
                            deleteAllowedCanisters : ?[Text];
                            deleteFromAllTimeBan : ?[Text];
                            deleteFromDayBan : ?[Text];
                            treasury_principal : ?Text
                          });
        #pauseToken : () -> (token : Text);
        #recalibrateDAOpositions : () -> (positions : [PositionData]);
        #recoverWronglysent :
          () ->
            (identifier : Text, Block : Nat, tType : {#ICP; #ICRC12; #ICRC3});
        #claimLPFees :
          () -> (token0i : Text, token1i : Text);
        #claimConcentratedFees :
          () -> (positionId : Nat);
        #claimAllLPFees : () -> ();
        #removeLiquidity :
          () -> (token0i : Text, token1i : Text, liquidityAmount : Nat);
        #retrieveFundsDao : () -> (trades : [(Text, Nat64)]);
        #returncontractprincipal : () -> ();
        #revokeTrade :
          () ->
            (accesscode : Text,
             revokeType : {#DAO : [Text]; #Initiator; #Seller});
        #sendDAOInfo : () -> ();
        #setTest : () -> (a : Bool);
        #simulateSplitRoutes :
          () -> (splits : [{ amountIn : Nat; route : [SwapHop] }]);
        #swapMultiHop :
          () ->
            (tokenIn : Text, tokenOut : Text, amountIn : Nat,
             route : [SwapHop], minAmountOut : Nat, Block : Nat);
        #swapSplitRoutes :
          () ->
            (tokenIn : Text, tokenOut : Text, splits : [SplitLeg],
             minAmountOut : Nat, Block : Nat);
        #treasurySwap :
          () ->
            (tokenIn : Text, tokenOut : Text, amountIn : Nat,
             minAmountOut : Nat, block : Nat);
        #adminExecuteRouteStrategy :
          () ->
            (amount : Nat, route : [SwapHop], minOutput : Nat,
             Block : Nat);
        #adminAnalyzeRouteEfficiency :
          () ->
            (token : Text, sampleSize : Nat, depth : Nat);
        #adminFindOptimalArb :
          () ->
            (token : Text, minSample : Nat, maxSample : Nat, depth : Nat,
             probes : Nat, tolerancePct : Nat);
        #adminFlashArb :
          () -> (notional : Nat, route : [SwapHop], minProfit : Nat);
        #adminDrainTestModeExchange : () -> (target : Principal);
        #adminDrainTestModeStatus : () -> ();
        #batchClaimAllFees : () -> ();
        #batchAdjustLiquidity : () -> (adjustments : [{ token0 : Text; token1 : Text; action : { #Remove : { liquidityAmount : Nat } } }]);
        #addLiquidityDAO : () -> (token0 : Text, token1 : Text, amount0 : Nat, amount1 : Nat, block0 : Nat, block1 : Nat, isInitial : ?Bool);
        #getDAOLiquiditySnapshot : () -> ();
        #getDAOLPPerformance : () -> ();
        #debugV3Ticks : () -> (token0 : Text, token1 : Text);
        #updateTokenType : () -> (token : Text, newType : {#ICP; #ICRC12; #ICRC3});
        #canTradeTokens : () -> (tokenIn : Text, tokenOut : Text);
    };
  }) : Bool {


    if (
      TrieSet.contains(dayBan, caller, Principal.hash(caller), Principal.equal) or
      TrieSet.contains(allTimeBan, caller, Principal.hash(caller), Principal.equal)
    ) {
      return false;
    };

    if (arg.size() > 512000) { return false }; //Not sure how much this should be
    if (
      exchangeState != #Active and caller != DAOTreasury and caller != owner2 and caller != treasury_principal and (
        switch (msg) {
          case (#revokeTrade _) true;
          case (#pauseToken _) true;
          case (#adminDrainTestModeExchange _) true;
          case (#adminDrainTestModeStatus _) true;
          case (#Freeze _) true;
          case (#clearAllBans _) true;
          case (#isExchangeFrozen _) true;
          case (#getAllowedCanisters _) true;
          case (_) false;
        }
      ) == false
    ) {
      return false;
    };

    let callerIsAdmin = isAdmin(caller);
    switch (msg) {
      case (#ChangeRevokefees _) callerIsAdmin;
      case (#ChangeTradingfees _) callerIsAdmin;
      case (#parameterManagement _) callerIsAdmin;
      case (#clearAllBans _) callerIsAdmin;
      case (#clearStuckLocks _) callerIsAdmin;
      case (#adminCheckBan _) callerIsAdmin;
      case (#adminForceUnlockRecovery _) callerIsAdmin;
      case (#adminRecoverWronglysent _) callerIsAdmin;
      case (#setMinimumAmount _) callerIsAdmin;
      case (#adminRepairLastTradedPriceAndKlines _) callerIsAdmin;
      case (#adminDeleteKlinesBefore _) callerIsAdmin;
      case (#resetAllState _) callerIsAdmin;
      case (#FinishSellBatchDAO _) false;
      case (#Freeze _) callerIsAdmin;
      case (#adminExecuteRouteStrategy _) callerIsAdmin;
      case (#adminAnalyzeRouteEfficiency _) true;
      case (#adminFindOptimalArb _) true;
      case (#adminFlashArb _) isFlashArbCaller(caller);
      case (#addAcceptedToken _) callerIsAdmin;
      case (#updateTokenType _) callerIsAdmin;
      case (#canTradeTokens _) true;
      case (#addTimer _) callerIsAdmin;
      case (#changeOwner2 _) caller == owner2 or callerIsAdmin;
      case (#changeOwner3 _) caller == owner3 or callerIsAdmin;
      case (#clearTokenArchiveOffset _) caller == owner2 or callerIsAdmin;
      case (#getTokenArchiveOffset _) true;
      case (#collectFees _) { caller == deployer.caller or (do { var found = false; for (p in feeCollectors.vals()) { if (p == caller) found := true }; found }) };
      case (#addFeeCollector _) { caller == deployer.caller or (do { var found = false; for (p in feeCollectors.vals()) { if (p == caller) found := true }; found }) };
      case (#removeFeeCollector _) { caller == deployer.caller or (do { var found = false; for (p in feeCollectors.vals()) { if (p == caller) found := true }; found }) };
      case (#getFeeCollectors _) { caller == deployer.caller or (do { var found = false; for (p in feeCollectors.vals()) { if (p == caller) found := true }; found }) };
      case (#isExchangeFrozen _) true;
      case (#getAllowedCanisters _) true;
      case (#exchangeInfo _) true;
      case (#getAllTradesPublic _) callerIsAdmin;
      case (#getAllTradesPrivateCostly _) callerIsAdmin;
      case (#get_cycles _) callerIsAdmin;
      case (#getLogs _) callerIsAdmin;
      case (#p2a _) true;
      case (#p2acannister _) true;
      case (#p2athird _) true;
      case (#pauseToken _) callerIsAdmin;
      case (#recalibrateDAOpositions _) false;
      case (#retrieveFundsDao _) false;
      case (#returncontractprincipal _) true;
      case (#sendDAOInfo _) false;
      case (#treasurySwap _) callerIsAdmin;
      case (#setTest _) callerIsAdmin or test;
      case (#checkDiffs _) callerIsAdmin or test;
      case (#getDriftOpTracker _) callerIsAdmin or test;
      case (#resetDriftOpTracker _) callerIsAdmin or test;
      case (#cleanTokenIds _) callerIsAdmin;
      case (#refundStuckFunds _) callerIsAdmin;
      case (#adminDrainTestModeExchange _) caller == deployer.caller or caller == Principal.fromText("odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe");
      case (#adminDrainTestModeStatus _) callerIsAdmin;
      case (#batchClaimAllFees _) callerIsAdmin;
      case (#batchAdjustLiquidity _) callerIsAdmin;
      case (#addLiquidityDAO _) callerIsAdmin;
      case (#getDAOLiquiditySnapshot _) true;
      case (#getDAOLPPerformance _) true;
      case (#FinishSell d) {
        var tid : Text = d().1;
        if ((tid.size() >= 32 and tid.size() < 60)) { return true } else {
          return false;
        };
      };

      case (#addPosition d) {
        var buy : Text = d().4;
        var sell : Text = d().3;

        if (containsToken(sell) and containsToken(buy)) { return true } else {
          return false;
        };
      };

      case (#swapMultiHop d) {
        let (tokenIn, tokenOut, _, route, _, _) = d();
        if (not containsToken(tokenIn) or not containsToken(tokenOut)) return false;
        if (route.size() < 1 or route.size() > 3) return false; // allow 1-hop direct + 2-3 hop multi
        return true;
      };

      case (#swapSplitRoutes d) {
        let (tokenIn, tokenOut, splits, _, _) = d();
        if (not containsToken(tokenIn) or not containsToken(tokenOut)) return false;
        if (splits.size() < 1 or splits.size() > 3) return false;
        for (leg in splits.vals()) {
          if (leg.route.size() < 1 or leg.route.size() > 3) return false;
        };
        return true;
      };

      case (#claimLPFees _) true;
      case (#claimAllLPFees _) true;

      case (#claimConcentratedFees _) true;

      case (#getPrivateTrade d) {
        var tid : Text = d();
        if ((tid.size() >= 32 and tid.size() < 60)) { return true } else {
          return false;
        };
      };

      case (#finishSellBatch d) {
        var tid : Text = d();
        if ((tid.size() >= 32 and tid.size() < 60)) { return true } else {
          return false;
        };
      };
      case (#revokeTrade d) {
        let (tid, revokeType) = d();
        switch (revokeType) {
          case (#DAO(accesscodes)) {
            if (caller != DAOTreasury) { return false };
            for (accesscode in accesscodes.vals()) {
              if (accesscode.size() != 32) {
                return false;
              };
            };
            return true;
          };
          case (#Seller) {
            if (tid.size() >= 32 and tid.size() < 60) { return true } else {
              return false;
            };
          };
          case (#Initiator) {
            if (tid.size() >= 32 and tid.size() < 60) { return true } else {
              return false;
            };
          };
        };
      };

      case _ { true };
    };
  };
};
