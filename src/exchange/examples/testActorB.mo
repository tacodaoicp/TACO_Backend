import Exchange "./exchange";
import ICPLedger "../src/Ledger";
import ICRC1 "mo:icrc1/ICRC1";
import Utils "../src/Utils";
import Principal "mo:base/Principal";
import Blob "mo:base/Blob";
import Buffer "mo:base/Buffer";
import Prim "mo:prim";
import { now } = "mo:base/Time";
import Int "mo:base/Int";
import Debug "mo:base/Debug";
import Error "mo:base/Error";
import DAO "./dao";
import fuzz "mo:fuzz";
import Array "mo:base/Array";
import Nat "mo:base/Nat";
import ExTypes "../exchangeTypes";

shared (deployer) persistent actor class testActorB() = this {

  func unwrapSwap(r : ExTypes.SwapResult) : Text { switch (r) { case (#Ok(ok)) { "done:" # Nat.toText(ok.amountOut) }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapOrder(r : ExTypes.OrderResult) : Text { switch (r) { case (#Ok(ok)) { if (ok.accessCode != "") { ok.accessCode } else { "done" } }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapAddLiq(r : ExTypes.AddLiquidityResult) : Text { switch (r) { case (#Ok(ok)) { Nat.toText(ok.liquidityMinted) }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapAddConc(r : ExTypes.AddConcentratedResult) : Text { switch (r) { case (#Ok(ok)) { "concentrated:" # Nat.toText(ok.liquidity) # ":" # Nat.toText(ok.positionId) }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapRemoveConc(r : ExTypes.RemoveConcentratedResult) : Text { switch (r) { case (#Ok(ok)) { "removed:" # Nat.toText(ok.amount0) # ":" # Nat.toText(ok.amount1) }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapRemoveLiq(r : ExTypes.RemoveLiquidityResult) : Text { switch (r) { case (#Ok(ok)) { "Liquidity removed successfully: " # Nat.toText(ok.amount0) # " " # Nat.toText(ok.amount1) }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapClaimFees(r : ExTypes.ClaimFeesResult) : Text { switch (r) { case (#Ok(ok)) { "claimed:" # Nat.toText(ok.fees0) # ":" # Nat.toText(ok.fees1) }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapAction(r : ExTypes.ActionResult) : Text { switch (r) { case (#Ok(msg)) { msg }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapRevoke(r : ExTypes.RevokeResult) : Text { switch (r) { case (#Ok(_)) { "Revoked" }; case (#Err(e)) { unwrapErr(e) } } };
  func unwrapErr(e : ExTypes.ExchangeError) : Text { switch (e) { case (#NotAuthorized) { "Not authorized" }; case (#Banned) { "Banned" }; case (#InvalidInput(t)) { t }; case (#TokenNotAccepted(t)) { t }; case (#TokenPaused(t)) { t }; case (#InsufficientFunds(t)) { t }; case (#PoolNotFound(t)) { t }; case (#SlippageExceeded(s)) { "Slippage" }; case (#RouteFailed(r)) { "Route failed" }; case (#OrderNotFound(t)) { t }; case (#ExchangeFrozen) { "Frozen" }; case (#TransferFailed(t)) { t }; case (#SystemError(t)) { t } } };
  transient let {
    natToNat64;
    nat64ToNat;
    intToNat64Wrap;
    nat8ToNat;
    natToNat8;
    nat64ToInt64;
  } = Prim;
  transient let Fuzz = fuzz.Fuzz();
  transient let allUsers = ["aanaa-xaaaa-aaaah-aaeiq-cai", "qtooy-2yaaa-aaaaq-aabvq-cai", "hhaaz-2aaaa-aaaaq-aacla-cai"];

  transient let actorPrincipalText = "qtooy-2yaaa-aaaaq-aabvq-cai";
  transient let actorPrincipal = Principal.fromText(actorPrincipalText);

  transient let actorAccount = {
    account = Utils.principalToAccount(actorPrincipal);
  };
  transient let TACOtoken = actor ("csyra-haaaa-aaaaq-aacva-cai") : ICRC1.FullInterface;
  transient let dao = actor ("hjcnr-bqaaa-aaaaq-aacka-cai") : DAO.Self;
  transient let actorAccountText = {
    account = Utils.accountToText(Utils.principalToAccount(actorPrincipal));
  };

  transient let exchangePrincipal = Principal.fromText("qbnpl-laaaa-aaaan-q52aq-cai");
  transient let exchange = actor ("qioex-5iaaa-aaaan-q52ba-cai") : Exchange.Self;

  transient let icp = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : ICPLedger.Interface;
  transient let icrcA = actor ("mxzaz-hqaaa-aaaar-qaada-cai") : ICRC1.FullInterface;
  transient let icrcB = actor ("zxeu2-7aaaa-aaaaq-aaafa-cai") : ICRC1.FullInterface;
  var add : Nat64 = 0;
  public func claimFees() : async () {
    ignore await exchange.claimFeesReferrer();
  };

  public func addLiquidity(
    token1 : Text,
    token2 : Text,
    amount1 : Nat,
    amount2 : Nat,
    block1 : Nat,
    block2 : Nat,
  ) : async Text {

    // Then call the exchange's addLiquidity function
    unwrapAddLiq(await exchange.addLiquidity(token1, token2, amount1, amount2, block1, block2));
  };

  public func getUserTrades() : async [{
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
    accesscode : Text;
    strictlyOTC : Bool;
  }] {
    await exchange.getUserTrades();
  };

  public func getUserPreviousTrades(token1 : Text, token2 : Text) : async [{
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
    await exchange.getUserPreviousTrades(token1, token2);
  };

  public func removeLiquidity(
    token1 : Text,
    token2 : Text,
    liquidity : Nat,
  ) : async Text {
    // Call the exchange's removeLiquidity function
    let result = unwrapRemoveLiq(await exchange.removeLiquidity(token1, token2, liquidity));
    result;
  };
  public func recoverUnprocessedTokens(
    transactions : [(Text, Nat, Nat)]
  ) : async [(Text, Nat, Bool)] {

    let results = Buffer.Buffer<(Text, Nat, Bool)>(transactions.size());

    for ((identifier, block, amount) in transactions.vals()) {
      let tType : { #ICP; #ICRC12; #ICRC3 } = if (identifier == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
        #ICP;
      } else { #ICRC12 };
      let recoveryResult = await exchange.recoverWronglysent(identifier, block, tType);
      results.add((identifier, amount, recoveryResult));
    };

    return Buffer.toArray(results);
  };

  public func CancelPosition(Secret : Text) : async Text {
    unwrapRevoke(await exchange.revokeTrade(Secret, #Initiator));
  };

  public func getICPbalance() : async Nat {
    nat64ToNat((await icp.account_balance_dfx(actorAccountText)).e8s);
  };

  public func getICRCAbalance() : async Nat {
    await icrcA.icrc1_balance_of({ owner = actorPrincipal; subaccount = null });
  };

  public func getICRCBbalance() : async Nat {
    await icrcB.icrc1_balance_of({ owner = actorPrincipal; subaccount = null });
  };
  public func getTACObalance() : async Nat {
    await TACOtoken.icrc1_balance_of({
      owner = actorPrincipal;
      subaccount = null;
    });
  };

  public func TransferICRCAtoExchange(amount : Nat, fee : Nat, TXnum : Nat) : async Nat {
    var errie = "";
    var Block = 0;
    let transferResult = await icrcA.icrc1_transfer({
      from_subaccount = null;
      to = { owner = exchangePrincipal; subaccount = null };
      amount = ((amount * (10000 +fee)) / 10000) +(TXnum * 10000);
      fee = ?10000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) -add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) { Block := value };
      case (#Err(error)) {

        switch (error) {
          case (#BadFee(_)) {
            errie := "Badfee";
            throw Error.reject(errie);
          };
          case (#InsufficientFunds(_)) {
            errie := "InsufficientFunds";
            throw Error.reject(errie);
          };
          case (#TxCreatedInFuture) {
            errie := "TxCreatedInFuture";
            throw Error.reject(errie);
          };
          case (#TxDuplicate(_)) {
            errie := "TxDuplicate";
            throw Error.reject(errie);
          };
          case (#TxTooOld(_)) {
            errie := "TxTooOld";
            throw Error.reject(errie);
          };
        };
      };
    };
    Block;
  };

  public func TransferICRCBtoExchange(amount : Nat, fee : Nat, TXnum : Nat) : async Nat {
    var Block = 0;
    let transferResult = await icrcB.icrc1_transfer({
      from_subaccount = null;
      to = { owner = exchangePrincipal; subaccount = null };
      amount = ((amount * (10000 +fee)) / 10000) +(TXnum * 10000);
      fee = ?10000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) { Block := value };
    };
    Block;
  };

  public func TransferICRCCtoExchange(amount : Nat, fee : Nat, TXnum : Nat) : async Nat {
    var Block = 0;
    let transferResult = await icrcB.icrc1_transfer({
      from_subaccount = null;
      to = { owner = exchangePrincipal; subaccount = null };
      amount = ((amount * (10000 +fee)) / 10000) +(TXnum * 10000);
      fee = ?10000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) { Block := value };
    };
    Block;
  };

  public func TransferICPtoExchange(amount : Nat, fee : Nat, TXnum : Nat) : async Nat {
    var Block : Nat64 = 0;
    let transferResult = await icp.transfer({
      memo : Nat64 = 0;
      from_subaccount = null;
      to = Principal.toLedgerAccount(exchangePrincipal, null);
      amount = {
        e8s = natToNat64(((amount * (10000 +fee)) / 10000) +(TXnum * 10000));
      };
      fee = { e8s = 10000 };
      created_at_time = ?{ timestamp_nanos = (natToNat64(Int.abs(now())) - add) };
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) { Block := value };
    };
    nat64ToNat(Block);
  };

  public func CreatePrivatePosition(
    Block : Nat,
    amount_sell : Nat,
    amount_init : Nat,
    token_sell_identifier : Text,
    token_init_identifier : Text,
  ) : async Text {
    unwrapOrder(await exchange.addPosition(Block, amount_sell, amount_init, token_sell_identifier, token_init_identifier, false, true, ?"kkk", allUsers[Fuzz.nat.randomRange(0, 2)], false, false));
  };

  public func CreatePublicPosition(
    Block : Nat,
    amount_sell : Nat,
    amount_init : Nat,
    token_sell_identifier : Text,
    token_init_identifier : Text,
  ) : async Text {
    unwrapOrder(await exchange.addPosition(Block, amount_sell, amount_init, token_sell_identifier, token_init_identifier, true, false, ?"kkk", allUsers[Fuzz.nat.randomRange(0, 2)], false, false));
  };

  public func CreatePublicPositionOTC(
    Block : Nat,
    amount_sell : Nat,
    amount_init : Nat,
    token_sell_identifier : Text,
    token_init_identifier : Text,
  ) : async Text {
    unwrapOrder(await exchange.addPosition(Block, amount_sell, amount_init, token_sell_identifier, token_init_identifier, true, false, ?"kkk", allUsers[Fuzz.nat.randomRange(0, 2)], false, true));
  };

  public func acceptPosition(
    Block : Nat,
    Secret : Text,
    amountSelling : Nat,
  ) : async Text {
    unwrapAction(await exchange.FinishSell(natToNat64(Block), Secret, amountSelling));
  };

  public func acceptBatchPositions(
    Block : Nat64,
    Secret : [Text],
    amount_Sell_by_Reactor : [Nat],
    token_sell_identifier : Text,
    token_init_identifier : Text,
  ) : async Text {
    unwrapAction(await exchange.FinishSellBatch(
      Block,
      Secret,
      amount_Sell_by_Reactor,
      token_sell_identifier,
      token_init_identifier,
    ));
  };
  public func voteOnDAO(vote : [{ token : Text; basisPoints : Nat }]) : async () {
    await dao.vote(vote);
  };
  type TransactionType = {
    #Burn;
    #Mint;
    #Vouch : Time;
  };

  type Transaction = {
    txType : TransactionType;
    sentToDAO : [(Text, Nat)];
    sentFromDAO : [(Text, Nat)];
    when : Time;
  };

  type Time = Int;
  public func getDAOTransactions() : async ?{
    transactions : [{
      txType : {
        #Burn;
        #Mint;
        #Vouch : Int;
      };
      sentToDAO : [(Text, Nat)];
      sentFromDAO : [(Text, Nat)];
      when : Time;

    }];
    totalTransactions : Nat;
  } {
    await dao.getUserTransactions();
  };
  public func burnTACO(amount : Nat) : async Nat {
    var errie = "";
    var Block = 0;
    let transferResult = await TACOtoken.icrc1_transfer({
      from_subaccount = null;
      to = {
        owner = Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai");
        subaccount = ?Blob.fromArray([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      };
      amount = amount +70000;
      fee = ?70000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) {
        Block := value;
        ignore await dao.BurnTaco(natToNat64(Block));
      };
      case (#Err(error)) {
        switch (error) {
          case (#BadFee(_)) {
            errie := "Badfee";
            throw Error.reject(errie);
          };
          case (#InsufficientFunds(_)) {
            errie := "InsufficientFunds";
            throw Error.reject(errie);
          };
          case (#TxCreatedInFuture) {
            errie := "TxCreatedInFuture";
            throw Error.reject(errie);
          };
          case (#TxDuplicate(_)) {
            errie := "TxDuplicate";
            throw Error.reject(errie);
          };
          case (#TxTooOld(_)) {
            errie := "TxTooOld";
            throw Error.reject(errie);
          };
        };
      };
    };
    Block;
  };

  public func mintTACO(amount : Nat, a : Bool) : async Nat {
    var Block : Nat64 = 0;
    let transferResult = await icp.transfer({
      memo : Nat64 = 0;
      from_subaccount = null;
      to = Principal.toLedgerAccount(Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai"), ?Blob.fromArray([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]));
      amount = {
        e8s = natToNat64(amount +10000);
      };
      fee = { e8s = 10000 };
      created_at_time = ?{
        timestamp_nanos = (natToNat64(Int.abs(now())) - add);
      };
    });
    add += 1;
    var b = "";
    switch (transferResult) {
      case (#Ok(value)) {
        Block := value;

        if a {
          b := await dao.mintTaco(Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"), Block);
        } else {
          b := await dao.mintTaco(Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"), Block);
        };
      };
    };
    if (b != "Success") {
      throw Error.reject("");
    };
    nat64ToNat(Block);
  };

  public func vouchInSNSstyleAuctionICRCA(amount : Nat) : async Nat {
    var errie = "";
    var Block = 0;
    let transferResult = await icrcA.icrc1_transfer({
      from_subaccount = null;
      to = {
        owner = Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai");
        subaccount = ?Blob.fromArray([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      };
      amount = amount +10000;
      fee = ?10000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) {
        Block := value;
        ignore await dao.vouchSNSstyleAuction(Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"), amount, Block);
      };
      case (#Err(error)) {
        switch (error) {
          case (#BadFee(_)) {
            errie := "Badfee";
            throw Error.reject(errie);
          };
          case (#InsufficientFunds(_)) {
            errie := "InsufficientFunds";
            throw Error.reject(errie);
          };
          case (#TxCreatedInFuture) {
            errie := "TxCreatedInFuture";
            throw Error.reject(errie);
          };
          case (#TxDuplicate(_)) {
            errie := "TxDuplicate";
            throw Error.reject(errie);
          };
          case (#TxTooOld(_)) {
            errie := "TxTooOld";
            throw Error.reject(errie);
          };
        };
      };
    };
    Block;
  };

  public func vouchInSNSstyleAuctionICRCB(amount : Nat) : async Nat {
    var errie = "";
    var Block = 0;
    let transferResult = await icrcB.icrc1_transfer({
      from_subaccount = null;
      to = {
        owner = Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai");
        subaccount = ?Blob.fromArray([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      };
      amount = amount +10000;
      fee = ?10000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) {
        Block := value;
        ignore await dao.vouchSNSstyleAuction(Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai"), amount, Block);
      };
      case (#Err(error)) {
        switch (error) {
          case (#BadFee(_)) {
            errie := "Badfee";
            throw Error.reject(errie);
          };
          case (#InsufficientFunds(_)) {
            errie := "InsufficientFunds";
            throw Error.reject(errie);
          };
          case (#TxCreatedInFuture) {
            errie := "TxCreatedInFuture";
            throw Error.reject(errie);
          };
          case (#TxDuplicate(_)) {
            errie := "TxDuplicate";
            throw Error.reject(errie);
          };
          case (#TxTooOld(_)) {
            errie := "TxTooOld";
            throw Error.reject(errie);
          };
        };
      };
    };
    Block;
  };

  public func createSNSstyleAuction(TACOAmount : Nat, duration : Nat) : async Nat {
    var errie = "";
    var Block = 0;

    let transferResult = await TACOtoken.icrc1_transfer({
      from_subaccount = null;
      to = {
        owner = Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai");
        subaccount = ?Blob.fromArray([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      };
      amount = TACOAmount +70000;
      fee = ?70000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;

    switch (transferResult) {
      case (#Ok(value)) {
        Block := value;
        ignore await dao.createSNSstyleAuction(TACOAmount, duration, Block);
      };
      case (#Err(error)) {
        switch (error) {
          case (#BadFee(_)) {
            errie := "Badfee";
            throw Error.reject(errie);
          };
          case (#InsufficientFunds(_)) {
            errie := "InsufficientFunds";
            throw Error.reject(errie);
          };
          case (#TxCreatedInFuture) {
            errie := "TxCreatedInFuture";
            throw Error.reject(errie);
          };
          case (#TxDuplicate(_)) {
            errie := "TxDuplicate";
            throw Error.reject(errie);
          };
          case (#TxTooOld(_)) {
            errie := "TxTooOld";
            throw Error.reject(errie);
          };
        };
      };
    };
    Block;
  };

  public func vouchInSNSstyleAuctionICRCC(amount : Nat) : async Nat {
    var errie = "";
    var Block = 0;
    let transferResult = await icrcB.icrc1_transfer({
      from_subaccount = ?Blob.fromArray([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      to = {
        owner = Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai");
        subaccount = null;
      };
      amount = amount +10000;
      fee = ?10000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) {
        Block := value;
        ignore await dao.vouchSNSstyleAuction(Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai"), amount, Block);
      };
      case (#Err(error)) {
        switch (error) {
          case (#BadFee(_)) {
            errie := "Badfee";
            throw Error.reject(errie);
          };
          case (#InsufficientFunds(_)) {
            errie := "InsufficientFunds";
            throw Error.reject(errie);
          };
          case (#TxCreatedInFuture) {
            errie := "TxCreatedInFuture";
            throw Error.reject(errie);
          };
          case (#TxDuplicate(_)) {
            errie := "TxDuplicate";
            throw Error.reject(errie);
          };
          case (#TxTooOld(_)) {
            errie := "TxTooOld";
            throw Error.reject(errie);
          };
        };
      };
    };
    Block;
  };

  public func vouchInSNSstyleAuctionICP(amount : Nat) : async Nat {
    var Block : Nat64 = 0;
    let transferResult = await icp.transfer({
      memo : Nat64 = 0;
      from_subaccount = null;
      to = Principal.toLedgerAccount(Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai"), ?Blob.fromArray([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]));
      amount = {
        e8s = natToNat64(amount +10000);
      };
      fee = { e8s = 10000 };
      created_at_time = ?{
        timestamp_nanos = (natToNat64(Int.abs(now())) - add);
      };
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) {
        Block := value;
        ignore await dao.vouchSNSstyleAuction(Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"), amount, nat64ToNat(Block));
      };
    };
    nat64ToNat(Block);
  };
  public func recoverUnprocessedTokensDAO(
    transactions : [(Text, Nat, Nat)]
  ) : async [(Text, Nat, Bool)] {

    let results = Buffer.Buffer<(Text, Nat, Bool)>(transactions.size());

    for ((identifier, block, amount) in transactions.vals()) {
      let tType : { #ICP; #ICRC12; #ICRC3 } = if (identifier == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
        #ICP;
      } else { #ICRC12 };
      let recoveryResult = await dao.recoverWronglysent(Principal.fromText(identifier), block, tType, true);
      results.add((identifier, amount, recoveryResult));
    };

    return Buffer.toArray(results);
  };
  public func TransferTACOtoDAO(amount : Nat) : async Nat64 {
    var Block = 0;
    let transferResult = await TACOtoken.icrc1_transfer({
      from_subaccount = null;
      to = {
        owner = Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai");
        subaccount = ?Blob.fromArray([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      };
      amount = amount + 70000;
      fee = ?70000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;

    switch (transferResult) {
      case (#Ok(value)) { Block := value };
      case (#Err(error)) {
        switch (error) {
          case (#BadFee(_)) { throw Error.reject("BadFee") };
          case (#InsufficientFunds(_)) {
            throw Error.reject("InsufficientFunds");
          };
          case (#TxCreatedInFuture) { throw Error.reject("TxCreatedInFuture") };
          case (#TxDuplicate(_)) { throw Error.reject("TxDuplicate") };
          case (#TxTooOld(_)) { throw Error.reject("TxTooOld") };
        };
      };
    };
    natToNat64(Block);
  };

  public func addTACOforMintBurn(block : Nat64) : async Bool {
    await dao.addTACOforMintBurn(block);
  };

  let ckusdc = actor ("xevnm-gaaaa-aaaar-qafnq-cai") : ICRC1.FullInterface;

  public func getCKUSDCbalance() : async Nat {
    await ckusdc.icrc1_balance_of({ owner = actorPrincipal; subaccount = null });
  };

  public func TransferCKUSDCtoExchange(amount : Nat, fee : Nat, TXnum : Nat) : async Nat {
    var Block = 0;
    let transferResult = await ckusdc.icrc1_transfer({
      from_subaccount = null;
      to = { owner = exchangePrincipal; subaccount = null };
      amount = ((amount * (10000 + fee)) / 10000) + (TXnum * 10000);
      fee = ?10000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) { Block := value };
      case (#Err(error)) {
        throw Error.reject("CKUSDC transfer failed: " # debug_show (error));
      };
    };
    Block;
  };

  public func vouchInSNSstyleAuctionCKUSDC(amount : Nat) : async Nat {
    var Block = 0;
    let transferResult = await ckusdc.icrc1_transfer({
      from_subaccount = null;
      to = {
        owner = Principal.fromText("ar2zl-5qaaa-aaaan-qavoa-cai");
        subaccount = ?Blob.fromArray([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
      };
      amount = amount;
      fee = ?10000;
      memo = null;
      created_at_time = ?(natToNat64(Int.abs(now())) - add);
    });
    add += 1;
    switch (transferResult) {
      case (#Ok(value)) {
        Block := value;
        ignore await dao.vouchSNSstyleAuction(Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai"), amount, Block);
      };
      case (#Err(error)) {
        throw Error.reject("CKUSDC vouching failed: " # debug_show (error));
      };
    };
    Block;
  };

  public func swapMultiHop(
    tokenIn : Text, tokenOut : Text, amountIn : Nat,
    route : [{ tokenIn : Text; tokenOut : Text }],
    minAmountOut : Nat, Block : Nat,
  ) : async Text {
    unwrapSwap(await exchange.swapMultiHop(tokenIn, tokenOut, amountIn, route, minAmountOut, Block));
  };

  public func swapSplitRoutes(
    tokenIn : Text, tokenOut : Text,
    splits : [{ amountIn : Nat; route : [{ tokenIn : Text; tokenOut : Text }]; minLegOut : Nat }],
    minAmountOut : Nat, Block : Nat,
  ) : async Text {
    unwrapSwap(await exchange.swapSplitRoutes(tokenIn, tokenOut, splits, minAmountOut, Block));
  };

  public func claimLPFees(token0 : Text, token1 : Text) : async Text {
    unwrapClaimFees(await exchange.claimLPFees(token0, token1));
  };

  public func getUserLiquidityDetailed() : async [{
    token0 : Text; token1 : Text; liquidity : Nat;
    token0Amount : Nat; token1Amount : Nat; shareOfPool : Float;
    fee0 : Nat; fee1 : Nat;
  }] {
    await exchange.getUserLiquidityDetailed();
  };

  public func addConcentratedLiquidity(
    t0 : Text, t1 : Text, a0 : Nat, a1 : Nat,
    pL : Nat, pU : Nat, b0 : Nat, b1 : Nat,
  ) : async Text {
    unwrapAddConc(await exchange.addConcentratedLiquidity(t0, t1, a0, a1, pL, pU, b0, b1));
  };

  public func removeConcentratedLiquidity(
    t0 : Text, t1 : Text, posId : Nat, liq : Nat,
  ) : async Text {
    unwrapRemoveConc(await exchange.removeConcentratedLiquidity(t0, t1, posId, liq));
  };

  public func getUserConcentratedPositions() : async [{
    positionId : Nat; token0 : Text; token1 : Text;
    liquidity : Nat; ratioLower : Nat; ratioUpper : Nat;
    lastFeeGrowth0 : Nat; lastFeeGrowth1 : Nat; lastUpdateTime : Int;
  }] {
    await exchange.getUserConcentratedPositions();
  };
};
