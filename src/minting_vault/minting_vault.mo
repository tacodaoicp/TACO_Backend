import Map "mo:map/Map";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Result "mo:base/Result";
import Vector "mo:vector";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Error "mo:base/Error";
import Blob "mo:base/Blob";
import Float "mo:base/Float";
import ICRC1 "mo:icrc1/ICRC1";
import ICRC2 "../helper/icrc.types";
import ICRC3 "mo:icrc3-mo/service";
import Time "mo:base/Time";
import Ledger "../helper/Ledger";
import LedgerType "mo:ledger-types";
import { setTimer; cancelTimer } = "mo:base/Timer";
import MintingVaultTypes "../minting_vault/minting_vault_types";
import SpamProtection "../helper/spam_protection";
import Nat8 "mo:base/Nat8";
import KongSwap "../swap/kong_swap";
import ICPSwap "../swap/icp_swap";
import swaptypes "../swap/swap_types";
import TreasuryTypes "../treasury/treasury_types";
import Debug "mo:base/Debug";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Logger "../helper/logger";
import DAO_types "../DAO_backend/dao_types"

actor MintingVaultDAO {
  // Type definitions
  type Token = Text;
  type Decimals = MintingVaultTypes.Decimals;
  type TransferFee = MintingVaultTypes.TransferFee;
  type Holdings = MintingVaultTypes.Holdings;

  type SwapError = MintingVaultTypes.SwapError;

  type SwapResult = MintingVaultTypes.SwapResult;

  // Add to type definitions
  type TokenAllocation = MintingVaultTypes.TokenAllocation;

  // Transfer types
  type TransferRecipient = MintingVaultTypes.TransferRecipient;

  type TransferResultICP = MintingVaultTypes.TransferResultICP;

  type SyncError = MintingVaultTypes.SyncError;

  type BlockData = MintingVaultTypes.BlockData;

  type TransferResult = MintingVaultTypes.TransferResult;

  type TokenDetails = MintingVaultTypes.TokenDetails;

  type PricePoint = MintingVaultTypes.PricePoint;

  type UpdateConfig = MintingVaultTypes.UpdateConfig;

  type LogLevel = DAO_types.LogLevel;

  type LogEntry = DAO_types.LogEntry;

  // Error types
  type AddTokenError = MintingVaultTypes.AddTokenError;

  let { phash; thash } = Map;

  let hashpp = TreasuryTypes.hashpp;

  let spamGuard = SpamProtection.SpamGuard();

  let logger = Logger.Logger();

  //stable var DAOprincipal = Principal.fromText("ywhqf-eyaaa-aaaad-qg6tq-cai");
  stable var DAOprincipal = Principal.fromText("vxqw7-iqaaa-aaaan-qzziq-cai");

  //stable var TreasuryPrincipal = Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai");
  stable var TreasuryPrincipal = Principal.fromText("v6t5d-6yaaa-aaaan-qzzja-cai");

  stable var TreasurySubaccount = Blob.fromArray([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

  // Admin other than controller that has access to logs
  stable var logAdmin = Principal.fromText("d7zib-qo5mr-qzmpb-dtyof-l7yiu-pu52k-wk7ng-cbm3n-ffmys-crbkz-nae");

  let TACOaddress = "csyra-haaaa-aaaaq-aacva-cai"; //change in production

  let redemptionAccountID = Principal.toLedgerAccount(TreasuryPrincipal, ?TreasurySubaccount);

  // Queue for failed transfers to retry later
  stable let transferQueue = Vector.new<(TransferRecipient, Nat, Principal, Nat8)>();

  // Map that saves all the blocks that have been used for transactions. So no-one can  make 2 orders with 1 transfer.
  stable let BlocksDone = Map.new<Text, Int>();

  // Map to track token details
  stable let tokenDetailsMap = Map.new<Principal, TokenDetails>();

  stable let tokenBalanceTemp = Map.new<Principal, Int>();

  stable let targetAllocations = Map.new<Principal, Nat>();

  // Exchange pool data
  stable var ICPswapPools = Map.new<(Principal, Principal), swaptypes.PoolData>();

  stable var sns_governance_canister_id : ?Principal = null;

  // Configuration variables
  stable var minPremiumPercent : Float = 1.0;
  stable var maxPremiumPercent : Float = 3.0;
  stable var balanceUpdateIntervalNS : Int = 15 * 60 * 1_000_000_000; // 15 minutes
  stable var blockCleanupIntervalNS : Int = 24 * 60 * 60 * 1_000_000_000; // 24 hours
  stable var maxSlippageBasisPoints : Nat = 50; // 0.5% default max slippage (configurable)
  stable var PRICE_HISTORY_WINDOW : Int = 2 * 60 * 60 * 1_000_000_000; // 2 hours
  stable var minSwapValueUSD : Float = 5.0;

  // Flag to control whether swapping is enabled
  stable var swappingEnabled : Bool = true;

  // Timer IDs for cleanup and sync
  stable var balanceUpdateTimerId : ?Nat = null;
  stable var blockCleanupTimerId : ?Nat = null;

  let dao = actor (Principal.toText(DAOprincipal)) : actor {
    getTokenDetails : shared () -> async [(Principal, TokenDetails)];
    getAggregateAllocation : shared () -> async [(Principal, Nat)];
  };

  let treasury = actor (Principal.toText(TreasuryPrincipal)) : actor {
    receiveTransferTasks : shared ([(TransferRecipient, Nat, Principal, Nat8)], Bool) -> async (Bool, ?[(Principal, Nat64)]);
    getTokenPriceHistory : shared ([Principal]) -> async ?[(Principal, [PricePoint])];
  };

  /**
   * Synchronize token details with DAO
   *
   * Updates token status from the DAO including:
   * - Active/Inactive status
   * - Paused/Unpaused state
   */
  public shared ({ caller }) func syncTokenDetailsFromDAO(tokenDetails : [(Principal, TokenDetails)]) : async Result.Result<Text, SyncError> {
    logger.info("Sync", "Token details sync request from " # Principal.toText(caller), "syncTokenDetailsFromDAO");

    if (caller != DAOprincipal) {
      logger.warn("Sync", "Unauthorized sync attempt from " # Principal.toText(caller), "syncTokenDetailsFromDAO");
      return #err(#NotDAO);
    };

    let startTime = Time.now();

    var activeTokenCount = 0;
    var pausedTokenCount = 0;
    var updatedTokenCount = 0;
    var newTokenCount = 0;

    for (tokenDetail in tokenDetails.vals()) {
      let token = tokenDetail.0;
      let details = tokenDetail.1;

      // Check if token already exists in our map
      switch (Map.get(tokenDetailsMap, phash, token)) {
        case (null) {
          newTokenCount += 1;
        };
        case (?_) {
          updatedTokenCount += 1;
        };
      };

      // Count active and paused tokens
      if (details.Active) {
        activeTokenCount += 1;
        if (details.isPaused) {
          pausedTokenCount += 1;
        };
      };

      // Set new details
      Map.set(
        tokenDetailsMap,
        phash,
        token,
        details,
      );
    };

    let executionTime = Time.now() - startTime;
    logger.info(
      "Sync",
      "Token details sync completed: "
      # Nat.toText(tokenDetails.size()) # " tokens received ("
      # Nat.toText(newTokenCount) # " new, "
      # Nat.toText(updatedTokenCount) # " updated, "
      # Nat.toText(activeTokenCount) # " active, "
      # Nat.toText(pausedTokenCount) # " paused) in "
      # Int.toText(executionTime / 1_000_000) # "ms",
      "syncTokenDetailsFromDAO",
    );

    #ok("Token details synced successfully");
  };

  private func getBlockData(token_identifier : Text, block : Nat, tType : { #ICP; #ICRC12; #ICRC3 }) : async* BlockData {

    if (token_identifier == "ryjl3-tyaaa-aaaaa-aaaba-cai") {
      let t = actor ("ryjl3-tyaaa-aaaaa-aaaba-cai") : actor {
        query_blocks : shared query { start : Nat64; length : Nat64 } -> async (LedgerType.QueryBlocksResponse);
      };
      let response = await t.query_blocks({
        start = Nat64.fromNat(block);
        length = 1;
      });

      if (response.blocks.size() > 0) {
        #ICP(response);
      } else {
        // Handle archived blocks
        switch (response.archived_blocks) {
          case (archived_blocks) {
            for (archive in archived_blocks.vals()) {
              if (block >= Nat64.toNat(archive.start) and block < Nat64.toNat(archive.start + archive.length)) {
                let archivedResult = await archived_blocks[0].callback({
                  start = Nat64.fromNat(block);
                  length = 1;
                });
                switch (archivedResult) {
                  case (#Ok(blockRange)) {
                    return #ICP({
                      certificate = null;
                      blocks = blockRange.blocks;
                      chain_length = 0;
                      first_block_index = Nat64.fromNat(block);
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
              first_block_index = Nat64.fromNat(block);
              archived_blocks = [];
            });
          };
        };
      };
    } else if (tType == #ICRC12) {
      let t = actor (token_identifier) : actor {
        get_transactions : shared query (ICRC2.GetTransactionsRequest) -> async (ICRC2.GetTransactionsResponse);
      };
      let ab = await t.get_transactions({ length = 1; start = block });
      if (block > (ab.first_index + ab.log_length)) {
        throw Error.reject("Block is in future");
      };
      if (block >= ab.first_index and block < (ab.first_index + ab.log_length)) {
        #ICRC12(ab.transactions);
      } else {
        #ICRC12((await ab.archived_transactions[0].callback({ length = 1; start = block })).transactions);
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

  // Verify block and return received amount
  private func verifyBlock(blockNumber : Nat, tokenAddress : Text, sender : Principal) : async* Nat {
    if test Debug.print("verifyBlock: Starting verification for block " # Nat.toText(blockNumber) # " of token " # tokenAddress # " from sender " # Principal.toText(sender));

    let tokenOpt = Map.get(tokenDetailsMap, phash, Principal.fromText(tokenAddress));
    switch (tokenOpt) {
      case (?tokenDetails) {
        if test Debug.print("verifyBlock: Found token details for " # tokenAddress);
        try {
          if test Debug.print("verifyBlock: Fetching block data...");
          let blockData : BlockData = await* getBlockData(tokenAddress, blockNumber, tokenDetails.tokenType);

          let timestamp = getTimestamp(blockData);
          if test Debug.print("verifyBlock: Block timestamp: " # Int.toText(timestamp));

          if (timestamp < Int.abs(Time.now() - 21 * 24 * 60 * 60 * 1_000_000_000)) {
            if test Debug.print("verifyBlock: Block too old (over 21 days), rejecting");
            return 0;
          };

          // Process block data based on token type to verify receipt by the minting vault
          switch (blockData) {
            case (#ICP(blocks)) {
              if test Debug.print("verifyBlock: Processing ICP block data with " # Nat.toText(blocks.blocks.size()) # " blocks");
              for (block in blocks.blocks.vals()) {
                switch (block.transaction.operation) {
                  case (? #Transfer({ amount; from; to })) {
                    // Check if transfer was TO the minting vault and FROM the sending user
                    let senderAccount = Principal.toLedgerAccount(sender, null);
                    if test Debug.print("verifyBlock: Checking ICP transfer - from: " # debug_show (Blob.fromArray(from)) # ", to: " # debug_show (Blob.fromArray(to)));
                    if (Blob.fromArray(from) == senderAccount and Blob.fromArray(to) == redemptionAccountID) {
                      if test Debug.print("verifyBlock: Valid ICP transfer verified, amount: " # Nat64.toText(amount.e8s) # " e8s");
                      return Nat64.toNat(amount.e8s);
                    } else if (Blob.fromArray(from) != senderAccount or Blob.fromArray(to) != redemptionAccountID) {
                      if test Debug.print("verifyBlock: Invalid ICP transfer, removing from BlocksDone");
                      Map.delete(BlocksDone, thash, tokenAddress # ":" # Nat.toText(blockNumber));
                      return 0;
                    };
                  };
                  case (_) {
                    if test Debug.print("verifyBlock: ICP operation not a transfer, skipping");
                  };
                };
              };
            };
            case (#ICRC12(transactions)) {
              if test Debug.print("verifyBlock: Processing ICRC1/2 transaction data with " # Nat.toText(transactions.size()) # " transactions");

              switch (transactions[0].transfer) {
                case (?{ to; from; amount }) {
                  // Check if transfer was TO the minting vault and FROM the sending user
                  if test Debug.print("verifyBlock: Checking ICRC1/2 transfer - from: " # Principal.toText(from.owner) # ", to: " # Principal.toText(to.owner));
                  if (
                    from.owner == sender and from.subaccount == null and
                    to.owner == TreasuryPrincipal and to.subaccount == ?Blob.toArray(TreasurySubaccount)
                  ) {
                    if test Debug.print("verifyBlock: Valid ICRC1/2 transfer verified, amount: " # Nat.toText(amount));
                    return amount;
                  } else if (from.owner != sender or from.subaccount != null or to.owner != TreasuryPrincipal or to.subaccount != ?Blob.toArray(TreasurySubaccount)) {
                    if test Debug.print("verifyBlock: Invalid ICRC1/2 transfer, removing from BlocksDone");
                    Map.delete(BlocksDone, thash, tokenAddress # ":" # Nat.toText(blockNumber));
                    return 0;
                  };
                };
                case _ {
                  if test Debug.print("verifyBlock: No transfer found in ICRC1/2 transaction");
                  return 0;
                };
              };
            };
            case (#ICRC3(result)) {
              if test Debug.print("verifyBlock: Processing ICRC3 block data with " # Nat.toText(result.blocks.size()) # " blocks");
              for (block in result.blocks.vals()) {
                switch (block.block) {
                  case (#Map(entries)) {
                    if test Debug.print("verifyBlock: Processing ICRC3 block entries: " # Nat.toText(entries.size()) # " entries");
                    var to : ?{ owner : Principal; subaccount : ?Blob } = null;
                    var from : ?{ owner : Principal; subaccount : ?Blob } = null;
                    var howMuchReceived : ?Nat = null;
                    var fee : ?Nat = null;

                    for ((key, value) in entries.vals()) {
                      if test Debug.print("verifyBlock: Processing ICRC3 entry: " # key);
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
                                          case (#Blob(subaccount)) {
                                            ?subaccount;
                                          };
                                          case _ { null };
                                        };
                                      } else {
                                        null // Default subaccount when only principal is provided
                                      };
                                    };
                                    if test Debug.print("verifyBlock: ICRC3 'to' parsed as principal: " # Principal.toText(Principal.fromBlob(owner)));
                                  };
                                  case _ {
                                    if test Debug.print("verifyBlock: ICRC3 'to' first item not a blob");
                                  };
                                };
                              };
                            };
                            case (#Blob(owner)) {
                              to := ?{
                                owner = Principal.fromBlob(owner);
                                subaccount = null;
                              };
                              if test Debug.print("verifyBlock: ICRC3 'to' parsed as blob principal: " # Principal.toText(Principal.fromBlob(owner)));
                            };
                            case _ {
                              if test Debug.print("verifyBlock: ICRC3 'to' not in expected format");
                            };
                          };
                        };
                        case "fee" {
                          switch (value) {
                            case (#Nat(f)) {
                              fee := ?f;
                              if test Debug.print("verifyBlock: ICRC3 fee parsed: " # Nat.toText(f));
                            };
                            case (#Int(f)) {
                              fee := ?Int.abs(f);
                              if test Debug.print("verifyBlock: ICRC3 fee parsed from Int: " # Int.toText(f));
                            };
                            case _ {
                              if test Debug.print("verifyBlock: ICRC3 fee not in expected format");
                            };
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
                                          case (#Blob(subaccount)) {
                                            ?subaccount;
                                          };
                                          case _ { null };
                                        };
                                      } else {
                                        null;
                                      };
                                    };
                                    if test Debug.print("verifyBlock: ICRC3 'from' parsed as principal: " # Principal.toText(Principal.fromBlob(owner)));
                                  };
                                  case _ {
                                    if test Debug.print("verifyBlock: ICRC3 'from' first item not a blob");
                                  };
                                };
                              };
                            };
                            case _ {
                              if test Debug.print("verifyBlock: ICRC3 'from' not in expected format");
                            };
                          };
                        };
                        case "amt" {
                          switch (value) {
                            case (#Nat(amt)) {
                              howMuchReceived := ?amt;
                              if test Debug.print("verifyBlock: ICRC3 amount parsed: " # Nat.toText(amt));
                            };
                            case (#Int(amt)) {
                              howMuchReceived := ?Int.abs(amt);
                              if test Debug.print("verifyBlock: ICRC3 amount parsed from Int: " # Int.toText(amt));
                            };
                            case _ {
                              if test Debug.print("verifyBlock: ICRC3 amount not in expected format");
                            };
                          };
                        };
                        case _ {
                          if test Debug.print("verifyBlock: Skipping ICRC3 entry: " # key);
                        };
                      };
                    };

                    switch (to, from, howMuchReceived) {
                      case (?to, ?from, ?amount) {
                        if test Debug.print("verifyBlock: Validating ICRC3 transfer - from: " # Principal.toText(from.owner) # ", to: " # Principal.toText(to.owner) # ", amount: " # Nat.toText(amount));
                        if (
                          from.owner == sender and from.subaccount == null and
                          to.owner == TreasuryPrincipal and to.subaccount == ?TreasurySubaccount
                        ) {
                          if test Debug.print("verifyBlock: Valid ICRC3 transfer verified, amount: " # Nat.toText(amount));
                          return amount;
                        } else if (from.owner != sender or from.subaccount != null or to.owner != TreasuryPrincipal or to.subaccount != ?TreasurySubaccount) {
                          if test Debug.print("verifyBlock: Invalid ICRC3 transfer, removing from BlocksDone");
                          Map.delete(BlocksDone, thash, tokenAddress # ":" # Nat.toText(blockNumber));
                          return 0;
                        };
                      };
                      case _ {
                        if test Debug.print("verifyBlock: ICRC3 transfer incomplete, missing to/from/amount");
                      };
                    };
                  };
                  case _ {
                    if test Debug.print("verifyBlock: ICRC3 block not in Map format");
                  };
                };
              };
            };
          };
        } catch (e) {
          if test Debug.print("verifyBlock: Exception during verification: " # Error.message(e));
          return 0;
        };
      };
      case null {
        if test Debug.print("verifyBlock: Token details not found for " # tokenAddress);
        return 0;
      };
    };
    if test Debug.print("verifyBlock: No valid transfer found in block");
    0;
  };

  // function to extract when an transaction was done. If older than 21 days we dont accept it.
  func getTimestamp(blockData : BlockData) : Int {
    let optTimestamp = switch blockData {
      case (#ICP(data)) {
        ?data.blocks[0].timestamp.timestamp_nanos;
      };
      case (#ICRC12(transactions)) {
        switch (transactions[0].transfer) {
          case (?{ created_at_time }) { created_at_time };
          case null { null };
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
            foundTimestamp;
          };
          case _ { null };
        };
      };
    };

    let timestamp = switch optTimestamp {
      case (?t) { Int.abs(Nat64.toNat(t)) };
      case null { 0 };
    };

    timestamp;
  };

  // Swap function for users to exchange tokens for TACO
  public shared ({ caller }) func swapTokenForTaco(token : Principal, block : Nat, minimumReceive : Nat) : async Result.Result<SwapResult, Text> {
    // Spam protection check
    let allowed = isAllowed(caller);
    if (not allowed and not test) {
      return #err("Rate limit exceeded");
    };
    if (not swappingEnabled) {
      return #err("Swapping is currently disabled");
    };

    let blockKey = Principal.toText(token) # ":" # Nat.toText(block);

    // Check if block already processed
    if (Map.has(BlocksDone, thash, blockKey)) {
      return #err("Block already processed, blockKey: " # blockKey);
    };

    // Mark block as processed immediately to prevent replay attacks
    // This needs to be done before any awaits, then deleted if needed
    Map.set(BlocksDone, thash, blockKey, Time.now());

    // Check token is active and not paused
    let tokenDetails = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) {
        if (not details.Active or details.isPaused) {
          // Delete block entry since we're not using it
          Map.delete(BlocksDone, thash, blockKey);
          return #err("Token is not active or is paused");
        };
        details;
      };
      case null {
        // Delete block entry since we're not using it
        Map.delete(BlocksDone, thash, blockKey);
        return #err("Token not found");
      };
    };

    // Get TACO token details
    let tacoDetails = switch (Map.get(tokenDetailsMap, phash, Principal.fromText(TACOaddress))) {
      case (?details) {
        details;
      };
      case null {
        // Delete block entry since we're not using it
        Map.delete(BlocksDone, thash, blockKey);
        return #err("TACO token not found");
      };
    };

    // Verify the transaction and get received amount
    let receivedAmount = try {
      await* verifyBlock(block, Principal.toText(token), caller);
    } catch (e) {
      Map.delete(BlocksDone, thash, blockKey);
      return #err("Error verifying block: " # Error.message(e));
    };

    if (receivedAmount == 0) {
      // Delete block entry since transaction is invalid
      return #err("Block verification failed");
    };

    logger.info(
      "Swap",
      "Block verification successful for token " # Principal.toText(token)
      # ", block " # Nat.toText(block) # ", amount " # Nat.toText(receivedAmount),
      "swapTokenForTaco",
    );

    // Prepare for transfer queue
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Principal, Nat8)>();

    // Check if the received amount meets minimum USD value requirement
    let tokenUsdPrice = tokenDetails.priceInUSD;
    let receivedValueUSD = Float.fromInt(receivedAmount) * tokenUsdPrice / Float.fromInt(10 ** tokenDetails.tokenDecimals);

    if (receivedValueUSD < minSwapValueUSD and not test) {
      logger.warn(
        "Swap",
        "Received amount worth $" # Float.toText(receivedValueUSD) #
        " is below minimum threshold of $" # Float.toText(minSwapValueUSD),
        "swapTokenForTaco",
      );

      // Delete block entry since we're rejecting this swap
      Map.delete(BlocksDone, thash, blockKey);

      if (receivedAmount > 3 * tokenDetails.tokenTransferFee) {
        Vector.add(
          tempTransferQueueLocal,
          (#principal(caller), receivedAmount - (3 * tokenDetails.tokenTransferFee), token, Nat8.fromNat(1)),
        );
        logger.info(
          "Swap",
          "Returning " # Nat.toText(receivedAmount - (3 * tokenDetails.tokenTransferFee)) #
          " tokens to caller due to minimum value requirement",
          "swapTokenForTaco",
        );
      };

      if ((try { (await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Principal, Nat8)>(tempTransferQueueLocal), false)).0 } catch (_) { false })) {
        logger.info("Swap", "Token return queued successfully", "swapTokenForTaco");
      } else {
        Vector.addFromIter(transferQueue, Vector.vals(tempTransferQueueLocal));
        logger.warn("Swap", "Failed to queue token return via treasury, added to local transfer queue", "swapTokenForTaco");
      };

      return #err("Swap amount below minimum threshold of $" # Float.toText(minSwapValueUSD));
    };

    // Get updated allocations from DAO
    await syncAllocationsFromDAO();
    logger.info("Swap", "DAO allocations synced for swap operation", "swapTokenForTaco");

    // Get quotes from DEXes for current prices
    let (tokenPrice, tacoPrice) = await* getBestPrices(token, Principal.fromText(TACOaddress));
    logger.info(
      "Swap",
      "Price data retrieved - token: " # Nat.toText(tokenPrice)
      # " ICP, TACO: " # Nat.toText(tacoPrice) # " ICP",
      "swapTokenForTaco",
    );

    if (tokenPrice == 0 or tacoPrice == 0) {
      logger.error(
        "Swap",
        "Failed to determine valid prices for token "
        # Principal.toText(token),
        "swapTokenForTaco",
      );

      // Return tokens if we failed to determine prices
      if (receivedAmount > tokenDetails.tokenTransferFee) {
        Vector.add(
          tempTransferQueueLocal,
          (#principal(caller), receivedAmount - tokenDetails.tokenTransferFee, token, Nat8.fromNat(1)),
        );
        logger.info(
          "Swap",
          "Returning " # Nat.toText(receivedAmount - tokenDetails.tokenTransferFee)
          # " tokens to caller due to price determination failure",
          "swapTokenForTaco",
        );
      };

      if ((try { (await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Principal, Nat8)>(tempTransferQueueLocal), false)).0 } catch (_) { false })) {
        logger.info("Swap", "Token return queued successfully", "swapTokenForTaco");
      } else {
        Vector.addFromIter(transferQueue, Vector.vals(tempTransferQueueLocal));
        logger.warn("Swap", "Failed to queue token return via treasury, added to local transfer queue", "swapTokenForTaco");
      };

      return #err("Failed to determine token prices");
    };

    // Calculate total value of treasury excluding paused tokens
    var activeTreasuryTotalValue = 0;
    var totalTargetBasisPoints = 0;

    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and not details.isPaused) {
        let balance = switch (Map.get(tokenBalanceTemp, phash, principal)) {
          case (?tempBalance) { details.balance + Int.abs(tempBalance) };
          case null { details.balance };
        };
        activeTreasuryTotalValue += (balance * details.priceInICP) / (10 ** details.tokenDecimals);

        // Sum up target basis points for active, non-paused tokens
        switch (Map.get(targetAllocations, phash, principal)) {
          case (?basisPoints) { totalTargetBasisPoints += basisPoints };
          case null {};
        };
      };
    };

    logger.info(
      "Swap",
      "Treasury valuation: " # Nat.toText(activeTreasuryTotalValue)
      # " ICP, target basis points total: " # Nat.toText(totalTargetBasisPoints),
      "swapTokenForTaco",
    );

    // Get current Treasury balance for this token (including pending transfers)
    let currentBalance = switch (Map.get(tokenBalanceTemp, phash, token)) {
      case (?tempBalance) { tokenDetails.balance + tempBalance };
      case null { tokenDetails.balance };
    };

    // Calculate current allocation in basis points and value in ICP
    let currentValueInICP = (currentBalance * tokenPrice) / (10 ** tokenDetails.tokenDecimals);
    let receivedValueICP = (receivedAmount * tokenPrice) / (10 ** tokenDetails.tokenDecimals);

    let currentBasisPoints = if (activeTreasuryTotalValue > 0) {
      let a = (currentValueInICP * 10000) / activeTreasuryTotalValue;
      if (a > 0) a else (if test 1 else 0);
    } else {
      if test 1 else 0;
    };

    // Get target basis points
    let targetBasisPoints = switch (Map.get(targetAllocations, phash, token)) {
      case (?basisPoints) {
        if (test and basisPoints <= currentBasisPoints) Nat.min(10000, Int.abs(currentBasisPoints) +100) else basisPoints;
      };
      case null {
        if test Nat.min(10000, Int.abs(currentBasisPoints) +100) else 0;
      }; // No allocation target
    };

    logger.info(
      "Swap",
      "Token " # Principal.toText(token)
      # " - current: " # Int.toText(currentBasisPoints) # " bps, target: "
      # Nat.toText(targetBasisPoints) # " bps",
      "swapTokenForTaco",
    );

    // Check if token has a target allocation
    if (targetBasisPoints == 0 or (currentBasisPoints == 0 and not test)) {
      logger.warn(
        "Swap",
        "Token " # Principal.toText(token)
        # " has no target allocation or current allocation",
        "swapTokenForTaco",
      );

      // Return tokens if the token has no target allocation
      if (receivedAmount > tokenDetails.tokenTransferFee) {
        Vector.add(
          tempTransferQueueLocal,
          (#principal(caller), receivedAmount - tokenDetails.tokenTransferFee, token, Nat8.fromNat(1)),
        );
        logger.info(
          "Swap",
          "Returning " # Nat.toText(receivedAmount - tokenDetails.tokenTransferFee)
          # " tokens to caller due to missing allocation",
          "swapTokenForTaco",
        );
      };

      if ((try { (await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Principal, Nat8)>(tempTransferQueueLocal), false)).0 } catch (_) { false })) {
        logger.info("Swap", "Token return queued successfully", "swapTokenForTaco");
      } else {
        Vector.addFromIter(transferQueue, Vector.vals(tempTransferQueueLocal));
        logger.warn("Swap", "Failed to queue token return via treasury, added to local transfer queue", "swapTokenForTaco");
      };

      return #err("Token has no target allocation in the vault");
    };

    // Calculate excess amount if this swap would exceed allocation target
    var usedAmount = receivedAmount;
    var tokenReturnAmount = 0;
    var usedValueICP = receivedValueICP;

    // Calculate new total value after this transaction
    let newTotalValue = activeTreasuryTotalValue + receivedValueICP;

    // Calculate new value with the entire received amount
    let newValueICP = currentValueInICP + receivedValueICP;

    // Calculate new basis points
    let newBasisPoints = (newValueICP * 10000) / newTotalValue;

    // If new allocation exceeds target, calculate excess
    if (newBasisPoints > targetBasisPoints) {
      // Calculate how much value is allowed based on target
      let maxAllowedValueICP = (targetBasisPoints * newTotalValue) / 10000;

      // Calculate excess value
      let excessValueICP = newValueICP - maxAllowedValueICP;

      // Convert excess to token amount
      var excessTokenAmount = (excessValueICP * (10 ** tokenDetails.tokenDecimals)) / tokenPrice;

      // Ensure we don't return more than received
      if (excessTokenAmount > receivedAmount) {
        excessTokenAmount := receivedAmount;
      };

      // Calculate used amount and value
      tokenReturnAmount := if (excessTokenAmount > 0) {
        Int.abs(excessTokenAmount);
      } else { 0 };
      usedAmount := receivedAmount - tokenReturnAmount;
      usedValueICP := receivedValueICP - ((tokenReturnAmount * tokenPrice) / (10 ** tokenDetails.tokenDecimals));

      logger.info(
        "Swap",
        "Allocation limit detected - using " # Nat.toText(usedAmount)
        # " tokens, returning " # Nat.toText(tokenReturnAmount)
        # " tokens as excess",
        "swapTokenForTaco",
      );
    } else {
      logger.info(
        "Swap",
        "Using entire received amount: " # Nat.toText(receivedAmount)
        # " tokens",
        "swapTokenForTaco",
      );
    };

    // Calculate premium based on how this trade moves the allocation toward target
    // Get allocation ratio BEFORE new tokens are added
    let initialRatio = if (targetBasisPoints == 0) {
      0.0; // Should not happen as we check above
    } else {
      Float.fromInt(currentBasisPoints) / Float.fromInt(targetBasisPoints);
    };

    // Get allocation ratio AFTER new tokens are added (using only the used amount)
    let finalRatio = Float.fromInt(newBasisPoints) / Float.fromInt(targetBasisPoints);

    // Ensure ratios are capped at 1.0 (100% of target)
    let cappedInitialRatio = if (initialRatio > 1.0) { 1.0 } else {
      initialRatio;
    };
    let cappedFinalRatio = if (finalRatio > 1.0) { 1.0 } else { finalRatio };

    // Calculate average ratio across this transaction (linear interpolation)
    // This represents the average fill level during this transaction
    let averageRatio = (cappedInitialRatio + cappedFinalRatio) / 2.0;

    // Calculate premium percentage - linear scale from min to max based on fill level
    // Lower fill = lower premium, higher fill = higher premium
    let startPremium = minPremiumPercent;
    let endPremium = maxPremiumPercent;

    // Interpolate between min and max premium based on average ratio
    let premiumPercent = startPremium + (endPremium - startPremium) * averageRatio;

    logger.info(
      "Swap",
      "Premium calculation - initial ratio: " # Float.toText(initialRatio)
      # ", final ratio: " # Float.toText(finalRatio)
      # ", premium: " # Float.toText(premiumPercent) # "%",
      "swapTokenForTaco",
    );

    // Calculate TACO amount with premium
    let tacoPriceWithPremium = Float.fromInt(tacoPrice) * (1.0 + premiumPercent / 100.0);
    let tacoAmount = (usedValueICP * (10 ** tacoDetails.tokenDecimals)) / Int.abs(Float.toInt(tacoPriceWithPremium));

    logger.info(
      "Swap",
      "TACO calculation - base price: " # Nat.toText(tacoPrice)
      # ", price with premium: " # Float.toText(tacoPriceWithPremium)
      # ", amount: " # Nat.toText(tacoAmount),
      "swapTokenForTaco",
    );

    // Check if there's enough TACO available
    let availableTACO : Int = tacoDetails.balance + (
      switch (Map.get(tokenBalanceTemp, phash, Principal.fromText(TACOaddress))) {
        case (?tempBalance) { tempBalance };
        case null { 0 };
      }
    );

    // Include fee in required amount
    let requiredTACO = tacoAmount + tacoDetails.tokenTransferFee;

    if (availableTACO < requiredTACO) {
      logger.error(
        "Swap",
        "Insufficient TACO balance - required: " # Nat.toText(requiredTACO)
        # ", available: " # Int.toText(availableTACO),
        "swapTokenForTaco",
      );

      // Return tokens if we don't have enough TACO
      if (receivedAmount > 3 * tokenDetails.tokenTransferFee) {
        Vector.add(
          tempTransferQueueLocal,
          (#principal(caller), receivedAmount - (3 * tokenDetails.tokenTransferFee), token, Nat8.fromNat(1)),
        );
        logger.info(
          "Swap",
          "Returning " # Nat.toText(receivedAmount - (3 * tokenDetails.tokenTransferFee))
          # " tokens to caller due to insufficient TACO",
          "swapTokenForTaco",
        );
      };

      if ((try { (await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Principal, Nat8)>(tempTransferQueueLocal), false)).0 } catch (_) { false })) {
        logger.info("Swap", "Token return queued successfully", "swapTokenForTaco");
      } else {
        Vector.addFromIter(transferQueue, Vector.vals(tempTransferQueueLocal));
        logger.warn("Swap", "Failed to queue token return via treasury, added to local transfer queue", "swapTokenForTaco");
      };

      return #err("Insufficient TACO balance in vault");
    };

    // Check if calculated TACO amount is less than minimum requested
    if (tacoAmount < minimumReceive) {
      logger.warn(
        "Swap",
        "Calculated TACO amount " # Nat.toText(tacoAmount)
        # " is less than minimum requested " # Nat.toText(minimumReceive),
        "swapTokenForTaco",
      );

      // Return all tokens except for fee
      if (receivedAmount > 3 * tokenDetails.tokenTransferFee) {
        Vector.add(
          tempTransferQueueLocal,
          (#principal(caller), receivedAmount - (3 * tokenDetails.tokenTransferFee), token, Nat8.fromNat(1)),
        );
        logger.info(
          "Swap",
          "Returning " # Nat.toText(receivedAmount - (3 * tokenDetails.tokenTransferFee))
          # " tokens to caller due to minimum amount not met",
          "swapTokenForTaco",
        );
      };

      if ((try { (await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Principal, Nat8)>(tempTransferQueueLocal), false)).0 } catch (_) { false })) {
        logger.info("Swap", "Token return queued successfully", "swapTokenForTaco");
      } else {
        Vector.addFromIter(transferQueue, Vector.vals(tempTransferQueueLocal));
        logger.warn("Swap", "Failed to queue token return via treasury, added to local transfer queue", "swapTokenForTaco");
      };

      return #err("Calculated TACO amount less than minimum requested");
    };

    // Update token balances in temp map
    switch (Map.get(tokenBalanceTemp, phash, token)) {
      case (?currentTemp) {
        Map.set(tokenBalanceTemp, phash, token, currentTemp + usedAmount);
        logger.info(
          "Swap",
          "Updated temporary balance for " # Principal.toText(token)
          # " by adding " # Nat.toText(usedAmount),
          "swapTokenForTaco",
        );
      };
      case null {
        Map.set(tokenBalanceTemp, phash, token, usedAmount);
        logger.info(
          "Swap",
          "Set new temporary balance for " # Principal.toText(token)
          # " to " # Nat.toText(usedAmount),
          "swapTokenForTaco",
        );
      };
    };

    // Update TACO temp balance
    let tacoToken = Principal.fromText(TACOaddress);
    switch (Map.get(tokenBalanceTemp, phash, tacoToken)) {
      case (?currentTemp) {
        if (currentTemp > tacoAmount) {
          Map.set(tokenBalanceTemp, phash, tacoToken, currentTemp - tacoAmount);
          logger.info(
            "Swap",
            "Updated temporary TACO balance by subtracting "
            # Nat.toText(tacoAmount),
            "swapTokenForTaco",
          );
        } else {
          Map.delete(tokenBalanceTemp, phash, tacoToken);
          logger.info("Swap", "Removed temporary TACO balance as it's exhausted", "swapTokenForTaco");
        };
      };
      case null {
        // If no temp balance, we need to update with a negative value
        Map.set(tokenBalanceTemp, phash, tacoToken, -tacoAmount);
        logger.info(
          "Swap",
          "Set new temporary TACO balance to -"
          # Nat.toText(tacoAmount),
          "swapTokenForTaco",
        );
      };
    };

    // Send TACO to user
    if (tacoAmount > tacoDetails.tokenTransferFee) {
      Vector.add(
        tempTransferQueueLocal,
        (#principal(caller), tacoAmount - tacoDetails.tokenTransferFee, tacoToken, Nat8.fromNat(0)),
      );
      logger.info(
        "Swap",
        "Queuing " # Nat.toText(tacoAmount - tacoDetails.tokenTransferFee)
        # " TACO to send to caller",
        "swapTokenForTaco",
      );
    };

    // Return excess tokens if any
    if (tokenReturnAmount > tokenDetails.tokenTransferFee) {
      Vector.add(
        tempTransferQueueLocal,
        (#principal(caller), tokenReturnAmount - tokenDetails.tokenTransferFee, token, Nat8.fromNat(1)),
      );
      logger.info(
        "Swap",
        "Queuing return of " # Nat.toText(tokenReturnAmount - tokenDetails.tokenTransferFee)
        # " excess tokens to caller",
        "swapTokenForTaco",
      );
    };

    if (usedAmount > 0) {
      // Create internal treasury transfer request
      Vector.add(
        tempTransferQueueLocal,
        (
          #principal(TreasuryPrincipal),
          usedAmount,
          token,
          Nat8.fromNat(0),
        ),
      );

      logger.info(
        "Swap",
        "Queueing internal transfer of " # Nat.toText(usedAmount) #
        " tokens from treasury subaccount 1 to main account",
        "swapTokenForTaco",
      );
    };

    if ((try { (await treasury.receiveTransferTasks(Vector.toArray<(TransferRecipient, Nat, Principal, Nat8)>(tempTransferQueueLocal), false)).0 } catch (_) { false })) {
      logger.info("Swap", "Token transfers queued successfully via treasury", "swapTokenForTaco");
    } else {
      Vector.addFromIter(transferQueue, Vector.vals(tempTransferQueueLocal));
      logger.warn("Swap", "Failed to queue transfers via treasury, added to local transfer queue", "swapTokenForTaco");
    };

    // Create swap result
    let result : SwapResult = {
      success = true;
      error = null;
      blockNumber = block;
      sentTokenAddress = Principal.toText(token);
      wantedTokenAddress = TACOaddress;
      swappedAmount = receivedAmount;
      returnedWantedAmount = tacoAmount;
      returnedSentAmount = tokenReturnAmount;
      usedSentAmount = usedAmount;
    };

    logger.info(
      "Swap",
      "Swap completed successfully - "
      # Principal.toText(token) # " to TACO: received "
      # Nat.toText(receivedAmount) # ", used " # Nat.toText(usedAmount)
      # ", returned " # Nat.toText(tokenReturnAmount)
      # ", sent TACO " # Nat.toText(tacoAmount),
      "swapTokenForTaco",
    );

    #ok(result);
  };

  /**
 * Allows users to recover tokens they sent to the vault by mistake
 *
 * This function checks a transaction block that was sent to the vault and
 * returns the tokens to the sender minus 3x the token's transfer fee.
 *
 * The function:
 * 1. Verifies the block exists and was sent by the caller
 * 2. Ensures the block isn't already processed for a swap
 * 3. Returns tokens to the sender (minus 3x transfer fee) via treasury
 * 4. Marks the block as processed to prevent double recovery
 *
 */
  public shared ({ caller }) func recoverWronglySentTokens(token : Principal, block : Nat) : async Result.Result<Text, Text> {
    // Spam protection check
    let allowed = isAllowed(caller);
    if (not allowed and not test) {
      return #err("Rate limit exceeded");
    };

    let blockKey = Principal.toText(token) # ":" # Nat.toText(block);

    // Check if block already processed
    if (Map.has(BlocksDone, thash, blockKey)) {
      return #err("Block already processed, blockKey: " # blockKey);
    };

    // Check token exists and is valid
    let tokenDetails = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) {
        details;
      };
      case null {
        return #err("Token not found");
      };
    };

    // Mark block as processed immediately to prevent replay attacks
    Map.set(BlocksDone, thash, blockKey, Time.now());

    // Verify the transaction and get received amount
    let receivedAmount = try {
      await* verifyBlock(block, Principal.toText(token), caller);
    } catch (e) {
      Map.delete(BlocksDone, thash, blockKey);
      return #err("Error verifying block: " # Error.message(e));
    };

    if (receivedAmount == 0) {
      // Delete block entry since transaction is invalid or not found
      return #err("Block verification failed: transaction not found or not sent by you");
    };

    logger.info(
      "Recovery",
      "Block verification successful for token " # Principal.toText(token) #
      ", block " # Nat.toText(block) # ", amount " # Nat.toText(receivedAmount),
      "recoverWronglySentTokens",
    );

    // Prepare for transfer queue
    let tempTransferQueueLocal = Vector.new<(TransferRecipient, Nat, Principal, Nat8)>();

    // Calculate fee (3x transfer fee)
    let fee = 3 * tokenDetails.tokenTransferFee;

    // Calculate return amount (minus fee)
    let returnAmount = if (receivedAmount > fee) {
      receivedAmount - fee;
    } else {
      0; // If amount is too small to cover fees, nothing to return
    };

    if (returnAmount > 0) {
      // Add transfer to queue
      Vector.add(
        tempTransferQueueLocal,
        (#principal(caller), returnAmount, token, Nat8.fromNat(1)),
      );

      logger.info(
        "Recovery",
        "Returning " # Nat.toText(returnAmount) # " tokens to caller (original amount: " #
        Nat.toText(receivedAmount) # ", fee: " # Nat.toText(fee) # ")",
        "recoverWronglySentTokens",
      );

      // Try to queue transfer via treasury, fall back to local queue if needed
      if ((
        try {
          (
            await treasury.receiveTransferTasks(
              Vector.toArray<(TransferRecipient, Nat, Principal, Nat8)>(tempTransferQueueLocal),
              false,
            )
          ).0;
        } catch (_) { false }
      )) {
        logger.info("Recovery", "Token return queued successfully via treasury", "recoverWronglySentTokens");
      } else {
        Vector.addFromIter(transferQueue, Vector.vals(tempTransferQueueLocal));
        logger.warn(
          "Recovery",
          "Failed to queue token return via treasury, added to local transfer queue",
          "recoverWronglySentTokens",
        );
      };

      #ok(
        "Recovery processed. " # Nat.toText(returnAmount) #
        " tokens will be returned (original amount: " # Nat.toText(receivedAmount) #
        ", fee deducted: " # Nat.toText(fee) # ")"
      );
    } else {
      logger.warn(
        "Recovery",
        "Amount " # Nat.toText(receivedAmount) # " too small to cover fee " # Nat.toText(fee),
        "recoverWronglySentTokens",
      );
      #err("Amount too small to cover recovery fee of " # Nat.toText(fee));
    };
  };

  public query func estimateSwapAmount(token : Principal, amount : Nat) : async Result.Result<{ maxAcceptedAmount : Nat; estimatedTacoAmount : Nat; premium : Float; tokenPrice : Nat; tacoPrice : Nat }, Text> {
    if test Debug.print("estimateSwapAmount: Starting estimate for token " # Principal.toText(token) # ", amount " # Nat.toText(amount));

    // Check if swapping is enabled
    if (not swappingEnabled) {
      if test Debug.print("estimateSwapAmount: Swapping is currently disabled");
      return #err("Swapping is currently disabled");
    };

    // Check token is active and not paused
    let tokenDetails = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) {
        if (not details.Active or details.isPaused) {
          if test Debug.print("estimateSwapAmount: Token is not active or is paused");
          return #err("Token is not active or is paused");
        };
        if test Debug.print("estimateSwapAmount: Found token details with symbol " # details.tokenSymbol);
        details;
      };
      case null {
        if test Debug.print("estimateSwapAmount: Token not found in details map");
        return #err("Token not found");
      };
    };

    // Get TACO token details
    let tacoDetails = switch (Map.get(tokenDetailsMap, phash, Principal.fromText(TACOaddress))) {
      case (?details) {
        if test Debug.print("estimateSwapAmount: Found TACO token details");
        details;
      };
      case null {
        if test Debug.print("estimateSwapAmount: TACO token not found in details map");
        return #err("TACO token not found");
      };
    };

    // Check the USD value requirement (similar to swapTokenForTaco)
    let tokenUsdPrice = tokenDetails.priceInUSD;
    let requestedValueUSD = Float.fromInt(amount) * tokenUsdPrice / Float.fromInt(10 ** tokenDetails.tokenDecimals);

    if test Debug.print(
      "estimateSwapAmount: Token USD price: " # Float.toText(tokenUsdPrice) #
      ", requested value: $" # Float.toText(requestedValueUSD) #
      ", min threshold: $" # Float.toText(minSwapValueUSD)
    );

    if (requestedValueUSD < minSwapValueUSD and not test) {
      if test Debug.print("estimateSwapAmount: Amount below minimum USD threshold");
      return #err("Swap amount below minimum threshold of $" # Float.toText(minSwapValueUSD));
    };

    // Get best historical prices from token details
    let currentTime = Time.now();

    // For token: use lowest historical price
    var tokenPrice = tokenDetails.priceInICP; // Start with current price
    if test Debug.print("estimateSwapAmount: Starting with current token price: " # Nat.toText(tokenPrice));

    for (pricePoint in tokenDetails.pastPrices.vals()) {
      // Check if price point is within our time window
      if (currentTime - pricePoint.time <= PRICE_HISTORY_WINDOW) {
        // Use minimum price for non-TACO tokens
        if (pricePoint.icpPrice < tokenPrice) {
          tokenPrice := pricePoint.icpPrice;
          if test Debug.print("estimateSwapAmount: Found lower historical token price: " # Nat.toText(tokenPrice));
        };
      };
    };

    // For TACO: use highest historical price
    var tacoPrice = tacoDetails.priceInICP; // Start with current price
    if test Debug.print("estimateSwapAmount: Starting with current TACO price: " # Nat.toText(tacoPrice));

    for (pricePoint in tacoDetails.pastPrices.vals()) {
      // Check if price point is within our time window
      if (currentTime - pricePoint.time <= PRICE_HISTORY_WINDOW) {
        // Use maximum price for TACO
        if (pricePoint.icpPrice > tacoPrice) {
          tacoPrice := pricePoint.icpPrice;
          if test Debug.print("estimateSwapAmount: Found higher historical TACO price: " # Nat.toText(tacoPrice));
        };
      };
    };

    // Check if we have valid prices
    if (tokenPrice == 0 or tacoPrice == 0) {
      if test Debug.print("estimateSwapAmount: Invalid prices - token: " # Nat.toText(tokenPrice) # ", TACO: " # Nat.toText(tacoPrice));
      return #err("Cannot determine valid token prices");
    };

    // Calculate total value of treasury excluding paused tokens
    var activeTreasuryTotalValue = 0;
    var totalTargetBasisPoints = 0;

    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and not details.isPaused) {
        let balance = switch (Map.get(tokenBalanceTemp, phash, principal)) {
          case (?tempBalance) {
            if test Debug.print("estimateSwapAmount: Found temp balance adjustment for " # Principal.toText(principal) # ": " # Int.toText(tempBalance));
            details.balance + Int.abs(tempBalance);
          };
          case null { details.balance };
        };
        let tokenValue = (balance * details.priceInICP) / (10 ** details.tokenDecimals);
        activeTreasuryTotalValue += tokenValue;
        if test Debug.print("estimateSwapAmount: Token " # Principal.toText(principal) # " value: " # Nat.toText(tokenValue) # " ICP");

        // Sum up target basis points for active, non-paused tokens
        switch (Map.get(targetAllocations, phash, principal)) {
          case (?basisPoints) {
            totalTargetBasisPoints += basisPoints;
            if test Debug.print("estimateSwapAmount: Target allocation for " # Principal.toText(principal) # ": " # Nat.toText(basisPoints) # " basis points");
          };
          case null {};
        };
      };
    };

    if test Debug.print("estimateSwapAmount: Total treasury value: " # Nat.toText(activeTreasuryTotalValue) # " ICP");
    if test Debug.print("estimateSwapAmount: Total target basis points: " # Nat.toText(totalTargetBasisPoints));

    // Get current Treasury balance for this token (including pending transfers)
    let currentBalance = switch (Map.get(tokenBalanceTemp, phash, token)) {
      case (?tempBalance) {
        if test Debug.print("estimateSwapAmount: Using adjusted balance for calculation: " # Int.toText(tokenDetails.balance + tempBalance));
        tokenDetails.balance + tempBalance;
      };
      case null { tokenDetails.balance };
    };

    // Calculate current allocation in basis points and value in ICP
    let currentValueInICP = (currentBalance * tokenPrice) / (10 ** tokenDetails.tokenDecimals);
    let requestedValueICP = (amount * tokenPrice) / (10 ** tokenDetails.tokenDecimals);

    if test Debug.print("estimateSwapAmount: Current token value: " # Int.toText(currentValueInICP) # " ICP");
    if test Debug.print("estimateSwapAmount: Requested swap value: " # Nat.toText(requestedValueICP) # " ICP");

    let currentBasisPoints = if (activeTreasuryTotalValue > 0) {
      (currentValueInICP * 10000) / activeTreasuryTotalValue;
    } else {
      0;
    };

    if test Debug.print("estimateSwapAmount: Current allocation: " # Int.toText(currentBasisPoints) # " basis points");

    // Get target basis points
    let targetBasisPoints = switch (Map.get(targetAllocations, phash, token)) {
      case (?basisPoints) {
        if (test and basisPoints <= currentBasisPoints) {
          let adjusted = Nat.min(10000, Int.abs(currentBasisPoints) + 100);
          if test Debug.print("estimateSwapAmount: Adjusting target basis points for test mode: " # Nat.toText(adjusted) # " (was " # Nat.toText(basisPoints) # ")");
          adjusted;
        } else {
          if test Debug.print("estimateSwapAmount: Using normal target basis points: " # Nat.toText(basisPoints));
          basisPoints;
        };
      };
      case null {
        if test {
          let defaultTarget = Nat.min(10000, Int.abs(currentBasisPoints) + 100);
          if test Debug.print("estimateSwapAmount: Using test default target basis points: " # Nat.toText(defaultTarget));
          defaultTarget;
        } else {
          if test Debug.print("estimateSwapAmount: No target allocation found");
          0;
        };
      }; // No allocation target
    };

    // Check if token has a target allocation
    if (targetBasisPoints == 0) {
      if test Debug.print("estimateSwapAmount: Token has no target allocation");
      return #err("Token has no target allocation in the vault");
    };

    // Calculate how many tokens would push us over target allocation
    var maxAcceptedAmount = amount;
    var usedValueICP = requestedValueICP;

    // Calculate new total value after this transaction
    let newTotalValue = activeTreasuryTotalValue + requestedValueICP;

    // Calculate new value with the entire requested amount
    let newValueICP = currentValueInICP + requestedValueICP;

    // Calculate new basis points
    let newBasisPoints = (newValueICP * 10000) / newTotalValue;

    if test Debug.print("estimateSwapAmount: New total value would be: " # Nat.toText(newTotalValue) # " ICP");
    if test Debug.print("estimateSwapAmount: New token value would be: " # Int.toText(newValueICP) # " ICP");
    if test Debug.print("estimateSwapAmount: New allocation would be: " # Int.toText(newBasisPoints) # " basis points");

    // Calculate premium based on how this trade moves the allocation toward target
    // Get allocation ratio BEFORE new tokens are added - EXACTLY as in swapTokenForTaco
    let initialRatio = if (targetBasisPoints == 0) {
      0.0; // Should not happen as we check above
    } else {
      Float.fromInt(currentBasisPoints) / Float.fromInt(targetBasisPoints);
    };

    if test Debug.print("estimateSwapAmount: Initial ratio: " # Float.toText(initialRatio));

    // Get allocation ratio AFTER new tokens are added (using only the used amount)
    // This must match the exact formula in swapTokenForTaco
    let finalValueICP = currentValueInICP + usedValueICP;
    let finalTotalValue = activeTreasuryTotalValue + usedValueICP;
    let finalBasisPoints = (finalValueICP * 10000) / finalTotalValue;

    let finalRatio = Float.fromInt(finalBasisPoints) / Float.fromInt(targetBasisPoints);
    if test Debug.print("estimateSwapAmount: Final ratio: " # Float.toText(finalRatio));

    // Ensure ratios are capped at 1.0 (100% of target)
    let cappedInitialRatio = if (initialRatio > 1.0) { 1.0 } else {
      initialRatio;
    };
    let cappedFinalRatio = if (finalRatio > 1.0) { 1.0 } else { finalRatio };

    if test Debug.print(
      "estimateSwapAmount: Capped initial ratio: " # Float.toText(cappedInitialRatio) #
      ", capped final ratio: " # Float.toText(cappedFinalRatio)
    );

    // Calculate average ratio across this transaction (linear interpolation)
    let averageRatio = (cappedInitialRatio + cappedFinalRatio) / 2.0;
    if test Debug.print("estimateSwapAmount: Average ratio: " # Float.toText(averageRatio));

    // Calculate premium percentage - EXACTLY as in swapTokenForTaco
    let premiumPercent = minPremiumPercent + (maxPremiumPercent - minPremiumPercent) * averageRatio;
    if test Debug.print("estimateSwapAmount: Calculated premium: " # Float.toText(premiumPercent) # "%");

    // Calculate TACO amount with premium - EXACTLY as in swapTokenForTaco
    let tacoPriceWithPremium = Float.fromInt(tacoPrice) * (1.0 + premiumPercent / 100.0);
    var estimatedTacoAmount = (usedValueICP * (10 ** tacoDetails.tokenDecimals)) / Int.abs(Float.toInt(tacoPriceWithPremium));

    if test Debug.print(
      "estimateSwapAmount: TACO price with premium: " # Float.toText(tacoPriceWithPremium) #
      ", estimated TACO amount: " # Nat.toText(estimatedTacoAmount)
    );

    // Check if there's enough TACO available
    let availableTACO : Int = tacoDetails.balance + (
      switch (Map.get(tokenBalanceTemp, phash, Principal.fromText(TACOaddress))) {
        case (?tempBalance) {
          if test Debug.print("estimateSwapAmount: Using adjusted TACO balance: " # Int.toText(tacoDetails.balance + tempBalance));
          tempBalance;
        };
        case null { 0 };
      }
    );

    if test Debug.print("estimateSwapAmount: Available TACO: " # Int.toText(availableTACO));

    // Include fee in required amount
    let requiredTACO = estimatedTacoAmount + tacoDetails.tokenTransferFee;
    if test Debug.print("estimateSwapAmount: Required TACO (with fee): " # Nat.toText(requiredTACO));

    // If not enough TACO, adjust maxAcceptedAmount proportionally
    if (availableTACO < requiredTACO and requiredTACO > 0) {
      let ratio = Float.fromInt(availableTACO) / Float.fromInt(requiredTACO);
      let originalAmount = maxAcceptedAmount;
      maxAcceptedAmount := Int.abs(Float.toInt(Float.fromInt(maxAcceptedAmount) * ratio));
      if test Debug.print(
        "estimateSwapAmount: Not enough TACO. Ratio: " # Float.toText(ratio) #
        ", adjusted max accepted amount from " # Nat.toText(originalAmount) #
        " to " # Nat.toText(maxAcceptedAmount)
      );

      // Recalculate TACO amount
      usedValueICP := (maxAcceptedAmount * tokenPrice) / (10 ** tokenDetails.tokenDecimals);
      let originalTacoAmount = estimatedTacoAmount;
      estimatedTacoAmount := (usedValueICP * (10 ** tacoDetails.tokenDecimals)) / Int.abs(Float.toInt(tacoPriceWithPremium));
      if test Debug.print(
        "estimateSwapAmount: Recalculated TACO amount: " # Nat.toText(estimatedTacoAmount) #
        " (reduced from " # Nat.toText(originalTacoAmount) # ")"
      );

      // Ensure we can at least pay the fee
      if (estimatedTacoAmount <= tacoDetails.tokenTransferFee) {
        if test Debug.print("estimateSwapAmount: Insufficient TACO to cover fee");
        return #err("Insufficient TACO in vault to complete swap");
      };
    };

    // Apply minimal safety buffer - just subtract 1
    if (estimatedTacoAmount > 0) {
      let originalAmount = estimatedTacoAmount;
      estimatedTacoAmount := estimatedTacoAmount - 1;
      if test Debug.print(
        "estimateSwapAmount: Applied safety buffer, reducing TACO from " #
        Nat.toText(originalAmount) # " to " # Nat.toText(estimatedTacoAmount)
      );
    };

    if test Debug.print(
      "estimateSwapAmount: Final result - max accepted amount: " # Nat.toText(maxAcceptedAmount) #
      ", estimated TACO: " # Nat.toText(estimatedTacoAmount)
    );

    // Return estimate details
    #ok({
      maxAcceptedAmount = maxAcceptedAmount;
      estimatedTacoAmount = estimatedTacoAmount;
      premium = premiumPercent;
      tokenPrice = tokenPrice;
      tacoPrice = tacoPrice;
    });
  };

  // Get the best prices from available sources (DEXes and price history)
  private func getBestPrices(token : Principal, tacoToken : Principal) : async* (Nat, Nat) {
    let currentTime = Time.now();

    // Try to get quotes from DEXes first
    let tokenDEXPrice = try {
      await* getBestPriceFromDEXes(token);
    } catch (e) {
      Debug.print("Error getting DEX price for token: " # Error.message(e));
      0;
    };

    let tacoDEXPrice = try {
      await* getBestPriceFromDEXes(tacoToken);
    } catch (e) {
      Debug.print("Error getting DEX price for TACO: " # Error.message(e));
      0;
    };

    // Get token details for historical prices
    let tokenDetails = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) { details };
      case null {
        Debug.print("Token details not found for: " # Principal.toText(token));
        return (0, 0);
      };
    };

    let tacoDetails = switch (Map.get(tokenDetailsMap, phash, tacoToken)) {
      case (?details) { details };
      case null {
        Debug.print("Token details not found for TACO");
        return (0, 0);
      };
    };

    // Get historical price data with proper time window
    var tokenHistoricalPrices = Vector.new<Nat>();
    var tacoHistoricalPrices = Vector.new<Nat>();

    // Add current prices to the vectors if valid
    if (tokenDEXPrice > 0) {
      Vector.add(tokenHistoricalPrices, tokenDEXPrice);
    };

    if (tacoDEXPrice > 0) {
      Vector.add(tacoHistoricalPrices, tacoDEXPrice);
    };

    // Try to get price history from the token details - process newest to oldest
    // Get price history for target token
    let tokenPricePoints = Array.reverse(tokenDetails.pastPrices);
    label tokenPriceLoop for (pricePoint in tokenPricePoints.vals()) {
      // Check if price point is within our time window
      if (currentTime - pricePoint.time <= PRICE_HISTORY_WINDOW) {
        Vector.add(tokenHistoricalPrices, pricePoint.icpPrice);
      } else {
        // Break if we're outside the time window (prices are in descending order by time)
        break tokenPriceLoop;
      };
    };

    // Get price history for TACO token
    let tacoPricePoints = Array.reverse(tacoDetails.pastPrices);
    label tacoPriceLoop for (pricePoint in tacoPricePoints.vals()) {
      // Check if price point is within our time window
      if (currentTime - pricePoint.time <= PRICE_HISTORY_WINDOW) {
        Vector.add(tacoHistoricalPrices, pricePoint.icpPrice);
      } else {
        // Break if we're outside the time window (prices are in descending order by time)
        break tacoPriceLoop;
      };
    };

    // If we don't have historical prices, try to query Treasury for more data
    if (Vector.size(tokenHistoricalPrices) <= 1 or Vector.size(tacoHistoricalPrices) <= 1) {
      try {
        let historyResponse = await treasury.getTokenPriceHistory([token, tacoToken]);

        switch (historyResponse) {
          case (?history) {
            for (entry in history.vals()) {
              if (entry.0 == token) {
                for (pricePoint in entry.1.vals()) {
                  // Check if price point is within our time window
                  if (currentTime - pricePoint.time <= PRICE_HISTORY_WINDOW) {
                    Vector.add(tokenHistoricalPrices, pricePoint.icpPrice);
                  };
                };
              } else if (entry.0 == tacoToken) {
                for (pricePoint in entry.1.vals()) {
                  // Check if price point is within our time window
                  if (currentTime - pricePoint.time <= PRICE_HISTORY_WINDOW) {
                    Vector.add(tacoHistoricalPrices, pricePoint.icpPrice);
                  };
                };
              };
            };
          };
          case null {
            Debug.print("Failed to get price history from Treasury");
          };
        };
      } catch (e) {
        Debug.print("Error querying Treasury for price history: " # Error.message(e));
      };
    };

    // Determine final prices with token-specific strategy
    // For TACO: use highest price (users get less TACO)
    // For other tokens: use lowest price (vault pays less)
    let finalTokenPrice = if (Vector.size(tokenHistoricalPrices) > 0) {
      var bestPrice = Vector.get(tokenHistoricalPrices, 0);

      // For non-TACO tokens, find the lowest price
      for (price in Vector.vals(tokenHistoricalPrices)) {
        if (price < bestPrice) {
          bestPrice := price;
        };
      };

      // If DEX price is available and lower, prefer it for non-TACO tokens
      if (tokenDEXPrice > 0 and tokenDEXPrice < bestPrice) {
        tokenDEXPrice;
      } else {
        bestPrice;
      };
    } else {
      // Fallback to stored price if no historical data
      tokenDetails.priceInICP;
    };

    let finalTacoPrice = if (Vector.size(tacoHistoricalPrices) > 0) {
      var bestPrice = Vector.get(tacoHistoricalPrices, 0);

      // For TACO, find the highest price
      for (price in Vector.vals(tacoHistoricalPrices)) {
        if (price > bestPrice) {
          bestPrice := price;
        };
      };

      // If DEX price is available and higher, prefer it for TACO
      if (tacoDEXPrice > 0 and tacoDEXPrice > bestPrice) {
        tacoDEXPrice;
      } else {
        bestPrice;
      };
    } else {
      // Fallback to stored price if no historical data
      tacoDetails.priceInICP;
    };

    Debug.print(
      "Final token price: " # Nat.toText(finalTokenPrice) #
      ", Final TACO price: " # Nat.toText(finalTacoPrice)
    );

    (finalTokenPrice, finalTacoPrice);
  };

  // Get best price from DEXes with proper slippage control
  private func getBestPriceFromDEXes(token : Principal) : async* Nat {
    let icpToken = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");

    // Validate token exists in our map
    let tokenDetails = switch (Map.get(tokenDetailsMap, phash, token)) {
      case (?details) { details };
      case null { return 0 };
    };

    // If token is ICP, return its price directly without querying DEXes
    if (token == icpToken) {
      return 100_000_000; // 1 ICP in e8s
    };

    // Use the token's symbol for KongSwap
    let tokenSymbol = tokenDetails.tokenSymbol;

    // Normalize amount to simulate a 1 ICP purchase for consistent price checking
    // This helps minimize price impact from large test amounts
    let normalizedAmount = Int.abs(Float.toInt((Float.fromInt(10 ** 8) / Float.fromInt(tokenDetails.priceInICP)) * Float.fromInt(10 ** tokenDetails.tokenDecimals)));

    Debug.print("Querying price for " # tokenSymbol # " with normalized amount: " # Nat.toText(normalizedAmount));

    // Initialize price results
    var kongSwapPrice : ?Nat = null;
    var icpSwapPrice : ?Nat = null;
    var kongSwapSlippage : Float = 0;
    var icpSwapSlippage : Float = 0;

    // Query KongSwap - Use try/catch to handle potential failures gracefully
    try {
      let kongResult = await KongSwap.getQuote(tokenSymbol, "ICP", normalizedAmount);

      switch (kongResult) {
        case (#ok(quote)) {
          // Verify that we got meaningful data back
          if (quote.receive_amount > 0) {
            kongSwapSlippage := quote.slippage;

            // Check if slippage is within acceptable range
            if (kongSwapSlippage <= Float.fromInt(maxSlippageBasisPoints) / 100.0) {
              // Calculate effective price in ICP (e8s)
              // Price = (output amount in ICP) / (input amount in token) * token decimals
              kongSwapPrice := ?((quote.receive_amount * (10 ** tokenDetails.tokenDecimals)) / normalizedAmount);
              Debug.print("KongSwap quote: " # Nat.toText(quote.receive_amount) # " ICP for " # Nat.toText(normalizedAmount) # " tokens, slippage: " # Float.toText(kongSwapSlippage));
            } else {
              Debug.print("KongSwap slippage too high: " # Float.toText(kongSwapSlippage) # "% > " # Float.toText(Float.fromInt(maxSlippageBasisPoints) / 100.0) # "%");
            };
          };
        };
        case (#err(e)) {
          Debug.print("KongSwap quote error: " # e);
        };
      };
    } catch (e) {
      Debug.print("Exception in KongSwap query: " # Error.message(e));
    };

    // Query ICPSwap - Use try/catch to handle potential failures gracefully
    try {
      // Find pool for token-ICP pair
      let poolData = switch (Map.get(ICPswapPools, hashpp, (token, icpToken))) {
        case (?data) { data };
        case null {
          // Try reverse order
          switch (Map.get(ICPswapPools, hashpp, (icpToken, token))) {
            case (?data) { data };
            case null {
              Debug.print("No ICPSwap pool found for " # Principal.toText(token));
              return switch (kongSwapPrice) {
                case (?price) { price };
                case null { 0 };
              };
            };
          };
        };
      };

      // Determine if token is token0 or token1 in the pool
      let isToken0 = token == Principal.fromText(poolData.token0.address);

      // Prepare quote args
      let quoteArgs : swaptypes.ICPSwapQuoteParams = {
        poolId = poolData.canisterId;
        amountIn = normalizedAmount;
        amountOutMinimum = 0;
        zeroForOne = isToken0; // If token is token0, we're selling token0 for token1
      };

      Debug.print("Querying ICPSwap pool " # Principal.toText(poolData.canisterId) # ", zeroForOne: " # debug_show (isToken0));

      // Get quote from ICPSwap
      let icpSwapResult = await ICPSwap.getQuote(quoteArgs);

      switch (icpSwapResult) {
        case (#ok(quote)) {
          icpSwapSlippage := quote.slippage;

          // Check if slippage is within acceptable range
          if (icpSwapSlippage <= Float.fromInt(maxSlippageBasisPoints) / 100.0) {
            // Calculate price in ICP (e8s)
            if (isToken0) {
              // If selling token for ICP (token is token0)
              icpSwapPrice := ?((quote.amountOut * (10 ** tokenDetails.tokenDecimals)) / normalizedAmount);
            } else {
              // If buying token with ICP (token is token1)
              // Need to invert the calculation for consistent price format
              icpSwapPrice := ?((normalizedAmount * 10 ** 8) / quote.amountOut);
            };
            Debug.print("ICPSwap quote: " # Nat.toText(quote.amountOut) # " output for " # Nat.toText(normalizedAmount) # " input, slippage: " # Float.toText(icpSwapSlippage));
          } else {
            Debug.print("ICPSwap slippage too high: " # Float.toText(icpSwapSlippage) # "% > " # Float.toText(Float.fromInt(maxSlippageBasisPoints) / 100.0) # "%");
          };
        };
        case (#err(e)) {
          Debug.print("ICPSwap quote error: " # e);
        };
      };
    } catch (e) {
      Debug.print("Exception in ICPSwap query: " # Error.message(e));
    };

    // Choose the best price, or fallback to stored price if both DEXes fail
    switch (kongSwapPrice, icpSwapPrice) {
      // Both DEXes returned valid prices, choose based on whether it's TACO or not
      case (?kongPrice, ?icpPrice) {
        Debug.print("Comparing prices - KongSwap: " # Nat.toText(kongPrice) # ", ICPSwap: " # Nat.toText(icpPrice));

        // For TACO, return the highest price (users get less TACO)
        // For other tokens, return the lowest price (vault pays less)
        let isTACO = Principal.toText(token) == TACOaddress;

        if (isTACO) {
          // For TACO: return highest price (maximizes premium)
          if (kongPrice > icpPrice) {
            Debug.print("Selected KongSwap price for TACO (highest): " # Nat.toText(kongPrice));
            return kongPrice;
          } else {
            Debug.print("Selected ICPSwap price for TACO (highest): " # Nat.toText(icpPrice));
            return icpPrice;
          };
        } else {
          // For other tokens: return lowest price (minimizes cost)
          if (kongPrice < icpPrice) {
            Debug.print("Selected KongSwap price for token (lowest): " # Nat.toText(kongPrice));
            return kongPrice;
          } else {
            Debug.print("Selected ICPSwap price for token (lowest): " # Nat.toText(icpPrice));
            return icpPrice;
          };
        };
      };

      // Only KongSwap returned a valid price
      case (?kongPrice, null) {
        Debug.print("Only KongSwap returned valid price: " # Nat.toText(kongPrice));
        return kongPrice;
      };

      // Only ICPSwap returned a valid price
      case (null, ?icpPrice) {
        Debug.print("Only ICPSwap returned valid price: " # Nat.toText(icpPrice));
        return icpPrice;
      };

      // Neither DEX returned a valid price, fallback to stored price
      case (null, null) {
        Debug.print("No valid DEX prices, returning stored price: " # Nat.toText(tokenDetails.priceInICP));
        return tokenDetails.priceInICP;
      };
    };
  };

  /**
   * Update and cache known ICPSwap pools
   *
   * Fetches all available pools and stores them for quick access.
   */
  private func updateICPSwapPools() : async () {
    logger.info("Pools", "Starting ICPSwap pool discovery", "updateICPSwapPools");
    let startTime = Time.now();

    try {
      let previousPoolCount = Map.size(ICPswapPools);
      logger.info("Pools", "Current pool count: " # Nat.toText(previousPoolCount), "updateICPSwapPools");

      let poolsResult = await ICPSwap.getAllPools();

      switch (poolsResult) {
        case (#ok(pools)) {
          // Clear existing pool mappings
          Map.clear(ICPswapPools);
          logger.info("Pools", "Retrieved " # Nat.toText(pools.size()) # " pools from ICPSwap", "updateICPSwapPools");

          var poolCount = 0;
          for (pool in pools.vals()) {
            let token0Principal = Principal.fromText(pool.token0.address);
            let token1Principal = Principal.fromText(pool.token1.address);

            // Store pool data in both directions
            Map.set(
              ICPswapPools,
              hashpp,
              (token0Principal, token1Principal),
              pool,
            );
            Map.set(
              ICPswapPools,
              hashpp,
              (token1Principal, token0Principal),
              pool,
            );
            poolCount += 1;
          };

          logger.info("Pools", "Added " # Nat.toText(poolCount) # " pools to the map", "updateICPSwapPools");
        };
        case (#err(e)) {
          logger.error("Pools", "Error fetching pools: " # e, "updateICPSwapPools");
        };
      };

      let executionTime = Time.now() - startTime;
      logger.info("Pools", "Pool discovery completed in " # Int.toText(executionTime / 1_000_000) # "ms", "updateICPSwapPools");
    } catch (e) {
      logger.error("Pools", "Exception in pool discovery: " # Error.message(e), "updateICPSwapPools");
    };
  };

  // Helper function to sync allocations from DAO
  private func syncAllocationsFromDAO() : async () {
    let allocationResult = await dao.getAggregateAllocation();
    Map.clear(targetAllocations);

    for ((principal, allocation) in allocationResult.vals()) {
      Map.set(targetAllocations, phash, principal, allocation);
    };
  };

  // Balance update timer function that runs every (by default) 15 minutes
  // Balance update timer function that runs every (by default) 15 minutes
  private func updateBalances() : async () {
    logger.info("Balance", "Starting balance update for all tokens", "updateBalances");
    let startTime = Time.now();

    try {
      // Get all token information from tokenDetailsMap
      let tokens = Vector.new<Principal>();
      let tokenCount = Map.size(tokenDetailsMap);
      logger.info("Balance", "Processing " # Nat.toText(tokenCount) # " tokens", "updateBalances");

      for ((principal, _) in Map.entries(tokenDetailsMap)) {
        Vector.add(tokens, principal);
      };

      // Prepare parallel balance checks
      let balanceFutures = Map.new<Principal, async Nat>();

      // For ICP
      let icpToken = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
      let icpLedger = actor (Principal.toText(icpToken)) : Ledger.Interface;
      let icpBalanceFuture = icpLedger.account_balance({
        account = redemptionAccountID;
      });
      logger.info("Balance", "ICP balance check initiated", "updateBalances");

      // For ICRC1 tokens
      for (principal in Vector.vals(tokens)) {
        if (principal != icpToken) {
          let token = actor (Principal.toText(principal)) : ICRC1.FullInterface;
          let balanceFuture = token.icrc1_balance_of({
            owner = TreasuryPrincipal;
            subaccount = ?TreasurySubaccount;
          });
          Map.set(balanceFutures, phash, principal, balanceFuture);
        };
      };
      logger.info("Balance", "Initiated " # Nat.toText(Map.size(balanceFutures)) # " ICRC1 balance checks", "updateBalances");

      // First await ICP balance
      let icpBalance = await icpBalanceFuture;
      logger.info("Balance", "Received ICP balance: " # Nat64.toText(icpBalance.e8s) # " e8s", "updateBalances");

      // Await all ICRC1 balances in parallel
      let balances = Map.new<Principal, Nat>();
      logger.info("Balance", "Awaiting responses from " # Nat.toText(Map.size(balanceFutures)) # " ICRC1 tokens", "updateBalances");

      for ((principal, future) in Map.entries(balanceFutures)) {
        let balance = await future;
        Map.set(balances, phash, principal, balance);
      };
      logger.info("Balance", "Received " # Nat.toText(Map.size(balances)) # " ICRC1 balances", "updateBalances");

      // Process ICP balance
      var updatedTokens = 0;
      try {
        switch (Map.get(tokenDetailsMap, phash, icpToken)) {
          case (?details) {
            Map.set(
              tokenDetailsMap,
              phash,
              icpToken,
              {
                details with
                balance = Nat64.toNat(icpBalance.e8s);
              },
            );
            updatedTokens += 1;
          };
          case null {
            logger.warn("Balance", "ICP token details not found in map", "updateBalances");
          };
        };
      } catch (e) {
        logger.error("Balance", "Error updating ICP token details: " # Error.message(e), "updateBalances");
      };

      // Process ICRC1 balances
      var successCount = 0;
      var failureCount = 0;

      for ((principal, balance) in Map.entries(balances)) {
        try {
          switch (Map.get(tokenDetailsMap, phash, principal)) {
            case (?details) {
              Map.set(
                tokenDetailsMap,
                phash,
                principal,
                {
                  details with
                  balance = balance;
                },
              );
              successCount += 1;
            };
            case null {
              logger.warn("Balance", "Token details not found for " # Principal.toText(principal), "updateBalances");
              failureCount += 1;
            };
          };
        } catch (e) {
          logger.error(
            "Balance",
            "Error updating token details for " # Principal.toText(principal)
            # ": " # Error.message(e),
            "updateBalances",
          );
          failureCount += 1;
        };
      };

      updatedTokens += successCount;
      logger.info(
        "Balance",
        "ICRC1 balance updates completed: " # Nat.toText(successCount)
        # " succeeded, " # Nat.toText(failureCount) # " failed",
        "updateBalances",
      );

      // Clear temporary balance tracking
      Map.clear(tokenBalanceTemp);
      logger.info("Balance", "Cleared temporary balance adjustments", "updateBalances");

      // Schedule next update
      startBalanceUpdateTimer<system>(false);

      let executionTime = Time.now() - startTime;
      logger.info(
        "Balance",
        "Balance update completed: updated " # Nat.toText(updatedTokens)
        # " tokens in " # Int.toText(executionTime / 1_000_000) # "ms",
        "updateBalances",
      );
    } catch (e) {
      logger.error("Balance", "Error in updateBalances: " # Error.message(e), "updateBalances");

      // On error, reschedule with a shorter interval
      balanceUpdateTimerId := ?setTimer<system>(
        #seconds(60),
        func() : async () {
          await updateBalances();
        },
      );
      logger.info("Balance", "Rescheduled balance update in 60 seconds due to error", "updateBalances");
    };
  };

  private func getTokenDetailsFromDAO() : async () {
    logger.info("Sync", "Fetching token details from DAO", "getTokenDetailsFromDAO");
    let startTime = Time.now();

    try {
      let tokenDetails = await dao.getTokenDetails();
      let previousCount = Map.size(tokenDetailsMap);

      Map.clear(tokenDetailsMap);

      var activeCount = 0;
      var inactiveCount = 0;
      var pausedCount = 0;

      for ((principal, details) in tokenDetails.vals()) {
        Map.set(tokenDetailsMap, phash, principal, details);

        if (details.Active) {
          activeCount += 1;
          if (details.isPaused) {
            pausedCount += 1;
          };
        } else {
          inactiveCount += 1;
        };
      };

      let executionTime = Time.now() - startTime;
      logger.info(
        "Sync",
        "Token details updated: " # Nat.toText(Map.size(tokenDetailsMap))
        # " tokens (" # Nat.toText(activeCount) # " active, "
        # Nat.toText(inactiveCount) # " inactive, "
        # Nat.toText(pausedCount) # " paused) in "
        # Int.toText(executionTime / 1_000_000) # "ms",
        "getTokenDetailsFromDAO",
      );

    } catch (e) {
      logger.error("Sync", "Failed to get token details from DAO: " # Error.message(e), "getTokenDetailsFromDAO");
    };
  };

  // Start the balance update timer
  private func startBalanceUpdateTimer<system>(now : Bool) {
    switch (balanceUpdateTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };

    balanceUpdateTimerId := ?setTimer<system>(
      #nanoseconds(if (now) { 0 } else { Int.abs(balanceUpdateIntervalNS) }),
      func() : async () {
        await updateBalances();
        await getTokenDetailsFromDAO();
      },
    );
  };

  // Block cleanup timer function
  private func cleanupOldBlocks() : async () {
    logger.info("Maintenance", "Starting old blocks cleanup", "cleanupOldBlocks");

    let nowVar = Time.now();
    let thirtyDaysAgo = nowVar - (30 * 24 * 3600 * 1_000_000_000); // 30 days in nanoseconds
    var processedCount = 0;
    var deletedCount = 0;
    var continueCleanup = false;
    let startTime = Time.now();

    logger.info("Maintenance", "Cleanup threshold: " # Int.toText(thirtyDaysAgo) # " (blocks older than 30 days)", "cleanupOldBlocks");

    label cleanup for ((blockKey, timestamp) in Map.entriesDesc(BlocksDone)) {
      if (timestamp < thirtyDaysAgo) {
        if (processedCount >= 4000) {
          continueCleanup := true;
          logger.info(
            "Maintenance",
            "Processing limit reached at " # Nat.toText(processedCount)
            # " entries, scheduling continued cleanup",
            "cleanupOldBlocks",
          );
          break cleanup;
        };
        // Remove old entry
        Map.delete(BlocksDone, thash, blockKey);
        deletedCount += 1;
      };
      processedCount += 1;

      // Early exit if we've reached entries younger than 30 days
      if (timestamp >= thirtyDaysAgo) {
        logger.info(
          "Maintenance",
          "Reached blocks newer than cleanup threshold at entry "
          # Nat.toText(processedCount),
          "cleanupOldBlocks",
        );
        break cleanup;
      };
    };

    let executionTime = Time.now() - startTime;
    logger.info(
      "Maintenance",
      "Block cleanup completed: processed " # Nat.toText(processedCount)
      # " entries, deleted " # Nat.toText(deletedCount) # " in "
      # Int.toText(executionTime / 1_000_000) # "ms",
      "cleanupOldBlocks",
    );

    // If we hit the processing limit but more old entries remain, schedule a follow-up
    if (continueCleanup) {
      blockCleanupTimerId := ?setTimer<system>(
        #seconds(60), // Schedule another cleanup in 1 minute
        func() : async () {
          await cleanupOldBlocks();
        },
      );
      logger.info("Maintenance", "Scheduled follow-up cleanup in 60 seconds", "cleanupOldBlocks");
    } else {
      // Schedule next regular cleanup
      startBlockCleanupTimer<system>(false);
      logger.info("Maintenance", "Scheduled next regular cleanup", "cleanupOldBlocks");
    };
  };

  // Start the block cleanup timer
  private func startBlockCleanupTimer<system>(now : Bool) {
    switch (blockCleanupTimerId) {
      case (?id) { cancelTimer(id) };
      case null {};
    };

    blockCleanupTimerId := ?setTimer<system>(
      #nanoseconds(if (now) { 0 } else { Int.abs(blockCleanupIntervalNS) }),
      func() : async () {
        await cleanupOldBlocks();
        await updateICPSwapPools();
      },
    );
  };

  // Admin function to update configuration values
  public shared ({ caller }) func updateConfiguration(
    config : MintingVaultTypes.UpdateConfig
  ) : async Result.Result<(), Text> {
    if (caller != DAOprincipal and caller != TreasuryPrincipal and not Principal.isController(caller) and sns_governance_canister_id != ?caller) {
      logger.warn("Config", "Unauthorized configuration update attempt by " # Principal.toText(caller), "updateConfiguration");
      return #err("Not authorized");
    };

    logger.info("Config", "Configuration update requested by " # Principal.toText(caller), "updateConfiguration");

    // Update swapping enabled if provided
    switch (config.swappingEnabled) {
      case (?enabled) {
        swappingEnabled := enabled;
        logger.info("Config", "Swapping enabled set to " # debug_show (enabled), "updateConfiguration");
      };
      case null {};
    };

    // Update price history window if provided
    switch (config.PRICE_HISTORY_WINDOW) {
      case (?window) {
        if (window < 60 * 60 * 1_000_000_000 or window > 24 * 60 * 60 * 1_000_000_000) {
          logger.error("Config", "Invalid price history window: " # Int.toText(window), "updateConfiguration");
          return #err("Invalid price history window: must be between 1 hour and 24 hours");
        };
        PRICE_HISTORY_WINDOW := window;
        logger.info("Config", "Price history window updated to " # Int.toText(window) # " ns", "updateConfiguration");
      };
      case null {};
    };

    // Update min swap value if provided
    switch (config.minSwapValueUSD) {
      case (?value) {
        minSwapValueUSD := value;
        logger.info("Config", "Min swap value updated to " # Float.toText(value), "updateConfiguration");
      };
      case null {};
    };

    // Update max slippage basis points if provided
    switch (config.maxSlippageBasisPoints) {
      case (?max) {
        if (max < 35 or max > 100) {
          logger.error("Config", "Invalid max slippage basis points: " # Nat.toText(max), "updateConfiguration");
          return #err("Invalid max slippage basis points: must be between 35 and 100");
        };
        maxSlippageBasisPoints := max;
        logger.info("Config", "Max slippage basis points updated to " # Nat.toText(max), "updateConfiguration");
      };
      case null {};
    };

    // Update min premium if provided
    switch (config.minPremium) {
      case (?min) {
        if (min < 0.0 or min >= maxPremiumPercent) {
          logger.error("Config", "Invalid min premium: " # Float.toText(min), "updateConfiguration");
          return #err("Invalid min premium: must be >= 0 and < maxPremium");
        };
        minPremiumPercent := min;
        logger.info("Config", "Min premium updated to " # Float.toText(min) # "%", "updateConfiguration");
      };
      case null {};
    };

    // Update max premium if provided
    switch (config.maxPremium) {
      case (?max) {
        if (max <= minPremiumPercent or max > 10.0) {
          logger.error("Config", "Invalid max premium: " # Float.toText(max), "updateConfiguration");
          return #err("Invalid max premium: must be > minPremium and <= 10");
        };
        maxPremiumPercent := max;
        logger.info("Config", "Max premium updated to " # Float.toText(max) # "%", "updateConfiguration");
      };
      case null {};
    };

    // Update balance update interval if provided
    switch (config.balanceUpdateInterval) {
      case (?interval) {
        if (interval < 60_000_000_000 or interval > 24 * 3600_000_000_000) {
          logger.error("Config", "Invalid balance update interval: " # Int.toText(interval), "updateConfiguration");
          return #err("Invalid balance update interval: must be between 1 minute and 24 hours");
        };
        balanceUpdateIntervalNS := interval;
        logger.info("Config", "Balance update interval updated to " # Int.toText(interval) # " ns", "updateConfiguration");
        startBalanceUpdateTimer<system>(false);
      };
      case null {};
    };

    // Update block cleanup interval if provided
    switch (config.blockCleanupInterval) {
      case (?interval) {
        if (interval < 3600_000_000_000 or interval > 7 * 24 * 3600_000_000_000) {
          logger.error("Config", "Invalid block cleanup interval: " # Int.toText(interval), "updateConfiguration");
          return #err("Invalid block cleanup interval: must be between 1 hour and 7 days");
        };
        blockCleanupIntervalNS := interval;
        logger.info("Config", "Block cleanup interval updated to " # Int.toText(interval) # " ns", "updateConfiguration");
        startBlockCleanupTimer<system>(false);
      };
      case null {};
    };

    logger.info("Config", "Configuration updated successfully", "updateConfiguration");
    #ok();
  };

  // Initialize timers on canister startup
  func init<system>() {
    ignore setTimer<system>(
      #nanoseconds(0),
      func() : async () {
        await getTokenDetailsFromDAO();
        await syncAllocationsFromDAO();
        startBalanceUpdateTimer<system>(true);
        startBlockCleanupTimer<system>(true);
      },
    );

  };

  private func isAllowed(principal : Principal) : Bool {
    switch (spamGuard.isAllowed(principal, sns_governance_canister_id)) {
      case (1) { return true };
      case (_) { return false };
    };
  };
  public shared ({ caller }) func setSnsGovernanceCanisterId(p : Principal) : async () {
    if (Principal.isController(caller) or sns_governance_canister_id == ?caller or sns_governance_canister_id == ?p) {
      sns_governance_canister_id := ?p;
    };
  };

  // Get vault status information for UI display and interaction
  public query func getVaultStatus() : async {
    tokenDetails : [(Principal, TokenDetails)];
    targetAllocations : [(Principal, Nat)];
    currentAllocations : [(Principal, Nat)];
    exchangeRates : [(Principal, Float)];
    premiumRange : { min : Float; max : Float };
    totalValueICP : Nat;
  } {
    // Calculate current value of tokens in vault
    var totalValueICP = 0;
    let currentAllocations = Vector.new<(Principal, Nat)>();
    let exchangeRates = Vector.new<(Principal, Float)>();

    // Calculate total value in ICP
    for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and not details.isPaused) {
        let balance = switch (Map.get(tokenBalanceTemp, phash, principal)) {
          case (?tempBalance) { details.balance + Int.abs(tempBalance) };
          case null { details.balance };
        };

        let valueInICP = (balance * details.priceInICP) / (10 ** details.tokenDecimals);
        totalValueICP += valueInICP;
      };
    };

    // Calculate current allocations and exchange rates
    label a for ((principal, details) in Map.entries(tokenDetailsMap)) {
      if (details.Active and not details.isPaused) {
        let balance = switch (Map.get(tokenBalanceTemp, phash, principal)) {
          case (?tempBalance) { details.balance + Int.abs(tempBalance) };
          case null { details.balance };
        };

        let valueInICP = (balance * details.priceInICP) / (10 ** details.tokenDecimals);

        // Calculate basis points
        let basisPoints = if (totalValueICP > 0) {
          (valueInICP * 10000) / totalValueICP;
        } else {
          0;
        };

        Vector.add(currentAllocations, (principal, basisPoints));

        // Calculate token to TACO exchange rate
        let tacoDetails = switch (Map.get(tokenDetailsMap, phash, Principal.fromText(TACOaddress))) {
          case (?td) { td };
          case null { continue a };
        };

        // Units of TACO per unit of token with no premium
        let baseExchangeRate = Float.fromInt(details.priceInICP * (10 ** tacoDetails.tokenDecimals)) / Float.fromInt(tacoDetails.priceInICP * (10 ** details.tokenDecimals));

        Vector.add(exchangeRates, (principal, baseExchangeRate));
      };
    };

    {
      tokenDetails = Iter.toArray(Map.entries(tokenDetailsMap));
      targetAllocations = Iter.toArray(Map.entries(targetAllocations));
      currentAllocations = Vector.toArray(currentAllocations);
      exchangeRates = Vector.toArray(exchangeRates);
      premiumRange = { min = minPremiumPercent; max = maxPremiumPercent };
      totalValueICP = totalValueICP;
    };
  };

  // Function to get logs - restricted to controllers only
  public query ({ caller }) func getLogs(count : Nat) : async [Logger.LogEntry] {
    if (caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logger.getLastLogs(count);
    } else { [] };
  };

  // Function to get logs by context - restricted to controllers only
  public query ({ caller }) func getLogsByContext(context : Text, count : Nat) : async [Logger.LogEntry] {
    if (caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logger.getContextLogs(context, count);
    } else { [] };
  };

  // Function to get logs by level - restricted to controllers only
  public query ({ caller }) func getLogsByLevel(level : Logger.LogLevel, count : Nat) : async [Logger.LogEntry] {
    if (caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logger.getLogsByLevel(level, count);
    } else { [] };
  };

  // Function to clear logs - restricted to controllers
  public shared ({ caller }) func clearLogs() : async () {
    if (caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logger.info("System", "Logs cleared by: " # Principal.toText(caller), "clearLogs");
      logger.clearLogs();
      logger.clearContextLogs("all");
    };
  };

  public shared ({ caller }) func setLogAdmin(newLogAdmin : Principal) : async () {
    if (caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      logAdmin := newLogAdmin;
    };
  };

  stable var test : Bool = false;

  public shared ({ caller }) func setTest(t : Bool) : async () {
    if (Principal.isController(caller) or sns_governance_canister_id == ?caller) {
      test := t;
    };
  };

  init<system>();

  system func inspect({
    arg : Blob;
    caller : Principal;
    msg : {
      #estimateSwapAmount : () -> (Principal, Nat);
      #swapTokenForTaco : () -> (Principal, Nat, Nat);
      #syncTokenDetailsFromDAO : () -> [(Principal, TokenDetails)];
      #updateConfiguration : () -> MintingVaultTypes.UpdateConfig;
      #getLogs : () -> Nat;
      #getLogsByContext : () -> (Text, Nat);
      #getLogsByLevel : () -> (LogLevel, Nat);
      #clearLogs : () -> ();
      #getVaultStatus : () -> ();
      #setLogAdmin : () -> Principal;
      #recoverWronglySentTokens : () -> (Principal, Nat);
      #setTest : () -> Bool;
      #setSnsGovernanceCanisterId : () -> Principal;
    };
  }) : Bool {
    if (arg.size() > 512) {
      return false;
    };
    // Caller validation based on message type
    switch (msg) {
      case (#syncTokenDetailsFromDAO _) {
        return caller == DAOprincipal;
      };
      case (#swapTokenForTaco _) {
        return isAllowed(caller);
      };
      case (#estimateSwapAmount _) {
        return true;
      };
      case (#updateConfiguration _) {
        return caller == DAOprincipal or caller == TreasuryPrincipal or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#getVaultStatus _) {
        return true;
      };
      case (#clearLogs _) {
        return caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#getLogs _) {
        return caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#getLogsByContext _) {
        return caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#getLogsByLevel _) {
        return caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#setLogAdmin _) {
        return caller == logAdmin or Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#recoverWronglySentTokens _) {
        return isAllowed(caller);
      };
      case (#setTest _) {
        return Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
      case (#setSnsGovernanceCanisterId _) {
        return Principal.isController(caller) or sns_governance_canister_id == ?caller;
      };
    };
    // If message doesn't match any of the above patterns, deny access
    false;
  };

};
