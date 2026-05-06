import Types "./taco_swap_types";
import Array "mo:base/Array";

module {

  // ═══════════════════════════════════════════════════════
  // OLD RECORD TYPES (deployed state — no fiat fields)
  // ═══════════════════════════════════════════════════════

  type OldOrderRecord = {
    id : Nat;
    principal : Principal;
    icpDeposited : Nat;
    tacoReceived : Nat;
    slippage : Float;
    poolId : Principal;
    timestamp : Int;
    transakOrderId : ?Text;
    tacoTxId : ?Nat;
    claimPath : Types.ClaimPath;
  };

  type OldNachosOrderRecord = {
    id : Nat;
    principal : Principal;
    icpDeposited : Nat;
    nachosReceived : Nat;
    navUsed : Nat;
    feeICP : Nat;
    timestamp : Int;
    nachosMintId : ?Nat;
    claimPath : Types.ClaimPath;
  };

  // ═══════════════════════════════════════════════════════
  // VECTOR INTERNAL REPRESENTATION
  // ═══════════════════════════════════════════════════════

  type OldOrderVector = {
    var data_blocks : [var [var ?OldOrderRecord]];
    var i_block : Nat;
    var i_element : Nat;
  };

  type NewOrderVector = {
    var data_blocks : [var [var ?Types.OrderRecord]];
    var i_block : Nat;
    var i_element : Nat;
  };

  type OldNachosOrderVector = {
    var data_blocks : [var [var ?OldNachosOrderRecord]];
    var i_block : Nat;
    var i_element : Nat;
  };

  type NewNachosOrderVector = {
    var data_blocks : [var [var ?Types.NachosOrderRecord]];
    var i_block : Nat;
    var i_element : Nat;
  };

  // ═══════════════════════════════════════════════════════
  // STATE TYPES (field names must match stable variables)
  // ═══════════════════════════════════════════════════════

  public type OldState = {
    completedOrders : OldOrderVector;
    nachosCompletedOrders : OldNachosOrderVector;
  };

  public type NewState = {
    completedOrders : NewOrderVector;
    nachosCompletedOrders : NewNachosOrderVector;
  };

  // ═══════════════════════════════════════════════════════
  // MIGRATION
  // ═══════════════════════════════════════════════════════

  func migrateOrderBlock(block : [var ?OldOrderRecord]) : [var ?Types.OrderRecord] {
    Array.tabulateVar<?Types.OrderRecord>(
      block.size(),
      func(i : Nat) : ?Types.OrderRecord {
        switch (block[i]) {
          case null null;
          case (?old) {
            ?{
              id = old.id;
              principal = old.principal;
              icpDeposited = old.icpDeposited;
              tacoReceived = old.tacoReceived;
              slippage = old.slippage;
              poolId = old.poolId;
              timestamp = old.timestamp;
              transakOrderId = old.transakOrderId;
              tacoTxId = old.tacoTxId;
              claimPath = old.claimPath;
              fiatAmount = null;
              fiatCurrency = null;
            };
          };
        };
      },
    );
  };

  func migrateNachosOrderBlock(block : [var ?OldNachosOrderRecord]) : [var ?Types.NachosOrderRecord] {
    Array.tabulateVar<?Types.NachosOrderRecord>(
      block.size(),
      func(i : Nat) : ?Types.NachosOrderRecord {
        switch (block[i]) {
          case null null;
          case (?old) {
            ?{
              id = old.id;
              principal = old.principal;
              icpDeposited = old.icpDeposited;
              nachosReceived = old.nachosReceived;
              navUsed = old.navUsed;
              feeICP = old.feeICP;
              timestamp = old.timestamp;
              nachosMintId = old.nachosMintId;
              claimPath = old.claimPath;
              fiatAmount = null;
              fiatCurrency = null;
            };
          };
        };
      },
    );
  };

  public func migrate(old : OldState) : NewState {
    let oldBlocks = old.completedOrders.data_blocks;
    let newBlocks = Array.tabulateVar<[var ?Types.OrderRecord]>(
      oldBlocks.size(),
      func(i : Nat) : [var ?Types.OrderRecord] {
        migrateOrderBlock(oldBlocks[i]);
      },
    );

    let oldNachosBlocks = old.nachosCompletedOrders.data_blocks;
    let newNachosBlocks = Array.tabulateVar<[var ?Types.NachosOrderRecord]>(
      oldNachosBlocks.size(),
      func(i : Nat) : [var ?Types.NachosOrderRecord] {
        migrateNachosOrderBlock(oldNachosBlocks[i]);
      },
    );

    {
      completedOrders = {
        var data_blocks = newBlocks;
        var i_block = old.completedOrders.i_block;
        var i_element = old.completedOrders.i_element;
      };
      nachosCompletedOrders = {
        var data_blocks = newNachosBlocks;
        var i_block = old.nachosCompletedOrders.i_block;
        var i_element = old.nachosCompletedOrders.i_element;
      };
    };
  };
};
