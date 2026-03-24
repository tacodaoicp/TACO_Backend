import NachosTypes "./nachos_vault_types";

module {

  // ═══════════════════════════════════════════════════════
  // INTERNAL REPRESENTATIONS (match current deployed state)
  // ═══════════════════════════════════════════════════════

  // Map<Text, ActiveDeposit> bucket representation
  type DepositBucket = (
    [var ?Text],
    [var ?NachosTypes.ActiveDeposit],
    [var Nat],
    [var Nat32],
  );

  // Vector<MintRecord> internal representation
  type MintHistoryVector = {
    var data_blocks : [var [var ?NachosTypes.MintRecord]];
    var i_block : Nat;
    var i_element : Nat;
  };

  // ═══════════════════════════════════════════════════════
  // STATE TYPES (field names match stable variable names)
  // Previous migration (fromSubaccount + recipient) already applied.
  // This is now an identity migration.
  // ═══════════════════════════════════════════════════════

  public type OldState = {
    activeDeposits : [var ?DepositBucket];
    mintHistory : MintHistoryVector;
  };

  public type NewState = {
    activeDeposits : [var ?DepositBucket];
    mintHistory : MintHistoryVector;
  };

  public func migrate(old : OldState) : NewState {
    old;
  };
};
