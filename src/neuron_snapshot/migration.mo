import T "./ns_types";
import Array "mo:base/Array";

module {
  // Old CumulativeVP type without raw fields
  public type OldCumulativeVP = {
    total_staked_vp : Nat;
    total_staked_vp_by_hotkey_setters : Nat;
  };

  // Map internal representation (tuple with labeled elements)
  public type OldMapData = (
    [var ?T.SnapshotId],  // keys
    [var ?OldCumulativeVP],  // values
    [var Nat],  // indexes
    [var Nat32],  // bounds
  );

  public type NewMapData = (
    [var ?T.SnapshotId],  // keys
    [var ?T.CumulativeVP],  // values
    [var Nat],  // indexes
    [var Nat32],  // bounds
  );

  // Old state (snapshotCumulativeValues with old CumulativeVP)
  public type OldState = {
    snapshotCumulativeValues : [var ?OldMapData];
  };

  // New state (snapshotCumulativeValues with new CumulativeVP)
  public type NewState = {
    snapshotCumulativeValues : [var ?NewMapData];
  };

  public func migrate(oldState : OldState) : NewState {
    let oldMap = oldState.snapshotCumulativeValues;
    let newMap : [var ?NewMapData] = [var null];

    // Process each bucket in the map
    switch (oldMap[0]) {
      case (null) {
        newMap[0] := null;
      };
      case (?bucket) {
        let oldKeys = bucket.0;
        let oldValues = bucket.1;
        let indexes = bucket.2;
        let bounds = bucket.3;

        let newValues : [var ?T.CumulativeVP] = Array.init(oldValues.size(), null);

        for (i in oldValues.keys()) {
          switch (oldValues[i]) {
            case (?oldVP) {
              newValues[i] := ?{
                total_staked_vp = oldVP.total_staked_vp;
                total_staked_vp_by_hotkey_setters = oldVP.total_staked_vp_by_hotkey_setters;
                // Old snapshots didn't have penalties, so raw = effective
                total_staked_vp_raw = ?oldVP.total_staked_vp;
                total_staked_vp_by_hotkey_setters_raw = ?oldVP.total_staked_vp_by_hotkey_setters;
              };
            };
            case (null) {
              newValues[i] := null;
            };
          };
        };

        newMap[0] := ?(oldKeys, newValues, indexes, bounds);
      };
    };

    { snapshotCumulativeValues = newMap };
  };
};
