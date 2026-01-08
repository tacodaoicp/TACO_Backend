import Principal "mo:base/Principal";
import Array "mo:base/Array";

module {
  // Old types (before penaltyMultiplier)
  public type Allocation = {
    token : Principal;
    basisPoints : Nat;
  };

  public type AllocationChangeType = {
    #UserUpdate: {userInitiated: Bool};
    #FollowAction: {followedUser: Principal};
    #SystemRebalance;
    #VotingPowerChange;
  };

  // Old NeuronAllocationChangeRecord without penaltyMultiplier
  public type OldNeuronAllocationChangeRecord = {
    timestamp: Int;
    neuronId: Blob;
    changeType: AllocationChangeType;
    oldAllocations: [Allocation];
    newAllocations: [Allocation];
    votingPower: Nat;
    maker: Principal;
    reason: ?Text;
  };

  // New NeuronAllocationChangeRecord with penaltyMultiplier
  public type NewNeuronAllocationChangeRecord = {
    timestamp: Int;
    neuronId: Blob;
    changeType: AllocationChangeType;
    oldAllocations: [Allocation];
    newAllocations: [Allocation];
    votingPower: Nat;
    maker: Principal;
    reason: ?Text;
    penaltyMultiplier: ?Nat; // null = no penalty, ?23 = 77% penalty
  };

  // Old state structure
  public type OldState = {
    neuronAllocationChanges : [var ?OldNeuronAllocationChangeRecord];
  };

  // New state structure
  public type NewState = {
    neuronAllocationChanges : [var ?NewNeuronAllocationChangeRecord];
  };

  // Migration function
  public func migrate(oldState : OldState) : NewState {
    let oldChanges = oldState.neuronAllocationChanges;
    let newChanges : [var ?NewNeuronAllocationChangeRecord] = Array.init(oldChanges.size(), null);

    for (i in oldChanges.keys()) {
      switch (oldChanges[i]) {
        case (?oldRecord) {
          newChanges[i] := ?{
            timestamp = oldRecord.timestamp;
            neuronId = oldRecord.neuronId;
            changeType = oldRecord.changeType;
            oldAllocations = oldRecord.oldAllocations;
            newAllocations = oldRecord.newAllocations;
            votingPower = oldRecord.votingPower;
            maker = oldRecord.maker;
            reason = oldRecord.reason;
            penaltyMultiplier = null; // null means no penalty (legacy records)
          };
        };
        case (null) {
          newChanges[i] := null;
        };
      };
    };

    { neuronAllocationChanges = newChanges };
  };
};
