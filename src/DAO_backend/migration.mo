import Principal "mo:base/Principal";
import Array "mo:base/Array";

module {
  public type Allocation = {
    token : Principal;
    basisPoints : Nat;
  };

  public type NeuronVP = {
    neuronId : Blob;
    votingPower : Nat;
  };

  // Old UserState: pastAllocations without note
  public type OldUserState = {
    allocations : [Allocation];
    votingPower : Nat;
    lastVotingPowerUpdate : Int;
    lastAllocationUpdate : Int;
    lastAllocationMaker : Principal;
    pastAllocations : [{
      from : Int;
      to : Int;
      allocation : [Allocation];
      allocationMaker : Principal;
    }];
    allocationFollows : [{ since : Int; follow : Principal }];
    allocationFollowedBy : [{ since : Int; follow : Principal }];
    followUnfollowActions : [Int];
    neurons : [NeuronVP];
  };

  // New UserState: pastAllocations with note
  public type NewUserState = {
    allocations : [Allocation];
    votingPower : Nat;
    lastVotingPowerUpdate : Int;
    lastAllocationUpdate : Int;
    lastAllocationMaker : Principal;
    pastAllocations : [{
      from : Int;
      to : Int;
      allocation : [Allocation];
      allocationMaker : Principal;
      note : ?Text;
    }];
    allocationFollows : [{ since : Int; follow : Principal }];
    allocationFollowedBy : [{ since : Int; follow : Principal }];
    followUnfollowActions : [Int];
    neurons : [NeuronVP];
  };

  // Raw Map type from mo:map
  public type OldState = {
    userStates : [var ?(
      keys : [var ?Principal],
      values : [var ?OldUserState],
      indexes : [var Nat],
      bounds : [var Nat32],
    )];
  };

  public type NewState = {
    userStates : [var ?(
      keys : [var ?Principal],
      values : [var ?NewUserState],
      indexes : [var Nat],
      bounds : [var Nat32],
    )];
  };

  public func migrate(oldState : OldState) : NewState {
    let oldMap = oldState.userStates;
    let newMap : [var ?(
      keys : [var ?Principal],
      values : [var ?NewUserState],
      indexes : [var Nat],
      bounds : [var Nat32],
    )] = Array.init(oldMap.size(), null);

    for (i in oldMap.keys()) {
      switch (oldMap[i]) {
        case (?(keys, values, indexes, bounds)) {
          let newValues : [var ?NewUserState] = Array.init(values.size(), null);
          for (j in values.keys()) {
            switch (values[j]) {
              case (?oldUser) {
                newValues[j] := ?{
                  allocations = oldUser.allocations;
                  votingPower = oldUser.votingPower;
                  lastVotingPowerUpdate = oldUser.lastVotingPowerUpdate;
                  lastAllocationUpdate = oldUser.lastAllocationUpdate;
                  lastAllocationMaker = oldUser.lastAllocationMaker;
                  pastAllocations = Array.map<
                    { from : Int; to : Int; allocation : [Allocation]; allocationMaker : Principal },
                    { from : Int; to : Int; allocation : [Allocation]; allocationMaker : Principal; note : ?Text }
                  >(oldUser.pastAllocations, func (pa) {
                    {
                      from = pa.from;
                      to = pa.to;
                      allocation = pa.allocation;
                      allocationMaker = pa.allocationMaker;
                      note = null;
                    }
                  });
                  allocationFollows = oldUser.allocationFollows;
                  allocationFollowedBy = oldUser.allocationFollowedBy;
                  followUnfollowActions = oldUser.followUnfollowActions;
                  neurons = oldUser.neurons;
                };
              };
              case null {
                newValues[j] := null;
              };
            };
          };
          newMap[i] := ?(keys, newValues, indexes, bounds);
        };
        case null {
          newMap[i] := null;
        };
      };
    };

    { userStates = newMap };
  };
};
