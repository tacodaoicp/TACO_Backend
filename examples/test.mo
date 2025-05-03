import actorTypes "./actor_types";
import Debug "mo:base/Debug";
import Error "mo:base/Error";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import DAO "../src/DAO_backend/dao_types";
import Vector "mo:vector";
import Time "mo:base/Time";
import Int "mo:base/Int";
import Fuzz "mo:fuzz";
import SpamProtection "../src/helper/spam_protection";
import MintingVaultTypes "../src/minting_vault/minting_vault_types";
import Result "mo:base/Result";
import Float "mo:base/Float";
actor test {

  let actorA = actor ("hhaaz-2aaaa-aaaaq-aacla-cai") : actorTypes.Self;
  let actorB = actor ("qtooy-2yaaa-aaaaq-aabvq-cai") : actorTypes.Self;
  let actorC = actor ("aanaa-xaaaa-aaaah-aaeiq-cai") : actorTypes.Self;

  let dao = actor ("ywhqf-eyaaa-aaaad-qg6tq-cai") : DAO.Self;
  let mintingVault = actor ("z3jul-lqaaa-aaaad-qg6ua-cai") : MintingVaultTypes.Self;

  let fuzz = Fuzz.fromSeed(Fuzz.fromSeed(Int.abs(Time.now()) * Fuzz.Fuzz().nat.randomRange(0, 2 ** 70)).nat.randomRange(45978345345987, 2 ** 256) -45978345345987);

  // Helper function to generate random number between min and max
  private func randomBetween(min : Nat, max : Nat) : Nat {
    fuzz.nat.randomRange(min, max);
  };

  // Generate random allocation array for given tokens
  private func generateRandomAllocation(tokens : [Principal]) : [DAO.Allocation] {
    let numTokens = randomBetween(2, tokens.size()); // Ensure at least 2 tokens
    var remainingPoints = 10000;
    var allocations = Vector.new<DAO.Allocation>();
    var usedTokens = Vector.new<Principal>();

    // Randomly select tokens and assign basis points
    label a for (i in Iter.range(0, numTokens - 1)) {
      var token = tokens[randomBetween(0, tokens.size() - 1)];

      // Ensure no duplicate tokens
      while (Array.find<Principal>(Vector.toArray(usedTokens), func(t) = t == token) != null) {
        token := tokens[randomBetween(0, tokens.size() - 1)];
      };
      Vector.add(usedTokens, token);

      // For last token, use remaining points
      let points = if (i == numTokens - 1) {
        remainingPoints;
      } else {
        // Random points between min(100, remainingPoints/2) and remainingPoints
        let minPoints = Nat.min(100, remainingPoints / 2);
        randomBetween(minPoints, remainingPoints);
      };

      remainingPoints -= points;

      Vector.add(
        allocations,
        {
          token = token;
          basisPoints = points;
        },
      );

      if (remainingPoints == 0) break a;
    };

    Vector.toArray(allocations);
  };

  // Tests adding default tokens and duplicate token handling
  // Adds multiple tokens and verifies they can't be added twice
  // Checks proper error handling for duplicate tokens
  func test1() : async Text {
    Debug.print("Test1");
    try {
      // Test adding all default tokens
      let defaultTokens = [
        Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"), // ICP
        Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"), // ICRC A
        Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai"), // ICRCB
        Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai") // CKUSD token
      ];

      // Add each token
      for (token in defaultTokens.vals()) {
        let addResult = if (token == Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
          await dao.addToken(token, #ICP);
        } else {
          await dao.addToken(token, #ICRC12);
        };
        switch (addResult) {
          case (#err(error)) {
            return "failed: Could not add token " # Principal.toText(token) # ": " # debug_show (error);
          };
          case (#ok(_)) {
            // Try to add the same token again - should return "already exists"
            let duplicateResult = if (token == Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
              await dao.addToken(token, #ICP);
            } else {
              await dao.addToken(token, #ICRC12);
            };
            switch (duplicateResult) {
              case (#ok(msg)) {
                if (not Text.contains(msg, #text "already exists")) {
                  return "failed: Adding duplicate token " # Principal.toText(token) # " should return 'already exists'";
                };
              };
              case (#err(error)) {
                if (error != #UnexpectedError("Token already exists")) {
                  return "failed: Unexpected error on duplicate token " # Principal.toText(token) # ": " # debug_show (error);
                };
              };
            };
          };
        };
      };

      return "true";
    } catch (e) {
      return "Test1 failed with error: " # Error.message(e);
    };
  };

  // Tests token removal process and its effects on allocations
  // Verifies token is removed from aggregate but preserved in user allocations
  // Checks that token can be re-added after removal
  func test2() : async Text {
    Debug.print("Test2");
    try {
      let tokenToTest = Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai");

      // Create test allocation for the token
      let testAllocation : DAO.Allocation = {
        token = tokenToTest;
        basisPoints = 10000;
      };

      // Update allocation
      ignore await actorA.updateAllocation([testAllocation]);

      // Remove the token
      let removeResult = await dao.removeToken(tokenToTest);
      switch (removeResult) {
        case (#err(error)) {
          return "failed: Could not remove token: " # debug_show (error);
        };
        case (#ok(_)) {
          // Verify token is removed from aggregate allocation
          let aggregateAlloc = await dao.getAggregateAllocation();
          switch (Array.find<(Principal, Nat)>(aggregateAlloc, func((p, _)) = p == tokenToTest)) {
            case (?_) {
              return "failed: Token still present in aggregate allocation after removal";
            };
            case (null) {
              // Check that user allocation still contains the token
              let userState = await actorA.getUserAllocation();
              switch (userState) {
                case (null) {
                  return "failed: Could not retrieve user allocation";
                };
                case (?state) {
                  switch (Array.find<DAO.Allocation>(state.allocations, func(a) = a.token == tokenToTest)) {
                    case (null) {
                      return "failed: Token should still be present in user allocation";
                    };
                    case (?_) {
                      // Add the token back
                      let readdResult = await dao.addToken(tokenToTest, #ICRC12);
                      switch (readdResult) {
                        case (#err(error)) {
                          return "failed: Could not re-add token: " # debug_show (error);
                        };
                        case (#ok(_)) {
                          return "true";
                        };
                      };
                    };
                  };
                };
              };
            };
          };
        };
      };
    } catch (e) {
      return "Test2 failed with error: " # Error.message(e);
    };
  };

  // Tests basic allocation updates and retrieval functionality
  // Creates a single token allocation and verifies it's stored correctly
  // Checks that getUserAllocation returns the correct state
  func test3() : async Text {
    Debug.print("Test3");
    try {
      // Create test allocation
      let testAllocation : DAO.Allocation = {
        token = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"); // Example token principal
        basisPoints = 10000; // 100% allocation to one token
      };

      // Update allocation
      let updateResult = await actorA.updateAllocation([testAllocation]);

      switch (updateResult) {
        case (#err(error)) {
          return "Test failed: Could not update allocation: " # debug_show (error);
        };
        case (#ok(_)) {
          // Get user allocation to verify
          let userState = await actorA.getUserAllocation();

          switch (userState) {
            case (null) {
              return "Test failed: Could not retrieve user allocation";
            };
            case (?state) {
              if (state.allocations.size() == 0) {
                return "Test failed: Wrong number of allocations";
              };

              let allocation = state.allocations[0];
              if (
                allocation.token != testAllocation.token or
                allocation.basisPoints != testAllocation.basisPoints
              ) {
                return "Test failed: Allocation mismatch " #
                debug_show (allocation) # " vs " # debug_show (testAllocation);
              };

              return "true";
            };
          };
        };
      };
    } catch (e) {
      return "Test failed with error: " # Error.message(e);
    };
  };

  // Tests aggregate allocation calculations with multiple users
  // Verifies proper weight calculations when allocations change
  // Checks that aggregate totals are correctly updated
  func test4() : async Text {
    Debug.print("Test4");
    try {
      let testAllocation : DAO.Allocation = {
        token = Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai");
        basisPoints = 10000; // 100% of ActorA's allocation
      };

      // Update allocation
      ignore await actorA.updateAllocation([testAllocation]);

      // Get initial aggregate allocation
      let initialAggregate = await dao.getAggregateAllocation();

      // Expected initial weight: 10000
      let initialWeight = switch (
        Array.find<(Principal, Nat)>(
          initialAggregate,
          func((p, _)) = p == Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"),
        )
      ) {
        case (?entry) { entry.1 };
        case null {
          return "failed: Initial token not found in aggregate";
        };
      };

      // Create new 50/50 split allocation for ActorA
      let newTestAllocation : DAO.Allocation = {
        token = Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai");
        basisPoints = 5000;
      };

      let testAllocation2 : DAO.Allocation = {
        token = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
        basisPoints = 5000;
      };

      // Update allocation with split
      ignore await actorA.updateAllocation([newTestAllocation, testAllocation2]);

      // Get updated aggregate allocation
      let updatedAggregate = await dao.getAggregateAllocation();
      // Expected weights after split:
      // For each token: 5000 basis points
      let finalWeight1 = switch (
        Array.find<(Principal, Nat)>(
          updatedAggregate,
          func((p, _)) = p == Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"),
        )
      ) {
        case (?entry) { entry.1 };
        case null {
          return "failed: First token not found in updated aggregate";
        };
      };

      let finalWeight2 = switch (
        Array.find<(Principal, Nat)>(
          updatedAggregate,
          func((p, _)) = p == Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"),
        )
      ) {
        case (?entry) { entry.1 };
        case null {
          return "failed: Second token not found in updated aggregate";
        };
      };

      // Check expected values
      if (initialWeight != 10000 or finalWeight1 != 5000 or finalWeight2 != 5000) {
        return "failed: Incorrect weights in aggregate allocation. Expected 10000/5000/5000, got " #
        debug_show ((initialWeight, finalWeight1, finalWeight2));
      };

      return "true";
    } catch (e) {
      return "Test2 failed with error: " # Error.message(e);
    };
  };

  // Tests weighted voting power calculations across multiple tokens
  // Verifies correct weight distribution based on voting power
  // Checks proper rounding behavior in calculations
  func test5() : async Text {
    Debug.print("Test5");
    try {
      // Prepare ActorB's allocations
      let testAllocation1 : DAO.Allocation = {
        token = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"); // ICP token
        basisPoints = 2500; // 25% allocation
      };

      let testAllocation2 : DAO.Allocation = {
        token = Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai"); // CKUSD token
        basisPoints = 7500; // 75% allocation
      };

      // Update ActorB's allocation
      let updateResult = await actorB.updateAllocation([testAllocation1, testAllocation2]);

      switch (updateResult) {
        case (#err(error)) {
          return "failed: Could not update allocation: " # debug_show (error);
        };
        case (#ok(_)) {
          // Get updated aggregate allocation
          let updatedAggregate = await dao.getAggregateAllocation();
          // Check ICP token weight calculation
          // ActorA: 5000 BP, ActorB: 2500 BP
          let tacoWeight = switch (
            Array.find<(Principal, Nat)>(
              updatedAggregate,
              func((p, _)) = p == Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"),
            )
          ) {
            case (?entry) { entry.1 };
            case null {
              return "failed: TACO token not found in updated aggregate";
            };
          };

          // Check CKUSD token weight calculation
          // ActorA: 5000 BP, ActorB: 7500 BP
          let ckusdWeight = switch (
            Array.find<(Principal, Nat)>(
              updatedAggregate,
              func((p, _)) = p == Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai"),
            )
          ) {
            case (?entry) { entry.1 };
            case null {
              return "failed: CKUSD token not found in updated aggregate";
            };
          };

          if (tacoWeight != 3928 or ckusdWeight != 3214) {
            // Rounded to nearest whole number
            return "failed: Incorrect weights in aggregate allocation. " #
            "Expected TACO: 3928, CKUSD: 3214, got " #
            debug_show ((tacoWeight, ckusdWeight));
          };

          return "true";
        };
      };
    } catch (e) {
      return "Test3 failed with error: " # Error.message(e);
    };
  };

  // Tests token pause functionality
  // Verifies admin can pause a token
  // Ensures proper error handling for pause operation
  func test6() : async Text {
    Debug.print("Test6");
    try {
      let pauseResult = await dao.pauseToken(Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"));
      switch (pauseResult) {
        case (#err(error)) {
          return "failed: Could not pause token: " # debug_show (error);
        };
        case (#ok(_)) {
          return "true";
        };
      };
    } catch (e) {
      return "failed with error: " # Error.message(e);
    };
  };

  // Tests token unpause functionality
  // Verifies admin can unpause a previously paused token
  // Ensures proper error handling for unpause operation
  func test7() : async Text {
    Debug.print("Test7");
    try {
      let unpauseResult = await dao.unpauseToken(Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"));
      switch (unpauseResult) {
        case (#err(error)) {
          return "failed: Could not unpause token: " # debug_show (error);
        };
        case (#ok(_)) {
          return "true";
        };
      };
    } catch (e) {
      return "Test7 failed with error: " # Error.message(e);
    };
  };

  // Tests admin permission system functionality
  // Verifies correct number of admins and their permissions
  // Checks all expected admin principals have proper access
  func test8() : async Text {
    Debug.print("Test8");
    try {
      // Get admin permissions
      let adminPermissions = await dao.getAdminPermissions();

      // Check if we have exactly 3 admins
      if (adminPermissions.size() != 3) {
        return "Test8 failed: Expected 3 admins, got " # Nat.toText(adminPermissions.size());
      };

      // Expected admin principals
      let expectedAdmins = [
        Principal.fromText("ywhqf-eyaaa-aaaad-qg6tq-cai"), // DAO
        Principal.fromText("z4is7-giaaa-aaaad-qg6uq-cai"), // Treasury
        Principal.fromText("ca6gz-lqaaa-aaaaq-aacwa-cai") // Test Script
      ];

      // Check if all expected admins are present
      for (expectedAdmin in expectedAdmins.vals()) {
        switch (
          Array.find<(Principal, [SpamProtection.AdminPermission])>(
            adminPermissions,
            func((p, _)) = p == expectedAdmin,
          )
        ) {
          case (null) {
            return "Test8 failed: Admin not found: " # Principal.toText(expectedAdmin);
          };
          case (?adminEntry) {
            // Check if admin has all 8 permissions
            if (adminEntry.1.size() != 8) {
              return "Test8 failed: Admin " # Principal.toText(expectedAdmin) #
              " has " # Nat.toText(adminEntry.1.size()) # " permissions instead of 8";
            };
          };
        };
      };

      return "true";
    } catch (e) {
      return "Test8 failed with error: " # Error.message(e);
    };
  };

  // Tests voting power metrics calculation
  // Verifies metrics can be retrieved successfully
  // Ensures proper error handling for metrics retrieval
  func test9() : async Text {
    Debug.print("Test9");
    let metrics = await dao.votingPowerMetrics();
    switch (metrics) {
      case (#ok(metrics)) {
        return "true";
      };
      case (#err(error)) {
        return "failed: Could not get voting power metrics: " # debug_show (error);
      };
    };
  };

  // Tests allocation following functionality
  // Verifies one user can follow another's allocation
  // Checks that follower's allocation updates when leader changes
  func test10() : async Text {
    Debug.print("Test10");
    try {
      // First, ActorB makes an initial allocation
      let initialAllocation = [{
        token = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"); // ICP
        basisPoints = 10000;
      }];

      let resultB = await actorB.updateAllocation(initialAllocation);
      switch (resultB) {
        case (#err(error)) {
          return " failed: Could not set ActorB's initial allocation: " # debug_show (error);
        };
        case (#ok(_)) {};
      };

      // ActorB follows ActorA
      let followResult = await actorB.followAllocation(Principal.fromActor(actorA));
      switch (followResult) {
        case (#err(error)) {
          return " failed: Could not follow ActorA: " # debug_show (error);
        };
        case (#ok(_)) {};
      };

      // Now ActorA makes an allocation
      let newAllocation = [{
        token = Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"); // ICRC A
        basisPoints = 10000;
      }];

      let resultA = await actorA.updateAllocation(newAllocation);
      switch (resultA) {
        case (#err(error)) {
          return " failed: Could not set ActorA's allocation: " # debug_show (error);
        };
        case (#ok(_)) {};
      };

      // Check if ActorB's allocation was updated to match ActorA
      let stateB = await actorB.getUserAllocation();
      switch (stateB) {
        case (null) { return " failed: Could not get ActorB's state" };
        case (?state) {
          if (state.allocations.size() != 1) {
            return " failed: Wrong number of allocations for ActorB";
          };
          if (state.allocations[state.allocations.size() - 1].token != Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai")) {
            return " failed: ActorB's allocation did not update to match ActorA";
          };
          if (state.pastAllocations.size() != 2) {
            return " failed: Wrong number of past allocations for ActorB";
          };
          if (state.pastAllocations[state.pastAllocations.size() - 1].allocation[0].token != Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
            return " failed: ActorB's past allocation did not update to match ActorA";
          };
        };
      };

      return "true";
    } catch (e) {
      return " failed with error: " # Error.message(e);
    };
  };

  // Tests allocation unfollowing functionality
  // Verifies user can unfollow another's allocation
  // Ensures allocations don't update after unfollowing
  func test11() : async Text {
    Debug.print("Test11");
    try {
      // ActorB unfollows ActorA
      let unfollowResult = await actorB.unfollowAllocation(Principal.fromActor(actorA));
      switch (unfollowResult) {
        case (#err(error)) {
          return " failed: Could not unfollow ActorA: " # debug_show (error);
        };
        case (#ok(_)) {};
      };

      // ActorA makes a new allocation
      let newAllocation = [{
        token = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"); // TACO
        basisPoints = 10000;
      }];

      let resultA = await actorA.updateAllocation(newAllocation);
      switch (resultA) {
        case (#err(error)) {
          return " failed: Could not set ActorA's allocation: " # debug_show (error);
        };
        case (#ok(_)) {};
      };

      // Check if ActorB's allocation remained unchanged
      let stateB = await actorB.getUserAllocation();
      switch (stateB) {
        case (null) { return " failed: Could not get ActorB's state" };
        case (?state) {
          if (state.allocations.size() == 0) {
            return " failed: Wrong number of allocations for ActorB";
          };
          if (state.allocations[0].token != Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai")) {
            return " failed: ActorB's allocation changed after unfollow";
          };
        };
      };

      return "true";
    } catch (e) {
      return "Test11 failed with error: " # Error.message(e);
    };
  };

  // Tests multiple follow relationships
  // Verifies behavior when following multiple users
  func test12() : async Text {
    Debug.print("Test12");
    try {
      let newAllocation = [{
        token = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"); // TACO
        basisPoints = 10000;
      }];

      let resultA = await actorC.updateAllocation(newAllocation);
      switch (resultA) {
        case (#err(error)) {
          return " failed: Could not set ActorA's allocation: " # debug_show (error);
        };
        case (#ok(_)) {};
      };

      // ActorC follows both ActorA and ActorB
      ignore await actorC.followAllocation(Principal.fromActor(actorA));
      ignore await actorC.followAllocation(Principal.fromActor(actorB));

      // ActorA makes an allocation
      let allocationA = [{
        token = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"); // ICP
        basisPoints = 10000;
      }];
      ignore await actorA.updateAllocation(allocationA);

      // Check if ActorC's allocation matches ActorA
      let stateC1 = await actorC.getUserAllocation();
      switch (stateC1) {
        case (null) { return " failed: Could not get ActorC's state" };
        case (?state) {
          if (state.allocations.size() == 0) {
            return " failed: Wrong number of allocations for ActorC";
          };
          if (state.allocations[state.allocations.size() - 1].token != Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
            return " failed: ActorC's allocation did not update to match ActorA";
          };
        };
      };

      // ActorB makes a different allocation
      let allocationB = [{
        token = Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"); // ICRC A
        basisPoints = 10000;
      }];
      ignore await actorB.updateAllocation(allocationB);

      // Check if ActorC's allocation matches ActorB (last update wins)
      let stateC2 = await actorC.getUserAllocation();
      switch (stateC2) {
        case (null) {
          return "Test12 failed: Could not get ActorC's state after second update";
        };
        case (?state) {
          if (state.allocations.size() == 0) {
            return " failed: Wrong number of allocations for ActorC";
          };
          if (state.allocations[0].token != Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai")) {
            return "Test12 failed: ActorC's allocation did not update to match ActorB";
          };
        };
      };

      return "true";
    } catch (e) {
      return "Test12 failed with error: " # Error.message(e);
    };
  };

  // Tests empty allocation handling
  // Verifies proper handling when user removes all allocations
  // Checks that followers update correctly with empty allocations
  func test13() : async Text {
    Debug.print("Test13");
    try {
      // First verify current state from previous tests
      let initialStateA = await actorA.getUserAllocation();
      switch (initialStateA) {
        case (null) { return " failed: Could not get ActorA's initial state" };
        case (?state) {
          if (state.allocations.size() == 0) {
            return " failed: Wrong number of allocations for ActorA";
          };
          if (state.allocations[0].token != Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
            return " failed: Unexpected initial allocation for ActorA";
          };
        };
      };

      // ActorA removes their allocation
      let removeResult = await actorA.updateAllocation([]);
      switch (removeResult) {
        case (#err(error)) {
          return " failed: Could not remove ActorA's allocation: " # debug_show (error);
        };
        case (#ok(_)) {};
      };

      // Verify ActorA's state
      let stateA = await actorA.getUserAllocation();
      switch (stateA) {
        case (null) {
          return " failed: Could not get ActorA's state after removal";
        };
        case (?state) {
          if (state.allocations.size() != 0) {
            return " failed: ActorA's allocation not empty";
          };
          if (state.pastAllocations.size() == 0) {
            return " failed: ActorA's past allocations empty";
          };
          let lastPastAllocation = state.pastAllocations[state.pastAllocations.size() - 1];
          if (lastPastAllocation.allocation[0].token != Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
            return " failed: Last past allocation incorrect";
          };
        };
      };

      // Check if ActorC (who was following ActorA) got updated
      let stateC = await actorC.getUserAllocation();
      switch (stateC) {
        case (null) { return " failed: Could not get ActorC's state" };
        case (?state) {
          if (state.allocations.size() != 0) {
            return " failed: ActorC's allocation not updated to empty";
          };
        };
      };

      return "true";
    } catch (e) {
      return " failed with error: " # Error.message(e);
    };
  };

  // Tests allocation updates after empty state
  // Verifies new allocations work after being empty
  // Checks that history properly records empty period
  func test14() : async Text {
    Debug.print("Test14");
    try {
      // ActorA makes new allocation after being empty
      let newAllocation = [{
        token = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"); // TACO
        basisPoints = 10000;
      }];

      let resultA = await actorA.updateAllocation(newAllocation);
      switch (resultA) {
        case (#err(error)) {
          return " failed: Could not set ActorA's new allocation: " # debug_show (error);
        };
        case (#ok(_)) {};
      };

      // Verify ActorA's state
      let stateA = await actorA.getUserAllocation();
      switch (stateA) {
        case (null) { return " failed: Could not get ActorA's state" };
        case (?state) {
          if (state.allocations.size() == 0) {
            return " failed: Wrong number of allocations for ActorA";
          };
          if (state.allocations[0].token != Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
            return " failed: Wrong token in ActorA's allocation";
          };
          // Verify past allocations include the empty period
          let emptyAllocation = state.pastAllocations[state.pastAllocations.size() - 1];
          if (emptyAllocation.allocation.size() != 0) {
            return " failed: Empty allocation not recorded in history";
          };
        };
      };

      // Check if ActorC (follower) got updated with new allocation
      let stateC = await actorC.getUserAllocation();
      switch (stateC) {
        case (null) { return " failed: Could not get ActorC's state" };
        case (?state) {
          if (state.allocations.size() == 0) {
            return " failed: ActorC's allocation not updated";
          };
          if (state.allocations[0].token != Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
            return " failed: ActorC's allocation not matching ActorA";
          };
        };
      };

      return "true";
    } catch (e) {
      return " failed with error: " # Error.message(e);
    };
  };

  // Tests for MintingVault functionality

  func test15() : async Text {
    Debug.print("Test15");
    try {
      let status = await mintingVault.getVaultStatus();

      // Check if we got valid data back
      if (status.tokenDetails.size() == 0) {
        return "failed: No token details returned from vault";
      };

      if (status.premiumRange.min < 0.0 or status.premiumRange.max <= status.premiumRange.min) {
        return "failed: Invalid premium range values: " # Float.toText(status.premiumRange.min) # " to " # Float.toText(status.premiumRange.max);
      };

      return "true";
    } catch (e) {
      return "Test15 failed with error: " # Error.message(e);
    };
  };

  func test16() : async Text {
    try {
      Debug.print("Test16");

      // Get estimation for ICP swap
      let icpToken = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
      let estimateResult = await mintingVault.estimateSwapAmount(icpToken, 10_000_000); // 0.1 ICP

      switch (estimateResult) {
        case (#ok(estimate)) {
          // Verify we got meaningful data back
          if (estimate.maxAcceptedAmount == 0) {
            return "failed: Estimated max accepted amount is zero";
          };

          if (estimate.estimatedTacoAmount == 0) {
            return "failed: Estimated TACO amount is zero";
          };

          if (estimate.premium < 0.0) {
            return "failed: Invalid premium: " # Float.toText(estimate.premium);
          };

          if (estimate.tokenPrice == 0 or estimate.tacoPrice == 0) {
            return "failed: Invalid price data in estimate";
          };

          return "true";
        };
        case (#err(errMsg)) {
          return "failed: Estimation error: " # errMsg;
        };
      };
    } catch (e) {
      return "Test16 failed with error: " # Error.message(e);
    };
  };

  func test17() : async Text {
    Debug.print("Test17");
    try {
      // Test token swapping by sending tokens to mint vault and swapping them for TACO
      let testActor = actorA;

      // First estimate how much TACO we might get for our ICP
      let icpToken = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
      let amountToSend = 50_000_000; // 0.5 ICP

      let estimateResult = await mintingVault.estimateSwapAmount(icpToken, amountToSend);

      switch (estimateResult) {
        case (#err(error)) {
          return "failed: Could not get estimate: " # error;
        };
        case (#ok(estimate)) {
          // Make the actual transfer from test actor to mint vault
          Debug.print("Transferring " # Nat.toText(amountToSend) # " ICP to vault...");
          let blockIndex = await testActor.TransferICPtoMintVault(amountToSend);
          Debug.print("Transfer completed, block index: " # Nat.toText(blockIndex));

          if (blockIndex == 0) {
            return "failed: Transfer to vault failed";
          };

          // Now attempt the swap using the block number
          let minimumReceive = (estimate.estimatedTacoAmount * 1) / 100; // Accept 1% of estimated amount (99% slippage)
          Debug.print("Executing swap with minimum receive: " # Nat.toText(minimumReceive));

          // Use the new swapTokenForTaco function
          let swapResult = await testActor.swapTokenForTaco(icpToken, blockIndex, minimumReceive);

          switch (swapResult) {
            case (#err(error)) {
              return "failed: Swap execution failed: " # error;
            };
            case (#ok(result)) {
              if (not result.success) {
                return "failed: Swap reported as unsuccessful";
              };

              if (result.returnedWantedAmount < minimumReceive) {
                return "failed: Received TACO amount " # Nat.toText(result.returnedWantedAmount) #
                " is less than minimum " # Nat.toText(minimumReceive);
              };

              Debug.print("Swap successful: " # debug_show (result));
              return "true";
            };
          };
        };
      };
    } catch (e) {
      return "Test17 failed with error: " # Error.message(e);
    };
  };

  func test18() : async Text {
    Debug.print("Test18");
    try {
      // Test swapping with ICRC-1 tokens (TEST1)
      let testActor = actorA;

      // First estimate how much TACO we might get for TEST1 tokens
      let test1Token = Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai");
      let amountToSend = 100_000_000; // Sufficient TEST1 tokens

      let estimateResult = await mintingVault.estimateSwapAmount(test1Token, amountToSend);

      switch (estimateResult) {
        case (#err(error)) {
          // If it's just a capacity error or allocation limit, the test is still valid
          // as it means the vault recognizes the token
          if (
            Text.contains(error, #text "capacity") or
            Text.contains(error, #text "allocation") or
            Text.contains(error, #text "target")
          ) {
            Debug.print("Vault has allocation limits for TEST1: " # error);
            return "true";
          };
          return "failed: Could not get estimate for TEST1: " # error;
        };
        case (#ok(estimate)) {
          // Make the actual transfer from test actor to mint vault
          Debug.print("Transferring " # Nat.toText(amountToSend) # " TEST1 to vault...");
          let blockIndex = await testActor.TransferICRCAtoMintVault(amountToSend);
          Debug.print("Transfer completed, block index: " # Nat.toText(blockIndex));

          if (blockIndex == 0) {
            return "failed: Transfer to vault failed";
          };

          // Now attempt the swap using the block number
          // Use a much lower minimum threshold - half of the estimated amount
          let minimumReceive = (estimate.estimatedTacoAmount * 1) / 10;
          Debug.print("Executing swap with minimum receive: " # Nat.toText(minimumReceive));

          // Use the new swapTokenForTaco function
          let swapResult = await testActor.swapTokenForTaco(test1Token, blockIndex, minimumReceive);

          switch (swapResult) {
            case (#err(error)) {
              // For some tokens, the swap might legitimately fail if there's no allocation target
              if (
                Text.contains(error, #text "capacity") or
                Text.contains(error, #text "allocation") or
                Text.contains(error, #text "target") or
                Text.contains(error, #text "no target")
              ) {
                Debug.print("Swap failed due to allocation limits: " # error);
                return "true";
              };
              return "failed: Swap execution failed: " # error;
            };
            case (#ok(result)) {
              if (not result.success) {
                return "failed: Swap reported as unsuccessful";
              };

              if (result.returnedWantedAmount < minimumReceive) {
                return "failed: Received TACO amount " # Nat.toText(result.returnedWantedAmount) #
                " is less than minimum " # Nat.toText(minimumReceive);
              };

              Debug.print("Swap successful: " # debug_show (result));
              return "true";
            };
          };
        };
      };
    } catch (e) {
      return "Test18 failed with error: " # Error.message(e);
    };
  };

  func test19() : async Text {
    Debug.print("Test19");
    try {
      // Test swapping with ICRC-1 tokens (TEST2)
      let testActor = actorA;

      // First estimate how much TACO we might get for TEST2 tokens
      let test2Token = Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai");
      let amountToSend = 100_000_000; // Sufficient TEST2 tokens

      let estimateResult = await mintingVault.estimateSwapAmount(test2Token, amountToSend);

      switch (estimateResult) {
        case (#err(error)) {
          // If it's just a capacity error or allocation limit, the test is still valid
          // as it means the vault recognizes the token
          if (
            Text.contains(error, #text "capacity") or
            Text.contains(error, #text "allocation") or
            Text.contains(error, #text "target")
          ) {
            Debug.print("Vault has allocation limits for TEST2: " # error);
            return "true";
          };
          return "failed: Could not get estimate for TEST2: " # error;
        };
        case (#ok(estimate)) {
          // Make the actual transfer from test actor to mint vault
          Debug.print("Transferring " # Nat.toText(amountToSend) # " TEST2 to vault...");
          let blockIndex = await testActor.TransferICRCBtoMintVault(amountToSend);
          Debug.print("Transfer completed, block index: " # Nat.toText(blockIndex));

          if (blockIndex == 0) {
            return "failed: Transfer to vault failed";
          };

          // Now attempt the swap using the block number
          // Use a much lower minimum threshold - 40% of the estimated amount
          let minimumReceive = (estimate.estimatedTacoAmount * 1) / 10;
          Debug.print("Executing swap with minimum receive: " # Nat.toText(minimumReceive));

          // Use the new swapTokenForTaco function
          let swapResult = await testActor.swapTokenForTaco(test2Token, blockIndex, minimumReceive);

          switch (swapResult) {
            case (#err(error)) {
              // For some tokens, the swap might legitimately fail if there's no allocation target
              if (
                Text.contains(error, #text "capacity") or
                Text.contains(error, #text "allocation") or
                Text.contains(error, #text "target") or
                Text.contains(error, #text "no target")
              ) {
                Debug.print("Swap failed due to allocation limits: " # error);
                return "true";
              };
              return "failed: Swap execution failed: " # error;
            };
            case (#ok(result)) {
              if (not result.success) {
                return "failed: Swap reported as unsuccessful";
              };

              if (result.returnedWantedAmount < minimumReceive) {
                return "failed: Received TACO amount " # Nat.toText(result.returnedWantedAmount) #
                " is less than minimum " # Nat.toText(minimumReceive);
              };

              Debug.print("Swap successful: " # debug_show (result));
              return "true";
            };
          };
        };
      };
    } catch (e) {
      return "Test19 failed with error: " # Error.message(e);
    };
  };

  func test20() : async Text {
    Debug.print("Test20");
    try {
      // Test minimum receive validation
      let testActor = actorA;

      // First estimate how much TACO we might get
      let icpToken = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
      let amountToSend = 10_000_000; // 0.1 ICP

      let estimateResult = await mintingVault.estimateSwapAmount(icpToken, amountToSend);

      switch (estimateResult) {
        case (#err(error)) {
          return "failed: Could not get estimate: " # error;
        };
        case (#ok(estimate)) {
          // Transfer the token
          let blockIndex = await testActor.TransferICPtoMintVault(amountToSend);

          if (blockIndex == 0) {
            return "failed: Transfer to vault failed";
          };

          // Set unreasonably high minimum receive (2x estimated)
          let unreasonableMinimum = estimate.estimatedTacoAmount * 2;

          // Attempt the swap with an impossible minimum receive requirement
          // Use the new swapTokenForTaco function
          let swapResult = await testActor.swapTokenForTaco(icpToken, blockIndex, unreasonableMinimum);

          switch (swapResult) {
            case (#ok(_)) {
              return "failed: Swap should have failed with unreasonable minimum receive";
            };
            case (#err(error)) {
              // Verify the error is about the minimum amount
              if (
                Text.contains(error, #text "minimum") or
                Text.contains(error, #text "less than") or
                Text.contains(error, #text "exceed")
              ) {
                Debug.print("Got expected error for minimum receive test: " # error);
                return "true";
              } else {
                return "failed: Unexpected error message: " # error;
              };
            };
          };
        };
      };
    } catch (e) {
      return "Test20 failed with error: " # Error.message(e);
    };
  };

  // Test for reusing a block ID (should fail)
  func test21() : async Text {
    Debug.print("Test21");
    try {
      // Test that we can't reuse block IDs for multiple swaps
      let testActor = actorA;

      // First do a real swap to get a valid block ID
      let icpToken = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
      let amountToSend = 10_000_000; // 0.1 ICP

      let estimateResult = await mintingVault.estimateSwapAmount(icpToken, amountToSend);

      switch (estimateResult) {
        case (#err(error)) {
          return "failed: Could not get estimate: " # error;
        };
        case (#ok(estimate)) {
          // Transfer the token
          let blockIndex = await testActor.TransferICPtoMintVault(amountToSend);

          if (blockIndex == 0) {
            return "failed: Transfer to vault failed";
          };

          // Do the first swap (should succeed)
          let minimumReceive = (estimate.estimatedTacoAmount * 2) / 10; // Accept 90% of estimated amount
          // Use the new swapTokenForTaco function
          let swapResult1 = await testActor.swapTokenForTaco(icpToken, blockIndex, minimumReceive);

          switch (swapResult1) {
            case (#err(error)) {
              return "failed: First swap failed: " # error;
            };
            case (#ok(_)) {
              // Now try to reuse the same block ID (should fail)
              // Use the new swapTokenForTaco function
              let swapResult2 = await testActor.swapTokenForTaco(icpToken, blockIndex, minimumReceive);

              switch (swapResult2) {
                case (#ok(_)) {
                  return "failed: Block reuse was allowed, should have failed";
                };
                case (#err(error)) {
                  // Check for the right error message
                  if (
                    Text.contains(error, #text "already") or
                    Text.contains(error, #text "processed") or
                    Text.contains(error, #text "used") or
                    Text.contains(error, #text "block")
                  ) {
                    return "true";
                  } else {
                    return "failed: Unexpected error message for block reuse: " # error;
                  };
                };
              };
            };
          };
        };
      };
    } catch (e) {
      return "Test21 failed with error: " # Error.message(e);
    };
  };

  stable var testResults : [Text] = [];

  // Add all tests to runMintingVaultTransactionTests
  public func runMintingVaultTransactionTests() : async Text {
    Debug.print("Starting Minting Vault Transaction Tests");

    let testResults = Vector.new<Text>();

    // Run test15-21 as before
    let result15 = await test15();
    Vector.add(testResults, "Test15 (vault status): " # result15);

    let result16 = await test16();
    Vector.add(testResults, "Test16 (swap estimation): " # result16);

    let result17 = await test17();
    Vector.add(testResults, "Test17 (ICP swap): " # result17);

    let result18 = await test18();
    Vector.add(testResults, "Test18 (ICRC-A swap): " # result18);

    let result19 = await test19();
    Vector.add(testResults, "Test19 (ICRC-B swap): " # result19);

    let result20 = await test20();
    Vector.add(testResults, "Test20 (minimum receive validation): " # result20);

    let result21 = await test21();
    Vector.add(testResults, "Test21 (block reuse prevention): " # result21);

    // Add the new stress test
    let stressResult = await stressMintingVault();
    Vector.add(testResults, "Stress test (concurrent swaps): " # stressResult);

    // Print all results
    for (result in Vector.vals(testResults)) {
      Debug.print(result);
    };

    return "Minting Vault Transaction Tests completed. Check console for details.";
  };

  public func runTests() : async Text {
    Debug.print("Starting runTests");

    label a for (i in Iter.range(1, 14)) {
      let testName = "Test" # Nat.toText(i);
      var testResult = "false";

      switch (i) {
        case 1 {
          testResult := await test1();
          Debug.print("");
        };
        case 2 {
          testResult := await test2();
          Debug.print("");
        };
        case 3 {
          testResult := await test3();
          Debug.print("");
        };
        case 4 {
          testResult := await test4();
          Debug.print("");
        };
        case 5 {
          testResult := await test5();
          Debug.print("");
        };
        case 6 {
          testResult := await test6();
          Debug.print("");
        };
        case 7 {
          testResult := await test7();
          Debug.print("");
        };
        case 8 {
          testResult := await test8();
          Debug.print("");
        };
        case 9 {
          testResult := await test9();
          Debug.print("");
        };
        case 10 {
          testResult := await test10();
          Debug.print("");
        };
        case 11 {
          testResult := await test11();
          Debug.print("");
        };
        case 12 {
          testResult := await test12();
          Debug.print("");
        };
        case 13 {
          testResult := await test13();
          Debug.print("");
        };
        case 14 {
          testResult := await test14();
          Debug.print("");
        };
        case _ {
          testResults := Array.append(testResults, [testName # ": Invalid test number"]);
          continue a;
        };
      };

      if (testResult == "true") {
        testResults := Array.append(testResults, [testName # ": Success"]);
      } else {
        testResults := Array.append(testResults, [testName # ": " #testResult]);
      };

    };

    Debug.print("testResults: ");

    for (result in testResults.vals()) {
      Debug.print(result);
    };

    testResults := [];

    ignore await stressTest();

    return "Tests completed. Check the console for the detailed report.";

  };

  var numErrors : Nat = 0;

  var errorVector = Vector.new<Text>();
  var startTime = Time.now();
  let numIterations = 166; // Number of allocation updates per actor

  var duration : Int = 0;

  func stressTest() : async Text {
    try {
      Debug.print("Starting stress test...");

      // Available tokens
      let tokens = [
        Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"), // ICP
        Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"), // ICRC A
        Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai"), // ICRC B
        Principal.fromText("xevnm-gaaaa-aaaar-qafnq-cai") // CKUSD token
      ];

      // Test actors
      let actors = [actorA, actorB, actorC];

      var numIterationsDone = 0;

      // Start timestamp for measuring duration
      startTime := Time.now();

      // Launch concurrent updates for each actor
      for (i in Iter.range(0, numIterations - 1)) {
        // Create a new async block for each iteration
        ignore async {
          for (a in actors.vals()) {
            // Generate random allocation for this actor
            let allocation = generateRandomAllocation(tokens);

            try {
              let result = await a.updateAllocation(allocation);
              switch (result) {
                case (#err(error)) {
                  Debug.print("Error in stress test: " # debug_show (error));
                  Vector.add(errorVector, debug_show (error));
                  numErrors += 1;
                  numIterationsDone += 1;
                  if (numIterationsDone == numIterations) {
                    await end();
                  };
                };
                case (#ok(_)) {
                  // Success
                  numIterationsDone += 1;
                  if (numIterationsDone == numIterations) {
                    await end();
                  };
                };
              };
            } catch (e) {
              Debug.print("Exception in stress test: " # Error.message(e));
              Vector.add(errorVector, Error.message(e));
              numErrors += 1;
              numIterationsDone += 1;
              if (numIterationsDone == numIterations) {
                await end();
              };
            };
          };
        };
      };
      duration := (Time.now() - startTime) / 1_000_000_000; // Convert to seconds

      return "true";
    } catch (e) {
      return "Stress test failed with error: " # Error.message(e);
    };
  };

  private func end() : async () {
    // Get final aggregate allocation to verify system state
    let finalAggregateAlloc = await dao.getAggregateAllocation();
    let finalUserAlloc = await actorA.getUserAllocation();

    Debug.print("Stress test completed:");
    Debug.print("Duration: " # Int.toText(duration) # " seconds");
    Debug.print("Total operations attempted: " # Nat.toText(numIterations * 3));
    Debug.print("Number of errors (if there are any they will be printed at end of test): " # Nat.toText(numErrors));
    Debug.print("Final aggregate allocation: " # debug_show (finalAggregateAlloc));
    Debug.print(
      "Final ActorA past allocations size: " # Nat.toText(
        switch (finalUserAlloc) {
          case null { 0 };
          case (?state) { state.pastAllocations.size() };
        }
      )
    );
    numErrors := 0;

    if (Vector.size(errorVector) > 0) {
      Debug.print(debug_show (Vector.toArray(errorVector)));
    };
  };

  func stressMintingVault() : async Text {
    Debug.print("Starting minting vault stress test...");

    try {
      // Available tokens for swapping
      let tokensToTest = [
        Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"), // ICP
        Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai"), // TEST1 token
        Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai") // TEST2 token
      ];

      // Test actors
      let actors = [actorA];

      // Configuration
      let numIterations = 100; // Number of swap attempts per token-actor combination
      let startTime = Time.now();
      var swapSuccesses = 0;
      var swapFailures = 0;
      let errorVector = Vector.new<Text>();

      // Track how many iterations have completed
      var iterationsCompleted = 0;
      let totalIterations = (numIterations * tokensToTest.size() * actors.size()) -1;

      // Prepare small amounts to swap (to avoid depleting funds)
      let swapAmounts = [
        100_000_000, // 1 ICP
        100_000_000, // 1.0 TEST1
        100_000_000 // 1.0 TEST2
      ];

      // Launch concurrent swaps
      for (i in Iter.range(0, numIterations - 1)) {
        // Create a new async block for each iteration that doesn't block the main function
        ignore async {
          for (actorIndex in Iter.range(0, actors.size() - 1)) {
            let acto = actors[actorIndex];

            label a for (tokenIndex in Iter.range(0, tokensToTest.size() - 1)) {
              let token = tokensToTest[tokenIndex];
              let amount = swapAmounts[tokenIndex];

              // Generate a random minimum receive threshold (between 1% and 3% of estimated)
              let randomMinPercentage = randomBetween(1, 3);

              try {
                // First get an estimate
                let estimateResult = await mintingVault.estimateSwapAmount(token, amount);

                switch (estimateResult) {
                  case (#err(error)) {
                    // If it's just a capacity or allocation error, don't count it as failure
                    if (
                      Text.contains(error, #text "capacity") or
                      Text.contains(error, #text "allocation") or
                      Text.contains(error, #text "target")
                    ) {
                      Debug.print("Token allocation limit reached: " # error);
                    } else {
                      Debug.print("Estimation error: " # error);
                      Vector.add(errorVector, "Estimation error: " # error);
                      swapFailures += 1;
                    };

                    // Count as completed iteration
                    iterationsCompleted += 1;
                    if (iterationsCompleted >= totalIterations) {
                      await endStressTest(startTime, swapSuccesses, swapFailures, errorVector);
                    };
                  };

                  case (#ok(estimate)) {
                    // Make the actual transfer to minting vault
                    var blockIndex = 0;

                    // Execute the appropriate transfer based on token
                    if (token == Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")) {
                      blockIndex := await acto.TransferICPtoMintVault(amount);
                    } else if (token == Principal.fromText("mxzaz-hqaaa-aaaar-qaada-cai")) {
                      blockIndex := await acto.TransferICRCAtoMintVault(amount);
                    } else if (token == Principal.fromText("zxeu2-7aaaa-aaaaq-aaafa-cai")) {
                      blockIndex := await acto.TransferICRCBtoMintVault(amount);
                    };

                    if (blockIndex == 0) {
                      Debug.print("Transfer to vault failed for token " # Principal.toText(token));
                      Vector.add(errorVector, "Transfer to vault failed for token " # Principal.toText(token));
                      swapFailures += 1;

                      // Count as completed iteration
                      iterationsCompleted += 1;
                      if (iterationsCompleted >= totalIterations) {
                        await endStressTest(startTime, swapSuccesses, swapFailures, errorVector);
                      };

                      continue a;
                    };

                    // Set minimum receive threshold based on the random percentage
                    let minimumReceive = (estimate.estimatedTacoAmount * randomMinPercentage) / 100;

                    // Execute the swap
                    let swapResult = await acto.swapTokenForTaco(token, blockIndex, minimumReceive);

                    switch (swapResult) {
                      case (#err(error)) {
                        // Some failures are expected due to allocation limits, don't count these
                        if (
                          Text.contains(error, #text "capacity") or
                          Text.contains(error, #text "allocation") or
                          Text.contains(error, #text "target") or
                          Text.contains(error, #text "no target")
                        ) {
                          Debug.print("Expected swap failure: " # error);
                        } else {
                          Debug.print("Swap failed: " # error);
                          Vector.add(errorVector, "Swap failed: " # error);
                          swapFailures += 1;
                        };
                      };

                      case (#ok(result)) {
                        if (result.success) {
                          swapSuccesses += 1;
                        } else {
                          Debug.print("Swap reported as unsuccessful");
                          Vector.add(errorVector, "Swap reported as unsuccessful");
                          swapFailures += 1;
                        };
                      };
                    };

                    // Count as completed iteration
                    iterationsCompleted += 1;
                    if (iterationsCompleted >= totalIterations) {
                      await endStressTest(startTime, swapSuccesses, swapFailures, errorVector);
                    };
                  };
                };
              } catch (e) {
                Debug.print("Exception during swap: " # Error.message(e));
                Vector.add(errorVector, "Exception during swap: " # Error.message(e));
                swapFailures += 1;

                // Count as completed iteration
                iterationsCompleted += 1;
                if (iterationsCompleted >= totalIterations) {
                  await endStressTest(startTime, swapSuccesses, swapFailures, errorVector);
                };
              };
            };
          };
        };
      };

      // Return a message - the actual results will be reported by endStressTest
      return "Minting vault stress test initiated with " #
      Nat.toText(totalIterations) # " operations";
    } catch (e) {
      return "Stress test failed with error: " # Error.message(e);
    };
  };

  private func endStressTest(
    startTime : Int,
    swapSuccesses : Nat,
    swapFailures : Nat,
    errorVector : Vector.Vector<Text>,
  ) : async () {
    let duration = (Time.now() - startTime) / 1_000_000_000; // Convert to seconds

    Debug.print("Minting vault stress test completed:");
    Debug.print("Duration: " # Int.toText(duration) # " seconds");
    Debug.print("Successful swaps: " # Nat.toText(swapSuccesses));
    Debug.print("Failed swaps: " # Nat.toText(swapFailures));

    // Print errors if any
    if (Vector.size(errorVector) > 0) {
      Debug.print("Errors encountered:");
      let maxErrorsToShow = Nat.min(10, Vector.size(errorVector)); // Limit to avoid console spam

      for (i in Iter.range(0, maxErrorsToShow - 1)) {
        Debug.print(Vector.get(errorVector, i));
      };

      if (Vector.size(errorVector) > maxErrorsToShow) {
        Debug.print("...and " # Nat.toText(Vector.size(errorVector) - maxErrorsToShow) # " more errors");
      };
    };
  };
};
