// SpamProtection.mo
import Principal "mo:base/Principal";
import TrieSet "mo:base/TrieSet";
import Map "mo:map/Map";
import { now } = "mo:base/Time";
import Vector "mo:vector";

module {

  public type AdminFunction = {
    #addToken;
    #removeToken;
    #pauseToken;
    #unpauseToken;
    #addAdmin;
    #removeAdmin;
    #updateSystemState;
    #updateSpamParameters;
    #updateSystemParameter;
    #createAuction;
    #setTest;
    #endAuctionPanic;
    #stopToken;
    #updateTreasuryConfig;
    #updateMintingVaultConfig;
    #setTacoAddress;
    #getLogs;
    #clearLogs;
    #startRebalancing;
    #stopRebalancing;
    #recoverPoolBalances;
  };

  public type AdminPermission = {
    function : AdminFunction;
    grantedBy : Principal;
    expiresAt : Int;
  };

  // Data structures
  public type SpamState = {
    var admins : TrieSet.Set<Principal>;
    var adminPermissions : Map.Map<Principal, Vector.Vector<AdminPermission>>;
    var self : Principal;
    var timeStartSpamCheck : Int;
    var timeStartSpamDayCheck : Int;
    var spamCheck : Map.Map<Principal, Nat>;
    var spamCheckOver10 : Map.Map<Principal, Nat>;
    var warnings : TrieSet.Set<Principal>;
    var dayBan : TrieSet.Set<Principal>;
    var dayBanRegister : TrieSet.Set<Principal>;
    var allTimeBan : TrieSet.Set<Principal>;
    var over10 : TrieSet.Set<Principal>;
    var allowedCalls : Nat;
    var allowedSilentWarnings : Nat;
    var timeWindowSpamCheck : Int;
    var allowedCanisters : TrieSet.Set<Principal>;
    var test : Bool;
  };

  public func initState() : SpamState {
    {
      var admins = TrieSet.empty<Principal>();
      var adminPermissions = Map.new<Principal, Vector.Vector<AdminPermission>>();
      var timeStartSpamCheck = now();
      var timeStartSpamDayCheck = now();
      var spamCheck = Map.new<Principal, Nat>();
      var spamCheckOver10 = Map.new<Principal, Nat>();
      var warnings = TrieSet.empty<Principal>();
      var dayBan = TrieSet.empty<Principal>();
      var dayBanRegister = TrieSet.empty<Principal>();
      var allTimeBan = TrieSet.empty<Principal>();
      var over10 = TrieSet.empty<Principal>();
      var allowedCalls = 1000;
      var allowedSilentWarnings = 6;
      var timeWindowSpamCheck = 90000000000;
      var allowedCanisters = TrieSet.empty<Principal>();
      var test = false;
      //var self = Principal.fromText("ywhqf-eyaaa-aaaad-qg6tq-cai");
      var self = Principal.fromText("vxqw7-iqaaa-aaaan-qzziq-cai");
    };
  };
  //0=not allowed 1=allowed 2=warning 3=day-ban 4=all-time ban
  //We are allowing X (allowedCalls) calls within 90 seconds, if an entity goes over that,
  //they get a warning and their 90 second spamCount is divided by 2.
  //If they go over the rate within a day while having a warning, they get a day-ban.
  //If the entity has gotten a day-ban before that occasion it gets an allTimeBan.
  //There is also a silent warning. If an user gets X (allowedSilentWarnings) of them
  // in 1 day, they also get a day-ban
  //0=not allowed 1=allowed 2=warning 3=day-ban 4=all-time ban
  //We are allowing X (allowedCalls) calls within 90 seconds, if an entity goes over that,
  //they get a warning and their 90 second spamCount is divided by 2.
  //If they go over the rate within a day while having a warning, they get a day-ban.
  //If the entity has gotten a day-ban before that occasion it gets an allTimeBan.
  //There is also a silent warning. If an user gets X (allowedSilentWarnings) of them
  // in 1 day, they also get a day-ban
  // *** To afat: 1. adminCheck indeed adds principals to the Dayban if someone tries to perform a functions thats not allowed.
  // *** This is done to directly discourage people who are sniffing around. As I would also go for admin functions as the first thing to try
  // *** This should not give problems considering these addresses will be different from the principals that use the exchange as they should.
  public class SpamGuard() {
    public var state : SpamState = initState();
    private let dayInNanos = 86400000000000;

    public func isAllowed(caller : Principal, sns_governance_canister_id : ?Principal) : Nat {

      if (
        (
          (
            TrieSet.contains(state.dayBan, caller, Principal.hash(caller), Principal.equal) or
            TrieSet.contains(state.allTimeBan, caller, Principal.hash(caller), Principal.equal)
          ) and not state.test
        ) or Principal.isAnonymous(caller)
      ) {
        return 0;
      };
      let callerText = Principal.toText(caller);
      let allowed = TrieSet.contains(state.allowedCanisters, caller, Principal.hash(caller), Principal.equal) or caller == state.self or TrieSet.contains(state.admins, caller, Principal.hash(caller), Principal.equal) or Principal.isController(caller) or sns_governance_canister_id == ?caller or state.test;

      if (allowed) { return 1 };

      let nowVar = now();
      if (nowVar > state.timeStartSpamCheck + state.timeWindowSpamCheck) {
        state.timeStartSpamCheck := nowVar;
        Map.clear(state.spamCheck);
        state.over10 := TrieSet.empty();
      } else if (nowVar > state.timeStartSpamDayCheck + dayInNanos) {
        state.warnings := TrieSet.empty();
        Map.clear(state.spamCheckOver10);
        state.dayBan := TrieSet.empty();
        state.timeStartSpamDayCheck := nowVar;
      };

      if (callerText.size() < 29) {
        return 0;
      };

      let temp = Map.get(state.spamCheck, Map.phash, caller);
      let num = (
        if (temp == null) { 0 } else {
          switch (temp) { case (?t) { t }; case (_) { 0 } };
        }
      ) + 1;
      Map.set(state.spamCheck, Map.phash, caller, num);

      if (num < state.allowedCalls) {
        if (num < state.allowedCalls / 2) {
          return 1;
        } else if (not TrieSet.contains(state.over10, caller, Principal.hash(caller), Principal.equal)) {
          state.over10 := TrieSet.put(state.over10, caller, Principal.hash(caller), Principal.equal);
          let temp = Map.get(state.spamCheckOver10, Map.phash, caller);
          let num = switch (temp) { case (?val) val + 1; case (null) 1 };
          if (num > state.allowedSilentWarnings) {
            if (not TrieSet.contains(state.dayBanRegister, caller, Principal.hash(caller), Principal.equal)) {
              state.dayBan := TrieSet.put(state.dayBan, caller, Principal.hash(caller), Principal.equal);
              state.dayBanRegister := TrieSet.put(state.dayBanRegister, caller, Principal.hash(caller), Principal.equal);
              return 1; // so it gets updated through inspection
            } else {
              state.allTimeBan := TrieSet.put(state.allTimeBan, caller, Principal.hash(caller), Principal.equal);
              return 1; // so it gets updated through inspection
            };
          } else {
            Map.set(state.spamCheckOver10, Map.phash, caller, num);
            return 1;
          };
        } else {
          return 1;
        };
      } else {
        if (not TrieSet.contains(state.warnings, caller, Principal.hash(caller), Principal.equal)) {
          state.warnings := TrieSet.put(state.warnings, caller, Principal.hash(caller), Principal.equal);
          Map.set(state.spamCheck, Map.phash, caller, num / 2);
          return 2;
        } else {
          if (not TrieSet.contains(state.dayBanRegister, caller, Principal.hash(caller), Principal.equal)) {
            state.dayBan := TrieSet.put(state.dayBan, caller, Principal.hash(caller), Principal.equal);
            state.dayBanRegister := TrieSet.put(state.dayBanRegister, caller, Principal.hash(caller), Principal.equal);
            return 1; // so it gets updated through inspection
          } else {
            state.allTimeBan := TrieSet.put(state.allTimeBan, caller, Principal.hash(caller), Principal.equal);
            return 1; // so it gets updated through inspection
          };
        };
      };
    };

    public func isAllowedQuery(caller : Principal) : Nat {
      let callerText = Principal.toText(caller);

      if (
        (
          TrieSet.contains(state.dayBan, caller, Principal.hash(caller), Principal.equal) or
          TrieSet.contains(state.allTimeBan, caller, Principal.hash(caller), Principal.equal)
        ) and not Principal.isAnonymous(caller) and not state.test
      ) {
        return 0;
      };

      let allowed = TrieSet.contains(state.allowedCanisters, caller, Principal.hash(caller), Principal.equal) or caller == state.self or TrieSet.contains(state.admins, caller, Principal.hash(caller), Principal.equal) or Principal.isController(caller);

      if (callerText.size() < 29 and not allowed and not Principal.isAnonymous(caller) and not state.test) {
        return 0;
      };

      return 1;
    };

    public func getSelf() : Principal {
      state.self;
    };

    public func grantAdminPermission(
      caller : Principal,
      admin : Principal,
      function : AdminFunction,
      durationDays : Nat,
      sns_governance_canister_id : ?Principal,
    ) : Bool {
      if (not Principal.isController(caller) and sns_governance_canister_id != ?caller) {
        return false;
      };

      if (durationDays < 1 or durationDays > 7) {
        return false;
      };

      if (not TrieSet.contains(state.admins, admin, Principal.hash(admin), Principal.equal)) {
        return false;
      };

      let permissions = switch (Map.get(state.adminPermissions, Map.phash, admin)) {
        case null {
          let newVec = Vector.new<AdminPermission>();
          Map.set(state.adminPermissions, Map.phash, admin, newVec);
          newVec;
        };
        case (?existingVec) { existingVec };
      };

      let permission : AdminPermission = {
        function = function;
        grantedBy = caller;
        expiresAt = now() + (durationDays * dayInNanos);
      };

      Vector.add(permissions, permission);
      true;
    };

    // Enhanced isAdmin to check specific function permissions
    public func isAdmin(caller : Principal, function : AdminFunction, sns_governance_canister_id : ?Principal) : Bool {
      if (Principal.isController(caller) or sns_governance_canister_id == ?caller) {
        return true;
      };

      switch (Map.get(state.adminPermissions, Map.phash, caller)) {
        case null {
          state.dayBan := TrieSet.put(state.dayBan, caller, Principal.hash(caller), Principal.equal);
          state.dayBanRegister := TrieSet.put(state.dayBanRegister, caller, Principal.hash(caller), Principal.equal);
          false;
        };
        case (?permissions) {
          let currentTime = now();
          var hasPermission = false;

          // Clean expired and check valid permissions
          let validPermissions = Vector.new<AdminPermission>();

          for (perm in Vector.vals(permissions)) {
            if (perm.expiresAt > currentTime) {
              Vector.add(validPermissions, perm);
              if (perm.function == function) {
                hasPermission := true;
              };
            };
          };

          // Update stored permissions, removing expired ones
          Map.set(state.adminPermissions, Map.phash, caller, validPermissions);

          hasPermission;
        };
      };
    };

    // Query method to check permissions
    public func getAdminPermissions() : [(Principal, [AdminPermission])] {
      let tempVector = Vector.new<(Principal, [AdminPermission])>();
      for ((key, value) in Map.entries(state.adminPermissions)) {
        Vector.add(tempVector, (key, Vector.toArray(value)));
      };
      return Vector.toArray(tempVector);
    };

    // Configuration methods
    public func setTest(value : Bool) {
      state.test := value;
    };

    public func setAllowedCanisters(canisters : [Principal]) {
      state.allowedCanisters := TrieSet.fromArray(canisters, Principal.hash, Principal.equal);
    };

    public func setAdmins(admins : [Principal]) {
      state.admins := TrieSet.fromArray(admins, Principal.hash, Principal.equal);
    };

    public func setSelf(self : Principal) {
      state.self := self;
    };

    public func updateSpamParameters(
      params : {
        allowedCalls : ?Nat;
        allowedSilentWarnings : ?Nat;
        timeWindowSpamCheck : ?Int;
      }
    ) {
      switch (params.allowedCalls) {
        case (?calls) if (calls >= 1 and calls <= 100) {
          state.allowedCalls := calls;
        };
        case null {};
      };

      switch (params.allowedSilentWarnings) {
        case (?warnings) if (warnings >= 1 and warnings <= 100) {
          state.allowedSilentWarnings := warnings;
        };
        case null {};
      };

      switch (params.timeWindowSpamCheck) {
        case (?window) { state.timeWindowSpamCheck := window };
        case null {};
      };
    };

    // Ban management
    public func manageBans(
      params : {
        deleteFromDayBan : ?[Principal];
        deleteFromAllTimeBan : ?[Principal];
        addToAllTimeBan : ?[Principal];
      }
    ) {
      switch (params.deleteFromDayBan) {
        case (?principals) {
          for (p in principals.vals()) {
            state.dayBan := TrieSet.delete(
              state.dayBan,
              p,
              Principal.hash(p),
              Principal.equal,
            );
          };
        };
        case null {};
      };

      switch (params.deleteFromAllTimeBan) {
        case (?principals) {
          for (p in principals.vals()) {
            state.allTimeBan := TrieSet.delete(
              state.allTimeBan,
              p,
              Principal.hash(p),
              Principal.equal,
            );
          };
        };
        case null {};
      };

      switch (params.addToAllTimeBan) {
        case (?principals) {
          for (p in principals.vals()) {
            state.allTimeBan := TrieSet.put(
              state.allTimeBan,
              p,
              Principal.hash(p),
              Principal.equal,
            );
          };
        };
        case null {};
      };
    };

    // Query methods
    public func getBannedUsers() : {
      dayBanned : [Principal];
      allTimeBanned : [Principal];
    } {
      {
        dayBanned = TrieSet.toArray(state.dayBan);
        allTimeBanned = TrieSet.toArray(state.allTimeBan);
      };
    };

    public func getAdmins() : [Principal] {
      TrieSet.toArray(state.admins);
    };

    public func getAllowedCanisters() : [Principal] {
      TrieSet.toArray(state.allowedCanisters);
    };

  };
};
