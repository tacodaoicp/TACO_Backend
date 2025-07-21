import Principal "mo:base/Principal";
import ArchiveTypes "../archives/archive_types";

module {
  public class ArchiveAuthorization(
    masterAdmins: [Principal],
    treasuryId: Principal,
    daoBackendId: Principal,
    getCanisterId: () -> Principal
  ) {
    
    public func isMasterAdmin(caller : Principal) : Bool {
      for (admin in masterAdmins.vals()) {
        if (admin == caller) {
          return true;
        };
      };
      false;
    };

    public func isAuthorized(caller : Principal, function : ArchiveTypes.AdminFunction) : Bool {
      if (isMasterAdmin(caller) or Principal.isController(caller)) {
        return true;
      };
      
      // Check if caller is Treasury or DAO
      if (caller == treasuryId or caller == daoBackendId) {
        return true;
      };
      
      // Allow self-authorization for batch imports/timers
      if (caller == getCanisterId()) {
        return true;
      };
      
      false;
    };

    public func isQueryAuthorized(caller : Principal) : Bool {
      if (isMasterAdmin(caller) or Principal.isController(caller)) {
        return true;
      };
      
      // Allow treasury and DAO to query
      if (caller == treasuryId or caller == daoBackendId) {
        return true;
      };
      
      // Allow public read access to archive data
      true;
    };
  };
} 