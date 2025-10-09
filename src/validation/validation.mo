import DAO_types "../DAO_backend/dao_types";
import Blob "mo:base/Blob";
import Result "mo:base/Result";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Cycles "mo:base/ExperimentalCycles";

actor validation {

    type Subaccount = Blob;
    type TokenType = DAO_types.TokenType;
    type AuthorizationError = DAO_types.AuthorizationError;

    type ValidationResult = {
        #Ok: Text;
        #Err :Text;
    };

    public shared func validate_addToken(token : Principal, tokenType : TokenType) : async ValidationResult {

      let msg : Text = 
        "token: " # Principal.toText(token) #  
        ", tokenType: " # debug_show(tokenType);

      #Ok(msg);

    };


    public query func validate_sendToken(token : Principal, amount_e8s : Nat, to_principal : Principal, to_subaccount : ?Subaccount) : async ValidationResult {

      let to_account = { owner = to_principal; subaccount = to_subaccount };

      let msg:Text = "amount_e8s: " # debug_show(amount_e8s) #  
      ", token: " # Principal.toText(token) # 
      ", to_account: " # debug_show(to_account);

      #Ok(msg);
    };

  public query func get_canister_cycles() : async { cycles : Nat } {
    { cycles = Cycles.balance() };
  };  

}