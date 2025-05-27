import DAO_types "../DAO_backend/dao_types";
import Result "mo:base/Result";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";

actor validation {

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

}