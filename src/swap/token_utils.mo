import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Error "mo:base/Error";
import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Debug "mo:base/Debug";
import Types "./swap_types";

module {
  public func getTokenMetadata(tokenId : Principal) : async Result.Result<Types.ICRC1TokenMetadata, Text> {
    Debug.print("TokenUtils.getTokenMetadata: Getting metadata for token " # Principal.toText(tokenId));
    try {
      let token : Types.ICRC1 = actor (Principal.toText(tokenId));
      let metadata = await token.icrc1_metadata();

      // Extract values from metadata array
      var fee : ?Nat = null;
      var decimals : ?Nat8 = null;
      var name : ?Text = null;
      var symbol : ?Text = null;

      for ((key, value) in metadata.vals()) {
        switch (key) {
          case "icrc1:fee" {
            switch (value) {
              case (#Nat(v)) { fee := ?v };
              case (_) {};
            };
          };
          case "icrc1:decimals" {
            switch (value) {
              case (#Nat(v)) { decimals := ?Nat8.fromNat(v) };
              case (_) {};
            };
          };
          case "icrc1:name" {
            switch (value) {
              case (#Text(v)) { name := ?v };
              case (_) {};
            };
          };
          case "icrc1:symbol" {
            switch (value) {
              case (#Text(v)) { symbol := ?v };
              case (_) {};
            };
          };
          case _ {};
        };
      };

      // Fall back to individual method calls for any missing fields
      if (fee == null) {
        let f = await token.icrc1_fee();
        fee := ?f;
      };
      if (decimals == null) {
        let d = await token.icrc1_decimals();
        decimals := ?d;
      };
      if (name == null) {
        let n = await token.icrc1_name();
        name := ?n;
      };
      if (symbol == null) {
        let s = await token.icrc1_symbol();
        symbol := ?s;
      };

      // Check if we got all required fields
      switch (fee, decimals, name, symbol) {
        case (?f, ?d, ?n, ?s) {
          #ok({
            fee = f;
            decimals = d;
            name = n;
            symbol = s;
          });
        };
        case _ {
          #err("Could not get required metadata fields even after fallback calls");
        };
      };
    } catch (e) {
      Debug.print("TokenUtils.getTokenMetadata: Exception: " # Error.message(e));
      #err("Error getting token metadata: " # Error.message(e));
    };
  };
};
