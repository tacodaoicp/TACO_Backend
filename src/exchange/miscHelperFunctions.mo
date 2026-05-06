import Text "mo:base/Text";
import Map "mo:map/Map";

module {
  let {
    hashText;
    hashNat32;
    hashNat;
  } = Map;

  type Ratio = {
    #Max;
    #Zero;
    #Value : Nat;
  };
  type TimeFrame = {
    #fivemin;
    #hour;
    #fourHours;
    #day;
    #week;
  };
  type KlineKey = (Text, Text, TimeFrame);

  // Helper function to calculate fee
  public func calculateFee(amount : Nat, Fee : Nat, RevokeFee : Nat) : Nat {
    (((((amount) * Fee)) - (((((amount) * Fee) * 100000) / RevokeFee) / 100000)) / 10000);
  };

  public func extractToken(index : Nat, text : (Text, Text)) : Text {
    if (index == 0) { text.0 } else { text.1 };
  };

  public func sqrt(y : Nat) : Nat {
    if (y < 2) {
      return y;
    };
    var x : Nat = y;
    var z : Nat = (x + 1) / 2;
    while (z < x) {
      x := z;
      z := (x + y / x) / 2;
    };
    // Ensure the result is the largest integer not greater than the true square root
    if (x * x > y) {
      x -= 1;
    };
    return x;
  };

  public func hashRatio(key : Ratio) : Nat32 {
    switch (key) {
      case (#Max) hashNat32(0xFFFFFFFF);
      case (#Zero) hashNat32(0);
      case (#Value(v)) hashNat(v);
    };
  };

  public func hashKlineKey(key : KlineKey) : Nat32 {
    hashText(
      key.0 # key.1 # (
        switch (key.2) {
          case (#fivemin) { "" };
          case (#hour) { "2" };
          case (#fourHours) { "3" };
          case (#day) { "4" };
          case (#week) { "5" };
        }
      )
    );
  };

  public func hashTextText(key : (Text, Text)) : Nat32 {
    return hashText(key.0 # key.1);
  };

  public let compareRatio = func(a : Ratio, b : Ratio) : {
    #less;
    #equal;
    #greater;
  } {
    switch (a, b) {
      case (#Value(v1), #Value(v2)) {
        if (v1 < v2) { #less } else if (v1 == v2) { #equal } else { #greater };
      };
      case (#Max, #Max) #equal;
      case (#Max, _) #greater;
      case (_, #Max) #less;
      case (#Zero, #Zero) #equal;
      case (#Zero, _) #less;
      case (_, #Zero) #greater;
    };
  };
  type Time = Int;
  public let compareTextTime = func(a : (Text, Time), b : (Text, Time)) : {
    #less;
    #equal;
    #greater;
  } {
    let (textA, timeA) = a;
    let (textB, timeB) = b;

    if (timeA < timeB) {
      #less;
    } else if (timeA > timeB) {
      #greater;
    } else {
      // If times are equal, compare the texts
      Text.compare(textA, textB);
    };
  };

  public let isLessThanRatio = func(a : Ratio, b : Ratio) : Bool {
    switch (compareRatio(a, b)) {
      case (#less) true;
      case _ false;
    };
  };

};
