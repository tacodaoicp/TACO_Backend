import Time "mo:base/Time";
import Int "mo:base/Int";
import Float "mo:base/Float";
module {
  type TimeFrame = {
    #fivemin;
    #hour;
    #fourHours;
    #day;
    #week;
  };

  type KlineData = {
    timestamp : Int;
    open : Float;
    high : Float;
    low : Float;
    close : Float;
    volume : Nat;
  };

  type KlineKey = (Text, Text, TimeFrame);

  public func alignTimestamp(timestamp : Int, seconds : Int) : Int {
    timestamp - (timestamp % (seconds * 1_000_000_000));
  };

  // Helper function to get time frame details
  public func getTimeFrameDetails(timeFrame : TimeFrame, timestamp : Int) : (Int, Int) {
    switch (timeFrame) {
      case (#fivemin) (alignTimestamp(timestamp, 300), 300_000_000_000);
      case (#hour) (alignTimestamp(timestamp, 3600), 3600_000_000_000);
      case (#fourHours) (alignTimestamp(timestamp, 14400), 14400_000_000_000);
      case (#day) (alignTimestamp(timestamp, 86400), 86400_000_000_000);
      case (#week) {
        let alignedTime = alignTimestamp(timestamp, 86400);
        let daysSinceEpoch = alignedTime / (86400_000_000_000);
        let dayOfWeek = daysSinceEpoch % 7;
        let weekStart = alignedTime - (dayOfWeek * 86400_000_000_000);
        (weekStart, 7 * 86400_000_000_000);
      };
    };
  };

  public func createEmptyKline(timestamp : Int, previousClose : Float) : KlineData {
    {
      timestamp = timestamp;
      open = previousClose;
      high = previousClose;
      low = previousClose;
      close = previousClose;
      volume = 0;
    };
  };

  public func calculateKlineStats(trades : [(Float, Nat)]) : (Float, Float, Float, Nat) {
    if (trades.size() == 0) {
      return (0, 0, 0, 0);
    };
    var high = trades[0].0;
    var low = trades[0].0;
    var totalVolume : Nat = 0;
    for ((price, volume) in trades.vals()) {
      high := Float.max(high, price);
      low := Float.min(low, price);
      totalVolume += volume;
    };
    (high, low, trades[trades.size() - 1].0, totalVolume);
  };

  public func mergeKlineData(a : KlineData, b : KlineData) : KlineData {
    {
      timestamp = Int.min(a.timestamp, b.timestamp);
      open = a.open;
      high = Float.max(a.high, b.high);
      low = Float.min(a.low, b.low);
      close = b.close;
      volume = a.volume + b.volume;
    };
  };

  public func aggregateScanResult(klines : [(Int, KlineData)], startTime : Int) : KlineData {
    if (klines.size() == 0) {
      // Return an empty kline if there's no data
      {
        timestamp = startTime;
        open = 0;
        high = 0;
        low = 0;
        close = 0;
        volume = 0;
      };
    } else {
      var open = klines[0].1.open;
      var high = klines[0].1.high;
      var low = klines[0].1.low;
      var close = klines[klines.size() - 1].1.close;
      var volume = 0;

      for ((_, kline) in klines.vals()) {
        high := Float.max(high, kline.high);
        low := Float.min(low, kline.low);
        volume += kline.volume;
      };

      {
        timestamp = startTime;
        open = open;
        high = high;
        low = low;
        close = close;
        volume = volume;
      };
    };
  };

  public func compareTime(t1 : Int, t2 : Int) : { #less; #equal; #greater } {
    if (t1 < t2) { #less } else if (t1 > t2) { #greater } else { #equal };
  };

};
