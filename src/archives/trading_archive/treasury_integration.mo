import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Float "mo:base/Float";
import Error "mo:base/Error";

import TradingArchiveTypes "./trading_archive_types";
import TreasuryTypes "../../treasury/treasury_types";
import CanisterIds "../../helper/CanisterIds";

module {
  // Type aliases for convenience
  public type TradeRecord = TreasuryTypes.TradeRecord;
  public type TokenDetails = TreasuryTypes.TokenDetails;
  public type PortfolioSnapshot = TreasuryTypes.PortfolioSnapshot;
  public type ArchiveError = TradingArchiveTypes.ArchiveError;
  public type TradeBlockData = TradingArchiveTypes.TradeBlockData;
  public type PortfolioBlockData = TradingArchiveTypes.PortfolioBlockData;
  public type CircuitBreakerBlockData = TradingArchiveTypes.CircuitBreakerBlockData;
  public type PriceBlockData = TradingArchiveTypes.PriceBlockData;

  // Trading Archive interface for treasury
  public type TradingArchiveInterface = actor {
    archiveTradeBlock : (TradeBlockData) -> async Result.Result<Nat, ArchiveError>;
    archivePortfolioBlock : (PortfolioBlockData) -> async Result.Result<Nat, ArchiveError>;
    archiveCircuitBreakerBlock : (CircuitBreakerBlockData) -> async Result.Result<Nat, ArchiveError>;
    archivePriceBlock : (PriceBlockData) -> async Result.Result<Nat, ArchiveError>;
  };

  // Helper functions to convert treasury data to archive format
  public func convertTradeRecord(
    trade : TradeRecord,
    trader : Principal
  ) : TradeBlockData {
    {
      trader = trader;
      tokenSold = trade.tokenSold;
      tokenBought = trade.tokenBought;
      amountSold = trade.amountSold;
      amountBought = trade.amountBought;
      exchange = trade.exchange;
      success = trade.success;
      slippage = trade.slippage;
      fee = 0; // Would need to extract from trade record if available
      error = trade.error;
    };
  };

  public func convertPortfolioSnapshot(
    snapshot : PortfolioSnapshot,
    tokenDetails : [(Principal, TokenDetails)]
  ) : PortfolioBlockData {
    
    let activeTokens = Array.mapFilter<(Principal, TokenDetails), Principal>(
      tokenDetails,
      func(entry) = if (entry.1.Active and not entry.1.isPaused) { ?entry.0 } else { null }
    );

    let pausedTokens = Array.mapFilter<(Principal, TokenDetails), Principal>(
      tokenDetails,
      func(entry) = if (entry.1.Active and entry.1.isPaused) { ?entry.0 } else { null }
    );

    {
      timestamp = snapshot.timestamp;
      totalValueICP = snapshot.totalValueICP;
      totalValueUSD = snapshot.totalValueUSD;
      tokenCount = tokenDetails.size();
      activeTokens = activeTokens;
      pausedTokens = pausedTokens;
      reason = #Scheduled; // Default reason, could be parameterized
    };
  };

  public func convertPriceData(
    token : Principal,
    details : TokenDetails
  ) : PriceBlockData {
    {
      token = token;
      priceICP = details.priceInICP;
      priceUSD = details.priceInUSD;
      source = #Aggregated; // Default source
      volume24h = null; // Would need to calculate if available
      change24h = null; // Would need to calculate if available
    };
  };

  // Helper class for treasury integration
  public class TreasuryArchiveHelper(tradingArchiveId : Principal) {
    
    private let TRADING_ARCHIVE_ID = tradingArchiveId;
    
    private let tradingArchive : TradingArchiveInterface = actor (Principal.toText(TRADING_ARCHIVE_ID));

    // Archive a trade with context
    public func archiveTradeWithContext(
      trade : TradeRecord,
      trader : Principal
    ) : async Result.Result<Nat, ArchiveError> {
      
      let tradeBlockData = convertTradeRecord(trade, trader);
      await tradingArchive.archiveTradeBlock(tradeBlockData);
    };

    // Archive portfolio snapshot with context
    public func archivePortfolioSnapshotWithContext(
      snapshot : PortfolioSnapshot,
      tokenDetails : [(Principal, TokenDetails)]
    ) : async Result.Result<Nat, ArchiveError> {
      
      let portfolioBlockData = convertPortfolioSnapshot(snapshot, tokenDetails);
      await tradingArchive.archivePortfolioBlock(portfolioBlockData);
    };

    // Archive price data for multiple tokens
    public func archivePriceDataBatch(
      tokenDetails : [(Principal, TokenDetails)]
    ) : async [Result.Result<Nat, ArchiveError>] {
      
      let results = Array.init<Result.Result<Nat, ArchiveError>>(tokenDetails.size(), #err(#SystemError("Not processed")));
      
      var i = 0;
      for ((token, details) in tokenDetails.vals()) {
        let priceBlockData = convertPriceData(token, details);
        try {
          let result = await tradingArchive.archivePriceBlock(priceBlockData);
          results[i] := result;
        } catch (e) {
          results[i] := #err(#SystemError("Failed to archive: " # Error.message(e)));
        };
        i += 1;
      };
      
      Array.freeze(results);
    };

    // Archive circuit breaker event
    public func archiveCircuitBreakerEventWithContext(
      eventType : TradingArchiveTypes.CircuitBreakerEventType,
      triggerToken : ?Principal,
      thresholdValue : Float,
      actualValue : Float,
      tokensAffected : [Principal],
      systemResponse : Text,
      severity : Text
    ) : async Result.Result<Nat, ArchiveError> {
      
      let circuitBreakerData : CircuitBreakerBlockData = {
        eventType = eventType;
        triggerToken = triggerToken;
        thresholdValue = thresholdValue;
        actualValue = actualValue;
        tokensAffected = tokensAffected;
        systemResponse = systemResponse;
        severity = severity;
      };
      
      await tradingArchive.archiveCircuitBreakerBlock(circuitBreakerData);
    };

    // Get the trading archive canister ID
    public func getTradingArchiveId() : Principal {
      TRADING_ARCHIVE_ID;
    };
  };

  // Utility functions
  public func shouldArchiveSnapshot(lastSnapshotTime : Int, intervalNS : Int) : Bool {
    let now = Time.now();
    now - lastSnapshotTime >= intervalNS;
  };

  // Note: In practice, you would get this from deployment configuration or environment
  public func shouldArchiveTrade(trade : TradeRecord) : Bool {
    // Logic to determine if a trade should be archived
    // For now, archive all successful trades
    trade.success;
  };
} 