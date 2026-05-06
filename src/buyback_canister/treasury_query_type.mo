// Buyback-canister-side actor type for the main treasury canister, narrowed to
// the methods the buyback canister calls — pulling the token-details cache for
// price/decimals/fee lookups, and reading the kongEnabled global kill switch.
//
// We could `import TreasuryTypes` from `treasury_types.mo` but doing that would
// pull in every type the treasury defines. Instead, this module declares only
// the minimal subset (TokenDetails-shape-equivalent and the actor signatures we
// invoke). Field shapes match exactly so Candid decoding succeeds.

module {

  // Minimal mirror of treasury's TokenDetails — only the fields buyback needs.
  // pastPrices is omitted (we don't iterate history; just the current price).
  // Active/isPaused are not consumed locally. tokenType variants are not used.
  public type TokenDetailsLite = {
    tokenSymbol : Text;
    tokenName : Text;
    tokenDecimals : Nat;
    tokenTransferFee : Nat;
    balance : Nat;        // treasury's view; we ignore for our own balance lookups
    priceInICP : Nat;
    priceInUSD : Float;
  };

  // ICPSwap pool data — mirror of treasury's getICPSwapPoolInfo return type.
  // Buyback canister queries this to discover the pool for a token pair before
  // issuing an ICPSwap quote.
  public type ICPSwapPoolInfo = {
    canisterId : Principal;
    token0 : Text;
    token1 : Text;
    fee : Nat;
  };

  public type Treasury = actor {
    // Returns full token details map. Buyback uses this to refresh its local
    // cache (only TokenDetailsLite-shaped fields are extracted).
    // Note: this method is `shared query` on treasury but extra fields it
    // returns are dropped at decode if not declared here — Candid is structural.
    getTokenDetails : shared () -> async [(Principal, TokenDetailsLite)];

    // Returns ICPSwap pool info for a token pair if a pool is mapped.
    // Treasury maintains the ICPswapPools map via its periodic discovery
    // (treasury.mo:11958+ updateICPSwapPools).
    getICPSwapPoolInfo : shared query (Principal, Principal) -> async ?ICPSwapPoolInfo;
  };

}
