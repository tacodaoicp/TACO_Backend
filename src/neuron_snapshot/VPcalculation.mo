import Nat64 "mo:base/Nat64";
import Int "mo:base/Int";
import Time "mo:base/Time";
import T "./ns_types";
import Nat "mo:base/Nat";

module {
  public type NeuronDetails = {
    id : ?T.NeuronId;
    staked_maturity_e8s_equivalent : ?Nat64;
    cached_neuron_stake_e8s : Nat64;
    aging_since_timestamp_seconds : Nat64;
    dissolve_state : ?DissolveState;
    voting_power_percentage_multiplier : Nat64;
  };

  public type DissolveState = {
    #DissolveDelaySeconds : Nat64;
    #WhenDissolvedTimestampSeconds : Nat64;
  };

  public class vp_calc() {
    public var PARAMS : ?T.NervousSystemParameters = null;

    // Parameter-derived values
    private var minDissolveDelay : Nat = 604800;
    private var maxDissolveDelay : Nat = 2628000;
    private var maxAge : Nat = 0;
    private var maxDissolveBonus : Nat = 0;
    private var maxAgeBonus : Nat = 0;

    public func setParams(params : T.NervousSystemParameters) {
      PARAMS := ?params;
      // Update all parameter-derived values at once
      switch (PARAMS) {
        case (null) {
          minDissolveDelay := 0;
          maxDissolveDelay := 0;
          maxAge := 0;
          maxDissolveBonus := 0;
          maxAgeBonus := 0;
        };
        case (?p) {
          minDissolveDelay := switch (p.neuron_minimum_dissolve_delay_to_vote_seconds) {
            case (null) { 0 };
            case (?min) { Nat64.toNat(min) };
          };
          maxDissolveDelay := switch (p.max_dissolve_delay_seconds) {
            case (null) { 0 };
            case (?v) { Nat64.toNat(v) };
          };
          maxAge := switch (p.max_neuron_age_for_age_bonus) {
            case (null) { 0 };
            case (?v) { Nat64.toNat(v) };
          };
          maxDissolveBonus := switch (p.max_dissolve_delay_bonus_percentage) {
            case (null) { 0 };
            case (?v) { Nat64.toNat(v) };
          };
          maxAgeBonus := switch (p.max_age_bonus_percentage) {
            case (null) { 0 };
            case (?v) { Nat64.toNat(v) };
          };
        };
      };
    };

    public func getVotingPower(n : NeuronDetails) : Nat {
      if (n.cached_neuron_stake_e8s == 0) {
        return 0;
      };

      let stake : Nat = Nat64.toNat(n.cached_neuron_stake_e8s) + (
        switch (n.staked_maturity_e8s_equivalent) {
          case (null) { 0 };
          case (?v) { Nat64.toNat(v) };
        }
      );

      let dissolveDelay : Nat = switch (n.dissolve_state) {
        case (null) { 0 };
        case (? #DissolveDelaySeconds(s)) { Nat64.toNat(s) };
        case (? #WhenDissolvedTimestampSeconds(ts)) {
          let now = Int.abs(Time.now()) / 1_000_000_000;
          if (ts > Nat64.fromNat(now)) {
            Nat64.toNat(ts) - now;
          } else { 0 };
        };
      };

      if (dissolveDelay < minDissolveDelay) {
        return 0;
      };

      let now = Int.abs(Time.now()) / 1_000_000_000;
      let agingSeconds = Nat64.toNat(n.aging_since_timestamp_seconds);
      // Add safety check to prevent underflow
      let age = if (agingSeconds > now) {
        0  // If aging timestamp is in the future, age is 0
      } else {
        now - agingSeconds
      };

      let cappedDissolveDelay = Nat.min(dissolveDelay, maxDissolveDelay);
      let cappedAge = Nat.min(age, maxAge);

      let dissolveBonus = if (maxDissolveDelay > 0 and cappedDissolveDelay > 0) {
        (stake * cappedDissolveDelay * maxDissolveBonus) / (100 * maxDissolveDelay);
      } else {
        0;
      };
      let stakeWithDissolveBonus = stake + dissolveBonus;

      let ageBonus = if (maxAge > 0 and cappedAge > 0) {
        (stakeWithDissolveBonus * cappedAge * maxAgeBonus) / (100 * maxAge);
      } else {
        0;
      };
      let stakeWithAllBonuses = stakeWithDissolveBonus + ageBonus;

      let multiplier = Nat64.toNat(n.voting_power_percentage_multiplier);
      if (multiplier > 0) {
        (stakeWithAllBonuses * multiplier) / 100;
      } else {
        0;
      };
    };
  };
};
