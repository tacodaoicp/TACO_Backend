import Principal "mo:base/Principal";
import Array "mo:base/Array";
import Debug "mo:base/Debug";

module CanisterId {
    
    // Define the canister types
    public type CanisterType = {
        #DAO_backend;
        #treasury;
        #neuronSnapshot;
        #validation;
        #trading_archive;
        #portfolio_archive;
        #price_archive;
        #dao_admin_archive;
        #dao_governance_archive;
        #dao_allocation_archive;
        #rewards;
        #dao_neuron_allocation_archive;
        #reward_distribution_archive;
        #reward_withdrawal_archive;
    };
    
    // Define the environments
    public type Environment = {
        #Local;
        #Staging;
        #Production;
    };
    
    public class CanisterIds(thisPrincipal: Principal) {
        
        private let canisterMappings = [
            // Local environment (uses staging dependencies for inter-canister calls)
            (#Local, #DAO_backend, Principal.fromText("tisou-7aaaa-aaaai-atiea-cai")),
            (#Local, #neuronSnapshot, Principal.fromText("tgqd4-eqaaa-aaaai-atifa-cai")),
            (#Local, #treasury, Principal.fromText("uxrrr-q7777-77774-qaaaq-cai")),
            (#Local, #validation, Principal.fromText("tbrfi-jiaaa-aaaai-atifq-cai")),
            (#Local, #trading_archive, Principal.fromText("jlycp-kqaaa-aaaan-qz4xa-cai")),
            (#Local, #portfolio_archive, Principal.fromText("lrekt-uaaaa-aaaan-qz4ya-cai")),
            (#Local, #price_archive, Principal.fromText("l7gh3-pqaaa-aaaan-qz4za-cai")),
            (#Local, #dao_admin_archive, Principal.fromText("b6ygs-xaaaa-aaaan-qz5ca-cai")),
            (#Local, #dao_governance_archive, Principal.fromText("bzzag-2yaaa-aaaan-qz5cq-cai")),
            (#Local, #dao_allocation_archive, Principal.fromText("bq2l2-mqaaa-aaaan-qz5da-cai")),
            (#Local, #rewards, Principal.fromText("uxrrr-q7777-77774-qaaaq-cai")),
            (#Local, #dao_neuron_allocation_archive, Principal.fromText("cajb4-qqaaa-aaaan-qz5la-cai")),
            (#Local, #reward_distribution_archive, Principal.fromText("ddfi2-eiaaa-aaaan-qz5nq-cai")),
            (#Local, #reward_withdrawal_archive, Principal.fromText("dwczx-faaaa-aaaan-qz5oa-cai")),

            // Staging environment
            (#Staging, #DAO_backend, Principal.fromText("tisou-7aaaa-aaaai-atiea-cai")),
            (#Staging, #neuronSnapshot, Principal.fromText("tgqd4-eqaaa-aaaai-atifa-cai")),
            (#Staging, #treasury, Principal.fromText("tptia-syaaa-aaaai-atieq-cai")),
            (#Staging, #validation, Principal.fromText("tbrfi-jiaaa-aaaai-atifq-cai")),
            (#Staging, #trading_archive, Principal.fromText("jlycp-kqaaa-aaaan-qz4xa-cai")),
            (#Staging, #portfolio_archive, Principal.fromText("lrekt-uaaaa-aaaan-qz4ya-cai")),
            (#Staging, #price_archive, Principal.fromText("l7gh3-pqaaa-aaaan-qz4za-cai")),
            (#Staging, #dao_admin_archive, Principal.fromText("b6ygs-xaaaa-aaaan-qz5ca-cai")),
            (#Staging, #dao_governance_archive, Principal.fromText("bzzag-2yaaa-aaaan-qz5cq-cai")),
            (#Staging, #dao_allocation_archive, Principal.fromText("bq2l2-mqaaa-aaaan-qz5da-cai")),
            (#Staging, #rewards, Principal.fromText("cjkka-gyaaa-aaaan-qz5kq-cai")), // Placeholder ID
            (#Staging, #dao_neuron_allocation_archive, Principal.fromText("cajb4-qqaaa-aaaan-qz5la-cai")), // Placeholder ID
            (#Staging, #reward_distribution_archive, Principal.fromText("ddfi2-eiaaa-aaaan-qz5nq-cai")),
            (#Staging, #reward_withdrawal_archive, Principal.fromText("dwczx-faaaa-aaaan-qz5oa-cai")),

            // Production environment
            (#Production, #DAO_backend, Principal.fromText("vxqw7-iqaaa-aaaan-qzziq-cai")),
            (#Production, #neuronSnapshot, Principal.fromText("vzs3x-taaaa-aaaan-qzzjq-cai")),
            (#Production, #treasury, Principal.fromText("v6t5d-6yaaa-aaaan-qzzja-cai")),
            (#Production, #validation, Principal.fromText("th44n-iyaaa-aaaan-qzz5a-cai")),
            (#Production, #trading_archive, Principal.fromText("jmze3-hiaaa-aaaan-qz4xq-cai")),
            (#Production, #portfolio_archive, Principal.fromText("bl7x7-wiaaa-aaaan-qz5bq-cai")),
            (#Production, #price_archive, Principal.fromText("bm6rl-3qaaa-aaaan-qz5ba-cai")),
            (#Production, #dao_admin_archive, Principal.fromText("cspwf-4aaaa-aaaan-qz5ia-cai")),
            (#Production, #dao_governance_archive, Principal.fromText("c4n3n-hqaaa-aaaan-qz5ja-cai")),
            (#Production, #dao_allocation_archive, Principal.fromText("cvoqr-ryaaa-aaaan-qz5iq-cai")),
            (#Production, #rewards, Principal.fromText("dkgdg-saaaa-aaaan-qz5ma-cai")), // Placeholder ID
            (#Production, #dao_neuron_allocation_archive, Principal.fromText("dnhfs-7yaaa-aaaan-qz5mq-cai")), // Placeholder ID
            (#Production, #reward_distribution_archive, Principal.fromText("uqkap-jiaaa-aaaan-qz6tq-cai")),
            (#Production, #reward_withdrawal_archive, Principal.fromText("v5eeb-gaaaa-aaaan-qz6ua-cai"))
        ];
        
        private func getEnvironmentForPrincipal(canisterId: Principal) : ?Environment {
            for ((env, _, principal) in canisterMappings.vals()) {
                if (Principal.equal(principal, canisterId)) {
                    return ?env;
                };
            };
            null;
        };
        
        // Store the environment for this instance
        private let environment: Environment = switch (getEnvironmentForPrincipal(thisPrincipal)) {
            case (?env) { env };
            case (null) { Debug.trap("Caller's canister ID not found in any environment") };
        };
        
        public func getEnvironment() : Environment {
            environment;
        };
        
        public func getCanisterId(canisterType: CanisterType) : Principal {
            for ((env, cType, principal) in canisterMappings.vals()) {
                if (env == environment and cType == canisterType) {
                    return principal;
                };
            };
            Debug.trap("Requested canister type not found in environment");
        };
        
        public func getAllCanisterIds() : [(CanisterType, Principal)] {
            Array.mapFilter<(Environment, CanisterType, Principal), (CanisterType, Principal)>(
                canisterMappings,
                func((env, cType, principal)) {
                    if (env == environment) {
                        ?(cType, principal);
                    } else {
                        null;
                    };
                }
            );
        };
        
        public func isKnownCanister(canisterId: Principal) : Bool {
            switch (getEnvironmentForPrincipal(canisterId)) {
                case (?_) { true };
                case (null) { false };
            };
        };
    }
}
