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
    };
    
    // Define the environments
    public type Environment = {
        #Staging;
        #Production;
    };
    
    public class CanisterIds(thisPrincipal: Principal) {
        
        private let canisterMappings = [
            // Staging environment
            (#Staging, #DAO_backend, Principal.fromText("tisou-7aaaa-aaaai-atiea-cai")),
            (#Staging, #neuronSnapshot, Principal.fromText("tgqd4-eqaaa-aaaai-atifa-cai")),
            (#Staging, #treasury, Principal.fromText("tptia-syaaa-aaaai-atieq-cai")),
            (#Staging, #validation, Principal.fromText("tbrfi-jiaaa-aaaai-atifq-cai")),
            (#Staging, #trading_archive, Principal.fromText("jlycp-kqaaa-aaaan-qz4xa-cai")),
            (#Staging, #portfolio_archive, Principal.fromText("lrekt-uaaaa-aaaan-qz4ya-cai")),
            
            // Production environment
            (#Production, #DAO_backend, Principal.fromText("vxqw7-iqaaa-aaaan-qzziq-cai")),
            (#Production, #neuronSnapshot, Principal.fromText("vzs3x-taaaa-aaaan-qzzjq-cai")),
            (#Production, #treasury, Principal.fromText("v6t5d-6yaaa-aaaan-qzzja-cai")),
            (#Production, #validation, Principal.fromText("th44n-iyaaa-aaaan-qzz5a-cai")),
            (#Production, #trading_archive, Principal.fromText("jmze3-hiaaa-aaaan-qz4xq-cai")),
            (#Production, #portfolio_archive, Principal.fromText("aaaaa-aa"))
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
