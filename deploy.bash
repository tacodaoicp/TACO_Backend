#!/bin/bash

# Canister IDs
export DAO="ywhqf-eyaaa-aaaad-qg6tq-cai"
export DAOtreasury="z4is7-giaaa-aaaad-qg6uq-cai"
export ActorA="hhaaz-2aaaa-aaaaq-aacla-cai"
export ActorB="qtooy-2yaaa-aaaaq-aabvq-cai"
export ActorC="aanaa-xaaaa-aaaah-aaeiq-cai"
export TestScript="ca6gz-lqaaa-aaaaq-aacwa-cai"
export NeuronSnapshot="zvlzd-qaaaa-aaaad-qg6va-cai"
export MockICPSwap="4mmnk-kiaaa-aaaag-qbllq-cai"
export MockKongswap="2ipq2-uqaaa-aaaar-qailq-cai"
export MintingVault="z3jul-lqaaa-aaaad-qg6ua-cai"

# Token IDs
export TestToken1="mxzaz-hqaaa-aaaar-qaada-cai"
export TestToken2="zxeu2-7aaaa-aaaaq-aaafa-cai"
export TacoToken="csyra-haaaa-aaaaq-aacva-cai"
export CKUSDC="xevnm-gaaaa-aaaar-qafnq-cai"

# Check if running with sudo
if [ "$EUID" -eq 0 ]; then
  echo "This script should not be run with sudo privileges."
  echo "Please run the script without using sudo."
  exit 1
fi

# Stop dfx and kill related processes
dfx stop

PORTS=("8080" "8000" "40049" "4943")

for port in "${PORTS[@]}"; do
    pids=$(sudo lsof -i tcp:"$port" | sudo awk 'NR!=1 {print $2}' | sudo xargs)
    if [ -n "$pids" ]; then
        sudo kill -9 $pids
    fi
done

# Start dfx clean
dfx start --background --clean

# Create and setup identities
dfx identity new defaultTACO --storage-mode=plaintext
dfx identity export defaultTACO > identity.pem
dfx identity use minterTACO
dfx identity remove defaultTACO
dfx identity import defaultTACO identity.pem --storage-mode=plaintext

dfx identity new minterTACO --storage-mode=plaintext
dfx identity export minterTACO > identity2.pem
dfx identity use defaultTACO
dfx identity remove minterTACO
dfx identity import minterTACO identity2.pem --storage-mode=plaintext

dfx identity new archive_controllerTACO --storage-mode=plaintext
dfx identity export archive_controllerTACO > identity3.pem
dfx identity use defaultTACO
dfx identity remove archive_controllerTACO
dfx identity import archive_controllerTACO identity3.pem --storage-mode=plaintext

# Setup ICP ledger
cd ../
directory_name="ledger_canister"

# Check if the directory exists
if [ -d "./$directory_name" ]; then
    # Delete the directory and its contents
    echo "Directory './$directory_name' already there."
    cd ledger_canister
else
    echo "Directory './$directory_name' does not exist."
    dfx new ledger_canister
    cd ledger_canister
    new_json_content='{
  "canisters": {
    "ledger_canister": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/d87954601e4b22972899e9957e800406a0a6b929/rs/rosetta-api/icp_ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/d87954601e4b22972899e9957e800406a0a6b929/canisters/ledger-canister.wasm.gz",
      "remote": {
        "id": {
          "ic": "ryjl3-tyaaa-aaaaa-aaaba-cai"
        }
      }
    }
  },
  "defaults": {
    "build": {
      "args": "",
      "packtool": ""
    }
  },
  "output_env_file": ".env",
  "version": 1
}'

echo "$new_json_content" > dfx.json
fi

# Setup identities and get account IDs
dfx identity new minterTACO --storage-mode=plaintext
dfx identity use minterTACO
export MINTER_ACCOUNT_ID=$(dfx ledger account-id)
dfx identity use defaultTACO
export DEFAULT_ACCOUNT_ID=$(dfx ledger account-id)

# Download and deploy ICP ledger
curl -o ledger-canister.wasm.gz "https://download.dfinity.systems/ic/d87954601e4b22972899e9957e800406a0a6b929/canisters/ledger-canister.wasm.gz"
mkdir -p ./.dfx/local/canisters/ledger_canister/
cp ledger-canister.wasm.gz ./.dfx/local/canisters/ledger_canister/

yes | dfx deploy --specified-id ryjl3-tyaaa-aaaaa-aaaba-cai ledger_canister --argument "
  (variant {
    Init = record {
      minting_account = \"$MINTER_ACCOUNT_ID\";
      initial_values = vec {
        record {
          \"$DEFAULT_ACCOUNT_ID\";
          record {
            e8s = 10_000_000_000_000_000 : nat64;
          };
        };
      };
      send_whitelist = vec {};
      transfer_fee = opt record {
        e8s = 10_000 : nat64;
      };
      token_symbol = opt \"LICP\";
      token_name = opt \"Local ICP\";
    }
  })
" --mode=reinstall

# Setup test actor accounts and transfer ICP
export TestActor1ACC="711a345146b5013508e630b4fca3645293f20de369b46d20219622cb84628c7e"
export TestActor2ACC="087bd635fda622aa0d7f2d66172a95a42b93eafe7dfd00de80429e9886c6189e"
export TestActor3ACC="758bdb7e54b73605d1d743da9f3aad70637d4cddcba03db13137eaf35f12d375"
export DAOACC="162143a44dd2307a24a809dc05a9022d646882bdc1aebabefdd5576432a624ca"

dfx ledger transfer --memo "433" --e8s 50000000000000 $TestActor1ACC &
dfx ledger transfer --memo "433" --e8s 50000000000000 $TestActor2ACC &
dfx ledger transfer --memo "433" --e8s 50000000000000 $TestActor3ACC &
dfx ledger transfer --memo "433" --e8s 3000000000 $DAOACC &

# Setup ICRC1 tokens
cd ../
directory_name="icrc1_ledger_canister"
#https://dashboard.internetcomputer.org/releases
# Check if the directory exists
if [ -d "./$directory_name" ]; then
    # Delete the directory and its contents
    echo "Directory './$directory_name' is there."
    cd icrc1_ledger_canister
    echo '{
  "canisters": {
    "icrc1_ledger_canister": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/rs/rosetta-api/icrc1/ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
    },
    "icrc1_ledger_canister2": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/rs/rosetta-api/icrc1/ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
    },
    "ckusdc": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/rs/rosetta-api/icrc1/ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
    },
    "tacoToken": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/rs/rosetta-api/icrc1/ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
    }
  },
  "defaults": {
    "build": {
      "args": "",
      "packtool": ""
    }
  },
  "output_env_file": ".env",
  "version": 1
}' > dfx.json
    
else
    echo "Directory './$directory_name' does not exist. Creating..."
    dfx new icrc1_ledger_canister
    cd icrc1_ledger_canister
    echo '{
  "canisters": {
    "icrc1_ledger_canister": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/rs/rosetta-api/icrc1/ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
    },
    "icrc1_ledger_canister2": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/rs/rosetta-api/icrc1/ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
    },
    "ckusdc": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/rs/rosetta-api/icrc1/ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
    },
    "tacoToken": {
      "type": "custom",
      "candid": "https://raw.githubusercontent.com/dfinity/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/rs/rosetta-api/icrc1/ledger/ledger.did",
      "wasm": "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
    }
  },
  "defaults": {
    "build": {
      "args": "",
      "packtool": ""
    }
  },
  "output_env_file": ".env",
  "version": 1
}' > dfx.json
  
fi

# Setup archive controller and other params
dfx identity new minterTACO --storage-mode=plaintext
dfx identity use minterTACO
export MINTER=$(dfx identity get-principal)
dfx identity use archive_controllerTACO
export ARCHIVE_CONTROLLER=$(dfx identity get-principal)
dfx identity use defaultTACO
export DEFAULT=$(dfx identity get-principal)

# Token deployment parameters
export PRE_MINTED_TOKENS=10_000_000_000_000_000_000
export TACO_PREMINT=777_777_700_000_000
export TRANSFER_FEE=10_000
export TRIGGER_THRESHOLD=2000
export NUM_OF_BLOCK_TO_ARCHIVE=1000
export CYCLE_FOR_ARCHIVE_CREATION=10000000000000

# Download ICRC1 ledger wasm
curl -o ic-icrc1-ledger.wasm.gz "https://download.dfinity.systems/ic/5849c6daf2037349bd36dcb6e26ce61c2c6570d0/canisters/ic-icrc1-ledger.wasm.gz"
mkdir -p ./.dfx/local/canisters/icrc1_ledger_canister/
cp ic-icrc1-ledger.wasm.gz ./.dfx/local/canisters/icrc1_ledger_canister/

# Deploy Test Token 1
yes | dfx deploy icrc1_ledger_canister --specified-id $TestToken1 --argument "(variant {Init = record {
     token_symbol = \"TEST1\";
     token_name = \"Test Token 1\";
     minting_account = record { owner = principal \"${MINTER}\" };
     transfer_fee = ${TRANSFER_FEE};
     metadata = vec {};
     initial_balances = vec { record { record { owner = principal \"${DEFAULT}\"; }; ${PRE_MINTED_TOKENS}; }; };
     archive_options = record {
         num_blocks_to_archive = ${NUM_OF_BLOCK_TO_ARCHIVE};
         trigger_threshold = ${TRIGGER_THRESHOLD};
         controller_id = principal \"${ARCHIVE_CONTROLLER}\";
         cycles_for_archive_creation = opt ${CYCLE_FOR_ARCHIVE_CREATION};
     };
 }
})" --mode=reinstall

# Transfer Test Token 1 to test accounts
dfx identity use defaultTACO
dfx canister call icrc1_ledger_canister icrc1_transfer "(record { to = record { owner = principal \"${ActorA}\";};  amount = 5_000_000_000_000;})" &
dfx canister call icrc1_ledger_canister icrc1_transfer "(record { to = record { owner = principal \"${ActorB}\";};  amount = 5_000_000_000_000;})" &
dfx canister call icrc1_ledger_canister icrc1_transfer "(record { to = record { owner = principal \"${ActorC}\";};  amount = 5_000_000_000_000;})" &
dfx canister call icrc1_ledger_canister icrc1_transfer "(record { to = record { owner = principal \"${DAOtreasury}\";};  amount = 3_000_000_000;})" &

# Deploy TACO Token
yes | dfx deploy tacoToken --specified-id $TacoToken --argument "(variant {Init = record {
     token_symbol = \"TACO\";
     token_name = \"TACO\";
     minting_account = record { owner = principal \"${MINTER}\" };
     transfer_fee = 70000;
     metadata = vec {};
     initial_balances = vec { record { record { owner = principal \"${DEFAULT}\"; }; ${TACO_PREMINT}; }; };
     archive_options = record {
         num_blocks_to_archive = ${NUM_OF_BLOCK_TO_ARCHIVE};
         trigger_threshold = ${TRIGGER_THRESHOLD};
         controller_id = principal \"${ARCHIVE_CONTROLLER}\";
         cycles_for_archive_creation = opt ${CYCLE_FOR_ARCHIVE_CREATION};
     };
 }
})" --mode=reinstall

# Transfer TACO to test accounts
dfx identity use defaultTACO
dfx canister call tacoToken icrc1_transfer "(record { to = record { owner = principal \"${ActorA}\";};  amount = 5_000_000_000_000;})" &
dfx canister call tacoToken icrc1_transfer "(record { to = record { owner = principal \"${ActorB}\";};  amount = 5_000_000_000_000;})" &
dfx canister call tacoToken icrc1_transfer "(record { to = record { owner = principal \"${ActorC}\";};  amount = 5_000_000_000_000;})" &
dfx canister call tacoToken icrc1_transfer "(record { to = record { owner = principal \"${DAOtreasury}\";};  amount = 30_000_000_000;})" &
dfx canister call tacoToken icrc1_transfer "(record { 
  to = record { 
    owner = principal \"${DAOtreasury}\"; 
    subaccount = opt vec {1; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0}
  };  
  amount = 3_000_000_000;
})" &

# Deploy Test Token 2
yes | dfx deploy icrc1_ledger_canister2 --specified-id $TestToken2 --argument "(variant {Init = record {
     token_symbol = \"TEST2\";
     token_name = \"Test Token 2\";
     minting_account = record { owner = principal \"${MINTER}\" };
     transfer_fee = ${TRANSFER_FEE};
     metadata = vec {};
     initial_balances = vec { record { record { owner = principal \"${DEFAULT}\"; }; ${PRE_MINTED_TOKENS}; }; };
     archive_options = record {
         num_blocks_to_archive = ${NUM_OF_BLOCK_TO_ARCHIVE};
         trigger_threshold = ${TRIGGER_THRESHOLD};
         controller_id = principal \"${ARCHIVE_CONTROLLER}\";
         cycles_for_archive_creation = opt ${CYCLE_FOR_ARCHIVE_CREATION};
     };
 }
})" --mode=reinstall

# Transfer Test Token 2 to test accounts
dfx identity use defaultTACO
dfx canister call icrc1_ledger_canister2 icrc1_transfer "(record { to = record { owner = principal \"${ActorA}\";};  amount = 5_000_000_000_000;})" &
dfx canister call icrc1_ledger_canister2 icrc1_transfer "(record { to = record { owner = principal \"${ActorB}\";};  amount = 5_000_000_000_000;})" &
dfx canister call icrc1_ledger_canister2 icrc1_transfer "(record { to = record { owner = principal \"${ActorC}\";};  amount = 5_000_000_000_000;})" &
dfx canister call icrc1_ledger_canister2 icrc1_transfer "(record { to = record { owner = principal \"${DAOtreasury}\";};  amount = 3_000_000_000;})" &

# Deploy CKUSDC Token
yes | dfx deploy ckusdc --specified-id $CKUSDC --argument "(variant {Init = record {
     token_symbol = \"CKUSDC\";
     token_name = \"Circle USDC\";
     minting_account = record { owner = principal \"${MINTER}\" };
     transfer_fee = ${TRANSFER_FEE};
     metadata = vec {};
     initial_balances = vec { record { record { owner = principal \"${DEFAULT}\"; }; ${PRE_MINTED_TOKENS}; }; };
     archive_options = record {
         num_blocks_to_archive = ${NUM_OF_BLOCK_TO_ARCHIVE};
         trigger_threshold = ${TRIGGER_THRESHOLD};
         controller_id = principal \"${ARCHIVE_CONTROLLER}\";
         cycles_for_archive_creation = opt ${CYCLE_FOR_ARCHIVE_CREATION};
     };
 }
})" --mode=reinstall

# Transfer CKUSDC to test accounts
dfx identity use defaultTACO
dfx canister call ckusdc icrc1_transfer "(record { to = record { owner = principal \"${ActorA}\";};  amount = 5_000_000_000_000;})" &
dfx canister call ckusdc icrc1_transfer "(record { to = record { owner = principal \"${ActorB}\";};  amount = 5_000_000_000_000;})" &
dfx canister call ckusdc icrc1_transfer "(record { to = record { owner = principal \"${ActorC}\";};  amount = 5_000_000_000_000;})" &
dfx canister call ckusdc icrc1_transfer "(record { to = record { owner = principal \"${DAOtreasury}\";};  amount = 3_000_000_000;})" &


cd ../DAO

# Deploy mock ICPswap
dfx canister create --specified-id $MockICPSwap mockICPswap
yes | dfx deploy --specified-id $MockICPSwap mockICPswap --with-cycles 10000000000000 --mode=reinstall

# Deploy mock Kongswap  
dfx canister create --specified-id $MockKongswap mockKongswap
yes | dfx deploy --specified-id $MockKongswap mockKongswap --with-cycles 10000000000000 --mode=reinstall  

# Deploy actor A
dfx canister create --specified-id $ActorA actorA
yes | dfx deploy --specified-id $ActorA actorA --with-cycles 10000000000000 --mode=reinstall 

# Deploy actor B
dfx canister create --specified-id $ActorB actorB
yes | dfx deploy --specified-id $ActorB actorB --with-cycles 10000000000000 --mode=reinstall 

# Deploy actor C
dfx canister create --specified-id $ActorC actorC
yes | dfx deploy --specified-id $ActorC actorC --with-cycles 10000000000000 --mode=reinstall 

# Deploy neuron snapshot canister
dfx canister create --specified-id $NeuronSnapshot neuronSnapshot
yes | dfx deploy --specified-id $NeuronSnapshot neuronSnapshot --with-cycles 10000000000000 --mode=reinstall

dfx canister call neuronSnapshot setTest '(true)'

# Deploy main DAO canister
dfx canister create --specified-id $DAO DAO_backend
yes | dfx deploy --specified-id $DAO DAO_backend --with-cycles 10000000000000 --mode=reinstall

dfx canister call DAO_backend updateSystemState '(variant {Active})'

# Deploy test script
dfx canister create --specified-id $TestScript testDAO
yes | dfx deploy --specified-id $TestScript testDAO --with-cycles 10000000000000 --mode=reinstall




# Setup admin permissions for DAO, Treasury and Test Script
# First add the admins
echo "Adding admins..."
dfx canister call DAO_backend addAdmin "(principal \"$DAO\")" &
dfx canister call DAO_backend addAdmin "(principal \"$DAOtreasury\")" &
dfx canister call DAO_backend addAdmin "(principal \"$TestScript\")" &

sleep 1;
echo "Setting up admin permissions..."
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAO\", variant {addAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAO\", variant {removeAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAO\", variant {updateSystemState}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAO\", variant {updateSpamParameters}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAO\", variant {addToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAO\", variant {removeToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAO\", variant {pauseToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAO\", variant {unpauseToken}, 7)" &

dfx canister call DAO_backend grantAdminPermission "(principal \"$DAOtreasury\", variant {addAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAOtreasury\", variant {removeAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAOtreasury\", variant {updateSystemState}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAOtreasury\", variant {updateSpamParameters}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAOtreasury\", variant {addToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAOtreasury\", variant {removeToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAOtreasury\", variant {pauseToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$DAOtreasury\", variant {unpauseToken}, 7)" &

dfx canister call DAO_backend grantAdminPermission "(principal \"$TestScript\", variant {addAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$TestScript\", variant {removeAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$TestScript\", variant {updateSystemState}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$TestScript\", variant {updateSpamParameters}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$TestScript\", variant {addToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$TestScript\", variant {removeToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$TestScript\", variant {pauseToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"$TestScript\", variant {unpauseToken}, 7)" &

dfx canister call neuronSnapshot setTest '(true)' &
dfx canister call DAO_backend setTacoAddress "(principal \"$TacoToken\")" &
# Add a small delay to ensure permissions are set
sleep 1

echo "Admin permissions setup complete!"
# Deploy minting vault



dfx canister call testDAO runTests '()'


# Deploy treasury
dfx canister create --specified-id $DAOtreasury treasury
yes | dfx deploy --specified-id $DAOtreasury treasury --with-cycles 10000000000000 --mode=reinstall


sleep 1

dfx canister create --specified-id "z3jul-lqaaa-aaaad-qg6ua-cai" mintingVault
yes | dfx deploy --specified-id "z3jul-lqaaa-aaaad-qg6ua-cai" mintingVault --with-cycles 10000000000000 --mode=reinstall
dfx canister call mintingVault updateConfiguration '(record {
    minPremium = opt null;
    maxPremium = opt null;
    balanceUpdateInterval = opt null;
    blockCleanupInterval = opt null;
    maxSlippageBasisPoints = opt null;
    PRICE_HISTORY_WINDOW = opt null;
    swappingEnabled = opt true;
})'
dfx canister call mintingVault setTest '(true)'

sleep 1

dfx canister call testDAO runMintingVaultTransactionTests '()'

echo "Deployment and initialization complete!"
sleep 3;
dfx canister call treasury startRebalancing
sleep 100
dfx canister call treasury stopRebalancing
exit





# These are added to copy-paste and test quickly without redeploying everything
dfx canister create --specified-id "ca6gz-lqaaa-aaaaq-aacwa-cai" testDAO
yes | dfx deploy --specified-id "ca6gz-lqaaa-aaaaq-aacwa-cai" testDAO --with-cycles 10000000000000 --mode=reinstall

dfx canister create --specified-id "ywhqf-eyaaa-aaaad-qg6tq-cai" DAO_backend
yes | dfx deploy --specified-id "ywhqf-eyaaa-aaaad-qg6tq-cai" DAO_backend --with-cycles 10000000000000 --mode=reinstall

echo "Adding admins..."
dfx canister call DAO_backend addAdmin "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\")" &
dfx canister call DAO_backend addAdmin "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\")" &
dfx canister call DAO_backend addAdmin "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\")" &

sleep 1;
echo "Setting up admin permissions..."
dfx canister call DAO_backend grantAdminPermission "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\", variant {addAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\", variant {removeAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\", variant {updateSystemState}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\", variant {updateSpamParameters}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\", variant {addToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\", variant {removeToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\", variant {pauseToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ywhqf-eyaaa-aaaad-qg6tq-cai\", variant {unpauseToken}, 7)" &

dfx canister call DAO_backend grantAdminPermission "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\", variant {addAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\", variant {removeAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\", variant {updateSystemState}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\", variant {updateSpamParameters}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\", variant {addToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\", variant {removeToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\", variant {pauseToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"z4is7-giaaa-aaaad-qg6uq-cai\", variant {unpauseToken}, 7)" &

dfx canister call DAO_backend grantAdminPermission "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\", variant {addAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\", variant {removeAdmin}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\", variant {updateSystemState}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\", variant {updateSpamParameters}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\", variant {addToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\", variant {removeToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\", variant {pauseToken}, 7)" &
dfx canister call DAO_backend grantAdminPermission "(principal \"ca6gz-lqaaa-aaaaq-aacwa-cai\", variant {unpauseToken}, 7)" &
dfx canister call DAO_backend setTacoAddress '(principal "csyra-haaaa-aaaaq-aacva-cai")'
dfx canister call DAO_backend updateSystemState '(variant {Active})'
sleep 1;

dfx canister call DAO_backend updateSystemState '(variant {Active})'


dfx canister call testDAO runTests '()'



dfx canister create --specified-id "z4is7-giaaa-aaaad-qg6uq-cai" treasury
yes | dfx deploy --specified-id "z4is7-giaaa-aaaad-qg6uq-cai" treasury --with-cycles 10000000000000 --mode=reinstall



