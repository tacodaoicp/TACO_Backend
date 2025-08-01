lib
Base Library for ICRC-3 Standards

This library includes the necessary functions, types, and classes to build an ICRC-3 standard transactionlog. It provides an implementation of the ICRC3 class which manages the transaction ledger, archives, and certificate store.

CurrentState
[source]
type CurrentState = MigrationTypes.Current.State;
Represents the current state of the migration

InitArgs
[source]
type InitArgs = MigrationTypes.Args;
Transaction
[source]
type Transaction = MigrationTypes.Current.Transaction;
Represents a transaction

BlockType
[source]
type BlockType = MigrationTypes.Current.BlockType;
Value
[source]
type Value = MigrationTypes.Current.Value;
State
[source]
type State = MigrationTypes.State;
Stats
[source]
type Stats = MigrationTypes.Current.Stats;
Environment
[source]
type Environment = MigrationTypes.Current.Environment;
TransactionRange
[source]
type TransactionRange = MigrationTypes.Current.TransactionRange;
GetTransactionsResult
[source]
type GetTransactionsResult = MigrationTypes.Current.GetTransactionsResult;
DataCertificate
[source]
type DataCertificate = MigrationTypes.Current.DataCertificate;
Tip
[source]
type Tip = MigrationTypes.Current.Tip;
GetArchivesArgs
[source]
type GetArchivesArgs = MigrationTypes.Current.GetArchivesArgs;
GetArchivesResult
[source]
type GetArchivesResult = MigrationTypes.Current.GetArchivesResult;
GetArchivesResultItem
[source]
type GetArchivesResultItem = MigrationTypes.Current.GetArchivesResultItem;
GetBlocksArgs
[source]
type GetBlocksArgs = MigrationTypes.Current.GetBlocksArgs;
GetBlocksResult
[source]
type GetBlocksResult = MigrationTypes.Current.GetBlocksResult;
UpdateSetting
[source]
type UpdateSetting = MigrationTypes.Current.UpdateSetting;
IC
[source]
type IC = MigrationTypes.Current.IC;
Represents the IC actor

CertTree
[source]
let CertTree;
initialState
[source]
func initialState() : State;
Initializes the initial state

Returns the initial state of the migration.

currentStateVersion
[source]
let currentStateVersion;
Returns the current state version

init
[source]
let init;
Initializes the migration

This function is used to initialize the migration with the provided stored state.

Arguments: - stored: The stored state of the migration (nullable) - canister: The canister ID of the migration - environment: The environment object containing optional callbacks and functions

Returns: - The current state of the migration

helper
[source]
let helper;
Helper library for common functions

Legacy
[source]
let Legacy;
Service
[source]
type Service = Service.Service;
Init
[source]
func Init<$>(config : {
	manager : ClassPlusLib.ClassPlusInitializationManager;
	initialState : State;
	args : ?InitArgs;
	pullEnvironment : ?) -> Environment);
	onInitialize : ?(ICRC3 -> async* (;
	onStorageChange : State) -> (;
}) : () -> ICRC3;
ICRC3
[source]
class ICRC3(stored : ?State, caller : Principal, canister : Principal, args : ?InitArgs, environment_passed : ?Environment, storageChanged : (State) -> ());
The ICRC3 class manages the transaction ledger, archives, and certificate store.

The ICRC3 class provides functions for adding a record to the ledger, getting archives, getting the certificate, and more.

environment
[source]
let environment
migrate
[source]
let migrate
The migrate function

add_record
[source]
func add_record<$>(new_record : Transaction, top_level : ?Value) : Nat
Adds a record to the transaction ledger

This function adds a new record to the transaction ledger.

Arguments: - new_record: The new record to add - top_level: The top level value (nullable)

Returns: - The index of the new record

Throws: - An error if the op field is missing from the transaction

get_archives
[source]
func get_archives(request : Service.GetArchivesArgs) : Service.GetArchivesResult
Returns the archive index for the ledger

This function returns the archive index for the ledger.

Arguments: - request: The archive request

Returns: - The archive index

get_tip_certificate
[source]
func get_tip_certificate() : ?Service.DataCertificate
Returns the certificate for the ledger

This function returns the certificate for the ledger.

Returns: - The data certificate (nullable)

get_tip
[source]
func get_tip() : Tip
Returns the latest hash and lastest index along with a witness

This function returns the latest hash, latest index, and the witness for the ledger.

Returns: - The tip information

update_supported_blocks
[source]
func update_supported_blocks(supported_blocks : [BlockType]) : ()
Updates the controllers for the given canister

This function updates the controllers for the given canister.

Arguments: - canisterId: The canister ID

update_settings
[source]
func update_settings(settings : [UpdateSetting]) : [Bool]
check_clean_up
[source]
func check_clean_up<$>() : async ()
Runs the clean up process to move records to archive canisters

This function runs the clean up process to move records to archive canisters.

stats
[source]
func stats() : Stats
Returns the statistics of the migration

This function returns the statistics of the migration.

Returns: - The migration statistics

get_state
[source]
func get_state() : CurrentState
Returns the statistics of the migration

This function returns the statistics of the migration.

Returns: - The migration statistics

supported_block_types
[source]
func supported_block_types() : [BlockType]
@returns {Array<BlockType>} The array of supported block types.

get_blocks
[source]
func get_blocks(args : Service.GetBlocksArgs) : Service.GetBlocksResult
This function returns a set of transactions and pointers to archives if necessary.

Arguments: - args: The transaction range

Returns: - The result of getting transactions

get_blocks_legacy
[source]
func get_blocks_legacy(args : Legacy.GetBlocksRequest) : Legacy.GetTransactionsResponse
Legacy version of get_blocks that returns transactions in the legacy format

This function uses the same core logic as get_blocks but converts the results to legacy transaction format and uses legacy archive callbacks.

Arguments: - args: The GetBlocksRequest range

Returns: - Legacy transaction response with converted transactions

type InitArgs
type Transaction
type BlockType
type Value
type State
type Stats
type Environment
type TransactionRange
type GetTransactionsResult
type DataCertificate
type Tip
type GetArchivesArgs
type GetArchivesResult
type GetArchivesResultItem
type GetBlocksArgs
type GetBlocksResult
type UpdateSetting
type IC
value CertTree
func initialState
value currentStateVersion
value init
value helper
value Legacy
type Service
func Init
class ICRC3
value environment
value migrate
func add_record
func get_archives
func get_tip_certificate
func get_tip
func update_supported_blocks
func update_settings
func check_clean_up
func stats
func get_state
func supported_block_types
func get_blocks
func get_blocks_legacy


