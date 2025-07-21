import BTree "mo:stableheapbtreemap/BTree";
import Vector "mo:vector";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Principal "mo:base/Principal";
import ICRC3 "mo:icrc3-mo/service";
import ArchiveTypes "../archives/archive_types";

module {
  public type Block = ArchiveTypes.Block;

  public class ArchiveICRC3<T>(
    getBlocks: () -> BTree.BTree<Nat, Block>,
    getNextBlockIndex: () -> Nat,
    getCanisterId: () -> Principal,
    supportedBlockTypes: [Text]
  ) {

    public func icrc3_get_archives(_args : ICRC3.GetArchivesArgs) : ICRC3.GetArchivesResult {
      // For now, this archive is the only one
      // In the future, this could return multiple archive canisters
      [{
        canister_id = getCanisterId();
        start = 0;
        end = getNextBlockIndex();
      }];
    };

    public func icrc3_get_tip_certificate() : ?ICRC3.DataCertificate {
      // This would need to implement proper certification
      // For now, return None - certification would be implemented with IC certification
      null;
    };

    public func icrc3_get_blocks(args : ICRC3.GetBlocksArgs) : ICRC3.GetBlocksResult {
      let results = Vector.new<Block>();
      let archivedBlocks = Vector.new<ICRC3.ArchivedBlock>();
      let blocks = getBlocks();
      let nextBlockIndex = getNextBlockIndex();

      for (arg in args.vals()) {
        let startIndex = arg.start;
        let length = arg.length;
        let endIndex = Nat.min(startIndex + length, nextBlockIndex);

        for (i in Iter.range(startIndex, endIndex - 1)) {
          switch (BTree.get(blocks, Nat.compare, i)) {
            case (?block) {
              Vector.add(results, block);
            };
            case null {
              // Block not found in this archive
            };
          };
        };
      };

      {
        blocks = Vector.toArray(results);
        log_length = nextBlockIndex;
        archived_blocks = Vector.toArray(archivedBlocks);
      };
    };

    public func icrc3_supported_block_types() : [ICRC3.BlockType] {
      Array.map<Text, ICRC3.BlockType>(supportedBlockTypes, func(btype) = {
        block_type = btype;
        url = "https://github.com/TACO-DAO/standards/blob/main/ICRC-3/" # btype # ".md";
      });
    };
  };
} 