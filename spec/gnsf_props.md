Here’s the essence: the “payload” for ExecuteGenericNervousSystemFunction is just Candid-encoded bytes of the arguments your SNS has defined for the registered generic function. In the frontend, you encode those args with Candid and pass the resulting Uint8Array as payload when making the proposal. DFINITY’s own docs describe that a generic proposal provides the same binary payload to both the validator and the executor methods; you simply need to serialize your arguments into Candid bytes. 
Internet Computer

What to encode

When your SNS registered the generic function (via AddGenericNervousSystemFunction), it specified:

target_canister_id

target_method_name (the execution method)

validator_canister_id / validator_method_name

(nowadays) a topic for the function

The payload you supply must be the Candid encoding of the arguments that the target method expects. The validator receives the exact same bytes to check and render. 
Internet Computer
+1

How to encode (frontend / JS)

Use the Candid IDL encoder from agent-js / @dfinity/candid:

import { IDL } from '@dfinity/candid';

// Example: target method expects (player_id : nat64)
function encodePayloadForGNSF(playerId: bigint): Uint8Array {
  const types = [IDL.Nat64];
  const values = [playerId];
  const bytes = IDL.encode(types, values);
  return new Uint8Array(bytes);
}

// Then create the proposal:
async function makeGNSFProposal({
  governanceActor,        // SNS Governance actor
  neuronId,               // { id: [{ NeuronId: BigInt(...) }] } or as used in your lib
  functionId,             // nat64 assigned when the GNSF was registered
  title, url, summary,
  argsBytes               // Uint8Array from encodePayloadForGNSF(...)
}) {
  return governanceActor.manage_neuron({
    id: [{ NeuronId: neuronId }],
    neuron_id_or_subaccount: [{ NeuronId: neuronId }],
    command: [{
      MakeProposal: {
        title, url, summary,
        action: {
          ExecuteGenericNervousSystemFunction: {
            function_id: functionId,
            payload: Array.from(argsBytes) // or pass as Buffer/Vec<u8> depending on your binding
          }
        }
      }
    }]
  });
}

The exact IDL.* types and value structure must match your function’s Candid signature (e.g., IDL.Record, IDL.Variant, nested types, etc.). The community example that inspired this uses exactly IDL.encode(...) to produce the payload. 
Internet Computer Developer Forum

CLI/other languages (for reference)

CLI: didc encode can turn Candid text into bytes/hex for proposals and is useful for verification and tooling. 
npm

Rust: candid::encode_args((arg1, arg2, ...))? (or encode_one) returns Vec<u8> suitable as the payload. (Same model as above; official docs outline Candid serialization.) 
Internet Computer

Go (agent-go): candid.Encode(types, values) or candid.EncodeValueString("(record {...})") produces the bytes. 
Go Packages

Putting it behind “a button”

Ensure the user is authenticated and has a hotkey that controls a neuron in your SNS (you’ll submit manage_neuron → MakeProposal from the frontend).

Build the Candid payload with IDL.encode(...) for your GNSF’s expected args.

Call sns_governance.manage_neuron with MakeProposal → ExecuteGenericNervousSystemFunction { function_id, payload } as above.

Surface the proposal ID / link to your users.

Two practical footnotes

Registration & topics: If you’re still adding the GNSF, note that recent governance updates require assigning a topic when registering/maintaining custom functions; otherwise you’ll see errors like “NervousSystemFunction must have a topic.” 
Internet Computer
+1

What ic-toolkit.app does: Tools like IC Toolkit perform the same step under the hood—Candid-encode the args and submit them via ExecuteGenericNervousSystemFunction using the registered function’s function_id. The official docs explicitly describe this flow for generic proposals.