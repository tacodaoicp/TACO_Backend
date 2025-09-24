import Text "mo:base/Text";
import Nat64 "mo:base/Nat64";
import Int32 "mo:base/Int32";
import Result "mo:base/Result";
import Error "mo:base/Error";
import Logger "../helper/logger";
import NNSTypes "./nns_types";

module {

  // Standard text template for copied NNS proposals
  public func getCopiedProposalTemplate() : Text {
    "🔗 **Copied from NNS Proposal**\n\n" #
    "This is a motion proposal to discuss and potentially adopt the same action as NNS Proposal #{proposal_id}.\n\n" #
    "**Original NNS Proposal Details:**\n" #
    "- **ID:** {proposal_id}\n" #
    "- **Type:** {proposal_type}\n" #
    "- **Title:** {title}\n" #
    "- **Link:** {link}\n\n" #
    "**Original Summary:**\n{summary}\n\n" #
    "---\n\n" #
    "Please review the original proposal and vote accordingly. This motion serves to gauge community sentiment and potentially coordinate our DAO's position on this matter.";
  };

  // SNS Proposal types for creating motion proposals
  public type Motion = {
    motion_text : Text;
  };

  public type Action = {
    #Motion : Motion;
  };

  public type Proposal = {
    url : Text;
    title : Text;
    action : ?Action;
    summary : Text;
  };

  public type Command = {
    #MakeProposal : Proposal;
  };

  public type ManageNeuron = {
    subaccount : Blob;
    command : ?Command;
  };

  public type GovernanceError = {
    error_message : Text;
    error_type : Int32;
  };

  public type GetProposal = {
    proposal_id : ?{ id : Nat64 };
  };

  public type ManageNeuronResponse = {
    command : ?{
      #Error : GovernanceError;
      #MakeProposal : GetProposal;
    };
  };

  // Result types for our functions
  public type CopyNNSProposalResult = Result.Result<Nat64, CopyNNSProposalError>;

  public type CopyNNSProposalError = {
    #NNSProposalNotFound;
    #SNSGovernanceError : GovernanceError;
    #NetworkError : Text;
    #InvalidProposalData : Text;
    #UnauthorizedCaller;
  };

  // NNS Governance canister actor type
  public type NNSGovernanceActor = actor {
    get_proposal_info : shared query (Nat64) -> async ?NNSTypes.ProposalInfo;
    list_proposals : shared query (NNSTypes.ListProposalInfo) -> async NNSTypes.ListProposalInfoResponse;
  };

  // SNS Governance canister actor type
  public type SNSGovernanceActor = actor {
    manage_neuron : shared (ManageNeuron) -> async ManageNeuronResponse;
  };

  // Helper function to format proposal text
  public func formatProposalText(
    proposalId : Nat64,
    proposalType : Text,
    title : Text,
    summary : Text,
    link : Text
  ) : Text {
    let template = getCopiedProposalTemplate();
    let formatted = Text.replace(template, #text("{proposal_id}"), Nat64.toText(proposalId));
    let formatted2 = Text.replace(formatted, #text("{proposal_type}"), proposalType);
    let formatted3 = Text.replace(formatted2, #text("{title}"), title);
    let formatted4 = Text.replace(formatted3, #text("{summary}"), summary);
    let formatted5 = Text.replace(formatted4, #text("{link}"), link);
    formatted5;
  };

  // Helper function to extract proposal type from Action
  public func getProposalTypeText(action : ?NNSTypes.Action) : Text {
    switch (action) {
      case (null) { "Unknown" };
      case (?act) {
        switch (act) {
          case (#Motion(_)) { "Motion" };
          case (#ManageNeuron(_)) { "Manage Neuron" };
          case (#ManageNetworkEconomics(_)) { "Network Economics" };
          case (#RewardNodeProvider(_)) { "Reward Node Provider" };
          case (#RewardNodeProviders(_)) { "Reward Node Providers" };
          case (#CreateServiceNervousSystem(_)) { "Create Service Nervous System" };
          case (#InstallCode(_)) { "Install Code" };
          case (#UpdateCanisterSettings(_)) { "Update Canister Settings" };
          case (#StopOrStartCanister(_)) { "Stop or Start Canister" };
          case (#ExecuteNnsFunction(_)) { "Execute NNS Function" };
          case (#RegisterKnownNeuron(_)) { "Register Known Neuron" };
          case (#DeregisterKnownNeuron(_)) { "Deregister Known Neuron" };
          case (#ApproveGenesisKyc(_)) { "Approve Genesis KYC" };
          case (#AddOrRemoveNodeProvider(_)) { "Add or Remove Node Provider" };
          case (#SetDefaultFollowees(_)) { "Set Default Followees" };
          case (#OpenSnsTokenSwap(_)) { "Open SNS Token Swap" };
          case (#SetSnsTokenSwapOpenTimeWindow(_)) { "Set SNS Token Swap Open Time Window" };
          case (#FulfillSubnetRentalRequest(_)) { "Fulfill Subnet Rental Request" };
        };
      };
    };
  };

  // Helper function to generate NNS proposal link
  public func generateNNSProposalLink(proposalId : Nat64) : Text {
    "https://nns.ic0.app/proposal/?u=qoctq-giaaa-aaaaa-aaaea-cai&proposal=" # Nat64.toText(proposalId);
  };

  // Test function to verify template formatting
  public func testFormatProposalText() : Text {
    formatProposalText(
      138601,
      "Motion",
      "Test Proposal Title",
      "This is a test summary of the original proposal.",
      "https://nns.ic0.app/proposal/?u=qoctq-giaaa-aaaaa-aaaea-cai&proposal=138601"
    );
  };

  // Main function to copy an NNS proposal to SNS
  public func copyNNSProposal(
    nnsProposalId : Nat64,
    nnsGovernance : NNSGovernanceActor,
    snsGovernance : SNSGovernanceActor,
    proposerSubaccount : Blob,
    logger : Logger.Logger
  ) : async CopyNNSProposalResult {
    logger.info("NNSPropCopy", "Starting copy of NNS proposal " # Nat64.toText(nnsProposalId), "copyNNSProposal");

    try {
      // Fetch the NNS proposal
      let nnsProposalOpt = await nnsGovernance.get_proposal_info(nnsProposalId);
      
      switch (nnsProposalOpt) {
        case (null) {
          logger.warn("NNSPropCopy", "NNS proposal not found: " # Nat64.toText(nnsProposalId), "copyNNSProposal");
          return #err(#NNSProposalNotFound);
        };
        case (?nnsProposal) {
          // Extract proposal details
          let proposalAction = switch (nnsProposal.proposal) {
            case (null) { null };
            case (?prop) { prop.action };
          };
          let proposalType = getProposalTypeText(proposalAction);
          
          let title = switch (nnsProposal.proposal) {
            case (null) { "Untitled Proposal" };
            case (?prop) { 
              switch (prop.title) {
                case (null) { "Untitled Proposal" };
                case (?t) { t };
              };
            };
          };
          
          let summary = switch (nnsProposal.proposal) {
            case (null) { "No summary provided" };
            case (?prop) { prop.summary };
          };
          let link = generateNNSProposalLink(nnsProposalId);

          // Format the motion text
          let motionText = formatProposalText(nnsProposalId, proposalType, title, summary, link);

          // Create the SNS motion proposal
          let snsProposal : Proposal = {
            url = link;
            title = "Motion: Copy of NNS Proposal #" # Nat64.toText(nnsProposalId) # " - " # title;
            action = ?#Motion({ motion_text = motionText });
            summary = "Motion to discuss NNS Proposal #" # Nat64.toText(nnsProposalId) # ". See full details in the motion text.";
          };

          let manageNeuronRequest : ManageNeuron = {
            subaccount = proposerSubaccount;
            command = ?#MakeProposal(snsProposal);
          };

          // Submit the proposal to SNS governance
          logger.info("NNSPropCopy", "Submitting motion proposal to SNS governance", "copyNNSProposal");
                  
          let response = await snsGovernance.manage_neuron(manageNeuronRequest);

          switch (response.command) {
            case (null) {
              logger.error("NNSPropCopy", "No response from SNS governance", "copyNNSProposal");
              return #err(#SNSGovernanceError({ error_message = "No response from SNS governance"; error_type = 0 }));
            };
            case (?cmd) {
              switch (cmd) {
                case (#Error(error)) {
                  logger.error("NNSPropCopy", "SNS governance error: " # error.error_message, "copyNNSProposal");
                  return #err(#SNSGovernanceError(error));
                };
                case (#MakeProposal(proposal)) {
                  let proposalId : Nat64 = switch (proposal.proposal_id) {
                    case (null) { 0 : Nat64 };
                    case (?pid) { pid.id };
                  };
                  logger.info("NNSPropCopy", "Successfully created SNS proposal " # Nat64.toText(proposalId), "copyNNSProposal");
                  return #ok(proposalId);
                };
              };
            };
          };
        };
      };
    } catch (error) {
      let errorMsg = "Network error while copying proposal: " # Error.message(error);
      logger.error("NNSPropCopy", errorMsg, "copyNNSProposal");
      return #err(#NetworkError(errorMsg));
    };
  };
}
