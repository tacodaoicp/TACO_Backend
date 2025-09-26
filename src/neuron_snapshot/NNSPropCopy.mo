import Text "mo:base/Text";
import Nat "mo:base/Nat";
import _Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Int32 "mo:base/Int32";
import Result "mo:base/Result";
import Error "mo:base/Error";
import Time "mo:base/Time";
import Int "mo:base/Int";
import _Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Map "mo:map/Map";
import Logger "../helper/logger";
import NNSTypes "./nns_types";
import Debug "mo:base/Debug";
import Principal "mo:base/Principal";

module {

  // Standard text template for copied NNS proposals
  public func getCopiedProposalTemplate() : Text {
    "🔗 **Copied from NNS Proposal**\n\n" #
    "This is a motion proposal that links to NNS Proposal #{proposal_id}. **Please vote ADOPT on this SNS motion as a formality** to confirm it, then cast your actual vote on our web app.\n\n" #
    "**Original NNS Proposal Details:**\n" #
    "- **ID:** {proposal_id}\n" #
    "- **Type:** {proposal_type}\n" #
    "- **Title:** {title}\n" #
    "- **Link:** {link}\n\n" #
    "**Original Summary:**\n{summary}\n\n" #
    "---\n\n" #
    "## How to Vote:\n\n" #
    "1. **Vote ADOPT on this SNS motion** (to avoid rejection costs and confirm the proposal)\n" #
    "2. **Cast your real vote** at: **https://tacodao.com/nnsprop/{proposal_id}**\n\n" #
    "Your voting power on the web app is calculated using your staked TACO DAO neurons. The collective outcome of these \"local votes\" will determine how our DAO's NNS neuron votes on the original NNS proposal.\n\n" #
    "This system allows you to vote REJECT on NNS proposals without rejecting the TACO SNS motion (which would cost the DAO staked TACO tokens in rejection fees).";
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

  public type GetSNSProposalFullResult = Result.Result<SNSProposalData, SNSProposalError>;
  public type GetSNSProposalSummaryResult = Result.Result<SNSProposalSummary, SNSProposalError>;

  public type SNSProposalError = {
    #ProposalNotFound;
    #SNSGovernanceError : GovernanceError;
    #NetworkError : Text;
    #InvalidProposalData : Text;
  };

  // Result type for checking if proposal should be copied
  public type ShouldCopyProposalResult = Result.Result<Bool, CopyNNSProposalError>;

  // Result types for processing sequential proposals
  public type ProcessSequentialProposalsResult = Result.Result<{
    processed_count : Nat;
    new_copied_count : Nat;
    already_copied_count : Nat;
    skipped_count : Nat;
    error_count : Nat;
    highest_processed_id : Nat64;
    newly_copied_proposals : [(Nat64, Nat64)]; // (NNS Proposal ID, SNS Proposal ID)
  }, CopyNNSProposalError>;

  // NNS Governance Topic IDs that should be copied to SNS
  // Based on the official Topic enum from governance.proto
  // Reference: https://github.com/dfinity/ic/blob/b716b47d017d2384a1860bf5e569d66e8072e94d/rs/nns/governance/proto/ic_nns_governance/pb/v1/governance.proto#L41
  private let TOPICS_TO_COPY : [Int32] = [
    5,  // TOPIC_NODE_ADMIN - Node Admin
    6,  // TOPIC_PARTICIPANT_MANAGEMENT - Participant Management  
    10, // TOPIC_NODE_PROVIDER_REWARDS - Node Provider Rewards
    14, // TOPIC_SNS_AND_COMMUNITY_FUND - SNS & Community Fund
  ];

  // NNS Governance canister actor type
  public type NNSGovernanceActor = actor {
    get_proposal_info : shared query (Nat64) -> async ?NNSTypes.ProposalInfo;
    list_proposals : shared query (NNSTypes.ListProposalInfo) -> async NNSTypes.ListProposalInfoResponse;
    manage_neuron : shared (NNSTypes.ManageNeuron) -> async NNSTypes.ManageNeuronResponse;
  };

  // SNS Proposal types for fetching proposals
  public type SNSProposalId = {
    id : Nat64;
  };

  public type GetSNSProposal = {
    proposal_id : ?SNSProposalId;
  };

  public type Tally = {
    no : Nat64;
    yes : Nat64;
    total : Nat64;
    timestamp_seconds : Nat64;
  };

  public type WaitForQuietState = {
    current_deadline_timestamp_seconds : Nat64;
  };

  public type Percentage = {
    basis_points : ?Nat64;
  };

  public type SNSProposalData = {
    id : ?SNSProposalId;
    payload_text_rendering : ?Text;
    action : Nat64;
    failure_reason : ?GovernanceError;
    ballots : [(Text, { vote : Int32; cast_timestamp_seconds : Nat64; voting_power : Nat64 })];
    minimum_yes_proportion_of_total : ?Percentage;
    reward_event_round : Nat64;
    failed_timestamp_seconds : Nat64;
    reward_event_end_timestamp_seconds : ?Nat64;
    proposal_creation_timestamp_seconds : Nat64;
    initial_voting_period_seconds : Nat64;
    reject_cost_e8s : Nat64;
    latest_tally : ?Tally;
    wait_for_quiet_deadline_increase_seconds : Nat64;
    decided_timestamp_seconds : Nat64;
    proposal : ?Proposal;
    proposer : ?{ id : Blob };
    wait_for_quiet_state : ?WaitForQuietState;
    minimum_yes_proportion_of_exercised : ?Percentage;
    is_eligible_for_rewards : Bool;
    executed_timestamp_seconds : Nat64;
  };

  public type GetSNSProposalResult = {
    #Error : GovernanceError;
    #Proposal : SNSProposalData;
  };

  public type GetSNSProposalResponse = {
    result : ?GetSNSProposalResult;
  };

  // SNS Proposal Summary types
  public type VotingStatus = {
    #YesLeading;
    #NoLeading;
    #Tied;
    #NotStarted;
    #Decided;
  };

  public type SNSProposalSummary = {
    proposal_id : Nat64;
    title : Text;
    voting_status : VotingStatus;
    yes_votes : Nat64;
    no_votes : Nat64;
    total_votes : Nat64;
    time_remaining_seconds : ?Nat64; // null if voting has ended
    voting_deadline : Nat64;
    is_decided : Bool;
  };

  // SNS Governance canister actor type
  public type SNSGovernanceActor = actor {
    manage_neuron : shared (ManageNeuron) -> async ManageNeuronResponse;
    get_proposal : shared query (GetSNSProposal) -> async GetSNSProposalResponse;
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

  // Helper function to get topic name from topic ID
  // Based on the official Topic enum from governance.proto
  // Reference: https://github.com/dfinity/ic/blob/b716b47d017d2384a1860bf5e569d66e8072e94d/rs/nns/governance/proto/ic_nns_governance/pb/v1/governance.proto#L41
  public func getTopicName(topicId : Int32) : Text {
    switch (topicId) {
      case (0) { "Unspecified" }; // TOPIC_UNSPECIFIED
      case (1) { "Neuron Management" }; // TOPIC_NEURON_MANAGEMENT
      case (2) { "Exchange Rate" }; // TOPIC_EXCHANGE_RATE
      case (3) { "Network Economics" }; // TOPIC_NETWORK_ECONOMICS
      case (4) { "Governance" }; // TOPIC_GOVERNANCE
      case (5) { "Node Admin" }; // TOPIC_NODE_ADMIN
      case (6) { "Participant Management" }; // TOPIC_PARTICIPANT_MANAGEMENT
      case (7) { "Subnet Management" }; // TOPIC_SUBNET_MANAGEMENT
      case (8) { "Network Canister Management" }; // TOPIC_NETWORK_CANISTER_MANAGEMENT
      case (9) { "KYC" }; // TOPIC_KYC
      case (10) { "Node Provider Rewards" }; // TOPIC_NODE_PROVIDER_REWARDS
      case (11) { "SNS Decentralization Sale (Deprecated)" }; // TOPIC_SNS_DECENTRALIZATION_SALE (superseded)
      case (12) { "Subnet Replica Version Management" }; // TOPIC_SUBNET_REPLICA_VERSION_MANAGEMENT
      case (13) { "Replica Version Management" }; // TOPIC_REPLICA_VERSION_MANAGEMENT
      case (14) { "SNS & Community Fund" }; // TOPIC_SNS_AND_COMMUNITY_FUND
      case (15) { "API Boundary Node Management" }; // TOPIC_API_BOUNDARY_NODE_MANAGEMENT
      case (_) { "Unknown Topic (" # Int32.toText(topicId) # ")" };
    };
  };

  // Helper function to check if a topic should be copied
  public func shouldCopyTopic(topicId : Int32) : Bool {
    for (copyTopicId in TOPICS_TO_COPY.vals()) {
      if (topicId == copyTopicId) {
        return true;
      };
    };
    false;
  };

  // Helper function to calculate current timestamp in seconds
  private func getCurrentTimestampSeconds() : Nat64 {
    Nat64.fromNat(Int.abs(Time.now()) / 1_000_000_000);
  };

  // Helper function to determine voting status from tally
  public func determineVotingStatus(tally : ?Tally, isDecided : Bool) : VotingStatus {
    if (isDecided) {
      return #Decided;
    };

    switch (tally) {
      case (null) { #NotStarted };
      case (?t) {
        if (t.yes > t.no) {
          #YesLeading;
        } else if (t.no > t.yes) {
          #NoLeading;
        } else {
          #Tied;
        };
      };
    };
  };

  // Helper function to calculate time remaining until voting deadline
  public func calculateTimeRemaining(
    proposalCreationTime : Nat64,
    initialVotingPeriod : Nat64,
    waitForQuietState : ?WaitForQuietState,
    isDecided : Bool
  ) : ?Nat64 {
    if (isDecided) {
      return null; // Voting has ended
    };

    let currentTime = getCurrentTimestampSeconds();
    
    // Calculate the deadline based on wait-for-quiet state or initial voting period
    let deadline = switch (waitForQuietState) {
      case (?wfq) { wfq.current_deadline_timestamp_seconds };
      case (null) { proposalCreationTime + initialVotingPeriod };
    };

    if (currentTime >= deadline) {
      ?0; // Voting should have ended
    } else {
      ?(deadline - currentTime);
    };
  };

  let (test_doSendSNSProp) = true;

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
          // First check if this proposal topic should be copied
          let topicId = nnsProposal.topic;
          let topicName = getTopicName(topicId);
          
          if (not shouldCopyTopic(topicId)) {
            logger.warn(
              "NNSPropCopy", 
              "Refusing to copy NNS proposal " # Nat64.toText(nnsProposalId) # 
              " - topic '" # topicName # "' (ID: " # Int32.toText(topicId) # ") is not in copy list",
              "copyNNSProposal"
            );
            return #err(#InvalidProposalData("Proposal topic '" # topicName # "' should not be copied to SNS"));
          };
          
          logger.info(
            "NNSPropCopy",
            "Topic check passed for NNS proposal " # Nat64.toText(nnsProposalId) # 
            " - topic '" # topicName # "' (ID: " # Int32.toText(topicId) # ") is approved for copying",
            "copyNNSProposal"
          );

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
          
          if (not test_doSendSNSProp) {
          //logger.info("NNSPropCopy", debug_show(snsProposal), "copyNNSProposal");
          Debug.print("manageNeuronRequest: " # debug_show(snsProposal));
          //Debug.print("manageNeuronRequest: " # debug_show(nnsProposalId));
          return #err(#NetworkError("TESTING"))
          } else {
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
      };
    } catch (error) {
      let errorMsg = "Network error while copying proposal: " # Error.message(error);
      logger.error("NNSPropCopy", errorMsg, "copyNNSProposal");
      return #err(#NetworkError(errorMsg));
    };
  };

  // Function to get full SNS proposal details
  public func getSNSProposalFull(
    proposalId : Nat64,
    snsGovernance : SNSGovernanceActor,
    logger : Logger.Logger
  ) : async GetSNSProposalFullResult {
    logger.info("SNSProposal", "Fetching full proposal details for ID: " # Nat64.toText(proposalId), "getSNSProposalFull");

    try {
      let request : GetSNSProposal = {
        proposal_id = ?{ id = proposalId };
      };

      let response = await snsGovernance.get_proposal(request);
      
      switch (response.result) {
        case (null) {
          logger.warn("SNSProposal", "No result in response for proposal " # Nat64.toText(proposalId), "getSNSProposalFull");
          return #err(#ProposalNotFound);
        };
        case (?result) {
          switch (result) {
            case (#Error(error)) {
              logger.error("SNSProposal", "SNS governance error: " # error.error_message, "getSNSProposalFull");
              return #err(#SNSGovernanceError(error));
            };
            case (#Proposal(proposalData)) {
              logger.info("SNSProposal", "Successfully fetched proposal " # Nat64.toText(proposalId), "getSNSProposalFull");
              return #ok(proposalData);
            };
          };
        };
      };
    } catch (error) {
      let errorMsg = "Network error while fetching proposal: " # Error.message(error);
      logger.error("SNSProposal", errorMsg, "getSNSProposalFull");
      return #err(#NetworkError(errorMsg));
    };
  };

  // Function to get SNS proposal summary with voting status
  public func getSNSProposalSummary(
    proposalId : Nat64,
    snsGovernance : SNSGovernanceActor,
    logger : Logger.Logger
  ) : async GetSNSProposalSummaryResult {
    logger.info("SNSProposal", "Fetching proposal summary for ID: " # Nat64.toText(proposalId), "getSNSProposalSummary");

    // First get the full proposal data
    let fullResult = await getSNSProposalFull(proposalId, snsGovernance, logger);
    
    switch (fullResult) {
      case (#err(error)) {
        return #err(error);
      };
      case (#ok(proposalData)) {
        // Extract title from proposal
        let title = switch (proposalData.proposal) {
          case (null) { "Untitled Proposal" };
          case (?prop) { prop.title };
        };

        // Determine if proposal is decided
        let isDecided = proposalData.decided_timestamp_seconds > 0;

        // Get voting status
        let votingStatus = determineVotingStatus(proposalData.latest_tally, isDecided);

        // Get vote counts
        let (yesVotes, noVotes, totalVotes) = switch (proposalData.latest_tally) {
          case (null) { (0 : Nat64, 0 : Nat64, 0 : Nat64) };
          case (?tally) { (tally.yes, tally.no, tally.total) };
        };

        // Calculate time remaining
        let timeRemaining = calculateTimeRemaining(
          proposalData.proposal_creation_timestamp_seconds,
          proposalData.initial_voting_period_seconds,
          proposalData.wait_for_quiet_state,
          isDecided
        );

        // Calculate voting deadline
        let votingDeadline = switch (proposalData.wait_for_quiet_state) {
          case (?wfq) { wfq.current_deadline_timestamp_seconds };
          case (null) { proposalData.proposal_creation_timestamp_seconds + proposalData.initial_voting_period_seconds };
        };

        let summary : SNSProposalSummary = {
          proposal_id = proposalId;
          title = title;
          voting_status = votingStatus;
          yes_votes = yesVotes;
          no_votes = noVotes;
          total_votes = totalVotes;
          time_remaining_seconds = timeRemaining;
          voting_deadline = votingDeadline;
          is_decided = isDecided;
        };

        logger.info("SNSProposal", "Successfully created summary for proposal " # Nat64.toText(proposalId), "getSNSProposalSummary");
        return #ok(summary);
      };
    };
  };

  // Function to check if an NNS proposal should be copied based on topic
  public func shouldCopyNNSProposal(
    nnsProposalId : Nat64,
    nnsGovernance : NNSGovernanceActor,
    logger : Logger.Logger
  ) : async ShouldCopyProposalResult {
    logger.info("NNSPropCopy", "Checking if NNS proposal " # Nat64.toText(nnsProposalId) # " should be copied", "shouldCopyNNSProposal");

    try {
      // Fetch the NNS proposal
      let nnsProposalOpt = await nnsGovernance.get_proposal_info(nnsProposalId);
      
      switch (nnsProposalOpt) {
        case (null) {
          logger.warn("NNSPropCopy", "NNS proposal not found: " # Nat64.toText(nnsProposalId), "shouldCopyNNSProposal");
          return #err(#NNSProposalNotFound);
        };
        case (?nnsProposal) {
          let topicId = nnsProposal.topic;
          let topicName = getTopicName(topicId);
          let shouldCopy = shouldCopyTopic(topicId);
          
          logger.info(
            "NNSPropCopy", 
            "NNS proposal " # Nat64.toText(nnsProposalId) # " has topic: " # topicName # 
            " (ID: " # Int32.toText(topicId) # "), should copy: " # (if shouldCopy "yes" else "no"),
            "shouldCopyNNSProposal"
          );
          
          return #ok(shouldCopy);
        };
      };
    } catch (error) {
      let errorMsg = "Network error while checking proposal: " # Error.message(error);
      logger.error("NNSPropCopy", errorMsg, "shouldCopyNNSProposal");
      return #err(#NetworkError(errorMsg));
    };
  };

  // Function to get detailed information about whether a proposal should be copied
  public func getNNSProposalCopyInfo(
    nnsProposalId : Nat64,
    nnsGovernance : NNSGovernanceActor,
    logger : Logger.Logger
  ) : async Result.Result<{
    proposal_id : Nat64;
    topic_id : Int32;
    topic_name : Text;
    should_copy : Bool;
    reason : Text;
  }, CopyNNSProposalError> {
    logger.info("NNSPropCopy", "Getting copy info for NNS proposal " # Nat64.toText(nnsProposalId), "getNNSProposalCopyInfo");

    try {
      // Fetch the NNS proposal
      let nnsProposalOpt = await nnsGovernance.get_proposal_info(nnsProposalId);
      
      switch (nnsProposalOpt) {
        case (null) {
          logger.warn("NNSPropCopy", "NNS proposal not found: " # Nat64.toText(nnsProposalId), "getNNSProposalCopyInfo");
          return #err(#NNSProposalNotFound);
        };
        case (?nnsProposal) {
          let topicId = nnsProposal.topic;
          let topicName = getTopicName(topicId);
          let shouldCopy = shouldCopyTopic(topicId);
          
          let reason = if (shouldCopy) {
            "Topic '" # topicName # "' is in the list of topics to copy to SNS";
          } else {
            "Topic '" # topicName # "' is not in the list of topics to copy to SNS";
          };
          
          let info = {
            proposal_id = nnsProposalId;
            topic_id = topicId;
            topic_name = topicName;
            should_copy = shouldCopy;
            reason = reason;
          };
          
          logger.info("NNSPropCopy", "Copy info generated for proposal " # Nat64.toText(nnsProposalId), "getNNSProposalCopyInfo");
          return #ok(info);
        };
      };
    } catch (error) {
      let errorMsg = "Network error while getting copy info: " # Error.message(error);
      logger.error("NNSPropCopy", errorMsg, "getNNSProposalCopyInfo");
      return #err(#NetworkError(errorMsg));
    };
  };

  // Function to process NNS proposals sequentially starting from last processed ID + 1
  public func processSequentialNNSProposals(
    startFromId : Nat64,
    maxProposals : Nat,
    copiedProposals : Map.Map<Nat64, Nat64>,
    nnsGovernance : NNSGovernanceActor,
    snsGovernance : SNSGovernanceActor,
    proposerSubaccount : Blob,
    logger : Logger.Logger
  ) : async ProcessSequentialProposalsResult {
    let { n64hash; phash = _ } = Map;
    let nextProposalId = startFromId + 1;
    logger.info("NNSPropCopy", "Starting sequential processing from NNS proposal ID " # Nat64.toText(nextProposalId) # " (max " # Nat.toText(maxProposals) # " proposals)", "processSequentialNNSProposals");

    try {
      var processedCount : Nat = 0;
      var newCopiedCount : Nat = 0;
      var alreadyCopiedCount : Nat = 0;
      var skippedCount : Nat = 0;
      var errorCount : Nat = 0;
      var currentProposalId : Nat64 = nextProposalId;
      var highestProcessedId : Nat64 = startFromId;
      let newlyCopiedProposals = Buffer.Buffer<(Nat64, Nat64)>(maxProposals);

      // Process proposals sequentially until we hit maxProposals or find no more proposals
      var continueProcessing = true;
      while (processedCount < maxProposals and continueProcessing) {
        logger.info("NNSPropCopy", "Checking NNS proposal ID " # Nat64.toText(currentProposalId), "processSequentialNNSProposals");
        
        // Try to get this specific proposal
        let proposalOpt = await nnsGovernance.get_proposal_info(currentProposalId);
        
        switch (proposalOpt) {
          case (null) {
            logger.info("NNSPropCopy", "No proposal found for ID " # Nat64.toText(currentProposalId) # " - stopping sequential processing", "processSequentialNNSProposals");
            // No more proposals found, stop processing
            continueProcessing := false;
          };
          case (?proposal) {
            processedCount += 1;
            highestProcessedId := currentProposalId;
            
            logger.info("NNSPropCopy", "Found NNS proposal " # Nat64.toText(currentProposalId) # " - processing", "processSequentialNNSProposals");
            
            // Check if we've already copied this proposal
            switch (Map.get(copiedProposals, n64hash, currentProposalId)) {
              case (?snsProposalId) {
                alreadyCopiedCount += 1;
                logger.info("NNSPropCopy", "NNS proposal " # Nat64.toText(currentProposalId) # " already copied to SNS proposal " # Nat64.toText(snsProposalId) # " - skipping", "processSequentialNNSProposals");
              };
              case (null) {
                // Not copied yet, check if we should copy it
                let topicId = proposal.topic;
                let topicName = getTopicName(topicId);
                let shouldCopy = shouldCopyTopic(topicId);

                if (shouldCopy) {
                  logger.info(
                    "NNSPropCopy",
                    "NNS proposal " # Nat64.toText(currentProposalId) # " has copyable topic: " # topicName # 
                    " (ID: " # Int32.toText(topicId) # ") and hasn't been copied yet - attempting to copy",
                    "processSequentialNNSProposals"
                  );

                  // Attempt to copy the proposal
                  let copyResult = await copyNNSProposal(
                    currentProposalId,
                    nnsGovernance,
                    snsGovernance,
                    proposerSubaccount,
                    logger
                  );

                  switch (copyResult) {
                    case (#ok(snsProposalId)) {
                      newCopiedCount += 1;
                      newlyCopiedProposals.add((currentProposalId, snsProposalId));
                      
                      // Add to the copied proposals map
                      Map.set(copiedProposals, n64hash, currentProposalId, snsProposalId);
                      
                      logger.info(
                        "NNSPropCopy",
                        "Successfully copied NNS proposal " # Nat64.toText(currentProposalId) # 
                        " to SNS proposal " # Nat64.toText(snsProposalId),
                        "processSequentialNNSProposals"
                      );
                    };
                    case (#err(error)) {
                      logger.error(
                        "NNSPropCopy",
                        "Failed to copy NNS proposal " # Nat64.toText(currentProposalId) # ": " # debug_show(error),
                        "processSequentialNNSProposals"
                      );
                      errorCount += 1;
                      // Continue processing other proposals even if one fails
                    };
                  };
                } else {
                  skippedCount += 1;
                  logger.info(
                    "NNSPropCopy",
                    "NNS proposal " # Nat64.toText(currentProposalId) # " has non-copyable topic: " # topicName # 
                    " (ID: " # Int32.toText(topicId) # ") - skipping",
                    "processSequentialNNSProposals"
                  );
                };
              };
            };
            
            // Move to next proposal ID
            currentProposalId += 1;
          };
        };
      };

      let result = {
        processed_count = processedCount;
        new_copied_count = newCopiedCount;
        already_copied_count = alreadyCopiedCount;
        skipped_count = skippedCount;
        error_count = errorCount;
        highest_processed_id = highestProcessedId;
        newly_copied_proposals = Buffer.toArray(newlyCopiedProposals);
      };

      logger.info(
        "NNSPropCopy",
        "Completed sequential processing: " # Nat.toText(processedCount) # " proposals processed, " #
        Nat.toText(newCopiedCount) # " newly copied, " # Nat.toText(alreadyCopiedCount) # 
        " already copied, " # Nat.toText(skippedCount) # " skipped, " # Nat.toText(errorCount) # 
        " errors, highest ID: " # Nat64.toText(highestProcessedId),
        "processSequentialNNSProposals"
      );

      return #ok(result);

    } catch (error) {
      let errorMsg = "Network error while processing proposals: " # Error.message(error);
      logger.error("NNSPropCopy", errorMsg, "processSequentialNNSProposals");
      return #err(#NetworkError(errorMsg));
    };
  };

  let test_doSendNNSProp = true;

  // Vote on NNS proposal using TACO DAO's neuron
  public func voteOnNNSProposal(
    nnsProposalId : Nat64, 
    vote : Int32, 
    nnsGovernanceCanisterId : Principal,
    tacoDAONeuronId : NNSTypes.NeuronId,
    logger : Logger.Logger
  ) : async Result.Result<Text, CopyNNSProposalError> {
    logger.info("NNSPropCopy", "Voting on NNS proposal " # Nat64.toText(nnsProposalId) # " with vote: " # Int32.toText(vote), "voteOnNNSProposal");

    try {
      // Create NNS governance actor
      let nnsGovernance : NNSGovernanceActor = actor(Principal.toText(nnsGovernanceCanisterId));

      // Create manage neuron command to vote
      let manageNeuronRequest : NNSTypes.ManageNeuron = {
        id = null;
        command = ?#RegisterVote({
          vote = vote;
          proposal = ?{ id = nnsProposalId };
        });
        neuron_id_or_subaccount = ?#NeuronId(tacoDAONeuronId);
      };

      logger.info("NNSPropCopy", "Submitting vote to NNS governance", "voteOnNNSProposal");
      if (not test_doSendNNSProp) {
      Debug.print("manageNeuronRequest: " # debug_show(manageNeuronRequest));
      //return #err(#NetworkError("TESTING"));
      return #ok("TEST OK");
      } else {
      // Submit the vote to NNS governance
      let response = await nnsGovernance.manage_neuron(manageNeuronRequest);

      switch (response.command) {
        case (?#RegisterVote(_)) {
          let voteText = if (vote == 1) { "Yes" } else { "No" };
          let successMsg = "Successfully voted " # voteText # " on NNS proposal " # Nat64.toText(nnsProposalId);
          logger.info("NNSPropCopy", successMsg, "voteOnNNSProposal");
          return #ok(successMsg);
        };
        case (?#Error(error)) {
          let errorMsg = "NNS governance error: " # debug_show(error);
          logger.error("NNSPropCopy", errorMsg, "voteOnNNSProposal");
          return #err(#NetworkError(errorMsg));
        };
        case (_) {
          let errorMsg = "Unexpected response from NNS governance: " # debug_show(response);
          logger.error("NNSPropCopy", errorMsg, "voteOnNNSProposal");
          return #err(#NetworkError(errorMsg));
        };
      };
      };
    } catch (error) {
      let errorMsg = "Network error while voting on NNS proposal: " # Error.message(error);
      logger.error("NNSPropCopy", errorMsg, "voteOnNNSProposal");
      return #err(#NetworkError(errorMsg));
    };
  };
}
