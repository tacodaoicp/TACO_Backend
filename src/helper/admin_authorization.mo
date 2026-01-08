import Principal "mo:base/Principal";

module {
  // Master admin principal text IDs - centralized list for all TACO canisters
  public let masterAdminTexts = [
    "lhdfz-wqaaa-aaaaq-aae3q-cai", // TACO DAO Governance canister
    "d7zib-qo5mr-qzmpb-dtyof-l7yiu-pu52k-wk7ng-cbm3n-ffmys-crbkz-nae",
    "uuyso-zydjd-tsb4o-lgpgj-dfsvq-awald-j2zfp-e6h72-d2je3-whmjr-xae", // lx7ws-diaaa-aaaag-aubda-cai.icp0.io identities
    "5uvsz-em754-ulbgb-vxihq-wqyzd-brdgs-snzlu-mhlqw-k74uu-4l5h3-2qe",
    "6mxg4-njnu6-qzizq-2ekit-rnagc-4d42s-qyayx-jghoe-nd72w-elbsy-xqe",
    "6q3ra-pds56-nqzzc-itigw-tsw4r-vs235-yqx5u-dg34n-nnsus-kkpqf-aqe",
    "chxs6-z6h3t-hjrgk-i5x57-rm7fm-3tvlz-b352m-heq2g-hu23b-sxasf-kqe", // tacodao.com identities
    "k2xol-5avzc-lf3wt-vwoft-pjx6k-77fjh-7pera-6b7qt-fwt5e-a3ekl-vqe",
    "as6jn-gaoo7-k4kji-tdkxg-jlsrk-avxkc-zu76j-vz7hj-di3su-2f74z-qqe",
    "qgjut-u3ase-3lxef-vcxtr-4g6jb-mazlw-jpins-wrvpv-jt5wn-2nrx6-sae",
    "r27hb-ckxon-xohqv-afcvx-yhemm-xoggl-37dg6-sfyt3-n6jer-ditge-6qe", // staging identities
    "yjdlk-jqx52-ha6xa-w6iqe-b4jrr-s5ova-mirv4-crlfi-xgsaa-ib3cg-3ae",
    "as6jn-gaoo7-k4kji-tdkxg-jlsrk-avxkc-zu76j-vz7hj-di3su-2f74z-qqe",
    "odoge-dr36c-i3lls-orjen-eapnp-now2f-dj63m-3bdcd-nztox-5gvzy-sqe",
    "nfzo4-i26mj-e2tuj-bt3ba-cuco4-vcqxx-ybjw7-gzyzh-kvyp7-wjeyp-hqe"
  ];

  // Check if caller is a master admin (human admin or known canister)
  public func isMasterAdmin(caller: Principal, isKnownCanister: (Principal) -> Bool) : Bool {
    // Check if caller is a human master admin
    for (adminText in masterAdminTexts.vals()) {
      if (Principal.fromText(adminText) == caller) {
        return true;
      };
    };
    
    // Check if caller is one of our own canisters
    isKnownCanister(caller);
  };

  // Standard admin check (master admin or controller)
  public func isAdmin(caller: Principal, isKnownCanister: (Principal) -> Bool) : Bool {
    isMasterAdmin(caller, isKnownCanister) or Principal.isController(caller)
  };

}