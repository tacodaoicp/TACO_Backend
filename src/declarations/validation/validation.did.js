export const idlFactory = ({ IDL }) => {
  const TokenType = IDL.Variant({
    'ICP' : IDL.Null,
    'ICRC3' : IDL.Null,
    'ICRC12' : IDL.Null,
  });
  const ValidationResult = IDL.Variant({ 'Ok' : IDL.Text, 'Err' : IDL.Text });
  const Subaccount = IDL.Vec(IDL.Nat8);
  return IDL.Service({
    'get_canister_cycles' : IDL.Func(
        [],
        [IDL.Record({ 'cycles' : IDL.Nat })],
        ['query'],
      ),
    'get_gnsf1_cnt' : IDL.Func([], [IDL.Nat], ['query']),
    'test_gnsf1' : IDL.Func([], [], []),
    'test_gnsf2' : IDL.Func([IDL.Principal], [], []),
    'validate_addToken' : IDL.Func(
        [IDL.Principal, TokenType],
        [ValidationResult],
        [],
      ),
    'validate_sendToken' : IDL.Func(
        [IDL.Principal, IDL.Nat, IDL.Principal, IDL.Opt(Subaccount)],
        [ValidationResult],
        ['query'],
      ),
    'validate_test_gnsf1' : IDL.Func([], [ValidationResult], ['query']),
    'validate_test_gnsf2' : IDL.Func([], [ValidationResult], ['query']),
  });
};
export const init = ({ IDL }) => { return []; };
