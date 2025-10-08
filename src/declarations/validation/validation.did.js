export const idlFactory = ({ IDL }) => {
  const TokenType = IDL.Variant({
    'ICP' : IDL.Null,
    'ICRC3' : IDL.Null,
    'ICRC12' : IDL.Null,
  });
  const ValidationResult = IDL.Variant({ 'Ok' : IDL.Text, 'Err' : IDL.Text });
  const Subaccount = IDL.Vec(IDL.Nat8);
  return IDL.Service({
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
  });
};
export const init = ({ IDL }) => { return []; };
