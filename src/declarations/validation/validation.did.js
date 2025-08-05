export const idlFactory = ({ IDL }) => {
  const TokenType = IDL.Variant({
    'ICP' : IDL.Null,
    'ICRC3' : IDL.Null,
    'ICRC12' : IDL.Null,
  });
  const ValidationResult = IDL.Variant({ 'Ok' : IDL.Text, 'Err' : IDL.Text });
  return IDL.Service({
    'validate_addToken' : IDL.Func(
        [IDL.Principal, TokenType],
        [ValidationResult],
        [],
      ),
  });
};
export const init = ({ IDL }) => { return []; };
