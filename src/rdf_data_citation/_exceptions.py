class MultipleAliasesInBindError(Exception):
    pass


class NoUniqueSortIndexError(Exception):
    pass


class MultipleSortIndexesError(Exception):
    pass


class NoDataSetError(Exception):
    pass


class ReservedPrefixError(Exception):
    pass


class NoVersioningMode(Exception):
    pass


class SortVariablesNotInSelectError(Exception):
    pass


class MissingSortVariables(Exception):
    pass


class QueryExistsError(Exception):
    pass


class NoQueryString(Exception):
    pass


class ExpressionNotCoveredException(Exception):
    pass


class RDFStarNotSupported(Exception):
    pass


class NoConnectionToRDFStore(Exception):
    pass


class InputMissing(Exception):
    pass


class WrongInputFormatException(Exception):
    pass
