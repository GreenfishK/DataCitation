from rdflib.term import Variable
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
from nested_lookup import nested_lookup
import pandas as pd
import hashlib
import datetime
from pandas.util import hash_pandas_object


def prefixes_to_sparql(prefixes):
    """
    Converts a dict of prefixes to a string with SPARQL syntax for prefixes
    :param prefixes:
    :return: SPARQL prefixes as string
    """
    sparql_prefixes = ""
    for key, value in prefixes.items():
        sparql_prefixes += "PREFIX " + key + ":" + "<" + value + "> \n"
    return sparql_prefixes


def _query_triples(query, sparql_prefixes: str = None) -> list:
    """
    Takes a query and transforms it into a result set with three columns: s, p, o. This result set includes all
    stored triples connected to the result set of the input query.
    :return: transformed result set with columns: s, p, o
    """

    statement = """
    {0} 

    {1}
    """

    if sparql_prefixes:
        statement = statement.format(sparql_prefixes, query)
    else:
        statement = statement.format("", query)

    q_desc = parser.parseQuery(statement)
    q_algebra = algebra.translateQuery(q_desc).algebra
    triples = nested_lookup('triples', q_algebra)

    n3_triples = []
    for triple_list in triples:
        for triple in triple_list:
            t = triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3()
            n3_triples.append(t)

    return sorted(n3_triples)


def _intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


def _query_algebra(query, sparql_prefixes):
    q_desc = parser.parseQuery(sparql_prefixes + " " + query)
    return algebra.translateQuery(q_desc).algebra


def _query_variables(query_algebra) -> list:
    """
    The query must be a valid query including prefixes. They can be already embedded in the query or will
    be embedded by providing them separately with the 'prefix' parameter.
    The query algebra is searched for "PV". There can be more than one PV-Nodes containing the select-clause
    variables within a list. However, each of these lists enumerates the same variables only the first list
    will be selected and returned.

    :param query_algebra:
    :return: a list of variables used in the query
    """

    query_triples = nested_lookup('triples', query_algebra)
    triple_variables = []
    for triple_list in query_triples:
        for triple in triple_list:
            if isinstance(triple[0], Variable):
                triple_variables.append(triple[0])
            if isinstance(triple[1], Variable):
                triple_variables.append(triple[1])
            if isinstance(triple[2], Variable):
                triple_variables.append(triple[2])

    triple_variables = list(dict.fromkeys(triple_variables))

    # So far this is used to identify bound variables found after the "bind" keyword
    other_variables = nested_lookup('var', query_algebra)

    variables = triple_variables + other_variables
    return variables


class Query:

    def __init__(self, query: str, prefixes: dict = None):
        """

        :param query: The SPARQL select statement that is used to retrieve the data set for citing
        :param prefixes: prefixes used in the SPARQL query
        """

        self.query = query
        self.query_for_execution = query
        self.citation_timestamp = None
        if prefixes is not None:
            self.sparql_prefixes = prefixes_to_sparql(prefixes)
        else:
            self.sparql_prefixes = ""
        self.query_algebra = _query_algebra(query, self.sparql_prefixes)
        self.query_checksum = None
        self.result_checksum = None
        self.variables = _query_variables(self.query_algebra)
        self.query_pid = None

    def detach_prefixes(self, query):
        """
        Cuts out prefixes from the passed
        query and moves them to the top. This is because wrapping
        around prefixes is not allowed in SPARQL. Prefixes must always be at the top of the statement.
        If no prefixes are found, the query just gets returned.
        :param query: A query string with or without prefixes
        :return:
        """

    def attach_prefixes(self) -> str:
        template = """
# prefixes
{0} 

Select * where {{
    # original query comes here
    {{ 
        {1}
    }}
}}
"""
        query_with_prefixes = template.format(self.sparql_prefixes, self.query)
        return query_with_prefixes

    def normalize_query_tree(self):
        """
        Normalizes the query tree by computing its algebra first. Most of the ambiguities are implicitly solved by the
        query algebra as different ways of writing a query usually boil down to one algebra. For case where
        normalization  is still needed, the query algebra is be updated. First, the query is wrapped with a select
        statement selecting all its variables in unambiguous order. The query tree of the extended query
        is then computed and normalization measures are taken.
        :return: normalized query tree object
        """

        """
        #3
        In case of an asterisk in the select-clause, all variables will be explicitly mentioned 
        and ordered alphabetically.
        """

        template = """
        Select {1} where {{
            # original query comes here
            {{ 
                {2}
            }}
        }}
        """

        variables_query_string = ""
        for v in self.variables:
            variables_query_string += v.n3() + " "
        self.query = template.format(variables_query_string, self.query)
        query = self.attach_prefixes()
        q_algebra = _query_algebra(query, self.sparql_prefixes)

        """
        #1
        A where clause will always be inserted
        """
        pass
        # Implicitly solved by query algebra

        """
        #5
        Triple statements will be ordered alphabetically by subject, predicate, object.

        The triple order is already unambiguous in the query algebra. The only requirement is
        that the variables in the select clause are in an unambiguous order. E.g. ?a ?b ?c ...
        """
        pass
        # Implicitly solved by query algebra

        """
        #7
        Variables in the query tree will be replaced by letters from the alphabet. For each variable 
        a letter from the alphabet will be assigned starting with 'a' and continuing in alphabetic order. 
        Two letters will be used  and combinations of letters will be assigned should there be more than 26 variables. 
        There is no need to sort the variables in the query tree as this is implicitly solved by the algebra. Variables
        are sorted by their bindings where the ones with the most bindings come first.
        """

        int_norm_var = 'a'
        variables_mapping = {}
        extended_variables = _query_variables(q_algebra)

        for i, v in enumerate(extended_variables):
            next_norm_var = chr(ord(int_norm_var) + i)
            variables_mapping.update({v: next_norm_var})

        # replace variables with normalized variables (letters from the alphabet)
        algebra.traverse(q_algebra,
                         lambda var: variables_mapping.get(var) if isinstance(var, Variable) else None
                         )

        # identify _var sets and sort the variables inside
        def norm_vars(x):
            return [variables_mapping[a] for a in x]
        # norm_vars = lambda x: [variables_mapping[a] for a in x]
        algebra.traverse(q_algebra,
                         lambda s: sorted(norm_vars(s)) if isinstance(s, set) else None
                         )

        # Sort variables in select statement
        algebra.traverse(q_algebra,
                         lambda l: sorted(l) if isinstance(l, list) else None
                         )
        return q_algebra

    def extend_query_with_timestamp(self, timestamp, colored: bool = False) -> str:
        """
        :param timestamp:
        :param colored: If colored is true, the injected strings within the statement template will be colored.
        Use this parameter only for presentation purpose as the code for color encoding will make the SPARQL
        query erroneous!
        :return: A query string extended with the given timestamp
        """

        if colored:
            template = """
Select \x1b[31m * \x1b[0m where {{
    # original query comes here
    {{ 
        \x1b[32m{0}\x1b[0m
    }}
    # citation timestamp
    bind("\x1b[34m{1}\x1b[0m"^^xsd:dateTime as ?TimeOfCiting) 

    # data versioning query extension
    \x1b[35m{2}\x1b[0m

}}
    """
        else:
            template = """
Select * where {{
    # original query comes here
    {{ 
        {0}
    }}
    # citation timestamp
    bind("{1}"^^xsd:dateTime as ?TimeOfCiting) 

    # data versioning query extension
    {2} 

}}
    """

        # Prefixes, and timestamp injection
        timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + timestamp.strftime("%z")[3:5]

        # Query extensions for versioning injection
        triples = _query_triples(self.query, self.sparql_prefixes)

        versioning_query_extensions_template = """
        <<{0}>> citing:valid_from {1}.
        <<{0}>> citing:valid_until {2}.
        filter({1} <= ?TimeOfCiting && ?TimeOfCiting < {2})
    """

        versioning_query_extensions = ""
        for i, triple in enumerate(triples):
            v = versioning_query_extensions_template
            versioning_query_extensions += v.format(triple, "?valid_from_" + str(i), "?valid_until_" + str(i))

        # Formatting and styling select statement
        indent = '        '
        normalized_query_formatted = self.query.replace('\n', '\n' + indent)

        timestamped_query = template.format(normalized_query_formatted,
                                            timestamp,
                                            versioning_query_extensions)
        return timestamped_query

    def extend_query_with_sort_operation(self, query, colored: bool = False):
        variables_query_string = ""
        for v in self.variables:
            variables_query_string += v.n3() + " "

        if colored:
            sort_extension = "\x1b[36m" + "order by " + ' ' + variables_query_string + "\x1b[0m"
        else:
            sort_extension = "order by " + ' ' + variables_query_string

        sorted_query = """
Select {0} where {{
    # original query comes here
    {{ 
        {1}
    }} 
}} {2}
                """

        if query is not None:
            sorted_query = sorted_query.format(variables_query_string, query, sort_extension)

        return sorted_query

    def compute_checksum(self, query_or_result, citation_object):
        """
        :param query_or_result:
        :param citation_object: A query string or result set
        :return: the hash value of either the query or query result
        """

        if query_or_result == "query":
            query_algebra_string = str(citation_object)
            checksum = hashlib.sha256()
            checksum.update(str.encode(query_algebra_string))
            self.query_checksum = checksum.hexdigest()
            return self.query_checksum

        if query_or_result == "result":
            if isinstance(citation_object, pd.DataFrame):
                self.result_checksum = hash_pandas_object(citation_object)
                return self.result_checksum.mean()

        # TODO: implement this

    def generate_query_pid(self, citation_timestamp: datetime, query_checksum: str):
        self.query_pid = citation_timestamp + query_checksum
        return self.query_pid

