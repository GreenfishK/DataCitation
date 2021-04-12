from src.rdf_data_citation.rdf_star import prefixes_to_sparql

from rdflib.term import Variable
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
from nested_lookup import nested_lookup
import pandas as pd
import hashlib
import datetime
from pandas.util import hash_pandas_object
import json
import numpy as np
import os
import re


def _query_triples(query, sparql_prefixes: str = None) -> list:
    """
    Takes a query and transforms it into a result set with three columns: s, p, o. This result set includes all
    stored triples connected to the result set of the input query.
    :return: transformed result set with columns: s, p, o
    """

    template = open(_template_path("templates/prefixes_query.txt"), "r").read()

    if sparql_prefixes:
        statement = template.format(sparql_prefixes, query)
    else:
        statement = template.format("", query)

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


def _query_algebra(query: str, sparql_prefixes: str):
    if sparql_prefixes:
        q_desc = parser.parseQuery(sparql_prefixes + " " + query)
    else:
        print(query)
        q_desc = parser.parseQuery(query)

    return algebra.translateQuery(q_desc).algebra


def _query_variables(query_algebra) -> list:
    """
    The query must be a valid query including prefixes.
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


def _citation_timestamp_format(citation_timestamp: datetime) -> str:
    return citation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + citation_timestamp.strftime("%z")[3:5]


def _template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)


def attach_prefixes(query, prefixes: dict) -> str:
    template = open(_template_path("templates/prefixes_query_wrapper.txt"), "r").read()
    sparql_prefixes = prefixes_to_sparql(prefixes)
    query_with_prefixes = template.format(sparql_prefixes, query)
    return query_with_prefixes


class QueryData:

    def __init__(self, query: str = None, citation_timestamp: datetime = None):
        """
        Initializes the QueryData object and presets most of the variables by calling functions from this class.

        :param query: The SPARQL select statement that is used to retrieve the data set for citing.
        :param citation_timestamp: The timestamp as of citation.
        :param prefixes: prefixes used in the SPARQL query.
        """

        if query is not None:
            self.sparql_prefixes, self.query = self.split_prefixes_query(query)
            self.variables = _query_variables(_query_algebra(self.query, self.sparql_prefixes))
            self.normalized_query_algebra = self.normalize_query_tree()
            self.checksum = self.compute_checksum()

            if citation_timestamp is not None:
                self.citation_timestamp = _citation_timestamp_format(citation_timestamp)  # -> str
                self.pid = self.generate_query_pid()
            else:
                self.citation_timestamp = None
                self.pid = None
        else:
            self.query = None
            self.variables = None
            self.normalized_query_algebra = None
            self.checksum = None

    def split_prefixes_query(self, query: str = None) -> list:
        """
        Separates the prefixes from the actual query and stores either information in self.query and
        self.sparql_prefixes respectively.

        :param query: A query string with or without prefixes
        :return: A list with the prefixes as the first element and the actual query string as the second element.
        """
        pattern = "PREFIX\\s*[a-zA-Z0-9]*:\\s*<.*>\\s*"

        if not query:
            query = self.query
        prefixes_list = re.findall(pattern, query, re.MULTILINE)
        prefixes = ''.join(prefixes_list)
        query_without_prefixes = re.sub(pattern, "", query, re.MULTILINE)

        if not query:
            self.sparql_prefixes = prefixes
            self.query = query_without_prefixes

        return [prefixes, query_without_prefixes]

    def normalize_query_tree(self, query: str = None):
        """
        Normalizes the query tree by computing its algebra first. Most of the ambiguities are implicitly solved by the
        query algebra as different ways of writing a query usually boil down to one algebra. For cases where
        normalization is still needed, the query algebra is be updated. First, the query is wrapped with a select
        statement selecting all its variables in unambiguous order. The query tree of the extended query
        is then computed and normalization measures are taken.
        :return: normalized query tree object
        """

        # Assertions and exception handling
        if query is None:
            if self.query is not None:
                prefixes = self.sparql_prefixes
                query = self.query
            else:
                return "Query could not be normalized because the query string was not set."
        else:
            prefixes, query = self.split_prefixes_query(query)

        if self.variables is not None:
            variables = self.variables
        else:
            return "Query could not be normalized because there are no variables in this query."

        template = open(_template_path("templates/simple_query_wrapper.txt"), "r").read()
        """
        #3
        In case of an asterisk in the select-clause, all variables will be explicitly mentioned 
        and ordered alphabetically.
        
        """
        pass
        # Implicitly solved by query algebra

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
        variables_query_string = ""
        for v in variables:
            variables_query_string += v.n3() + " "
        query = template.format(variables_query_string, query)
        q_algebra = _query_algebra(query, prefixes)

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

        return str(q_algebra)

    def decorate_query(self, query: str = None, citation_timestamp: datetime = None,
                       sort_variables: tuple = None, colored: bool = False):
        """
        Binds a citation timestamp to the variable ?TimeOfCiting and wraps it around the query. Also extends
        the query with a code snippet that ensures that a snapshot of the data as of citation
        time gets returned when the query is executed. Optionally, but recommended, the order by clause
        is attached to the query to ensure a unique sort of the data.

        :param query:
        :param prefixes:
        :param citation_timestamp:
        :param sort_variables:
        :param colored: f colored is true, the injected strings within the statement template will be colored.
        Use this parameter only for presentation purpose as the code for color encoding will make the SPARQL
        query erroneous!
        :return: A query string extended with the given timestamp
        """

        if colored:
            red = ("\x1b[31m", "\x1b[0m")
            green = ("\x1b[32m", "\x1b[0m")
            blue = ("\x1b[34m", "\x1b[0m")
            magenta = ("\x1b[35m", "\x1b[0m")
            cyan = ("\x1b[36m", "\x1b[0m")
        else:
            red = ("", "")
            green = ("", "")
            blue = ("", "")
            magenta = ("", "")
            cyan = ("", "")

        template = open(_template_path("templates/query_wrapper.txt"), "r").read()

        # Assertions and exception handling
        if query is None:
            if self.query is not None:
                prefixes = self.sparql_prefixes
                query = self.query
            else:
                return "Query could not be normalized because the query string was not set."
        else:
            prefixes, query = self.split_prefixes_query(query)

        if self.variables is not None:
            variables = self.variables
        else:
            variables = _query_variables(_query_algebra(query, prefixes))

        # Variables from the original query
        variables_string = ""
        for v in variables:
            variables_string += v.n3() + " "

        # QueryData extensions for versioning injection
        triples = _query_triples(query, prefixes)

        versioning_query_extensions_template = \
            open(_template_path("templates/versioning_query_extensions.txt"), "r").read()

        versioning_query_extensions = ""
        for i, triple in enumerate(triples):
            v = versioning_query_extensions_template
            versioning_query_extensions += v.format(triple,
                                                    "?triple_statement_{0}_valid_from".format(str(i)),
                                                    "?triple_statement_{0}_valid_until".format(str(i)))

        # Formatting and styling select statement
        normalized_query_formatted = query.replace('\n', '\n \t')

        if citation_timestamp is not None:
            timestamp = _citation_timestamp_format(citation_timestamp)
        else:
            timestamp = self.citation_timestamp

        # Extending query with order by
        if sort_variables is not None:
            sort_variables_string = ""
            for v in sort_variables:
                v_n3 = Variable(v)
                sort_variables_string += v_n3.n3() + " "
            sort_extension = "order by " + ' ' + sort_variables_string
        else:
            sort_extension = ""

        decorated_query = template.format(prefixes,
                                          "{0}{1}{2}".format(red[0], "*", red[1]),
                                          "{0}{1}{2}".format(green[0], normalized_query_formatted, green[1]),
                                          "{0}{1}{2}".format(blue[0], timestamp, blue[1]),
                                          "{0}{1}{2}".format(magenta[0], versioning_query_extensions, magenta[1]),
                                          "{0}{1}{2}".format(cyan[0], sort_extension, cyan[1]))
        return decorated_query

    def compute_checksum(self, string: str = None) -> str:
        """
        Computes the checksum based on the provided :string or on
        the normalized query algebra (self.normalized_query_algebra) if no :string is provided
        with the sha256 algorithm.
        :string: The string to normalize. If no string is provided the checksum will be computed based on
        the normalized query algebra.
        :return: the hash value of either string or normalized query algebra
        """

        checksum = hashlib.sha256()
        if string is None:
            if self.normalized_query_algebra is not None:
                checksum.update(str.encode(self.normalized_query_algebra))
            else:
                return "Checksum could not be computed because the normalized query algebra is missing."
        else:
            checksum.update(str.encode(string))

        return checksum.hexdigest()

    def generate_query_pid(self, query_checksum: str = None, citation_timestamp: datetime = None):
        """
        Generates a query PID by concatenating the provided query checksum and the citation timestamp from the
        QueryData object.

        :return:
        """
        if query_checksum is None:
            query_checksum = self.checksum
        if citation_timestamp is None:
            citation_timestamp = self.citation_timestamp
        else:
            citation_timestamp = _citation_timestamp_format(citation_timestamp)

        if None not in (query_checksum, citation_timestamp):
            query_pid = query_checksum + citation_timestamp
        else:
            query_pid = "No query PID could be generated because of missing parameters."
        return query_pid


class RDFDataSetData:

    def __init__(self, dataset: pd.DataFrame = None, description: str = None, sort_order: tuple = None):
        """

        :param description:
        :param sort_order:
        """
        self.dataset = None
        self.description = None
        self.sort_order = None
        self.checksum = None

        if dataset is not None:
            self.dataset = dataset
            self.description = self.describe(description)
            self.sort_order = self.create_sort_index(sort_order, True)[0]
            self.checksum = self.compute_checksum()

    def describe(self, description: str):
        """
        Generates a description from the dataset

        :return:
        """

        # TODO: Use a natural language processor for RDF data to generate a description
        #  for the dataset which will be suggested.

        if description is None:
            # generate description from self.dataset
            inferred_description = ""
            return inferred_description
        else:
            return description

    def compute_checksum(self, column_order_dependent: bool = False):
        """
        A column order dependent or independent computation of the dataset checksum.

        Algorithm:
        Computes a checksum for each cell of the dataset. Then, a sum is calculated for each row making the
        checksum computation independent of the column order. Last, the vector of row sums is hashed again
        and the mean is taken.

        :param column_order_dependent: Tells the algorithm whether to make the checksum computation dependent or
        independent of the column order.
        :return:
        """

        if column_order_dependent:
            row_hashsum_series = hash_pandas_object(self.dataset)
        else:
            def cell_hash_value(cell):
                checksum = hashlib.sha256()
                # numpy data types need to be converted to native python data types before they can be encoded
                checksum.update(str.encode(str(cell)))
                return int(checksum.hexdigest(), 16)

            # TODO: take care of empty result sets
            hashed_dataframe = self.dataset.apply(np.vectorize(cell_hash_value))
            row_hashsum_series = hashed_dataframe.sum(axis=1)

        row_hashsum_series = row_hashsum_series.astype(str)
        hash_sum = row_hashsum_series.str.cat()

        checksum = hashlib.sha256()
        checksum.update(str.encode(hash_sum))
        checksum = checksum.hexdigest()
        return checksum

    def create_sort_index(self, sort_order: tuple = None, suggest_one_key: bool = False) -> list:
        """
        Infers the primary key from a dataset.
        Two datasets with differently permuted columns but otherwise identical
        will yield a different order of key attributes if one or more composite keys are suggested.
        Two datasets with different re-mapped column names that would result into a different order when sorted
        will not affect the composition of the keys and therefore also not affect the sorting.
        A reduction of suggested composite keys will be made if there are two or more suggested composite keys and
        the number of "distinct summed key attribute values" of each of these composite keys differ from each other.
        In fact, the composite keys with the minimum sum of distinct key attribute values will be returned.

        :param sort_order: A predefined sort order by the user
        :param suggest_one_key: If True, the first (composite) key tuple will be returned from the list of potential keys
        :param dataset: The dataset from which the (composite) indexes shall be inferred
        :return:
        """

        if sort_order is not None:
            return list(sort_order)

        # TODO: Think abou whether the order of columns should yield a different permutation of key attributes
        #  withing composite keys, thus, meaning a different sorting or not.

        def combination_util(combos: list, arr, n, tuple_size, data, index=0, i=0):
            """
            Returns a dictionary with combinations of size tuple_size.

            :param combos: list to be populated with the output combinations from this algorithm
            :param arr: Input Array
            :param n: Size of input array
            :param tuple_size: size of a combination to be printed
            :param index: Current index in data[]
            :param data: temporary list to store current combination
            :param i: index of current element in arr[]
            :return:
            """

            # Current combination is ready, print it
            if index == tuple_size:
                combo = []
                for j in range(tuple_size):
                    combo.append(data[j])
                combos.append(combo)
                return

            # When no more elements are there to put in data[]
            if i >= n:
                return

            # current is included, put next at next location
            data[index] = arr[i]
            combination_util(combos, arr, n, tuple_size, data, index + 1, i + 1)

            # current is excluded, replace it with next (Note that i+1 is passed, but index is not changed)
            combination_util(combos, arr, n, tuple_size, data, index, i + 1)

        sufficient_tuple_size = False
        df_key_finder = self.dataset.copy()
        df_key_finder.drop_duplicates(inplace=True)
        cnt_columns = len(df_key_finder.columns)

        """columns_mapping = {}
        for idx, column in enumerate(df_key_finder.columns):
            columns_mapping[column] = str(idx)
        df_key_finder.rename(columns=columns_mapping, inplace=True)"""
        # columns = sorted(df_key_finder.columns)
        columns = df_key_finder.columns

        distinct_occurences = {}
        for column in columns:
            distinct_occurences[column] = len(df_key_finder[column].unique().flat)

        attribute_combos_min_tuple_size = {}
        cnt_index_attrs = 1
        while not sufficient_tuple_size:
            attribute_combos = []
            combination_util(combos=attribute_combos, arr=columns, n=cnt_columns,
                             tuple_size=cnt_index_attrs, data=[0] * cnt_index_attrs)
            attribute_combos_tuple_size_k = {}
            for combo in attribute_combos:
                attribute_combos_tuple_size_k[tuple(combo)] = df_key_finder.set_index(combo).index.is_unique

            if any(attribute_combos_tuple_size_k.values()):
                sufficient_tuple_size = True
                attribute_combos_min_tuple_size = attribute_combos_tuple_size_k
            cnt_index_attrs += 1

        dist_stacked_key_attr_values = {}
        for composition, is_potential_key in attribute_combos_min_tuple_size.items():
            if is_potential_key:
                cnt_dist_stacked_attr_values = 0
                for attribute in composition:
                    cnt_distinct_attribute_values = distinct_occurences[attribute]
                    cnt_dist_stacked_attr_values += cnt_distinct_attribute_values
                dist_stacked_key_attr_values[composition] = cnt_dist_stacked_attr_values

        min_val = min(dist_stacked_key_attr_values.values())
        suggested_pks = [k for k, v in dist_stacked_key_attr_values.items() if v == min_val]

        if suggest_one_key:
            return [suggested_pks[0]]  # return one tuple in a list
        # TODO: Request input from user if the sort index to be used is ambiguous, thus, if there are more than one
        #  possible indexes.
        return suggested_pks

    def sort(self, sort_order: tuple = None):
        """
        Sorts by the index that is derived from create_sort_index. As this method can return more than one possible
        unique indexes, the first one will be taken.

        :sort_order: A user defined sort order for the dataset. If nothing is provided, the sort index will be derived
        from the dataset. Sometimes, this can yield ambiguous multi-indexes in which case the first multiindex will
        be taken from the list of possible unique indexes.
        :return:
        """

        if sort_order is None:
            sort_index = self.create_sort_index(suggest_one_key=True)[0]
        else:
            sort_index = sort_order
        self.sort_order = sort_index
        sorted_df = self.dataset.set_index(list(sort_index)).sort_index()
        return sorted_df


class CitationData:

    def __init__(self, identifier: str = None, creator: str = None, title: str = None, publisher: str = None,
                 publication_year: str = None, resource_type: str = None,
                 other_citation_data: dict = None, citation_snippet: str = None):
        """
        Initialize the mandatory fields from DataCite's metadata model version 4.3
        """
        # TODO: Data operator defines which metadata to store. The user is able to change the description
        #  and other citation data. Possible solution: change other_citation_data to kwargs* (?)

        # Recommended fields to be populated. They are mandatory in the DataCite's metadata model
        self.identifier = identifier
        self.creator = creator
        self.title = title
        self.publisher = publisher
        self.publication_year = publication_year
        self.resource_type = resource_type

        # Other user-defined provenance data and other metadata
        self.other_citation_data = other_citation_data

        self.citation_snippet = citation_snippet

    def to_json(self):
        citation_data = vars(self).copy()
        del citation_data['citation_snippet']
        citation_data_json = json.dumps(citation_data, indent=4)
        return citation_data_json


def read_json(json_string) -> CitationData:
    citation_data_dict = json.loads(json_string)

    citation_data = CitationData(citation_data_dict['identifier'], citation_data_dict['creator'],
                                 citation_data_dict['title'], citation_data_dict['publisher'],
                                 citation_data_dict['publication_year'], citation_data_dict['resource_type'],
                                 citation_data_dict['other_citation_data'])

    return citation_data


def generate_citation_snippet(query_pid: str, citation_data: CitationData) -> str:
    """
    Generates the citation snippet out of DataCite's mandatory attributes within its metadata schema.
    :param query_pid:
    :param citation_data:
    :return:
    """
    # TODO: Let the order of data within the snippet be defined by the user
    #  also: the user should be able to define which attributes are to be used in the citation snippet
    snippet = "{0}, {1}, {2}, {3}, {4}, {5}, pid: {6} \n".format(citation_data.identifier,
                                                                 citation_data.creator,
                                                                 citation_data.title,
                                                                 citation_data.publisher,
                                                                 citation_data.publication_year,
                                                                 citation_data.resource_type,
                                                                 query_pid)
    return snippet
