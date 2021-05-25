import logging

from tests.test_base import Test, TestExecution, format_text
from src.rdf_data_citation.citation_utils import QueryUtils


class TestNormalization(TestExecution):
    """
    Issues:
    #1: Algebra tree: Printing node.triples (e.g. if node is a BGP) shows different results than printing node.
        While print(node.triples) seems to show a state where changes made prior to printing are not reflected
        print(node) shows the state with all changes made prior to this call incorporated.
        It also seems that print(node.triples) shows the right state.
        See citation_utils.normalize_query_tree.reorder_triples()

    """

    def __init__(self, annotated_tests: bool = False):
        super().__init__(annotated_tests)
        self.rdf_engine = None
        self.query_data_alt1 = None
        self.query_data_alt2 = None

    def before_single_test(self, test_name: str):
        """

        :return:
        """

        print("Executing before_single_tests ...")

        if self.annotated_tests:
            test_name = test_name[2:]

        logging.info("Query1:")
        query_alt1 = open("test_data/{0}_alt1.txt".format(test_name), "r").read()
        self.query_data_alt1 = QueryUtils(query=query_alt1)
        logging.info(self.query_data_alt1.normalized_query)
        logging.info(self.query_data_alt1.normalized_query_algebra.algebra)
        logging.info("Query2:")
        query_alt2 = open("test_data/{0}_alt2.txt".format(test_name), "r").read()
        self.query_data_alt2 = QueryUtils(query=query_alt2)
        logging.info(self.query_data_alt2.normalized_query)
        logging.info(self.query_data_alt2.normalized_query_algebra.algebra)

    def test_normalization__optional_where_clause(self):
        test = Test(test_number=1,
                    tc_desc='Tests if leaving out the "where" keyword yields the same checksum.',
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__rdf_type_predicate(self):
        test = Test(test_number=2,
                    tc_desc='Tests if replacing the predicate rdf:type by "a" yields the same checksum.',
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__asterisk(self):
        """
        Fails because the asterisk gets resolved in an order that might not be the same as the explicitly
        stated variables in the select clause. Only for one out of n combinations it will pass.
        :return:
        """
        test = Test(test_number=3,
                    tc_desc="Tests if replacing the variable names in the select clause with an asterisk yields the "
                            "same checksum. If the variable order is important this test will usually not pass. In "
                            "this case, it will only pass if the order of variables after the asterisk gets resolved "
                            "is the same as the user would place them. ",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__leave_out_subject_in_triple_statements(self):
        test = Test(test_number=4,
                    tc_desc="If the same subject is used multiple times in subsequent triple statements (separated by "
                            "a dot) it can be left out in all the subsequent triple statements where the subject "
                            "occurs. Instead of the subject variable name a semicolon is written in subsequent triple "
                            "statements where the same subject as in the first statement should be used.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__order_of_triple_statements(self):
        test = Test(test_number=5,
                    tc_desc="Tests if differently permuted tripled statements yield the same checksum.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__alias_via_bind(self):
        test = Test(test_number=6,
                    tc_desc="Test if binding an alias to a variable using the BIND keyword yields the same checksum "
                            "as when not using any alias.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__alias_in_select(self):
        test = Test(test_number=7,
                    tc_desc="Test if binding an alias to a variable using the BIND keyword yields the same checksum "
                            "as when not using any alias.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__variable_names(self):
        test = Test(test_number=8,
                    tc_desc="Test if two queries where as one has one variable renamed within the whole query"
                            " (select statement, triple statements, filter, ...) yields the same checksum.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__variables_not_bound(self):
        test = Test(test_number=9,
                    tc_desc='Finding variables that are not bound can be written in two ways: '
                            '1. with optional keyword adding the optional triplet combined with filter condition: '
                            'OPTIONAL { ?x dc:date ?date }'
                            'filter (!bound(?date)) '
                            '2. with "filter not exists {triple}"',
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__inverted_paths(self):
        test = Test(test_number=10,
                    tc_desc='Test if inverted paths and its explicit version using no paths '
                            'but only triple statements yield the same checksum. ',
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__sequence_paths(self):
        test = Test(test_number=11,
                    tc_desc="Test if two queries - one with a sequence path and the second with "
                            "the sequence path resolved as explicit triple statements yield the same checksum. "
                            "The resolved triple statements are 'in the same order' as "
                            "the sequence path",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__sequence_paths2(self):
        test = Test(test_number=12,
                    tc_desc="Test if two queries - one with a sequence path and the second with "
                            "the sequence path resolved as explicit triple statements yield the same checksum."
                            "The resolved triple statements are in different order "
                            "compared to the sequence path. ",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__prefix_alias(self):
        test = Test(test_number=13,
                    tc_desc="Prefixes can be interchanged in the prefix section before the query "
                            "and subsequently in the query without changing the outcome.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__switched_filter_statements(self):
        """
        Fails because the algebra tree nesting of filters is switched.
        :return:
        """
        test = Test(test_number=14,
                    tc_desc="Filters can stated in different orders in a MultiSet or Basic Graph Pattern (BGP) without "
                            "affecting the result. ",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__complex_bind_expression(self):
        test = Test(test_number=15,
                    tc_desc="Test if two queries where a complex bind expression (e.g. arithmetic operations) is given"
                            "different names yields the same query checksum. "
                            "The bind must be used in the select clause.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__complex_bind_expression2(self):
        # While the two query algebras are not completely equal to each other the normalized queries are.
        # This is because the nesting withing the query algebra for bindings is different between
        # the implicit binding (in the select clause) and the explicit via BIND keyword.
        # When the normalized query algebra gets back-translated into a query all the bindings appear
        # within the select clause.

        test = Test(test_number=16,
                    tc_desc="Test if two queries where a complex expression (e.g. arithmetic operations) is given"
                            "different names yields the same query checksum."
                            "The bind must be explicitly used via BIND keyword.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test


t = TestNormalization(annotated_tests=False)
t.run_tests()
t.print_test_results()
