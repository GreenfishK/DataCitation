import sys

from tests.test_base import Test, TestExecution, format_text
from src.rdf_data_citation.citation_utils import QueryUtils
from src.rdf_data_citation import rdf_star
from src.rdf_data_citation.citation_utils import _translate_algebra, _pprint_query
import logging


class TestAlgebraToTest(TestExecution):

    def __init__(self, annotated_tests: bool = False):
        super().__init__(annotated_tests)
        self.rdf_engine = None
        self.query_text = None
        self.query_from_algebr = None
        self.query_from_query_from_algebra = None

    def before_single_test(self, test_name: str):
        """

        :return:
        """

        print("Executing before_single_tests ...")

        if self.annotated_tests:
            test_name = test_name[2:]

        self.query_text = open("test_data/{0}.txt".format(test_name), "r").read()
        self.rdf_engine = rdf_star.TripleStoreEngine(self.test_config.get('RDFSTORE', 'get'),
                                                     self.test_config.get('RDFSTORE', 'post'))
        self.query_from_algebra = _translate_algebra(self.query_text)
        self.query_from_query_from_algebra = _translate_algebra(self.query_from_algebra)
        _pprint_query(self.query_from_query_from_algebra)

    def test_functions__functional_forms(self):
        test = Test(test_number=1,
                    tc_desc='Test if functional forms are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_functions__functions_on_rdf_terms(self):
        test = Test(test_number=2,
                    tc_desc='Test if functions on rdf terms are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_functions__functions_on_strings(self):
        test = Test(test_number=3,
                    tc_desc='Test if functions on strings are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_functions__functions_on_numerics(self):
        test = Test(test_number=4,
                    tc_desc='Test if functions on numerics are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_functions__hash_functions(self):
        test = Test(test_number=5,
                    tc_desc='Test if hash functions are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_functions__functions_on_dates_and_time(self):
        test = Test(test_number=6,
                    tc_desc='Test if functions on dates and time are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__aggregate_join(self):
        test = Test(test_number=7,
                    tc_desc='Test if aggregate join including all aggregation functions '
                            'are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__bgp(self):
        test = Test(test_number=8,
                    tc_desc='Test if basic graph patterns are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__extend(self):
        test = Test(test_number=9,
                    tc_desc='Test if "extend" (=Bind explicitly or implicitly in projection) '
                            'gets properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__filter(self):
        test = Test(test_number=10,
                    tc_desc='Test if filter gets properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__graph(self):
        test = Test(test_number=11,
                    tc_desc='Test if "graph" gets properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__group(self):
        test = Test(test_number=12,
                    tc_desc='Test if "group" gets properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__join(self):
        test = Test(test_number=13,
                    tc_desc='Test if "join" gets properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__left_join(self):
        test = Test(test_number=14,
                    tc_desc='Test if "left join" gets properly translated into "OPTIONAL {...}" in the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__minus(self):
        test = Test(test_number=15,
                    tc_desc='Test if "minus" gets properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test

    def test_graph_patterns__union(self):
        test = Test(test_number=16,
                    tc_desc='Test if "union" gets properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=self.query_from_algebra,
                    actual_result=self.query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(self.query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception as e:
            print(e)
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test


t = TestAlgebraToTest(annotated_tests=False)
t.run_tests()
t.print_test_results()
