from tests.test_base import Test, TestExecution, format_text
from src.rdf_data_citation.citation_utils import QueryUtils
from src.rdf_data_citation import rdf_star
from src.rdf_data_citation.citation_utils import _to_sparql_query_text
import logging


class TestAlgebraToTest(TestExecution):

    def __init__(self, annotated_tests: bool = False):
        super().__init__(annotated_tests)
        self.rdf_engine = None
        self.query_text = None

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

    def test_functions__functional_forms(self):
        query_from_algebra = _to_sparql_query_text(self.query_text)
        query_from_query_from_algebra = _to_sparql_query_text(query_from_algebra)

        test = Test(test_number=1,
                    tc_desc='Test if functional forms are properly translated into the query text. '
                            'The query must also be executable and shall not violate any SPARQL query syntax.',
                    expected_result=query_from_algebra,
                    actual_result=query_from_query_from_algebra)

        try:
            self.rdf_engine.get_data(query_from_query_from_algebra, yn_timestamp_query=False)
        except Exception:
            print("The query must be executable. Otherwise, the test has failed.")
            return Test(test_number=test.test_number, tc_desc=test.tc_desc, expected_result="0",
                        actual_result="not_executable")

        return test


t = TestAlgebraToTest(annotated_tests=False)
t.run_tests()
t.print_test_results()
