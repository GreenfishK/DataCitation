from tests.test_base import Test, TestExecution, format_text
import src.rdf_data_citation.citation as ct
from src.rdf_data_citation.citation_utils import CitationData, QueryData, RDFDataSetData, generate_citation_snippet
from src.rdf_data_citation.query_store import QueryStore


class TestCitation(TestExecution):

    def __init__(self, annotated_tests: bool = False):
        super().__init__(annotated_tests)
        self.rdf_engine = None

    def before_all_tests(self):
        """

        :return:
        """

        print("Executing before_tests ...")

    def before_single_test(self, test_name: str):
        """

        :return:
        """

        print("Executing before_single_tests ...")

    def after_single_test(self):
        """

        :return:
        """
        print("Executing after_single_test")

    def after_all_tests(self):
        """

        :return:
        """

        print("Executing after_tests ...")

    def x_test_citation__empty_dataset(self):
        citation = ct.Citation(self.test_config.get('RDFSTORE', 'get'),
                               self.test_config.get('RDFSTORE', 'post'))

        # TODO: assign identifier in ct.citation()
        citation_metadata = CitationData(identifier="https://doi.org/pid/of/query",
                                         creator="Filip Kovacevic",
                                         title="Obama occurrences",
                                         publisher="Filip Kovacevic",
                                         publication_year="2021",
                                         resource_type="Dataset/RDF data",
                                         other_citation_data={'Contributor': 'Tomasz Miksa'})

        select_statement = open("test_data/test_citation__empty_dataset.txt", "r").read()
        citation.cite(select_statement=select_statement, citation_metadata=citation_metadata)
        citation_snippet = generate_citation_snippet(query_pid=citation.query_data.pid,
                                                     citation_data=citation_metadata)

        test = Test(test_number=1,
                    tc_desc='Test if an empty dataset can be cited',
                    expected_result="This is an empty dataset. We cannot infer any description from it.\n"
                                    + citation_snippet,
                    actual_result=citation.result_set_data.description
                                    + "\n"
                                    + citation.citation_metadata.citation_snippet)
        # Clean up
        query_store = QueryStore()
        query_store._remove(citation.query_data.checksum)

        return test

    def test_citation__non_empty_dataset(self):
        test = Test(test_number=2,
                    tc_desc='',
                    expected_result='',
                    actual_result='')

        return test

    def test_citation__changed_dataset(self):
        test = Test(test_number=3,
                    tc_desc='',
                    expected_result='',
                    actual_result='')

        return test

    def test_citation__changed_provenance(self):
        test = Test(test_number=4,
                    tc_desc='',
                    expected_result='',
                    actual_result='')

        return test

    def test_citation__recite_existing(self):
        test = Test(test_number=5,
                    tc_desc='',
                    expected_result='',
                    actual_result='')

        return test

    def test_citation__recite_semantically_identical(self):
        test = Test(test_number=6,
                    tc_desc='',
                    expected_result='',
                    actual_result='')

        return test

    def test_citation__cite_non_unique_sort(self):
        test = Test(test_number=7,
                    tc_desc='',
                    expected_result='',
                    actual_result='')

        return test

    def test_citation__sort_by_not_select_variable(self):
        test = Test(test_number=8,
                    tc_desc='',
                    expected_result='',
                    actual_result='')

        return test

    def test_citation__select_clause_differently_ordered_variables(self):
        test = Test(test_number=9,
                    tc_desc='',
                    expected_result='',
                    actual_result='')

        return test


t = TestCitation(annotated_tests=True)
t.run_tests()
t.print_test_results()







# Test 1: Cite one query, then cite it again
# Test 2: Cite one query with an empty dataset
# Test 3: Cite one query, then change the order of two triple statements and cite again
# Test 4: Try to cite a query with a non-unique order-by
# Test 5: Try to cite a query with an order by variable that is not in the select statement
# Test 6: Cite a query with differently ordered variables in the select clause
# Test 7: Cite a query, change the result set and try to cite again
