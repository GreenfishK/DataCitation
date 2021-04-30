from datetime import datetime, timezone, timedelta

from src.rdf_data_citation.rdf_star import TripleStoreEngine
from tests.test_base import Test, TestExecution, format_text
import src.rdf_data_citation.citation as ct
from src.rdf_data_citation.citation_utils import CitationData, QueryData, RDFDataSetData, generate_citation_snippet
from src.rdf_data_citation.query_store import QueryStore


class TestCitation(TestExecution):

    def __init__(self, annotated_tests: bool = False):
        super().__init__(annotated_tests)
        self.rdf_engine = None
        self.citation_metadata = None
        self.citation = None
        self.initial_timestamp = None
        self.citation_timestamp = None
        self.select_statement = None
        self.rdf_engine = None
        # self.dataset_utils = None

    def before_all_tests(self):
        """

        :return:
        """

        print("Executing before_tests ...")
        self.citation_metadata = CitationData(identifier="https://doi.org/pid/of/query",
                                              creator="Filip Kovacevic",
                                              title="Obama occurrences",
                                              publisher="Filip Kovacevic",
                                              publication_year="2021",
                                              resource_type="Dataset/RDF data",
                                              other_citation_data={'Contributor': 'Tomasz Miksa'})

        self.citation = ct.Citation(self.test_config.get('RDFSTORE', 'get'), self.test_config.get('RDFSTORE', 'post'))
        vie_tz = timezone(timedelta(hours=2))
        citation_timestamp = datetime(2021, 4, 30, 12, 11, 21, 941000, vie_tz)
        self.citation_timestamp = citation_timestamp
        initial_timestamp = datetime(2020, 9, 1, 12, 11, 21, 941000, vie_tz)
        self.initial_timestamp = initial_timestamp
        self.rdf_engine = TripleStoreEngine(self.test_config.get('RDFSTORE', 'get'),
                                            self.test_config.get('RDFSTORE', 'post'))
        self.rdf_engine.version_all_rows(self.initial_timestamp)

    def before_single_test(self, test_name: str):
        """

        :return:
        """
        if self.annotated_tests:
            test_name = test_name[2:]

        select_statement = open("test_data/{0}.txt".format(test_name), "r").read()
        self.select_statement = select_statement
        print("Executing before_single_tests ...")

    def after_single_test(self):
        """

        :return:
        """
        print("Executing after_single_test")

        # Clean up
        query_store = QueryStore()
        query_store._remove(self.citation.query_data.checksum)

    def after_all_tests(self):
        """

        :return:
        """

        print("Executing after_tests ...")
        self.rdf_engine.reset_all_versions()

    def x_test_citation__empty_dataset(self):
        citation = self.citation
        self.citation_metadata.title = "Obama occurrences as Republican"

        # Actual results
        citation.cite(select_statement=self.select_statement,
                      citation_metadata=self.citation_metadata,
                      citation_timestamp=self.citation_timestamp)
        actual_result = citation.result_set_data.description + "\n" + citation.citation_metadata.citation_snippet
        self.citation = citation

        # Expected results
        query_utils = QueryData(query=self.select_statement, citation_timestamp=self.citation_timestamp)
        citation_snippet = generate_citation_snippet(query_pid=query_utils.pid,
                                                     citation_data=self.citation_metadata)
        expected_result = "This is an empty dataset. We cannot infer any description from it.\n" + citation_snippet

        # Test object
        test = Test(test_number=1,
                    tc_desc='Test if an empty dataset can be cited and a citation snippet is returned.',
                    expected_result=expected_result,
                    actual_result=actual_result)

        return test

    def x_test_citation__non_empty_dataset(self):
        citation = self.citation
        self.citation_metadata.title = "Obama occurrences"

        # Actual results
        citation.cite(select_statement=self.select_statement,
                      citation_metadata=self.citation_metadata,
                      citation_timestamp=self.citation_timestamp)
        actual_result = citation.result_set_data.description + "\n" + citation.citation_metadata.citation_snippet
        self.citation = citation

        # Expected results
        query_utils = QueryData(query=self.select_statement, citation_timestamp=self.citation_timestamp)
        citation_snippet = generate_citation_snippet(query_pid=query_utils.pid,
                                                     citation_data=self.citation_metadata)
        timestamped_query = query_utils.timestamp_query()
        result_set = self.rdf_engine.get_data(timestamped_query)
        dataset_utils = RDFDataSetData(dataset=result_set)
        dataset_description = dataset_utils.describe()
        expected_result = dataset_description + "\n" + citation_snippet

        # Test object
        test = Test(test_number=2,
                    tc_desc='Test if a non-empty dataset can be cited and a citation snippet is returned.',
                    expected_result=expected_result,
                    actual_result=actual_result)

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
