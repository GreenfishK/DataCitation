from rdf_data_citation._exceptions import NoUniqueSortIndexError, SortVariablesNotInSelectError, MissingSortVariables
from rdf_data_citation.rdf_star import TripleStoreEngine
import rdf_data_citation.citation as ct
from rdf_data_citation.citation_utils import QueryUtils, RDFDataSetUtils, generate_citation_snippet, MetaData
from rdf_data_citation.query_store import QueryStore
from tests.test_base import Test, TestExecution, format_text
from datetime import datetime, timezone, timedelta
import logging


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
        self.citation_metadata = MetaData(identifier="https://doi.org/pid/of/query",
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
        query_store._remove(self.citation.query_utils.checksum)

    def after_all_tests(self):
        """

        :return:
        """

        print("Executing after_tests ...")
        self.rdf_engine.reset_all_versions()

    def test_citation__empty_dataset(self):
        citation = self.citation
        self.citation_metadata.title = "Obama occurrences as Republican"

        # Actual results
        citation.cite(select_statement=self.select_statement,
                      citation_metadata=self.citation_metadata,
                      citation_timestamp=self.citation_timestamp)
        actual_result = citation.result_set_utils.description + "\n" + citation.citation_metadata.citation_snippet
        self.citation = citation

        # Expected results
        query_utils = QueryUtils(query=self.select_statement, citation_timestamp=self.citation_timestamp)
        citation_snippet = generate_citation_snippet(identifier=query_utils.pid,
                                                     creator=self.citation_metadata.creator,
                                                     title=self.citation_metadata.title,
                                                     publisher=self.citation_metadata.publisher,
                                                     pulication_year=self.citation_metadata.publication_year,
                                                     resource_type=self.citation_metadata.resource_type)
        expected_result = "This is an empty dataset. We cannot infer any description from it.\n" + citation_snippet

        # Test object
        test = Test(test_number=1,
                    tc_desc='Test if an empty dataset can be cited and a citation snippet is returned.',
                    expected_result=expected_result,
                    actual_result=actual_result)

        return test

    def test_citation__non_empty_dataset(self):
        citation = self.citation
        self.citation_metadata.title = "Obama occurrences"

        # Actual results
        citation.cite(select_statement=self.select_statement,
                      citation_metadata=self.citation_metadata,
                      citation_timestamp=self.citation_timestamp)
        actual_result = citation.result_set_utils.description + "\n" + citation.citation_metadata.citation_snippet
        self.citation = citation

        # Expected results
        query_utils = QueryUtils(query=self.select_statement, citation_timestamp=self.citation_timestamp)
        citation_snippet = generate_citation_snippet(identifier=query_utils.pid,
                                                     creator=self.citation_metadata.creator,
                                                     title=self.citation_metadata.title,
                                                     publisher=self.citation_metadata.publisher,
                                                     pulication_year=self.citation_metadata.publication_year,
                                                     resource_type=self.citation_metadata.resource_type)
        result_set = self.rdf_engine.get_data(self.select_statement, self.citation_timestamp)
        dataset_utils = RDFDataSetUtils(dataset=result_set)
        dataset_description = dataset_utils.describe()
        expected_result = dataset_description + "\n" + citation_snippet

        # Test object
        test = Test(test_number=2,
                    tc_desc='Test if a non-empty dataset can be cited and a citation snippet is returned.',
                    expected_result=expected_result,
                    actual_result=actual_result)

        return test

    def test_citation__changed_dataset(self):
        citation1 = self.citation
        citation2 = ct.Citation(self.test_config.get('RDFSTORE', 'get'), self.test_config.get('RDFSTORE', 'post'))
        self.citation_metadata.title = "Obama occurrences, new mention"

        # Actual results
        # Citation1
        citation1.cite(select_statement=self.select_statement,
                       citation_metadata=self.citation_metadata,
                       citation_timestamp=self.citation_timestamp)

        # Insert two new triple
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        mention = "<http://data.ontotext.com/publishing#newMention>"
        hasInstance = "publishing:hasInstance"
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        self.rdf_engine.insert_triples([mention, hasInstance, person], prefixes)

        document = "<http://www.reuters.com/article/2021/01/01/newDocument>"
        containsMention = "publishing:containsMention"
        self.rdf_engine.insert_triples([document, containsMention, mention], prefixes)

        # Citation2
        vie_tz = timezone(timedelta(hours=2))
        citation_timestamp2 = datetime.now(vie_tz)
        citation2.cite(select_statement=self.select_statement,
                       citation_metadata=self.citation_metadata,
                       citation_timestamp=citation_timestamp2)

        # Concat Citation1 + Citation2 into actual results
        actual_result = citation1.citation_metadata.citation_snippet \
                        + "\n" + citation2.citation_metadata.citation_snippet

        self.citation = citation1

        # Expected results
        # Citation1
        query_utils = QueryUtils(query=self.select_statement, citation_timestamp=self.citation_timestamp)
        citation_snippet1 = generate_citation_snippet(identifier=query_utils.pid,
                                                      creator=self.citation_metadata.creator,
                                                      title=self.citation_metadata.title,
                                                      publisher=self.citation_metadata.publisher,
                                                      pulication_year=self.citation_metadata.publication_year,
                                                      resource_type=self.citation_metadata.resource_type)
        # Citation2
        query_utils = QueryUtils(query=self.select_statement, citation_timestamp=citation_timestamp2)
        citation_snippet2 = generate_citation_snippet(identifier=query_utils.pid,
                                                      creator=self.citation_metadata.creator,
                                                      title=self.citation_metadata.title,
                                                      publisher=self.citation_metadata.publisher,
                                                      pulication_year=self.citation_metadata.publication_year,
                                                      resource_type=self.citation_metadata.resource_type)

        expected_result = citation_snippet1 + "\n" + citation_snippet2

        # Clean up
        self.rdf_engine._delete_triples([mention, hasInstance, person], prefixes)
        self.rdf_engine._delete_triples([document, containsMention, mention], prefixes)

        test = Test(test_number=3,
                    tc_desc='Test if a new query PID is created if the dataset has changed since the last citation and'
                            'the query stayed the same. Check also if the query checksum must stayed the same.',
                    expected_result=expected_result,
                    actual_result=actual_result)

        return test

    def test_citation__recite_unchanged_dataset(self):
        citation1 = self.citation
        citation2 = ct.Citation(self.test_config.get('RDFSTORE', 'get'), self.test_config.get('RDFSTORE', 'post'))
        self.citation_metadata.title = "Obama occurrences, new mention"

        # Actual results
        # Citation1
        citation1.cite(select_statement=self.select_statement,
                       citation_metadata=self.citation_metadata,
                       citation_timestamp=self.citation_timestamp)

        # Citation2
        vie_tz = timezone(timedelta(hours=2))
        citation_timestamp2 = datetime.now(vie_tz)
        citation2.cite(select_statement=self.select_statement,
                       citation_metadata=self.citation_metadata,
                       citation_timestamp=citation_timestamp2)

        # Concat Citation1 + Citation2 into actual results
        actual_result = citation1.citation_metadata.citation_snippet \
                          + "\n" + citation2.citation_metadata.citation_snippet

        self.citation = citation1

        # Expected results
        # Citation1
        query_utils = QueryUtils(query=self.select_statement, citation_timestamp=self.citation_timestamp)
        citation_snippet1 = generate_citation_snippet(identifier=query_utils.pid,
                                                      creator=self.citation_metadata.creator,
                                                      title=self.citation_metadata.title,
                                                      publisher=self.citation_metadata.publisher,
                                                      pulication_year=self.citation_metadata.publication_year,
                                                      resource_type=self.citation_metadata.resource_type)

        expected_result = citation_snippet1 + "\n" + citation_snippet1

        test = Test(test_number=4,
                    tc_desc='Test if reciting a query where the dataset has not changed since the last citation '
                            'returns the citation snippet as of last citation where the query was either new '
                            'or the dataset changed.',
                    expected_result=expected_result,
                    actual_result=actual_result)

        return test

    def test_citation__recite_semantically_identical(self):
        citation1 = self.citation
        citation2 = ct.Citation(self.test_config.get('RDFSTORE', 'get'), self.test_config.get('RDFSTORE', 'post'))
        self.citation_metadata.title = "Obama occurrences, new mention"

        # Actual results
        # Citation1
        citation1.cite(select_statement=self.select_statement,
                       citation_metadata=self.citation_metadata,
                       citation_timestamp=self.citation_timestamp)

        # Citation2
        vie_tz = timezone(timedelta(hours=2))
        citation_timestamp2 = datetime.now(vie_tz)
        select_statement2 = open("test_data/test_citation__recite_semantically_identical2.txt", "r").read()
        citation2.cite(select_statement=select_statement2,
                       citation_metadata=self.citation_metadata,
                       citation_timestamp=citation_timestamp2)

        # Concat Citation1 + Citation2 into actual results
        actual_result = citation1.citation_metadata.citation_snippet \
                          + "\n" + citation2.citation_metadata.citation_snippet

        self.citation = citation1

        # Expected results
        # Citation1
        query_utils = QueryUtils(query=self.select_statement, citation_timestamp=self.citation_timestamp)
        citation_snippet1 = generate_citation_snippet(identifier=query_utils.pid,
                                                      creator=self.citation_metadata.creator,
                                                      title=self.citation_metadata.title,
                                                      publisher=self.citation_metadata.publisher,
                                                      pulication_year=self.citation_metadata.publication_year,
                                                      resource_type=self.citation_metadata.resource_type)

        expected_result = citation_snippet1 + "\n" + citation_snippet1
        test = Test(test_number=5,
                    tc_desc='Test if citing a query which is semantically equivalent to an existing query in the '
                            'query store will return the existing citation snippet. The test needs to pass only '
                            'for covered normalization measures where two queries would be transformed to the same '
                            'normal query and thus recognized as semantically identical.',
                    expected_result=expected_result,
                    actual_result=actual_result)

        return test

    def test_citation__non_unique_sort(self):
        # Actual results
        citation = self.citation

        test = Test(test_number=6,
                    tc_desc='Test if a query with a non-unique order by clause written by the user will throw a '
                            'NoUniqueSortIndexError exception.',
                    expected_result='The "order by"-clause in your query does not yield a uniquely sorted dataset. '
                                    'Please provide a primary key or another unique sort index',
                    actual_result="No exception thrown")
        try:
            citation.cite(select_statement=self.select_statement,
                          citation_metadata=self.citation_metadata,
                          citation_timestamp=self.citation_timestamp)
        except NoUniqueSortIndexError as e:
            test.actual_result = str(e)

        return test

    def test_citation__sort_var_not_in_select(self):
        # Actual results
        citation = self.citation

        test = Test(test_number=7,
                    tc_desc='Test if a query that contains a variable in the order by clause which is not in the '
                            'select clause throws a SortVariablesNotInSelectError exception.',
                    expected_result="There are variables in the order by clause that are not listed "
                                    "in the select clause. While this is syntactically correct "
                                    "a unique sort index should only contain variables from the "
                                    "select clause or dataset columns respectively.",
                    actual_result="No exception thrown")

        try:
            citation.cite(select_statement=self.select_statement,
                          citation_metadata=self.citation_metadata,
                          citation_timestamp=self.citation_timestamp)
        except SortVariablesNotInSelectError as e:
            test.actual_result = str(e)

        return test

    def test_citation__missing_order_by(self):
        # Actual results
        citation = self.citation

        test = Test(test_number=8,
                    tc_desc='Test if a query that does not have an "order by" clause throws '
                            'a MissingSortVariables exception.',
                    expected_result="There is no order by clause. Please provide an order by clause with "
                                    "variables that yield a unique sort.",
                    actual_result="No exception thrown")

        try:
            citation.cite(select_statement=self.select_statement,
                          citation_metadata=self.citation_metadata,
                          citation_timestamp=self.citation_timestamp)
        except MissingSortVariables as e:
            test.actual_result = str(e)

        return test

    def test_citation__aggregated_dataset(self):
        citation1 = self.citation
        citation2 = ct.Citation(self.test_config.get('RDFSTORE', 'get'), self.test_config.get('RDFSTORE', 'post'))
        self.citation_metadata.title = "Count Obama mentions with Democratic party."

        # Actual results
        # Citation1
        citation1.cite(select_statement=self.select_statement,
                       citation_metadata=self.citation_metadata,
                       citation_timestamp=self.citation_timestamp)

        # Insert two new triple
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        mention = "<http://data.ontotext.com/publishing#newMention>"
        hasInstance = "publishing:hasInstance"
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        self.rdf_engine.insert_triples([mention, hasInstance, person], prefixes)

        document = "<http://www.reuters.com/article/2021/01/01/newDocument>"
        containsMention = "publishing:containsMention"
        self.rdf_engine.insert_triples([document, containsMention, mention], prefixes)

        # Citation2
        vie_tz = timezone(timedelta(hours=2))
        citation_timestamp2 = datetime.now(vie_tz)
        citation2.cite(select_statement=self.select_statement,
                       citation_metadata=self.citation_metadata,
                       citation_timestamp=citation_timestamp2)

        # Concat Citation1 + Citation2 into actual results
        actual_result = citation1.citation_metadata.citation_snippet \
                        + "\n" + citation2.citation_metadata.citation_snippet

        self.citation = citation1

        # Expected results
        # Citation1
        query_utils = QueryUtils(query=self.select_statement, citation_timestamp=self.citation_timestamp)
        citation_snippet1 = generate_citation_snippet(identifier=query_utils.pid,
                                                      creator=self.citation_metadata.creator,
                                                      title=self.citation_metadata.title,
                                                      publisher=self.citation_metadata.publisher,
                                                      pulication_year=self.citation_metadata.publication_year,
                                                      resource_type=self.citation_metadata.resource_type)
        # Citation2
        query_utils = QueryUtils(query=self.select_statement, citation_timestamp=citation_timestamp2)
        citation_snippet2 = generate_citation_snippet(identifier=query_utils.pid,
                                                      creator=self.citation_metadata.creator,
                                                      title=self.citation_metadata.title,
                                                      publisher=self.citation_metadata.publisher,
                                                      pulication_year=self.citation_metadata.publication_year,
                                                      resource_type=self.citation_metadata.resource_type)

        expected_result = citation_snippet1 + "\n" + citation_snippet2

        # Clean up
        self.rdf_engine._delete_triples([mention, hasInstance, person], prefixes)
        self.rdf_engine._delete_triples([document, containsMention, mention], prefixes)

        test = Test(test_number=9,
                    tc_desc='Test if a query that uses aggregation functions yields the right result before '
                            'and after an insert.',
                    expected_result=expected_result,
                    actual_result=actual_result)

        return test

    def test_citation__aggregated_dataset_2(self):
        pass
        # TODO implement

    def test_citation__complex_query1(self):
        pass
        # TODO implement


t = TestCitation(annotated_tests=False)
t.run_tests()
t.print_test_results()

