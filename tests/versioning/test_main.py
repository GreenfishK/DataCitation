from src.rdf_data_citation.rdf_star import TripleStoreEngine
from datetime import timezone, timedelta, datetime
import pandas as pd


def get_test_config() -> dict:
    test_configs_file = open("../test.config", "r").readlines()
    test_configs = {}
    for line in test_configs_file:
        key_value = line.strip().split('::')
        test_configs[key_value[0]] = key_value[1]
    return test_configs


class Test:

    def __init__(self, test_number: int, tc_desc: str, expected_result: str, actual_result: str = None):
        self.test_number = test_number
        self.tc_desc = tc_desc
        self.actual_result = actual_result
        self.expected_result = expected_result
        self.yn_passed = False

    def test(self):
        """

        :return:
        """
        assert self.actual_result

        if self.expected_result == self.actual_result:
            self.yn_passed = True


class TestExecution:

    def __init__(self):
        self.test_configs = get_test_config()

        # Custom members for test execution
        self.rdf_engine = None
        self.initial_timestamp = None
        self.cnt_initial_triples = None
        self.cnt_actual_triples = None
        self.tests = []

    def print_test_results(self):
        """

        :return:
        """
        tests_df = pd.DataFrame(columns=['test_number', 'test_case_description',
                                         'expected_result', 'actual_result',
                                         'test_passed'])
        for test in self.tests:
            if isinstance(test, Test):
                tests_df = tests_df.append({'test_number': test.test_number, 'test_case_description': test.tc_desc,
                                            'expected_result': test.expected_result, 'actual_result': test.actual_result,
                                            'test_passed': test.yn_passed}, ignore_index=True)
        pd.set_option('display.width', 320)
        pd.set_option('display.max_columns', 10)
        print(tests_df)

    def before_all_tests(self):
        """

        :return:
        """

        print("Executing before_tests ...")

        vieTZObject = timezone(timedelta(hours=2))
        self.initial_timestamp = datetime(2020, 9, 1, 12, 11, 21, 941000, vieTZObject)
        if isinstance(self.test_configs, dict):
            self.rdf_engine = TripleStoreEngine(self.test_configs['get_endpoint'], self.test_configs['post_endpoint'])
        self.query_cnt_triples = "select (count(*) as ?cnt) where {?s ?p ?o.}"
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
        self.cnt_initial_triples = int(cnt_triples_df['cnt'].item().split(" ")[0])

    def before_single_test(self):
        self.query_cnt_triples = "select (count(*) as ?cnt) where {?s ?p ?o.}"
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
        self.cnt_actual_triples = int(cnt_triples_df['cnt'].item().split(" ")[0])
        self.rdf_engine.version_all_rows(self.initial_timestamp)

    def test_1(self):
        t1 = Test(test_number=1,
                  tc_desc="The number of triples after calling version_all_triples and then reset_all_triples "
                          "consecutively must equal the initial number of triples.",
                  expected_result=self.cnt_initial_triples)
        # version_all_rows is executed in before_single_test
        self.rdf_engine.reset_all_versions()
        test_query = """
               select ?s ?p ?o {
                   ?s ?p ?o .
               } 
               """
        df = self.rdf_engine.get_data(test_query)  # dataframe
        cnt_triples_after_versioning_and_resetting = len(df.index)
        t1.actual_result = cnt_triples_after_versioning_and_resetting
        t1.test()
        self.tests.append(t1)

    def test_2(self):
        t2 = Test(test_number=2,
                  tc_desc="The number of triples after initial versioning should "
                          "be three times the initial number of triples.",
                  expected_result=self.cnt_initial_triples * 3)

        self.rdf_engine.version_all_rows(self.initial_timestamp)
        test_query = """
                select ?s ?p ?o {
                    ?s ?p ?o .
                } 
                """
        df = self.rdf_engine.get_data(test_query)  # dataframe
        cnt_triples_after_versioning = len(df.index)
        self.rdf_engine.reset_all_versions()
        t2.actual_result = cnt_triples_after_versioning
        t2.test()
        self.tests.append(t2)

    def _test_3(self):
        t3 = Test(test_number=3,
                  tc_desc="The number of added triples after an update of one triple must equal to 3. ",
                  expected_result=(self.cnt_initial_triples * 3) + 3)
        self.rdf_engine.version_all_rows(self.initial_timestamp)
        triples_to_update = """
                PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
                PREFIX publishing: <http://ontology.ontotext.com/publishing#>

                select ?subjectToUpdate ?predicateToUpdate ?objectToUpdate {
                    ?person pub:memberOfPoliticalParty ?party .
                    ?person pub:preferredLabel ?personLabel .
                    ?party pub:hasValue ?value .
                    ?value pub:preferredLabel ?party_label .
                    filter(?personLabel = "Barack Obama"@en) 

                    bind(?person as ?subjectToUpdate)
                    bind(pub:memberOfPoliticalParty as ?predicateToUpdate)
                    bind(?party as ?objectToUpdate)
                } 
                """
        new_value = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"
        self.rdf_engine.update(triples_to_update, new_value)
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
        t3.actual_result = int(cnt_triples_df['cnt'].item().split(" ")[0])
        self.rdf_engine.reset_all_versions()

    def run_tests(self):
        """

        :return:
        """

        self.before_all_tests()
        test_functions = [func for func in dir(t) if callable(getattr(t, func)) and func.startswith('test_')]
        for func in test_functions:
            print("hier")
            self.before_single_test()
            test_function = getattr(self, func)
            test_function()
            self.after_single_test()
        self.after_all_tests()

        print("Executing tests ...")

    def after_single_test(self):
        self.rdf_engine.reset_all_versions()

    def after_all_tests(self):
        """

        :return:
        """
        print("Executing after_tests ...")
        rdf_engine = self.rdf_engine

        rdf_engine.reset_all_versions()


t = TestExecution()
t.run_tests()
t.print_test_results()
