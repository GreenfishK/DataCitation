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

    def __init__(self, tc_desc: str, expected_result: str, actual_result: str = None):
        self.tc_desc = tc_desc
        self.actual_result = actual_result
        self.expected_result = expected_result

    def test(self):
        """

        :return:
        """
        assert self.actual_result

        if self.expected_result == self.actual_result:
            return True
        else:
            return False


class TestExecution:

    def __init__(self):
        self.test_configs = get_test_config()

        # Custom members for test execution
        self.rdf_engine = None
        self.initial_timestamp = None
        self.cnt_initial_triples = None
        self.test_results = {}

    def before_test(self):
        """

        :return:
        """

        print("Executing before_tests ...")

        vieTZObject = timezone(timedelta(hours=2))
        self.initial_timestamp = datetime(2020, 9, 1, 12, 11, 21, 941000, vieTZObject)
        if isinstance(self.test_configs, dict):
            self.rdf_engine = TripleStoreEngine(self.test_configs['get_endpoint'], self.test_configs['post_endpoint'])
        query_cnt_triples = "select (count(*) as ?cnt) where {?s ?p ?o.}"
        cnt_triples_df = self.rdf_engine.get_data(query_cnt_triples)
        self.cnt_initial_triples = cnt_triples_df['cnt'].item().split(" ")[0]

    def run_tests(self):
        """

        :return:
        """
        print("Executing tests ...")
        test_query_1 = """
        select ?s ?p ?o {
            ?s ?p ?o .
        } 
        """

        # Test 1
        t1 = Test(tc_desc="The number of triples after initial versioning should "
                          "be three times the initial number of triples.",
                  expected_result=self.cnt_initial_triples * 3)

        rdf_engine = self.rdf_engine

        #rdf_engine.version_all_rows(self.initial_timestamp)
        df = rdf_engine.get_data(test_query_1)  # dataframe
        cnt_triples_after_versioning = df.count()

        snapshot_1 = rdf_engine.get_data(test_query_1)  # dataframe

        vieTZObject = timezone(timedelta(hours=2))
        initial_timestamp = datetime(2020, 9, 1, 12, 11, 21, 941000, vieTZObject)
        rdf_engine.version_all_rows(initial_timestamp)
        rdf_engine.reset_all_versions()

        snapshot_2 = rdf_engine.get_data(test_query_1)  # dataframe

        duplicates = pd.concat([snapshot_1, snapshot_2]).drop_duplicates(keep=False)

        print(snapshot_1.count())
        print(snapshot_2.count())
        print(duplicates)

    def after_test(self):
        """

        :return:
        """
        print("Executing after_tests ...")
        rdf_engine = self.rdf_engine

        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}

        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        memberOfPoliticalParty = "pub:memberOfPoliticalParty"
        party = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"
        rdf_engine._delete_triples((person, memberOfPoliticalParty, party), prefixes)

        mention = "<hhttp://data.ontotext.com/publishing#Mention-dbaa4de4563be5f6b927c87e09f90461c09451296f4b52b1f80dcb6e941a5acd>"
        hasInstance = "publishing:hasInstance"
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        rdf_engine._delete_triples((mention, hasInstance, person), prefixes)

        document = "<http://www.reuters.com/article/2014/10/10/us-usa-california-mountains-idUSKCN0HZ0U720141010>"
        containsMention = "publishing:containsMention"
        mention = "<hhttp://data.ontotext.com/publishing#Mention-dbaa4de4563be5f6b927c87e09f90461c09451296f4b52b1f80dcb6e941a5acd>"
        rdf_engine._delete_triples((document, containsMention, mention), prefixes)

        rdf_engine.reset_all_versions()


t = TestExecution()
t.before_test()
t.run_tests()
#t.after_test()