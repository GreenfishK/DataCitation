from src.rdf_data_citation.rdf_star import TripleStoreEngine
from datetime import timezone, timedelta, datetime
from tests.test_base import Test
import pandas as pd
import configparser


class TestExecution:

    def __init__(self):
        """Load configuration from .ini file."""
        self.test_config = configparser.ConfigParser()
        self.test_config.read('../../config.ini')

        # Custom members for test execution
        self.rdf_engine = None
        self.initial_timestamp = None
        self.cnt_initial_triples = None
        self.cnt_actual_triples = None
        self.query_cnt_triples = "select (count(*) as ?cnt) where {?s ?p ?o.}"
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
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('display.max_colwidth', 150)
        pd.set_option('display.max_columns', 10)
        print(tests_df)

    def before_all_tests(self):
        """

        :return:
        """

        print("Executing before_tests ...")

        vieTZObject = timezone(timedelta(hours=2))
        self.initial_timestamp = datetime(2020, 9, 1, 12, 11, 21, 941000, vieTZObject)
        self.rdf_engine = TripleStoreEngine(self.test_config.get('RDFSTORE', 'get'),
                                            self.test_config.get('RDFSTORE', 'post'))
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
        self.cnt_initial_triples = int(cnt_triples_df['cnt'].item().split(" ")[0])
        self.rdf_engine.reset_all_versions()

    def before_single_test(self):
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
        return t1

    def test_2(self):
        t2 = Test(test_number=2,
                  tc_desc="The number of triples after initial versioning should "
                          "be three times the initial number of triples.",
                  expected_result=self.cnt_initial_triples * 3)

        test_query = """
                select ?s ?p ?o {
                    ?s ?p ?o .
                } 
                """
        df = self.rdf_engine.get_data(test_query)  # dataframe
        cnt_triples_after_versioning = len(df.index)
        t2.actual_result = cnt_triples_after_versioning
        return t2

    def test_3(self):
        test = Test(test_number=3,
                    tc_desc='After a single triple update the triples''s annotation, namely, '
                            '<<?s ?p ?o>> citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime '
                            'must not exist anymore. Thus, before the update there should be one match and '
                            'after the update there must be no match anymore. In total this adds up to one.',
                    expected_result="1")
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()
        new_value = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"

        test_query = 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ' \
                     'PREFIX pub: <http://ontology.ontotext.com/taxonomy/>' \
                     'PREFIX citing: <https://github.com/GreenfishK/DataCitation/citing/>' \
                     '' \
                     'select ?s ?p ?o where { <<<http://ontology.ontotext.com/resource/tsk4wye1ftog> ' \
                     'pub:memberOfPoliticalParty <http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>>>' \
                     ' citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime ' \
                     '' \
                     'bind(<http://ontology.ontotext.com/resource/tsk4wye1ftog> as ?s) ' \
                     'bind(pub:memberOfPoliticalParty as ?p) ' \
                     'bind(<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900> as ?o)}'

        result_set_before_update = self.rdf_engine.get_data(test_query)
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query)
        test.actual_result = str(len(result_set_before_update.index) + len(result_set_after_update.index))

        # Clean up
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        memberOfPoliticalParty = "pub:memberOfPoliticalParty"
        party = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"
        self.rdf_engine._delete_triples((person, memberOfPoliticalParty, party), prefixes)

        return test

    def test_4(self):
        test = Test(test_number=4,
                    tc_desc='After a single triple update four additional triples must be added. '
                            'One of them must be for outdating the triple and annotated with a '
                            'timestamp < "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime ',
                    expected_result="1")
        # Update
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()
        new_value = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"

        # Read
        test_query = open("test_data/test_4_query.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query)
        print(result_set_after_update)
        test.actual_result = str(len(result_set_after_update.index))

        # Clean up
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        memberOfPoliticalParty = "pub:memberOfPoliticalParty"
        party = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"
        self.rdf_engine._delete_triples((person, memberOfPoliticalParty, party), prefixes)

        return test

    def test_5(self):
        new_value = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"

        test = Test(test_number=5,
                    tc_desc='After a single triple update four additional triples must be added. '
                            'One of them must be the new triple with the new object value. ',
                    expected_result="1_{0}".format(new_value[1:-1]))
        # Update
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()

        # Read
        test_query = open("test_data/test_5_query.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query)
        new_value = result_set_after_update['party'].iloc[0]
        test.actual_result = str(len(result_set_after_update.index)) + "_" + new_value

        # Clean up
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        memberOfPoliticalParty = "pub:memberOfPoliticalParty"
        party = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"
        self.rdf_engine._delete_triples((person, memberOfPoliticalParty, party), prefixes)

        return test

    def test_6(self):
        new_value = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"

        test = Test(test_number=6,
                    tc_desc='After a single triple update four additional triples must be added. '
                            'One of them must be a nested triple where the new triple is annotated with a version '
                            'timestamp. ',
                    expected_result="1")
        # Update
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()

        # Read
        test_query = open("test_data/test_6_query.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query)
        test.actual_result = str(len(result_set_after_update.index))

        # Clean up
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        memberOfPoliticalParty = "pub:memberOfPoliticalParty"
        party = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"
        self.rdf_engine._delete_triples((person, memberOfPoliticalParty, party), prefixes)

        return test

    def test_7(self):
        new_value = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"

        test = Test(test_number=7,
                    tc_desc='After a single triple update four additional triples must be added. '
                            'One of them must be a nested triple where the new triple is annotated with '
                            '"9999-12-31T00:00:00.000+02:00" stating that it is valid until further notice. ',
                    expected_result="1_9999-12-31T00:00:00.000+02:00 [http://www.w3.org/2001/XMLSchema#dateTime]")
        # Update
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()

        # Read
        test_query = open("test_data/test_7_query.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query)
        test.actual_result = str(len(result_set_after_update.index)) + "_" \
                             + result_set_after_update['valid_until'].iloc[0]

        # Clean up
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        memberOfPoliticalParty = "pub:memberOfPoliticalParty"
        party = "<http://ontology.ontotext.com/resource/Q201795S476DFED9-C64A-4E56-B4C3-CFB368801FBF>"
        self.rdf_engine._delete_triples((person, memberOfPoliticalParty, party), prefixes)

        return test

    def run_tests(self):
        """

        :return:
        """
        print("Executing tests ...")

        self.before_all_tests()
        test_functions = [func for func in dir(t) if callable(getattr(t, func)) and func.startswith('test_')]
        try:
            test_number = 1
            for func in test_functions:
                self.before_single_test()
                test_function = getattr(self, func)
                test = test_function()
                test_number += 1
                test.test()
                self.tests.append(test)
                self.after_single_test()
        except Exception as e:
            print(e)
        finally:
            self.after_all_tests()

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
