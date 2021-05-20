from src.rdf_data_citation.rdf_star import TripleStoreEngine, VersioningMode
from tests.test_base import Test, TestExecution, format_text
from datetime import timezone, timedelta, datetime
import logging


class TestVersioning(TestExecution):

    def __init__(self, annotated_tests: bool = False):
        super().__init__(annotated_tests)
        self.rdf_engine = None
        self.initial_timestamp = None
        self.cnt_initial_triples = None
        self.cnt_actual_triples = None
        self.query_cnt_triples = "select (count(*) as ?cnt) where {?s ?p ?o.}"

    def before_all_tests(self):
        """
        Make sure that versioning_modes has not been applied to the RDF store before executing the tests!

        :return:
        """

        print("Executing before_tests ...")

        vieTZObject = timezone(timedelta(hours=2))
        self.initial_timestamp = datetime(2020, 9, 1, 12, 11, 21, 941000, vieTZObject)
        self.rdf_engine = TripleStoreEngine(self.test_config.get('RDFSTORE', 'get'),
                                            self.test_config.get('RDFSTORE', 'post'))
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        self.cnt_initial_triples = int(cnt_triples_df['cnt'].item().split(" ")[0])
        self.rdf_engine.reset_all_versions()

    def before_single_test(self, test_name: str):
        """

        :return:
        """

        print("Executing before_single_tests ...")

        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        self.cnt_actual_triples = int(cnt_triples_df['cnt'].item().split(" ")[0])
        self.rdf_engine.version_all_rows(self.initial_timestamp)

    def after_single_test(self):
        """

        :return:
        """

        print("Executing after_single_tests ...")
        self.rdf_engine.reset_all_versions()

    def after_all_tests(self):
        """

        :return:
        """

        print("Executing after_tests ...")
        rdf_engine = self.rdf_engine
        rdf_engine.reset_all_versions()

    def test_version__init_reset(self):
        test = Test(test_number=1,
                    tc_desc="The number of triples after calling version_all_triples and then reset_all_triples "
                            "consecutively must equal the initial number of triples.",
                    expected_result=str(self.cnt_initial_triples))
        # version_all_rows is executed in before_single_test
        self.rdf_engine.reset_all_versions()
        test_query = """
               select ?s ?p ?o {
                   ?s ?p ?o .
               } 
               """
        df = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)  # dataframe
        cnt_triples_after_versioning_and_resetting = str(len(df.index))
        test.actual_result = cnt_triples_after_versioning_and_resetting
        return test

    def test_version__init(self):
        versioning_mode = self.test_config.get('VERSIONING', 'versioning_mode')
        if versioning_mode == "Q_PERF":
            expected_result = str(self.cnt_initial_triples * 3)
        elif versioning_mode == "SAVE_MEM":
            expected_result = str(self.cnt_initial_triples * 2)
        else:
            expected_result = "This is an exception and not the real expected result! " \
                              "The versioning_modes mode in config.ini must be set either to " \
                              "Q_PERF or SVE_MEM."

        test = Test(test_number=2,
                    tc_desc="The number of triples after initial versioning_modes should "
                            "be three times the initial number of triples.",
                    expected_result=expected_result)

        test_query = """
                select ?s ?p ?o {
                    ?s ?p ?o .
                } 
                """
        df = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)  # dataframe
        cnt_triples_after_versioning = str(len(df.index))
        test.actual_result = cnt_triples_after_versioning
        return test

    def test_update_single__delete_valid_until(self):
        test = Test(test_number=3,
                    tc_desc='After a single triple update the triples''s annotation, namely, '
                            '<<?s ?p ?o>> citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime '
                            'must not exist anymore. Thus, before the update there should be one match and '
                            'after the update there must be no match anymore.',
                    expected_result="1_0")
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        test_query = open("test_data/test_update_single__delete_valid_until.txt", "r").read()

        result_set_before_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        test.actual_result = str(len(result_set_before_update.index)) + "_" + str(len(result_set_after_update.index))

        # Clean up - Delete newly added triple. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        member = "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>"
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_single__outdate(self):
        test = Test(test_number=4,
                    tc_desc='After a single triple update four additional triples must be added. '
                            'One of them must be for outdating the triple and annotated with a '
                            'timestamp < "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime ',
                    expected_result="1")
        # Update
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine.update(triples_to_update, new_value)

        # Read
        test_query = open("test_data/test_update_single__outdate.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        print(result_set_after_update)
        test.actual_result = str(len(result_set_after_update.index))

        # Clean up - Delete newly added triple. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        member = "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>"
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_single__add_new_triple(self):
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        test = Test(test_number=5,
                    tc_desc='After a single triple update four additional triples must be added. '
                            'One of them must be the new triple with the new object value. ',
                    expected_result="1_{0}".format(new_value[1:-1]))
        # Update
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)

        # Read
        test_query = open("test_data/test_update_single__add_new_triple.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        new_value = result_set_after_update['party'].iloc[0]
        test.actual_result = str(len(result_set_after_update.index)) + "_" + new_value

        # Clean up - Delete newly added triple. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        member = "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>"
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_single__timestamp_new_triple_valid_from(self):
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        test = Test(test_number=6,
                    tc_desc='After a single triple update four additional triples must be added. '
                            'One of them must be a nested triple where the new triple is annotated with a version '
                            'timestamp that is in the past. ',
                    expected_result="1")
        # Update
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)

        # Read
        test_query = open("test_data/test_update_single__timestamp_new_triple_valid_from.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        test.actual_result = str(len(result_set_after_update.index))

        # Clean up - Delete newly added triple. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        member = "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>"
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_single__timestamp_new_triple_valid_until(self):
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        test = Test(test_number=7,
                    tc_desc='After a single triple update four additional triples must be added. '
                            'One of them must be a nested triple where the new triple is annotated with '
                            '"9999-12-31T00:00:00.000+02:00" stating that it is valid until further notice. ',
                    expected_result="1_9999-12-31T00:00:00.000+02:00 [http://www.w3.org/2001/XMLSchema#dateTime]")
        # Update
        triples_to_update = open("test_data/sinlge_triple_update.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)

        # Read
        test_query = open("test_data/test_update_single__timestamp_new_triple_valid_until.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        test.actual_result = str(len(result_set_after_update.index)) + "_" \
                             + result_set_after_update['valid_until'].iloc[0]

        # Clean up - Delete newly added triple. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        member = "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>"
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_multi__delete_valid_until(self):
        test = Test(test_number=8,
                    tc_desc='For each triple matched in the SPARQL select statement the triples'' annotation, namely, '
                            '<<?s ?p ?o>> citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime '
                            'must not exist anymore. Before the update there should be three matches '
                            '(see multi_triples_update.txt) and after the update there must be no match anymore.',
                    expected_result="3_0")
        triples_to_update = open("test_data/multi_triples_update.txt", "r").read()
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        test_query = open("test_data/test_update_multi__delete_valid_until.txt", "r").read()

        result_set_before_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        test.actual_result = str(len(result_set_before_update.index)) + "_" + str(len(result_set_after_update.index))

        # Clean up - Delete newly added triples. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        members = ["<http://ontology.ontotext.com/resource/Q6176439SB95442FA-96D2-44D3-A994-3560AFAFD7A0>",
                   "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>",
                   "<http://ontology.ontotext.com/resource/Q460035S071C8FD6-DA5F-4189-81A7-D589D13B2D09>"]
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        for member in members:
            self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_multi__outdate(self):
        test = Test(test_number=9,
                    tc_desc='For each triple that is matched in the provided SPARQL select query (triples_to_update) '
                            'four additional triples must be added. '
                            'One of them must be for outdating the triple and annotated with a '
                            'timestamp < "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime ',
                    expected_result="3")
        # Update
        triples_to_update = open("test_data/multi_triples_update.txt", "r").read()
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine.update(triples_to_update, new_value)

        # Read
        test_query = open("test_data/test_update_multi__outdate.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        print(result_set_after_update)
        test.actual_result = str(len(result_set_after_update.index))



        # Clean up - Delete newly added triples. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        members = ["<http://ontology.ontotext.com/resource/Q6176439SB95442FA-96D2-44D3-A994-3560AFAFD7A0>",
                   "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>",
                   "<http://ontology.ontotext.com/resource/Q460035S071C8FD6-DA5F-4189-81A7-D589D13B2D09>"]
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        for member in members:
            self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_multi__add_new_triple(self):
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        test = Test(test_number=10,
                    tc_desc='For each triple that is matched in the provided SPARQL select query (triples_to_update) '
                            'four additional triples must be added. '
                            'One of them must be the new triple with the new object value. ',
                    expected_result="3_{0}".format(new_value[1:-1]))
        # Update
        triples_to_update = open("test_data/multi_triples_update.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)

        # Read
        test_query = open("test_data/test_update_multi__add_new_triple.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        new_value = result_set_after_update['party'].iloc[0]
        test.actual_result = str(len(result_set_after_update.index)) + "_" + new_value

        # Clean up - Delete newly added triples. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        members = ["<http://ontology.ontotext.com/resource/Q6176439SB95442FA-96D2-44D3-A994-3560AFAFD7A0>",
                   "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>",
                   "<http://ontology.ontotext.com/resource/Q460035S071C8FD6-DA5F-4189-81A7-D589D13B2D09>"]
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        for member in members:
            self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_multi__timestamp_new_triple_valid_from(self):
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        test = Test(test_number=11,
                    tc_desc='For each triple that is matched in the provided SPARQL select query (triples_to_update) '
                            'four additional triples must be added. '
                            'One of them must be a nested triple where the new triple is annotated with a version '
                            'timestamp that is in the past. ',
                    expected_result="3")
        # Update
        triples_to_update = open("test_data/multi_triples_update.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)

        # Read
        test_query = open("test_data/test_update_multi__timestamp_new_triple_valid_from.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        test.actual_result = str(len(result_set_after_update.index))

        # Clean up - Delete newly added triples. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        members = ["<http://ontology.ontotext.com/resource/Q6176439SB95442FA-96D2-44D3-A994-3560AFAFD7A0>",
                   "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>",
                   "<http://ontology.ontotext.com/resource/Q460035S071C8FD6-DA5F-4189-81A7-D589D13B2D09>"]
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        for member in members:
            self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_multi__timestamp_new_triple_valid_until(self):
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        test = Test(test_number=12,
                    tc_desc='For each triple that is matched in the provided SPARQL select query (triples_to_update) '
                            'four additional triples must be added. '
                            'One of them must be a nested triple where the new triple is annotated with '
                            '"9999-12-31T00:00:00.000+02:00" stating that it is valid until further notice. ',
                    expected_result="3_9999-12-31T00:00:00.000+02:00 [http://www.w3.org/2001/XMLSchema#dateTime]")
        # Update
        triples_to_update = open("test_data/multi_triples_update.txt", "r").read()
        self.rdf_engine.update(triples_to_update, new_value)

        # Read
        test_query = open("test_data/test_update_multi__timestamp_new_triple_valid_until.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        test.actual_result = str(len(result_set_after_update.index)) + "_" \
                             + result_set_after_update['valid_until'].iloc[0]

        # Clean up - Delete newly added triples. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        members = ["<http://ontology.ontotext.com/resource/Q6176439SB95442FA-96D2-44D3-A994-3560AFAFD7A0>",
                   "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>",
                   "<http://ontology.ontotext.com/resource/Q460035S071C8FD6-DA5F-4189-81A7-D589D13B2D09>"]
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        for member in members:
            self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_multi__same_update_twice(self):
        new_value = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"

        # Update
        triples_to_update = open("test_data/multi_triples_update.txt", "r").read()

        # Read
        self.rdf_engine.update(triples_to_update, new_value)
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        cnt_triples_after_first_update = int(cnt_triples_df['cnt'].item().split(" ")[0])

        test = Test(test_number=13,
                    tc_desc='If the object to be updated has the same value as the new value, no update shall occur. '
                            'The number of rows must stay the same.',
                    expected_result=str(cnt_triples_after_first_update))

        self.rdf_engine.update(triples_to_update, new_value)
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        cnt_triples_after_second_update = int(cnt_triples_df['cnt'].item().split(" ")[0])

        test.actual_result = str(cnt_triples_after_second_update)

        # Clean up - Delete newly added triples. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        members = ["<http://ontology.ontotext.com/resource/Q6176439SB95442FA-96D2-44D3-A994-3560AFAFD7A0>",
                   "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>",
                   "<http://ontology.ontotext.com/resource/Q460035S071C8FD6-DA5F-4189-81A7-D589D13B2D09>"]
        hasValue = "pub:hasValue"
        party = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        for member in members:
            self.rdf_engine._delete_triples((member, hasValue, party), prefixes)

        return test

    def test_update_multi__two_updates(self):
        # Read
        triples_to_update = open("test_data/multi_triples_update.txt", "r").read()
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        cnt_triples_before_first_update = int(cnt_triples_df['cnt'].item().split(" ")[0])

        # Update 1
        new_value_1 = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine.update(triples_to_update, new_value_1)
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        cnt_triples_after_first_update = int(cnt_triples_df['cnt'].item().split(" ")[0])

        test = Test(test_number=14,
                    tc_desc='If a set of triples that has already been updated is updated again the number of '
                            'additional rows must be the same as after the first update.',
                    expected_result=str(cnt_triples_after_first_update - cnt_triples_before_first_update))

        # Update 2
        new_value_2 = "<http://ontology.ontotext.com/resource/tsk6i4bhsdfk>"
        self.rdf_engine.update(triples_to_update, new_value_2)
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        cnt_triples_after_second_update = int(cnt_triples_df['cnt'].item().split(" ")[0])

        test.actual_result = str(cnt_triples_after_second_update - cnt_triples_after_first_update)

        # Clean up - Delete newly added triples. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        members = ["<http://ontology.ontotext.com/resource/Q6176439SB95442FA-96D2-44D3-A994-3560AFAFD7A0>",
                   "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>",
                   "<http://ontology.ontotext.com/resource/Q460035S071C8FD6-DA5F-4189-81A7-D589D13B2D09>"]
        hasValue = "pub:hasValue"
        party_1 = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        party_2 = "<http://ontology.ontotext.com/resource/tsk6i4bhsdfk>"
        for member in members:
            self.rdf_engine._delete_triples((member, hasValue, party_1), prefixes)
            self.rdf_engine._delete_triples((member, hasValue, party_2), prefixes)

        return test

    def test_update_multi__timeline_consistency(self):
        versioning_mode = self.test_config.get('VERSIONING', 'versioning_mode')
        if versioning_mode == "Q_PERF":
            expected_result = "valid_from_positive_deltas: 2"
        elif versioning_mode == "SAVE_MEM":
            expected_result = "valid_from_positive_deltas: 1"
        else:
            expected_result = "This is an exception and not the real expected result! " \
                              "The versioning_modes mode in config.ini must be set either to " \
                              "Q_PERF or SVE_MEM."

        test = Test(test_number=15,
                    tc_desc='If a set of triples is updated multiple times each consecutive update must come '
                            'with a newer citing:valid_until timestamp. The most recent one must have the value'
                            '"9999-12-31T00:00:00.000+02:00".',
                    expected_result=expected_result)
        # Read
        triples_to_update = open("test_data/multi_triples_update.txt", "r").read()

        # Update 1
        new_value_1 = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine.update(triples_to_update, new_value_1)

        # Update 2
        new_value_2 = "<http://ontology.ontotext.com/resource/tsk6i4bhsdfk>"
        self.rdf_engine.update(triples_to_update, new_value_2)

        # Read
        test_query = open("test_data/test_update_multi__timeline_consistency.txt", "r").read()
        result_set_after_update = self.rdf_engine.get_data(test_query, yn_timestamp_query=False)
        result_set_after_update = result_set_after_update.pivot(index=['valid_from', 'valid_until'], columns='member',
                                                                values='party')
        result_set_after_update.reset_index(inplace=True)
        valid_from_series = result_set_after_update['valid_from'].str.split(" ").str[0]
        valid_from_series = valid_from_series.apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f%z'))
        valid_from_series_diff = valid_from_series.diff()
        valid_from_series_diff_seconds = valid_from_series_diff.apply(lambda x: x.total_seconds())
        valid_from_series_consistency = valid_from_series_diff_seconds.apply(lambda x: 1 if x > 0 else 0)

        test.actual_result = "valid_from_positive_deltas: {0}".format(str(valid_from_series_consistency.sum()))

        # Clean up - Delete newly added triples. The nested triples are deleted in after_single_test()
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'xsd': 'http://www.w3.org/2001/XMLSchema#',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}
        members = ["<http://ontology.ontotext.com/resource/Q6176439SB95442FA-96D2-44D3-A994-3560AFAFD7A0>",
                   "<http://ontology.ontotext.com/resource/Q76SBFD36E46-359B-445A-8EC2-A3BDDFF5E900>",
                   "<http://ontology.ontotext.com/resource/Q460035S071C8FD6-DA5F-4189-81A7-D589D13B2D09>"]
        hasValue = "pub:hasValue"
        party_1 = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        party_2 = "<http://ontology.ontotext.com/resource/tsk6i4bhsdfk>"
        for member in members:
            self.rdf_engine._delete_triples((member, hasValue, party_1), prefixes)
            self.rdf_engine._delete_triples((member, hasValue, party_2), prefixes)

        return test

    def test_get_data__query_with_union(self):
        test = Test(test_number=16,
                    tc_desc='Test if a query with a "union" will yield correct results after it has been '
                            'extended with versioning extensions and executed to retrieve live data.',
                    expected_result='',
                    actual_result='')

        return test

    def x_test_get_data__nested_select(self):
        dataset_query = open("test_data/test_get_data__nested_select.txt", "r").read()
        df = self.rdf_engine.get_data(dataset_query)

        test = Test(test_number=17,
                    tc_desc='Test if a query with a subselect will yield correct results after it has been '
                            'extended with versioning extensions and executed to retrieve live data.',
                    expected_result='10',
                    actual_result=str(len(df.index)))

        return test

    def test_insert__two_consecutive_inserts(self):
        prefixes = {'pub': 'http://ontology.ontotext.com/taxonomy/',
                    'publishing': 'http://ontology.ontotext.com/publishing#'}

        # Data before insert
        dataset_query = open("test_data/test_insert__dataset_query.txt", "r").read()

        # +2 hours timezone because this is how graphDB sets it for the vienna timezone.
        vieTZObject = timezone(timedelta(hours=2))
        timestamp_before_insert = datetime.now(vieTZObject)

        mention = "<http://data.ontotext.com/publishing#Mention-dbaa4de4563be5f6b927c87e09f90461c09451296f4b52b1f80dcb6e941a5acd>"
        hasInstance = "publishing:hasInstance"
        person = "<http://ontology.ontotext.com/resource/tsk4wye1ftog>"
        document = "<http://www.reuters.com/article/2014/10/10/us-usa-california-mountains-idUSKCN0HZ0U720141010>"
        containsMention = "publishing:containsMention"

        self.rdf_engine.insert_triple((mention, hasInstance, person), prefixes)
        self.rdf_engine.insert_triple((document, containsMention, mention), prefixes)
        dataset_before_insert = self.rdf_engine.get_data(dataset_query, timestamp_before_insert)
        dataset_after_insert = self.rdf_engine.get_data(dataset_query)

        test = Test(test_number=18,
                    tc_desc='Make two consecutive inserts and retrieve the dataset as it was before, between '
                            'and after the inserts. Check that the datasets reflect the right information as of'
                            'each timestamp. ',
                    expected_result=str(len(dataset_before_insert.index) + 1),
                    actual_result=str(len(dataset_after_insert.index)))

        # Clean up
        self.rdf_engine._delete_triples((mention, hasInstance, person), prefixes)
        self.rdf_engine._delete_triples((document, containsMention, mention), prefixes)

        return test

    def test_outdate__outdate_triples(self):
        # Data before insert
        dataset_query = open("test_data/test_outdate__dataset_query.txt", "r").read()
        triples_to_outdate = open("test_data/test_outdate__outdate_triples.txt", "r").read()

        # +2 hours timezone because this is how graphDB sets it for the vienna timezone.
        vieTZObject = timezone(timedelta(hours=2))
        timestamp_before_outdate = datetime.now(vieTZObject)

        # Count triples before outdate
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        cnt_before_outdate = int(cnt_triples_df['cnt'].item().split(" ")[0])

        # Outdate
        self.rdf_engine.outdate_triples(triples_to_outdate)

        # Count triples after outdate
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples, yn_timestamp_query=False)
        cnt_after_outdate = int(cnt_triples_df['cnt'].item().split(" ")[0])

        dataset_before_outdate = self.rdf_engine.get_data(dataset_query, timestamp_before_outdate)
        # data_set_before_outdate_non_empty = dataset_before_outdate.empty
        dataset_after_outdate = self.rdf_engine.get_data(dataset_query)

        test = Test(test_number=19,
                    tc_desc='Test if the number of triples in the RDF store after outdating a set of triples '
                            'did not change. Moreover, test if the result set after the triples have been outdated '
                            'is empty. ',
                    expected_result="number of triples in db: {0}; number of rows in "
                                    "dataset after outdate: {1}".format(str(cnt_before_outdate), 0),
                    actual_result="number of triples in db: {0}; number of rows in "
                                    "dataset after outdate: {1}".format(str(cnt_after_outdate),
                                                                        len(dataset_after_outdate.index)))

        return test


t = TestVersioning(annotated_tests=True)
t.run_tests()
t.print_test_results()
