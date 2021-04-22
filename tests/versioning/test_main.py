import numpy as np

from src.rdf_data_citation.rdf_star import TripleStoreEngine
from datetime import timezone, timedelta, datetime
from tests.test_base import Test
import pandas as pd
import configparser
from tabulate import tabulate


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

        def format_text(comment, max_line_length):
            # accumulated line length
            ACC_length = 0
            words = comment.split(" ")
            formatted_text = ""
            for word in words:
                # if ACC_length + len(word) and a space is <= max_line_length
                if ACC_length + (len(word) + 1) <= max_line_length:
                    # append the word and a space
                    formatted_text = formatted_text + word + " "
                    # length = length + length of word + length of space
                    ACC_length = ACC_length + len(word) + 1
                else:
                    # append a line break, then the word and a space
                    formatted_text = formatted_text + "\n" + word + " "
                    # reset counter of length to the length of a word and a space
                    ACC_length = len(word) + 1
            return formatted_text

        tests_df = pd.DataFrame(columns=['test_number', 'test_name', 'test_case_description',
                                         'expected_result', 'actual_result', 'test_passed'])
        for test in self.tests:
            if isinstance(test, Test):
                formatted_tc_desc = format_text(test.tc_desc, 100)
                formatted_expected_result = format_text(test.expected_result, 50)
                formatted_actual_result = format_text(test.actual_result, 50)

                tests_df = tests_df.append({'test_number': test.test_number,
                                            'test_name': test.test_name,
                                            'test_case_description': formatted_tc_desc,
                                            'expected_result': formatted_expected_result,
                                            'actual_result': formatted_actual_result,
                                            'test_passed': test.yn_passed}, ignore_index=True)

        tests_df.sort_values('test_number', inplace=True)
        pdtabulate = lambda df: tabulate(df, headers='keys', tablefmt='grid', )
        print(pdtabulate(tests_df))

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

    def test_version__init_reset(self):
        t1 = Test(test_number=1,
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
        df = self.rdf_engine.get_data(test_query)  # dataframe
        cnt_triples_after_versioning_and_resetting = str(len(df.index))
        t1.actual_result = cnt_triples_after_versioning_and_resetting
        return t1

    def test_version__init(self):
        t2 = Test(test_number=2,
                  tc_desc="The number of triples after initial versioning should "
                          "be three times the initial number of triples.",
                  expected_result=str(self.cnt_initial_triples * 3))

        test_query = """
                select ?s ?p ?o {
                    ?s ?p ?o .
                } 
                """
        df = self.rdf_engine.get_data(test_query)  # dataframe
        cnt_triples_after_versioning = str(len(df.index))
        t2.actual_result = cnt_triples_after_versioning
        return t2

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

        result_set_before_update = self.rdf_engine.get_data(test_query)
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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

        result_set_before_update = self.rdf_engine.get_data(test_query)
        self.rdf_engine.update(triples_to_update, new_value)
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
        cnt_triples_after_first_update = int(cnt_triples_df['cnt'].item().split(" ")[0])

        test = Test(test_number=13,
                    tc_desc='If the object to be updated has the same value as the new value, no update shall occur. '
                            'The number of rows must stay the same.',
                    expected_result=str(cnt_triples_after_first_update))

        self.rdf_engine.update(triples_to_update, new_value)
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
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
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
        cnt_triples_before_first_update = int(cnt_triples_df['cnt'].item().split(" ")[0])

        # Update 1
        new_value_1 = "<http://ontology.ontotext.com/resource/tsk8e8v43mrk>"
        self.rdf_engine.update(triples_to_update, new_value_1)
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
        cnt_triples_after_first_update = int(cnt_triples_df['cnt'].item().split(" ")[0])

        test = Test(test_number=14,
                    tc_desc='If a set of triples that has already been updated is updated again the number of '
                            'additional rows must be the same as after the first update.',
                    expected_result=str(cnt_triples_after_first_update - cnt_triples_before_first_update))

        # Update 2
        new_value_2 = "<http://ontology.ontotext.com/resource/tsk6i4bhsdfk>"
        self.rdf_engine.update(triples_to_update, new_value_2)
        cnt_triples_df = self.rdf_engine.get_data(self.query_cnt_triples)
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
        test = Test(test_number=15,
                    tc_desc='If a set of triples is updated multiple times each consecutive update must come'
                            'with a newer citing:valid_until timestamp. The most recent one must have the value'
                            '"9999-12-31T00:00:00.000+02:00".',
                    expected_result="valid_from_positive_deltas: 2")
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
        result_set_after_update = self.rdf_engine.get_data(test_query)
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

    def run_tests(self, annotated_tests: bool = False):
        """
        :param annotated_tests: If this flag is set only tests with the prefix "x_test" will be executed.
        :return:
        """
        print("Executing tests ...")

        self.before_all_tests()
        test_prefix = 'test_'
        if annotated_tests:
            test_prefix = 'x_test_'
        test_functions = [func for func in dir(t) if callable(getattr(t, func)) and func.startswith(test_prefix)]
        try:
            test_number = 1
            for func in test_functions:
                self.before_single_test()
                test_function = getattr(self, func)
                test = test_function()
                test_number += 1
                test.test_name = func
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
t.run_tests(annotated_tests=False)
t.print_test_results()
