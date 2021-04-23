import numpy as np
from src.rdf_data_citation.rdf_star import TripleStoreEngine
from datetime import timezone, timedelta, datetime
from tests.test_base import Test
import pandas as pd
import configparser
from tabulate import tabulate
from src.rdf_data_citation.citation_utils import QueryData


class TestExecution:

    def __init__(self, annotated_tests: bool = False):
        """Load configuration from .ini file."""
        self.test_config = configparser.ConfigParser()
        self.test_config.read('../../config.ini')
        self.annotated_tests = annotated_tests
        self.tests = []

        # Custom members for test execution
        self.rdf_engine = None
        self.query_alt1 = None
        self.query_alt2 = None

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

    def before_single_test(self, test_name: str):
        """

        :return:
        """

        print("Executing before_single_tests ...")

        if self.annotated_tests:
            test_name = test_name[2:]

        self.query_alt1 = open("test_data/{0}_alt1.txt".format(test_name), "r").read()
        self.query_alt2 = open("test_data/{0}_alt2.txt".format(test_name), "r").read()

    def x_test_normalization__optional_where_clause(self):
        q_utils_alt1 = QueryData(query=self.query_alt1)
        test = Test(test_number=1,
                    tc_desc='Tests if leaving out the "where" keyword yields the same checksum.',
                    expected_result=q_utils_alt1.checksum)
        q_utils_alt2 = QueryData(query=self.query_alt2)
        test.actual_result = q_utils_alt2.checksum

        return test

    def x_test_normalization__rdf_type_predicate(self):
        q_utils_alt1 = QueryData(query=self.query_alt1)
        test = Test(test_number=2,
                    tc_desc='Tests if replacing the predicate rdf:type by "a" yields the same checksum.',
                    expected_result=q_utils_alt1.checksum)
        q_utils_alt2 = QueryData(query=self.query_alt2)
        test.actual_result = q_utils_alt2.checksum

        return test

    def test_normalization__asterisk(self):
        test = Test(test_number=3,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def test_normalization__leave_out_subject_in_triple_statements(self):
        test = Test(test_number=4,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def test_normalization__order_of_triple_statements(self):
        test = Test(test_number=5,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def test_normalization__alias_via_bind(self):
        test = Test(test_number=6,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def test_normalization__variable_names(self):
        test = Test(test_number=7,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def test_normalization__variables_not_bound(self):
        test = Test(test_number=8,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def test_normalization__circumflex_invert(self):
        test = Test(test_number=9,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def test_normalization__sequence_paths(self):
        test = Test(test_number=10,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def test_normalization__prefix_alias(self):
        test = Test(test_number=11,
                    tc_desc="",
                    expected_result="")

        test.actual_result = ""
        return test

    def run_tests(self):
        """
        :param annotated_tests: If this flag is set only tests with the prefix "x_test" will be executed.
        :return:
        """
        print("Executing tests ...")

        self.before_all_tests()
        test_prefix = 'test_'
        if self.annotated_tests:
            test_prefix = 'x_test_'
        test_functions = [func for func in dir(t) if callable(getattr(t, func)) and func.startswith(test_prefix)]
        try:
            test_number = 1
            for func in test_functions:
                self.before_single_test(func)
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
        """

        :return:
        """
        print("Executing after_single_test")

    def after_all_tests(self):
        """

        :return:
        """

        print("Executing after_tests ...")


t = TestExecution(annotated_tests=True)
t.run_tests()
t.print_test_results()
