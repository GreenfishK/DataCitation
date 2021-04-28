import numpy as np
from src.rdf_data_citation.rdf_star import TripleStoreEngine
from datetime import timezone, timedelta, datetime
from tests.test_base import Test, format_text
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
        self.query_data_alt1 = None
        self.query_data_alt2 = None

    def print_test_results(self):
        """

        :return:
        """

        tests_df = pd.DataFrame(columns=['test_number', 'test_passed', 'test_name', 'test_case_description',
                                         'expected_result', 'actual_result'])
        for test in self.tests:
            if isinstance(test, Test):
                formatted_tc_desc = format_text(test.tc_desc, 100)
                formatted_expected_result = format_text(test.expected_result, 50)
                formatted_actual_result = format_text(test.actual_result, 50)

                tests_df = tests_df.append({'test_number': test.test_number,
                                            'test_passed': test.yn_passed,
                                            'test_name': test.test_name,
                                            'test_case_description': formatted_tc_desc,
                                            'expected_result': formatted_expected_result,
                                            'actual_result': formatted_actual_result}, ignore_index=True)

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

        query_alt1 = open("test_data/{0}_alt1.txt".format(test_name), "r").read()
        self.query_data_alt1 = QueryData(query=query_alt1)
        query_alt2 = open("test_data/{0}_alt2.txt".format(test_name), "r").read()
        self.query_data_alt2 = QueryData(query=query_alt2)

    def test_normalization__optional_where_clause(self):
        test = Test(test_number=1,
                    tc_desc='Tests if leaving out the "where" keyword yields the same checksum.',
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__rdf_type_predicate(self):
        test = Test(test_number=2,
                    tc_desc='Tests if replacing the predicate rdf:type by "a" yields the same checksum.',
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__asterisk(self):
        # Order of variables is not important
        test = Test(test_number=3,
                    tc_desc="Tests if replacing the variable names in the select clause with an asterisk yields the "
                            "same checksum. If the variable order is important this test will usually not pass. In "
                            "this case, it would only pass if the order of variables after the asterisk gets resolved"
                            "is the same as the user would place them. ",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__leave_out_subject_in_triple_statements(self):
        test = Test(test_number=4,
                    tc_desc="If the same subject is used multiple times in subsequent triple statements (separated by "
                            "a dot) it can be left out in all the subsequent triple statements where the subject "
                            "occurs. Instead of the subject variable name a semicolon is written in subsequent triple "
                            "statements where the same subject as in the first statement should be used.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__order_of_triple_statements(self):
        test = Test(test_number=5,
                    tc_desc="Tests if differently permuted tripled statements yield the same checksum.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__alias_via_bind(self):
        test = Test(test_number=6,
                    tc_desc="Test if binding an alias to a variable using the BIND keyword yields the same checksum "
                            "as when not using any alias.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__alias_in_select(self):
        test = Test(test_number=7,
                    tc_desc="Test if binding an alias to a variable using the BIND keyword yields the same checksum "
                            "as when not using any alias.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__variable_names(self):
        test = Test(test_number=8,
                    tc_desc="Test if two queries where as one has one variable renamed within the whole query"
                            " (select statement, triple statements, filter, ...) yields the same checksum.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__variables_not_bound(self):
        test = Test(test_number=9,
                    tc_desc='Finding variables that are not bound can be written in two ways: '
                            '1. with optional keyword adding the optional triplet combined with filter condition: '
                            'OPTIONAL { ?x dc:date ?date }'
                            'filter (!bound(?date)) '
                            '2. with "filter not exists {triple}"',
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__circumflex_invert(self):
        test = Test(test_number=10,
                    tc_desc='Test if inverting the order of the triple statement (object predicate subject '
                            'instead of subject predicate object) using "^" yields the same result as if actually '
                            'exchanging the subject and object within the triple statement. ',
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__sequence_paths(self):
        test = Test(test_number=11,
                    tc_desc="Sequence paths can reduce the number of triples in the query "
                            "and are commonly used.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

        return test

    def test_normalization__prefix_alias(self):
        test = Test(test_number=12,
                    tc_desc="Prefixes can be interchanged in the prefix section before the query "
                            "and subsequently in the query without changing the outcome.",
                    expected_result=self.query_data_alt1.checksum,
                    actual_result=self.query_data_alt2.checksum)

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


t = TestExecution(annotated_tests=False)
t.run_tests()
t.print_test_results()
