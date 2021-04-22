class Test:

    def __init__(self, tc_desc: str, expected_result: str, actual_result: str = None, test_number: int = None,
                 test_name: str = None):
        self.test_number = test_number
        self.test_name = test_name
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
