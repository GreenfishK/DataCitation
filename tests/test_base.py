def get_test_config() -> dict:
    test_configs_file = open("../test.config", "r").readlines()
    test_configs = {}
    for line in test_configs_file:
        key_value = line.strip().split('::')
        test_configs[key_value[0]] = key_value[1]
    return test_configs


class Test:

    def __init__(self, tc_desc: str, expected_result: str, actual_result: str = None, test_number: int = None):
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
