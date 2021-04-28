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


class Test:

    def __init__(self, tc_desc: str, expected_result: str = None, actual_result: str = None, test_number: int = None,
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
