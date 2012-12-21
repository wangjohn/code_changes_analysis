import re

class CyclomaticComplexity:
    """Note that you must initialize this class such that code_lines
    does not include any comments."""
    def __init__(self, code):
        self.code = code

    def compute_complexity(self):
        total_complexity = self.get_complexity(self.code)

    def remove_comments(self, code):
        next_code = re.sub(r"=begin\s*($|\n)(.*?)=end\s*($|\n)", '', code, flags=re.DOTALL)
        return re.sub(r"#(.*?)$(?<=[^\"])", '', next_code, flags=re.MULTILINE)

    def get_complexity(self, line):
        num_selections = 0
        # the if and unless statements
        num_selections += len(re.findall(r"(^|\s|\()(if|unless)\s", line))
        num_selections += len(re.findall(r"(.*?)\s+\?\s+(.*?)\s+:\s+(.*?).", line)) # takes care of ruby ternary operators

        # all other control/syntax statements
        num_selections += len(re.findall(r"^\s*(else|elsif|when|for|while|break|continue|until|return)", line))
        num_selections += len(re.findall(r"\.each(\{|\()", line))
        return num_selections 

if __name__ == '__main__':
    f = open("/home/john/test_controller.rb", 'r')
    c = CyclomaticComplexity(f.read())
    print c.remove_comments(c.code)
