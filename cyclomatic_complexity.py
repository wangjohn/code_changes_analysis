import re

class CyclomaticComplexity:
    def __init__(self, code):
        self.total_lines = len(re.findall(r"$", code, re.MULTILINE))
        self.code = self.remove_comments(code)
        self.total_uncommented_lines = len(re.findall(r"$", self.code, re.MULTILINE))
        self.commented_lines = self.total_lines - self.total_uncommented_lines
        self.complexity = None

    def compute_complexity(self):
        if self.complexity != None:
            return self.complexity
        self.complexity = self.get_complexity(self.code)
        return self.complexity

    def remove_comments(self, code):
        next_code = re.sub(r"=begin\s*($|\n)(.*?)=end\s*($|\n)", '', code, flags=re.DOTALL)
        return re.sub(r"#(.*?)$(?<=[^\"])", '', next_code, flags=re.MULTILINE)

    def get_complexity(self, line):
        complexity = 0
        # the if and unless statements
        complexity += len(re.findall(r"(^|\s|\()(if|unless)\s", line, flags=re.MULTILINE))
        complexity += len(re.findall(r"(.*?)\s+\?\s+(.*?)\s+:\s+(.*?).", line)) # takes care of ruby ternary operators

        # all other control/syntax statements
        complexity += len(re.findall(r"^\s*(else|elsif|when|for|while|break|continue|until|return)", line, flags=re.MULTILINE))
        complexity += len(re.findall(r"\.each(\{|\(|\s+do)", line, flags=re.MULTILINE))
        complexity += len(re.findall(r"(&&|\|\||==|\sand\s|\sor\s|(\W|\s|^)!|\snot\s)", line))
        return complexity 
