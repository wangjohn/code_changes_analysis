import re

class CommitCodeQuality:
    def __init__(self, commit_diff, complexity_coef=1.0, comment_coef=0.25):
        self.commit_diff = commit_diff
        self.added_code = None
        self.removed_code = None
        self.complexity_coef = complexity_coef
        self.comment_coef = comment_coef
        self.quality = None
        self._get_added_and_removed_code(commit_diff)

    def _get_added_and_removed_code(self, diff)
        self.added_code = '\n'.join(re.findall(r"^+[^++](.*?)$", diff, flags=re.MULTILINE))
        self.removed_code = '\n'.join(re.findall(r"^-[^--](.*?)$", diff, flags=re.MULTILINE))

    def get_quality(self):
        if self.quality == None
            added_code_comp_obj = CyclomaticComplexity(self.added_code)
            removed_code_comp_obj = CyclomaticCompleixty(self.removed_code)

            # get the average complexity per line of the lines that have 
            # been added, as well as those which have been removed
            added_avg_complexity = self._compute_avg(added_code_comp_obj.compute_complexity(), added_code_comp_obj.total_uncommented_lines)
            removed_avg_complexity = self._compute_avg(removed_code_comp_obj.compute_complexity(), removed_code_comp_obj.total_uncommented_lines)

            # get the percentage of comments per line for added and removed
            # lines of code
            comments_percentage_added = self._compute_avg(added_code_comp_obj.commented_lines, added_code_comp_obj.total_lines)
            comments_percentage_removed = self._compute_avg(removed_code_comp_obj.commented_lines, removed_code_comp_obj.total_lines)

            self.quality = self.quality_equation(added_avg_complexity, removed_avg_complexity, comments_percentage_added, comments_percentage_removed)
        return self.quality

    def _compute_avg(self, sample, total):
        if total == 0:
            return 0
        return float(sample)/total
    
    def quality_equation(added_avg_complexity, removed_avg_complexity, comments_percentage_added, comments_percentage_removed):
        return self.complexity_coef*(added_avg_complexity-removed_avg_complexity) + self.comment_coef*(comments_percentage_added-comments_percentage_removed)


class CyclomaticComplexity:
    def __init__(self, code):
        self.total_lines = len(re.findall(r"$", code, re.MULTILINE))
        self.code = self.remove_comments(code)
        self.total_uncommented_lines = len(re.findall(r"$", self.code, re.MULTILINE))
        self.commented_lines = self.total_lines - self.total_uncommented_lines
        self.complexity = None

    def compute_complexity(self):
        if self.complexity == None:
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
