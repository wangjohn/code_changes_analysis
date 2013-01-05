import re
import settings
import run_analysis
from commit_data import *


class MessageCategorizer:
    def __init__(self):
        self.category_regex_list = [
            ("bugfix", re.compile("(\s|^)(fix(ed|es|ing)?|bug(s)?)(\s|$)")),
            ("reintegrate", re.compile("(\s|^)(branch|sync|reintegrat(ed|e|ing)?|merge(d)?|trunk)(\s|$)")),
            ("cleanup", re.compile("(\s|^)(clean(ed|up)?)(\s|$)")),
            ("addition", re.compile("(\s|^)(add(ed|ing)?|creat(ed|e|ing)?)(\s|$)")),
            ("removal", re.compile("(\s|^)(remov(ed|ing|al|es|e)?|delet(ed|e|ing|es)?)(\s|$)")),
        ]

    def get_category_results(self, message):
        return [len(regex.findall(message)) for cat, regex in self.category_regex_list]

    def categorize_message(self, message):
        results = self.get_category_results(message) 
        index, max_val = max(enumerate(results), key=lambda k : k[1])
        if max_val == 0:
            return None
        return self.category_regex_list[max_val][0]

class NGramParser:
    def __init__(self, commits):
        self.commits = commits
        self.compiled_separator = re.compile(r"\s+")
        self.cleaning_regex = re.compile(r"(\s[.,?'\"!-\)\(]*|[.,?'\"!-\)\(]*(\s|$))")
        self.meaningless_words = re.compile(r"(\s|^)(the|to|in|for|and|a|of|that|when|we|i|is|if|are|on|as|no|be|it)(\s|$)")
        self.message_categorizer = MessageCategorizer()

    def parse_ngrams(self, n):
        all_ngrams = {}
        for commit in self.commits:

            for ngram in self._parse_ngrams_of_message(commit.message, n):
                if ngram in all_ngrams:
                    all_ngrams[ngram] += 1
                else:
                    all_ngrams[ngram] = 1
        ngram_occurence_tuples = [(ngram, occurences) for ngram, occurences in all_ngrams.iteritems()]
        return self.sort_ngram_tuples(ngram_occurence_tuples)

    def categorize_message(self, message, default_group=None):
        return self.message_categorizer.categorize_message(message)

    def categorize_messages(self):
        categories = {}
        for commit in self.commits:
            message = self._clean_message(commit.message)
            category = self.categorize_message(message)
            commit.category = category

    def _parse_ngrams_of_message(self, message, n):
        message = self._clean_message(message)
        words = self.compiled_separator.split(message)
        ngrams = []
        for i in xrange(len(words)-n+1):
            new_ngram = words[i:(i+n)]
            ngrams.append(" ".join(new_ngram))
        return ngrams

    def _clean_message(self, message):
        new_message = self.cleaning_regex.sub(" ", message.lower())
        sans_meaningless_words = self.meaningless_words.sub(" ", new_message)
        return sans_meaningless_words

    def sort_ngram_tuples(self, ngram_tuples):
        return sorted(ngram_tuples, key = lambda k : k[1], reverse=True)

    def print_parsed_ngrams(self, n, num_to_show=50):
        ngrams = self.parse_ngrams(n)
        for ngram in ngrams[:num_to_show]:
            print ngram[0], ": ", ngram[1]


def iterate_through_controllers(settings_obj, eval_statement):
    for controller in settings_obj.get("git_scraper_controllers"):
        git_commit_scraper = GitCommitScraper(settings_obj.get("git_scraper_directory_path"), run_analysis.get_follow_path_from_controller(controller), controller)
        commits = git_commit_scraper.get_controller_commits(settings_obj.get("global_end"), settings_obj.get("global_start"))
        parser = NGramParser(commits)

        print "Using controller: " + controller
        eval(eval_statement)

def print_ngrams(settings_obj, n, num_to_show=50):
    eval_statement = "parser.print_parsed_ngrams(%s, %s)" % (n, num_to_show)
    iterate_through_controllers(settings_obj, eval_statement) 

def categorize_commits(settings_obj):
    eval_statement = "parser.categorize_messages()"
    iterate_through_controllers(settings_obj, eval_statement)
        
if __name__ == '__main__':
    settings_obj = settings.Settings(production_env=False)
    categorize_commits(settings_obj)
