import re

class NGramParser:
    def __init__(self, commit_storage):
        self.commit_storage = commit_storage
        self.compiled_separator = re.compile(r"[.,?'\"!-]?\s+")
        
    def parse_ngrams(self, n):
        all_ngrams = {}
        for commit in self.commit_storage.get_commits():
            for ngram in self._parse_ngrams_of_message(commit.message):
                if ngram in all_ngrams:
                    all_ngrams[ngram] += 1
                else:
                    all_ngrams[ngram] = 1
        ngram_occurence_tuples = [(ngram, occurences) for ngram, occurences in all_ngrams.iteritems()]
        return self.sort_ngram_tuples(ngram_occurence_tuples)

    def _parse_ngrams_of_message(self, message, n):
        words = self.compiled_separator.split(message)
        ngrams = []
        for i in xrange(len(words)-n+1):
            new_ngram = words[i:(i+n)]
            ngrams.append(new_ngram)
        return ngrams

    def sort_ngram_tuples(self, ngram_tuples):
        return sorted(ngram_tuples, key = lambda k : k[1], reverse=True)


