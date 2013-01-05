import os
import re
from dateutil import parser
import cyclomatic_complexity
import ngram_parser

class Commit:

    attributes_and_default_values = [
        ("datetime", None),
        ("commit_id", None),
        ("controller", None),
        ("commit_quality_obj", None),
        ("commit_author", None),
        ("message", None),
        ("num_files_changed", None),
        ("num_insertions", None),
        ("num_deletions", None),
        ("category", None),
        ("controller_change", False),
        ("view_change", False)
    ]
    initial_required_attributes = [
        "commit_id",
        "controller",
        "datetime",
        "num_files_changed",
        "num_insertions",
        "num_deletions",
        "author_name",
        "message"
    ]

    def __init__(self, initial_variables_hash, error_checking=True):
        self._initialize_attributes(error_checking)

    def _initialize_attributes(self, initial_variables_hash, error_checking=True):
        for attribute, default_val in Commit.attributes_and_default_values:
            self.set_attribute(attribute, default_val)
        # do some error checking
        if error_checking:
            self._error_checking_of_initial_attributes(initial_variables_hash)
        for required_attribute in Commit.initial_required_attributes:
            self.set_attribute(required_attribute, initial_variables_hash[required_attribute])

    def _error_checking_of_initial_attributes(self, initial_variables_hash):
        if len(Commit.initial_required_attributes) != len(initial_variables_hash):
            raise Exception("Passed an initial variables hash which does not match the initial required attributes for a Commit initialization.")
        for required_attribute in Commit.initial_required_attributes:
            if required_attribute not in initial_variables_hash:
                raise Exception("Initial variables hash for a Commit initialization does not contain the required attribute: " + required_attribute)

    def set_commit_quality_obj(self, commit_quality_obj):
        self.commit_quality_obj = commit_quality_obj

    def set_attribute(self, attribute, value):
        setattr(self, attribute, value)

    def get_quality(self):
        if self.commit_quality_obj != None:
            return self.commit_quality_obj.get_quality()
        return None

class CommitMerger:
    def __init__(self, settings_obj):
        self.settings_obj = settings_obj

    def merge_commits(commit_list):
        # copy the initial attributes from the first commit
        first_commit = commit_list[0]
        new_commit_attributes = {}
        for attribute in Commit.initial_required_attributes:
            new_commit_attributes[attribute] = getattr(first_commit, attribute)

        # overwrite select attributes

        new_commit = Commit(new_commit_attributes)

    def _get_shortstats(self, commit_id):
        git_shortstats_command = "cd {0}; git show {1} --oneline --shortstat"
        result = os.popen(git_shortstats_command).read()
        shortstat_line = result.split("\n")[0]
        shortstat_results = CommitShortStats.get_commit_shortstats(shortstat_line)
        return shortstat_results

    def _get_quality_obj(self, commit_id):
        git_diff_command = "cd {0}; git show {1}"
        diff = os.popen(git_diff_command).read()
        complexity_obj = cyclomatic_complexity.CommitCodeQuality(diff)
        return complexity_obj

class CommitStorage:
    def __init__(self, list_of_commits):
        self.list_of_commits = list_of_commits
        self.percentiles = {}

    def get_data_percentiles(self):
        self._get_percentiles("num_insertions")
        self._get_percentiles("num_deletions")
        self._get_percentiles("num_files_changed")

    def get_commits(self):
        return self.list_of_commits

    def categorize_commits(self):
        parser = ngram_parser.NGramParser(self.list_of_commits)
        parser.categorize_messages()

    def get_commit_percentile(self, percentile_header, commit):
        return self.percentiles[percentile_header][commit.commit_id]

    def _get_percentiles(self, attribute):
        percentile_hash = {}
        sorted_commits = sorted(self.list_of_commits, key = lambda k : getattr(k, attribute))
        total_num = len(sorted_commits)
        last_value = None
        length_same_seq = 0
        for i in xrange(total_num):
            next_commit = sorted_commits[i]
            next_value = getattr(next_commit, attribute)
            if next_value != None and last_value == next_value and i < total_num-1:
                length_same_seq += 1
            else:
                if length_same_seq > 0:
                    self._set_mid_percentage(i, length_same_seq, total_num, percentile_hash, sorted_commits)
                length_same_seq = 0
                if i < total_num-1 or last_value != next_value:
                    percentile_hash[next_commit.commit_id] = float(i)/total_num 

            last_value = next_value
        self.percentiles[attribute] = percentile_hash

    def _set_mid_percentage(self, i, length_same_seq, total_num, percentile_hash, sorted_commits):
        mid_percentage = float(i-1-length_same_seq/2)/total_num
        for j in xrange(i-length_same_seq-1,i,1):
            current_commit = sorted_commits[j]
            percentile_hash[current_commit.commit_id] = mid_percentage
        if i >= total_num-1:
            percentile_hash[sorted_commits[i].commit_id] = mid_percentage

class CommitShortStats:
    @staticmethod
    def get_commit_shortstats(result):
        amount_changed_match = re.search("([0-9]+) files? changed(, ([0-9]+) insertions?\\(\\+\\))?(, ([0-9]+) deletions?\\(\\-\\))?", result)
        files_changed = CommitShortStats._get_matched_group(amount_changed_match, 1)
        insertions = CommitShortStats._get_matched_group(amount_changed_match, 3)
        deletions = CommitShortStats._get_matched_group(amount_changed_match, 5)
        return (files_changed, insertions, deletions)

    @staticmethod
    def _get_matched_group(match, group_number):
        result = match.group(group_number)
        if result == None:
            return 0
        return int(result)

class GitCommitScraper:
    def __init__(self, directory_path, follow_path, controller):
        self.directory_path = directory_path
        self.follow_path = follow_path
        self.controller = controller

    def _merge_commits(self, commits_multilist):
        commit_dict = {}
        for commit_list in commits_multilist:
            for commit in commit_list:
                current_list = commit_dict.setdefault(commit.commit_id, [])
                current_list.append(commit)
        merged_commits = []
        for commit_id, commit_list in commit_dict.iteritems():
            if len(commit_list) >= 2:
                resulting_merge = CommitMerger.merge_commits(commit_list)
                merged_commits.append(resulting_merge)
            else:
                merged_commits.append(commit_list[0])
        return merged_commits

    def get_view_commits(self, before, after, include_quality=True):
        follow_path = "/views/" + self.controller
        commits = self.get_commits_from_followpath(before, after, follow_path, include_quality)
        for commit in commits:
            commit.set_attribute("view_change", True)
        return commits

    def get_controller_commits(self, before, after, include_quality=True):
        follow_path = "/controllers/" + self.controller + "_controller.rb"
        commits = self.get_commits_from_followpath(before, after, follow_path, include_quality)
        for commit in commits:
            commit.set_attribute("controller_change", True)
        return commits

    def get_commits_from_followpath(self, before, after, follow_path, include_quality=True):
        git_command = "cd {0}; git log --format='end_commit%H%n%ad%n%cn%n%s' --shortstat --before='{1}' --after='{2}' --follow {3}".format(self.directory_path, before, after, follow_path) 
        result = os.popen(git_command).read()
        split_result = result.split("end_commit")
        first = True 

        all_commits = []
        for commit_info in split_result:
            if first:
                first = False
                continue
            commit_info_lines = commit_info.split("\n")
            commit_id = commit_info_lines[0]
            files_changed, insertions, deletions = CommitShortStats.get_commit_shortstats(commit_info_lines[5])
            time = parser.parse(commit_info_lines[1]).replace(tzinfo=None)
            author_name = commit_info_lines[2]
            message = commit_info_lines[3]

            # create a commit object and append it to the list of commits
            initial_values_hash = {
                "commit_id": commit_id, 
                "controller": self.controller, 
                "datetime": time,
                "num_files_changed": files_changed,
                "num_insertions": insertions,
                "num_deletions": deletions,
                "author_name": author_name,
                "message": message,
            }

            commit = Commit(initial_values_hash)
            all_commits.append(commit)

        if include_quality:
            self.get_quality_of_commits(all_commits)
        print "    Found " + str(len(all_commits)) + " commits."
        return all_commits

    def get_quality_of_commits(self, commits):
        last_commit = None
        for next_commit in commits:
            if last_commit != None:
                diff = self._get_diff_with_commit_ids(last_commit.commit_id, next_commit.commit_id)
                quality_obj = cyclomatic_complexity.CommitCodeQuality(diff)
                next_commit.set_commit_quality_obj(quality_obj)
            last_commit = next_commit

    def _get_diff_with_commit_ids(self, commit_id1, commit_id2):
        git_command = "cd %s; git diff %s %s" % (self.directory_path, commit_id1, commit_id2)
        return os.popen(git_command).read()


