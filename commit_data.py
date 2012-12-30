import os
import re
from dateutil import parser
import cyclomatic_complexity

class Commit:
    def __init__(self, commit_id, controller, datetime, num_files_changed, num_insertions, num_deletions, author_name):
        self.datetime = datetime
        self.commit_id = commit_id
        self.controller = controller
        self.commit_quality_obj = None
        self.commit_author = author_name

        self.num_files_changed = num_files_changed
        self.num_insertions = num_insertions
        self.num_deletions = num_deletions

    def set_commit_quality_obj(self, commit_quality_obj):
        self.commit_quality_obj = commit_quality_obj

    def get_quality(self):
        if self.commit_quality_obj != None:
            return self.commit_quality_obj.get_quality()
        return None

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

class GitCommitScraper:
    def __init__(self, directory_path, follow_path, controller):
        self.directory_path = directory_path
        self.follow_path = follow_path
        self.controller = controller

    def get_commit_shortstats(self, result):
        amount_changed_match = re.search("([0-9]+) files? changed(, ([0-9]+) insertions?\\(\\+\\))?(, ([0-9]+) deletions?\\(\\-\\))?", result)
        files_changed = self._get_matched_group(amount_changed_match, 1)
        insertions = self._get_matched_group(amount_changed_match, 3)
        deletions = self._get_matched_group(amount_changed_match, 5)
        return (files_changed, insertions, deletions)

    def _get_matched_group(self, match, group_number):
        result = match.group(group_number)
        if result == None:
            return 0
        return int(result)
    
    def get_controller_commits(self, before, after, include_quality=True):
        git_command = "cd {0}; git log --format='end_commit%H%n%ad%n%cn' --shortstat --before='{1}' --after='{2}' --follow {3}".format(self.directory_path, before, after, self.follow_path) 
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
            files_changed, insertions, deletions = self.get_commit_shortstats(commit_info_lines[4])
            time = parser.parse(commit_info_lines[1]).replace(tzinfo=None)
            author_name = commit_info_lines[2]

            # create a commit object and append it to the list of commits
            commit = Commit(commit_id, self.controller, time, files_changed, insertions, deletions, author_name)
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


