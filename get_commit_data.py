import os
import re
from dateutil import parser
from clean_activity_logs import *

class Commit:
    def __init__(self, commit_id, filenames, datetime, num_files_changed, num_insertions, num_deletions):
        self.datetime = datetime
        self.commit_id = commit_id
        self.filenames = filenames
        
        self.num_files_changed = num_files_changed
        self.num_insertions = num_insertions
        self.num_deletions = num_deletions

class CommitStorage:
    def __init__(self, list_of_commits):
        self.list_of_commits = list_of_commits
        self.percentiles = {}

    def get_data_percentiles(self):
        self.get_percentiles("num_insertions")
        self.get_percentiles("num_deletions")
        self.get_percentiles("num_files_changed")

    def get_percentiles(self, attribute):
        percentile_hash = {}
        sorted_commits = sorted(self.list_of_commits, key = lambda k : getattr(k, attribute))
        total_num = len(sorted_commits)
        last_value = None
        length_same_seq = 0
        for i in xrange(total_num):
            next_commit = sorted_commits[i]
            next_value = getattr(next_commit, attribute)
            if last_value == next_value:
                length_same_seq += 1
            else:
                if length_same_seq > 0:
                    self.set_mid_percentage(i, length_same_seq, total_num, percentile_hash, sorted_commits)
                length_same_seq = 0
                percentile_hash[next_commit] = float(i)/total_num 

            last_value = next_value
        self.percentiles[attribute] = percentile_hash
        return self.percentiles[attribute]

    def set_mid_percentage(self, i, length_same_seq, total_num, percentile_hash, sorted_commits):
        mid_percentage = float(i-1-length_same_seq/2)/total_num
        for j in xrange(i-length_same_seq-1,i,1):
            current_commit = sorted_commits[j]
            percentile_hash[current_commit] = mid_percentage

class GitCommitScraper:
    def __init__(self, directory_path, follow_path, filenames):
        self.directory_path = directory_path
        self.follow_path = follow_path
        self.filenames = filenames

    def get_commit_shortstats(self, result):
        amount_changed_match = re.search("([0-9]+) files? changed(, ([0-9]+) insertions?\\(\\+\\))?(, ([0-9]+) deletions?\\(\\-\\))?", result)
        files_changed = self._get_matched_group(amount_changed_match, 1)
        insertions = self._get_matched_group(amount_changed_match, 3)
        deletions = self._get_matched_group(amount_changed_match, 5)

    def _get_matched_group(self, match, group_number):
        result = match.group(group_number)
        if result == None:
            return 0
        return int(result)
    
    def get_controller_commits(self, before, after):
        git_command = "cd " + self.directorypath + "; git log --format='%H%n%ad%nend_commit' --shortstat --before='" + before + "' --after='" + after + "' --follow " + self.followpath  
        result = os.popen(git_command).read()
        split_result = result.split("end_commit")

        all_commits = []
        for commit_info in split_result:
            commit_info_lines = commit_info.split("\n")
            commit_id = commit_info_lines[1]
            files_changed, insertions, deletions = get_commit_shortstats(commit_info_lines[0])
            time = parser.parse(commit_info_lines[2]).replace(tzinfo=None)

            # create a commit object and append it to the list of commits
            commit = Commit(commit_id, self.filenames, time, files_changed, insertions, deletions)
            all_commits.append(commit)

        return all_commits

