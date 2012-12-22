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

    def get_percentiles(self, attribute):
        percentile_hash = {}
        sorted_commits = sorted(self.list_of_commits, key = lambda k : getattr(k, attribute))
        total_num = len(sorted_commits)
        last_value = None
        length_same_seq = 0
        for i in xrange(total_num):
            percentage = float(i)/total_num
            next_commit = self.list_of_commits[i]
            next_value = getattr(next_commit, attribute)
            if last_value == next_value:
                length_same_seq += 1
            else:
                if length_same_seq > 0:
                    self.set_mid_percentage(i, length_same_seq, total_num, percentile_hash)
                length_same_seq = 0
                percentile_hash[next_commit] = percentage

            last_value = next_value
        self.percentiles[attribute] = percentile_hash
        return self.percentiles[attribute]

    def set_mid_percentage(self, i, length_same_seq, total_num, percentile_hash):
        mid_percentage = float(i - length_same_seq/2)/total_num
        for j in xrange(i-length_same_seq,i,1):
            current_commit = self.list_of_commits[j]
            percentile_hash[current_commit] = mid_percentage

def get_all_commits(begin_date, end_date):
    git_command = "cd ~/panjiva_web_branches/web; git rev-list --after='%s' --before='%s' --all" % (begin_date, end_date)    
    result = os.popen(git_command).read()
    return result.split("\n")

def get_commit_shortstats(result):
    amount_changed_match = re.search("([0-9]+) files? changed, ([0-9]+) insertions?\\(\\+\\), ([0-9]+) deletions?\\(\\-\\)", result)
    if amount_changed_match:
        files_changed = amount_changed_match.group(1)
        insertions = amount_changed_match.group(2)
        deletions = amount_changed_match.group(3)
    else:
        match2 = re.search("([0-9]+) files? changed, ([0-9]+) insertions?\\(\\+\\)", result)
        if match2:
            files_changed = match2.group(1)
            insertions = match2.group(2)
            deletions = 0
        else:
            match3 = re.search("([0-9]+) files? changed, ([0-9]+) deletions?\\(\\-\\)", result)
            if match3:
                files_changed = match3.group(1)
                insertions = 0
                deletions = match3.group(2)
            else:
                files_changed = 0
                insertions = 0
                deletions = 0

    return (int(files_changed), int(insertions), int(deletions))


def get_controller_commits(controller_name, before, after):
    git_command = "cd ~/panjiva_web_branches/web; git log --format=' %ad' --shortstat --before='" + before + "' --after='" + after + "' --follow app/controllers/" + controller_name  
    result = os.popen(git_command).read()
    split_result = result.split("\n")

    all_commits = []
    for i in xrange(len(split_result)/3):
        time = parser.parse(split_result[i*3])
        files_changed, insertions, deletions = get_commit_shortstats(split_result[i*3+2])
        commit = FileCommit(controller_name, time, files_changed, insertions, deletions)
        all_commits.append(commit)

    # go through the commits and figure out the percentiles
    # for fileschanged, lineschanged, insertions, deletions
    all_commits = sorted(all_commits, key = lambda k: k.files_changed)
    for i in xrange(len(all_commits)):
        all_commits[i].fileschangedpercentile = float(i)/len(all_commits)
    all_commits = sorted(all_commits, key = lambda k: k.insertions + k.deletions)
    for i in xrange(len(all_commits)):
        all_commits[i].lineschangedpercentile = float(i)/len(all_commits)
    all_commits = sorted(all_commits, key = lambda k: k.insertions)
    for i in xrange(len(all_commits)):
        all_commits[i].insertionspercentile = float(i)/len(all_commits)
    all_commits = sorted(all_commits, key = lambda k: k.deletions)
    for i in xrange(len(all_commits)):
        all_commits[i].deletionspercentile = float(i)/len(all_commits)
    return all_commits

def write_out_data(commits, find_user_set, controller, k, output_object):
    both_ba_list = []
    for commit in commits:
        both_ba = find_user_set.find_users(find_user_set.activity_logs, controller, commit.datetime, k)
        both_ba_list.append(both_ba)
    find_user_set.get_user_account_views(find_user_set.activity_logs, find_user_set.touched_logs)
    for i in xrange(len(both_ba_list)):
        both_ba = both_ba_list[i]
        commit = commits[i]
        find_user_set.format_both_ba_into_rows_meancentered(both_ba, output_object, commit)

def writerows(filename, rows):
    with open(filename, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(rows)


if __name__ == '__main__':
    # controller = 'my_panjiva'
    controllers = ['my_panjiva', 'us_imports', 'us_exports', 'customs', 'communication', 'profile', 'project', 'info', 'search']
    commits = {}
    for controller in controllers:
        commits[controller] = get_controller_commits(controller + "_controller.rb", "11/25/2012", "7/14/2011")

    all_logs = read_in_data("data/activity_log_out.csv")
    print 'all_loaded'
    fus = FindUserSets(all_logs)
    headers = ['id', 'user_account_id', 'controller', 'action', 'model_id', 'created_at', 'ip_address', 'next_profile_activity_log_id', 'session_id', 'impersonated', 'time_from_event', 'num_controller_views', 'after_commit', 'num_views_day_later', 'commit_date', 'fileschanged', 'insertions', 'deletions', 'fileschangedpercentile', 'lineschangedpercentile', 'insertionspercentile', 'deletionspercentile']
    output_object = OutputRowResult(headers)
    for controller in controllers:
        current_commits = commits[controller]
        write_out_data(current_commits, fus, controller, 3, output_object)
    writerows("all_controller_data.csv", output_object.get_output()):w
    

