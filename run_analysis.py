import settings
import read_csv_data
import write_csv_data
import parse_date
import datetime
from activity_log_storage import *
from add_attributes import *
from find_user_sets import *
from commit_data import *


def run_data(settings_obj):
    global_start = datetime.datetime.strptime(settings_obj.get("global_start"), settings_obj.get("str_date_format"))
    global_end = datetime.datetime.strptime(settings_obj.get("global_end"), settings_obj.get("str_date_format"))
    
    # start and jump by increments according to the settings obj
    current_end = global_start
    old_activity_logs = None
    while current_end < global_end:
        current_start = current_end
        current_end = current_start + datetime.timedelta(days=settings_obj.get("days_per_segment_interval"))
        current_start_date_str = convert_to_date(current_start)
        current_end_date_str = convert_to_date(current_end)
        print "Using current time window: {0} to {1}".format(current_start_date_str, current_end_date_str)
        old_activity_logs = run_data_subset(settings_obj, current_end_date_str, current_start_date_str, old_activity_logs)

def convert_to_date(datetime_obj):
    return datetime_obj.strftime("%m/%d/%Y")

def run_data_subset(settings_obj, current_end_date_str, current_start_date_str, old_activity_logs=None):
    # get the commits for each controller
    for controller in settings_obj.get("git_scraper_controllers"):
        print "Begin working on controller: " + controller
        print "  Beginning scrape of git logs."
        git_commit_scraper = GitCommitScraper(settings_obj, controller)
        print "  Getting all commits for controller: " + controller
        commits = git_commit_scraper.get_all_commits(current_end_date_str, current_start_date_str)
        commit_storage = CommitStorage(commits)
        print "  Obtaining data percentiles for commits."
        commit_storage.get_data_percentiles()
        print "  Categorizing commits by messages."
        commit_storage.categorize_commits()

    # get the activity_log_storage object and create the finduserset
    print "Importing CSV data and converting to activity_logs..."
    current_end_datetime = datetime.datetime.strptime(current_end_date_str, settings_obj.get("str_date_format"))
    current_start_datetime = datetime.datetime.strptime(current_start_date_str, settings_obj.get("str_date_format"))
    activity_log_storage_obj = read_csv_data.read_csv_to_activity_log_storage(settings_obj.get("csv_data_directory_path") + settings_obj.get("csv_data_filename"), settings_obj, current_start_datetime, current_end_datetime, settings_obj.get("csv_data_contains_header"), old_activity_logs)
    print "Finished converting to activity_log_storage_object."
    print "Finding user sets..."
    find_user_set_obj = FindUserSet(activity_log_storage_obj, settings_obj)
    print "Found user sets."

    writer_obj = write_csv_data.WriteCSV(settings_obj.get("output_filename"), settings_obj)
    # For each controller in the git_scraper_controllers, get commits
    # associated with them and perform the requisite operations
    # for creating discrete_difference_logs and outputting
    for controller in settings_obj.get("git_scraper_controllers"):
        # Create the commit attribute factory, used to generate the 
        # discrete difference logs
        commit_attribute_factory = CommitAttributeFactory(commit_storage, activity_log_storage_obj, controller, settings_obj)

        # Once we have percentiles for each commit, start looking
        # at each commit and creating discrete difference logs
        for commit in commit_storage.get_commits():
            print "  Working on commit: " + commit.commit_id
            print "    Commited on: " + str(commit.datetime)
            ba_user_ids, only_before_user_ids = find_user_set_obj.compute_user_sets(controller, commit.datetime)
            print "    Found " + str(len(ba_user_ids)) + " Before and After Users, " + str(len(only_before_user_ids)) + " Only Before Users"
            ba_logs = commit_attribute_factory.get_discrete_differences(commit, settings_obj.get("commit_window_interval"), settings_obj.get("commit_half_window"), ba_user_ids, 1)
            only_before_logs = commit_attribute_factory.get_discrete_differences(commit, settings_obj.get("commit_window_interval"), settings_obj.get("commit_half_window"), only_before_user_ids, 0)
            print "    Created " + str(len(ba_logs) + len(only_before_logs)) + " discrete_difference_logs"
            print "Writing data to CSV."
            writer_obj.add_logs_to_csv(ba_logs)
            writer_obj.add_logs_to_csv(only_before_logs)
            print "Data written to: " + settings_obj.get("output_filename")

    # return the activity logs that we still need to keep track of 
    # the next time we run this function
    last_half_window = current_end_datetime - datetime.timedelta(days=settings_obj.get("commit_half_window"))
    old_logs = activity_log_storage_obj.get_logs_in_window(last_half_window, current_end_datetime) 
    return old_logs

def run_data_production():
    settings_obj = settings.Settings(production_env=True)
    run_data(settings_obj)

def run_data_test():
    settings_obj = settings.Settings(production_env=False)
    run_data(settings_obj)

if __name__ == '__main__':
    run_data_test()
