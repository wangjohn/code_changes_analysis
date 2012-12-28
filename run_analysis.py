import settings
import read_csv_data
from activity_log_storage import *
from add_attributes import *
from find_user_sets import *
from commit_data import *


def get_follow_path_from_controller(controller_name):
    return controller_name + "_controller.rb"

def run_data(settings_obj):
    # get the activity_log_storage object and create the finduserset
    print "Importing CSV data..."
    csv_rows = read_csv_data.read_csv_data(settings_obj.get("csv_data_filename"), settings_obj.get("csv_data_contains_header"))
    print "Finished importing CSV data."
    print "Converting data to activity_log_storage object..."
    activity_log_storage_obj = read_csv_data.convert_to_activity_logs(csv_rows)
    print "Finished converting to activity_log_storage_object."
    print "Finding user sets..."
    find_user_set_obj = FindUserSet(activity_log_storage_obj, settings_obj)
    print "Found user sets."

    discrete_difference_logs = []
    # For each controller in the git_scraper_controllers, get commits
    # associated with them and perform the requisite operations
    # for creating discrete_difference_logs and outputting
    for controller in settings_obj.get("git_scraper_controllers"):
        print "Begin working on controller: " + controller
        print "  Beginning scrape."
        git_commit_scraper = GitCommitScraper(settings_obj.get("git_scraper_directory_path"), get_follow_path_from_controller(controller), controller)
        print "  Getting all commits for controller: " + controller
        commits = git_commit_scraper.get_controller_commits(settings_obj.get("global_end"), settings_obj.get("global_start"))
        commit_storage = CommitStorage(commits)
        print "  Obtaining data percentiles for commits."
        commit_storage.get_data_percentiles()

        # Create the commit attribute factory, used to generate the 
        # discrete difference logs
        commit_attribute_factory = CommitAttributeFactory(commit_storage, activity_log_storage_obj, controller)

        # Once we have percentiles for each commit, start looking
        # at each commit and creating discrete difference logs
        for commit in commit_storage.get_commits():
            ba_user_ids, only_before_user_ids = find_user_set_obj.compute_user_sets(controller, commit.datetime)
            ba_logs = commit_attribute_factory.get_discrete_differences(commit, settings_obj.get("commit_window_interval"), settings_obj.get("commit_half_window")*2, ba_user_ids, True)
            only_before_logs = commit_attribute_factory.get_discrete_differences(commit, settings_obj.get("commit_window_interval"), settings_obj.get("commit_half_window")*2, only_before_user_ids, False)
            discrete_difference_logs.extend(ba_logs)
            discrete_difference_logs.extend(only_before_logs)

    # use the write_csv_data.py module to write the data
    writer_obj = write_csv_data.WriteCSV()
    writer_obj.convert_to_csv(discrete_difference_logs, settings_obj.get("output_filename"))

def run_data_production():
    settings_obj = settings.Settings(production_env=True)
    run_data(settings_obj)

def run_data_test():
    settings_obj = settings.Settings(production_env=False)
    run_data(settings_obj)

if __name__ == '__main__':
    run_data_test()
