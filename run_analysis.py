import settings
import read_csv_data
import write_csv_data
from activity_log_storage import *
from add_attributes import *
from find_user_sets import *
from commit_data import *


def get_follow_path_from_controller(controller_name):
    return controller_name + "_controller.rb"

def run_data(settings_obj):
    # get the commits for each controller
    for controller in settings_obj.get("git_scraper_controllers"):
        print "Begin working on controller: " + controller
        print "  Beginning scrape of git logs."
        git_commit_scraper = GitCommitScraper(settings_obj.get("git_scraper_directory_path"), get_follow_path_from_controller(controller), controller)
        print "  Getting all commits for controller: " + controller
        commits = git_commit_scraper.get_controller_commits(settings_obj.get("global_end"), settings_obj.get("global_start"))
        commit_storage = CommitStorage(commits)
        print "  Obtaining data percentiles for commits."
        commit_storage.get_data_percentiles()

    # get the activity_log_storage object and create the finduserset
    print "Importing CSV data..."
    csv_rows = read_csv_data.read_csv_data(settings_obj.get("csv_data_filename"), settings_obj.get("csv_data_contains_header"))
    print "Finished importing CSV data."
    print "Converting data to activity_log_storage object..."
    activity_log_storage_obj = read_csv_data.convert_to_activity_logs(csv_rows, settings_obj)
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

def run_data_production():
    settings_obj = settings.Settings(production_env=True)
    run_data(settings_obj)

def run_data_test():
    settings_obj = settings.Settings(production_env=False)
    run_data(settings_obj)

if __name__ == '__main__':
    run_data_test()
