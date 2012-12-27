import settings
from activity_log_storage import *
from add_attributes import *
from find_user_sets import *
from commit_data import *

def get_follow_path_from_controller(controller_name):
    return controller_name + "_controller.rb"

def run_data(settings_obj):
    find_user_set = FindUserSet(activity_log_storage, settings_obj)
    for controller in settings_obj.get("git_scraper_controllers"):
        git_commit_scraper = GitCommitScraper(settings.git_scraper_directory_path, get_follow_path_from_controller(controller), controller)
        commits = git_commit_scraper.get_controller_commits(settings_obj.get("global_start"), settings_obj.get("global_end"))
        commit_storage = CommitStorage(commits)
        commit_storage.get_data_percentiles()


    find_user_set.find_users(

def run_data_production():
    settings_obj = settings.Settings(production_env=True)
    run_data(settings_obj)

def run_data_test():
    settings_obj = settings.Settings(production_env=False)
    run_data(settings_obj)
