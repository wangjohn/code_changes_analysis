import sets

class Settings:
    def __init__(self, production_env=True):
        self._get_production_settings()
        if not production_env:
            self._overwrite_with_test_settings()

    def _get_production_settings(self):
        # Global start and end dates
        self.global_start = "7/4/2011"
        self.global_end = "11/24/2012"
        self.str_date_format = "%m/%d/%Y"

        # Items that govern the segments (if the input csv is too 
        # large for everything to fit into memory)
        self.days_per_segment_interval = 125 
        self.remove_old_csv_data = False

        # The time before and after a commit to examine
        self.commit_half_window = 15
        self.commit_window_interval = 1

        # The minimum number of actions needed in a half_window in 
        # order to be included in the sample.
        self.min_actions_threshold = 5

        # Git scraper file locations
        self.git_scraper_directory_path = "/home/john/panjiva_web_branches/web/app"
        self.git_scraper_controllers = [
            "search",
            "my_panjiva",
            "us_imports",
            "us_exports",
            "profile",
            "project",
            "communication",
            "customs",
            "info"
        ]

        # Assertions and checking
        self.check_assertions = False

        # CSV data
        self.csv_data_filename = "/home/john/panjiva_data_files/activity_logs_1-1-2008.csv"
        self.csv_data_contains_header = True
        self.output_filename = "/home/john/panjiva_data_files/discrete_difference_logs_15hw.csv"
        self.csv_unchanged_headers = [
            ("controller", 2),
            ("action", 3),
        ]
        self.csv_integer_headers = [
            ("id", 0),
            ("user_account_id", 1),
            ("session_id", 5)
        ]
        self.csv_date_headers = [
            ("created_at", 4)
        ]
        self.csv_output_row_headers = self._convert_indexed_headers(self.csv_unchanged_headers) + self._convert_indexed_headers(self.csv_integer_headers) + self._convert_indexed_headers(self.csv_date_headers) 

        # Format of the output data
        self.data_output_headers = sets.Set([
            "user_account_id",
            "controller",
            "days_after_commit",
            "datetime",
            "commit_id",
            "commit_datetime",
            "commit_quality",
            "commit_author",
            "commit_files_changed",
            "commit_insertions",
            "commit_deletions",
            "commit_files_changed_percentile",
            "commit_insertions_percentile",
            "commit_deletions_percentile",
            "commit_category",
            "commit_controller_change",
            "commit_view_change",
            "actions_total_moving_avg",
            "sessions_total_moving_avg",
            "actions_controller_moving_avg",
            "sessions_controller_moving_avg",
            "moving_avg_timewindow",
            "ba_user_set"
        ])

    def _convert_indexed_headers(self, indexed_headers):
        return [attribute for attribute, index in indexed_headers]

    def _overwrite_with_test_settings(self):
        self.check_assertions = True
        self.git_scraper_controllers = ["search"]

    def overwrite_with_ngram_csv_settings(self):
        self.csv_data_filename = "/home/john/code_changes_analysis/test_categorization.csv"
        self.data_output_headers = [
            "category",
            "commit_message"
        ]

    def get(self, setting):
        return getattr(self, setting)
