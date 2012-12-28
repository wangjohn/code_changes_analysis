class Settings:
    def __init__(self, production_env=True):
        self._get_production_settings()
        if not production_env:
            self._overwrite_with_test_settings()

    def _get_production_settings(self):
        # Global start and end dates
        self.global_start = "7/4/2011"
        self.global_end = "11/24/2012"

        # The time before and after a commit to examine
        self.commit_half_window = 2
        self.commit_window_interval = 1

        # Git scraper file locations
        self.git_scraper_directory_path = "/home/john/panjiva_web_branches/web/app/controllers"
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

        # CSV data
        self.csv_data_filename = "/home/john/activity_log_out.csv"
        self.csv_data_contains_header = True
        self.output_filename = "/home/john/discrete_difference_logs.csv"
        self.csv_unchanged_headers = [
            ("controller", 2),
            ("action", 3),
        ]
        self.csv_integer_headers = [
            ("id", 0),
            ("user_account_id", 1),
            ("session_id", 10)
        ]
        self.csv_date_headers = [
            ("created_at", 6)
        ]
        self.csv_output_row_headers = self._convert_indexed_headers(self.csv_unchanged_headers) + self._convert_indexed_headers(self.csv_integer_headers) + self._convert_indexed_headers(self.csv_date_headers) 

    def _convert_indexed_headers(self, indexed_headers):
        return [attribute for attribute, index in indexed_headers]

    def _overwrite_with_test_settings(self):
        self.global_start = "10/1/2012"
        self.global_end = "11/24/2012"

        self.git_scraper_controllers = ["search"]
        #self.csv_data_filename = "/home/john/activity_log_out_test.csv"

    def get(self, setting):
        return getattr(self, setting)
