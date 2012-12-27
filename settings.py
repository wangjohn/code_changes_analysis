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
    
    def _overwrite_with_test_settings(self):
        self.global_start = "10/1/2012"
        self.global_end = "11/24/2012"

        self.git_scraper_controllers = ["search"]

    def get(self, setting):
        return getattr(self, setting)
