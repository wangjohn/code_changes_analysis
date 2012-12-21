class Settings:
    def __init__(self):
        self.settings = {
            # the time before and after a commit to examine
            "commit_half_window": 2
        }

    def get(self, setting):
        return self.settings[setting]
