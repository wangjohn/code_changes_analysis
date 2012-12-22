import sets 
import datetime
from activity_log_storage import *

class CommitAttributeFactory:
    def __init__(self, commits, activity_log_storage):
        self.commits = commits
        self.activity_log_storage = activity_log_storage
        self.header_obj = DiscreteDifferenceHeader()

    def get_discrete_differences(self, commit, time_interval, total_time):
        increments = (time_interval / total_time)
        for i in xrange(increments):
            time_delta = datetime.timedelta(days=time_interval*i)
            before_datetime = commit.datetime-time_delta
            after_datetime = commit.datetime+time_delta

    def create_discrete_difference_log(self, datetime, commit):
        data_attributes = {'datetime': datetime}
        return DiscreteDifferenceLog(data_attributes, self.header_obj)


class ActivityLogAttributeFactory:
    def __init__(self, logs_to_augment, activity_log_storage):
        self.logs_to_augment = logs_to_augment
        self.activity_log_storage = activity_log_storage

    def get_moving_average_actions(self, time_lag):
        self.get_moving_avg(time_lag, "actions", "user_account_id", "created_at", self._actions_moving_average_subprod))

    def get_moving_average_sessions(self, time_lag):
        self.get_moving_avg(time_lag, "sessions", "user_account_id", "created_at", self._sessions_moving_average_subprod))

    def get_total_actions(self):
        self.get_total_count("actions", "user_account_id", "created_at", False)

    def get_total_sessions(self):
        self.get_total_count("sessions", "user_account_id", "created_at", True)

    def get_total_count(self, attribute_name, cluster_val, secondary_sort_val, function_to_call, sessions=False):
        user_accounts = self.activity_log_storage.get_clustered_by(cluster_val, secondary_sort_val)
        attribute_to_update = attribute_name + "_total_count"
        for log in self.logs_to_augment:
            ua_id = log.data_attributes["user_account_id"]
            logs_to_check = user_accounts[ua_id]
            if sessions:
                session_ids = sets.Set()
                for current_log in logs_to_check:
                    session_ids.add(current_log.data_attributes["session_id"])
                output = len(session_ids)
            else:
                output = len(logs_to_check)
            log.add_attribute(attribute_to_update, output)

    def get_moving_avg(self, time_lag, attribute_name, cluster_val, secondary_sort_val, function_to_call):
        user_accounts = self.activity_log_storage.get_clustered_by(cluster_val, secondary_sort_val)
        attribute_to_update = attribute_name + "_mvavg_" + time_lag  + "_days"
        for log in self.logs_to_augment:
            output = function_to_call(log)
            log.add_attribute(attribute_to_update, output)

    def _actions_moving_average_subprod(self, log, user_accounts):
        # get the relevant info about this activity log
        ua_id = log.data_attributes["user_account_id"]
        current_created_at = log.data_attributes["created_at"]
        logs_to_check = user_accounts[ua_id]

        # get the indices corresponding to current log and also the
        # log time_lag days into the future
        end_index = binary_search_on_attribute(logs_to_check, current_created_at, 0, len(logs_to_check)-1, 
                "created_at")
        start_index = binary_search_on_attribute(logs_to_check, current_created_at-datetime.timedelta(days=time_lag), 0, end_index, "created_at")
        actions_in_window = end_index - start_index + 1
        return actions_in_window

    def _sessions_moving_average_subprod(self, log, user_accounts):
        ua_id = log.data_attributes["user_account_id"]
        current_created_at = log.data_attributes["created_at"]
        logs_to_check = user_accounts[ua_id]

        end_index = binary_search_on_attribute(logs_to_check, current_created_at, 0, len(logs_to_check)-1, "created_at")
        start_index = binary_search_on_attribute(logs_to_check, current_created_at-datetime.timedelta(days=time_lag), 0, end_index, "created_at")

        session_ids = sets.Set() 
        for i in xrange(start_index, end_index+1):
            current_log = logs_to_check[i]
            session_ids.add(current_log.data_attributes["session_id"])

        # returns distinct session_ids in the time span
        return len(session_ids)

