import sets 
import datetime
from activity_log_storage import *
from find_user_sets import *

class CommitAttributeFactory:
    """
    Class for creating discrete difference logs about a given commit.
    We track a number of users, and create a single difference log
    for each user, no matter how many actions they have around a commit.
    However, we have some number of logs surrounding the commit, 
    depending on how finely we want to observe the commit. We will have
    a difference log every x days from -W to +W days after the commit.
    """
    def __init__(self, commits, activity_log_storage, controller):
        self.commits = commits
        self.activity_log_storage = activity_log_storage
        self.header_obj = DiscreteDifferenceHeader()
        self.controller = controller

    def get_discrete_differences(self, commit, time_interval, total_time, users):
        increments = (time_interval / total_time)
        single_time_delta = datetime.timedelta(days=time_interval)
        time_sorted_logs = self.activity_log_storage.sorted_by["created_at"]
        for i in xrange(increments):
            time_delta = datetime.timedelta(days=time_interval*i)

            # get the logs in the before time window
            before_index = binary_search_on_attribute(time_sorted_logs, commit.datetime-time_delta, 0, len(time_sorted_logs)-1, "created_at")
            before_prev_index = binary_search_on_attribute(time_sorted_logs, commit.datetime-time_delta-single_time_delta, 0, before_index, "created_at")
            user_clustered_logs_before = self.filter_logs_in_index_window(before_prev_index, before_index, time_sorted_logs, "controller", self.controller, users)

            # get the logs in the after time window
            after_index = binary_search_on_attribute(time_sorted_logs, commit.datetime+time_delta, 0, len(time_sorted_logs)-1, "created_at")
            after_prev_index = binary_search_on_attribute(time_sorted_logs, commit.datetime+time_delta-single_time_delta, 0, after_index, "created_at")
            user_clustered_logs_after = self.filter_logs_in_index_window(after_prev_index, after_index, time_sorted_logs, "controller", self.controller, users)
    
    def filter_logs_in_index_window(self, prev_index, end_index, activity_logs, data_attribute, attribute, users):
        user_clustered_output = {}
        for i in xrange(prev_index, end_index+1, 1):
            current_log = activity_logs[i]
            uaid = current_log.data_attributes["user_account_id"]
            if current_log.data_attributes[data_attribute] == attribute and uaid in users:
                if uaid in user_clustered_output:
                    user_clustered_output[uaid].append(current_log)
                else:
                    user_clustered_output[uaid] = [current_log]
        return user_clustered_output

    def create_logs_from_user_clustered_logs(self, user_clustered_logs, users, commit, datetime):
        for user_account_id in users.iterkeys():
            if user_account_id in user_clustered_logs:
                attributes = self.compute_attributes_from_user_logs(user_clustered_logs[user_account_id])
            else:
                attributes = {"num_actions_in_controller": 0, "moving_avg_30_day": 0}

    def compute_attributes_from_user_logs(self, user_logs):
        num_actions_in_controller = len(user_logs)
        num_actions_total = ""
        moving_avg_10_day = "" # this comes on the day of the commit - time.delta
        # get_moving_average of all actions here

    def create_discrete_difference_log(self, datetime, commit, days_after_commit, user_account_id):
        difference_log = DiscreteDifferenceLog({}, self.header_obj)
        difference_log.add_attribute("user_account_id", user_account_id)
        difference_log.add_attribute("controller", commit.controller)
        difference_log.add_attribute("days_after_commit", days_after_commit)
        difference_log.add_attribute("datetime", datetime)
        return DiscreteDifferenceLog(data_attributes, self.header_obj)

class MovingAverages:
    def __init__(self, activity_log_storage):
        self.users = users
        self.activity_log_storage = activity_log_storage
        self.allowable_averages = sets.Set(["actions","sessions"])

    # this method provides a batched way to get moving averages a certain length of
    # days before a datetime. Works best for large number of users (large enough that
    # the user set constitutes a non-trivial number of the total users in that
    # time period.
    def get_moving_averages_batched(self, time_lag, end_time, users):
        raise Exception("Not yet implemented.")

    # Returns moving averages of actions performed in a given time period. The 
    # time period is specified with an end_time and a time_lag before that.
    # The number of actions or distinct sessions occuring inside of the time
    # period will be returned. 
    #
    # One can also specify controllers to focus in on. If only_controllers
    # is true, then only the actions and sessions for the controller will be
    # presented
    def get_moving_averages(self, time_lag, end_time, users, average_types={"actions","sessions"}, controllers=[]):
        self.check_average_types(average_types)
        sorted_user_logs = self.activity_log_storage.get_clustered_by("user_account_id", "created_at")
        averages = {}
        for user_account_id in users:
            current_sorted = sorted_user_logs[user_account_id]
            end_time_index = binary_search_on_attribute(current_sorted, end_time, 0, len(current_sorted)-1, "created_at")        
            time_lag_index = binary_search_on_attribute(current_sorted, end_time-datetime.timedelta(days=time_lag), 0, end_time_index, "created_at")
            user_results = {}
            windowed_logs = sorted_user_logs[time_lag_index:(end_time_index+1)]
            if "actions" in average_types:
                user_results["actions"] = self._get_actions(windowed_logs, controllers)
            if "sessions" in average_types:
                user_results["sessions"] = self._get_sessions(windowed_logs, controllers)
            averages[user_account_id] = user_results
        return averages

    # Submethod which takes a list of logs in particular window, and returns the
    # number of actions overall and for each controller.
    def _get_actions(self, windowed_logs, controllers=[]):
        total_actions = len(windowed_logs)
        output = {"total_actions": total_actions}
        if controllers:
            for controller in controllers:
                output[controller] = 0
            for log in windowed_logs:
                current_controller = log.data_attributes["controller"]
                if current_controller in output:
                    output[current_controller] += 1
        if only_controllers:
            del output["total_actions"]
        return output

    def _get_sessions(self, windowed_logs, controllers=[]):
        session_sets = {"total_sessions" = sets.Set()}
        for controller in controllers:
            session_sets[controller] = sets.Set()
        for log in windowed_logs:
            current_controller = log.data_attributes["controller"]
            session_id = log.data_attributes["session_id"]
            session_sets["total_sessions"].add(session_id)
            if current_controller in session_sets:
                session_sets[current_controller].add(session_id)

        # convert the sets into a count of the number of distinct sessions
        output = {}
        for name, s_set in session_sets.iteritems():
            output[name] = len(s_set)
        return output

    def check_average_types(self, average_types):
        for atype in average_types:
            if atype not in self.allowable_averages:
                raise Exception("Unknown average type: " + average_type)


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

