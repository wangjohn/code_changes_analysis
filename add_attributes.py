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
    def __init__(self, commit_storage, activity_log_storage, controller, settings_obj):
        self.commit_storage = commit_storage
        self.activity_log_storage = activity_log_storage
        self.settings_obj = settings_obj
        self.controller = controller
        self.moving_averages = MovingAverages(activity_log_storage, settings_obj)

    def get_discrete_differences(self, commit, time_interval, half_window, users, ba_user_set=True):
        single_time_delta = datetime.timedelta(days=time_interval)
        time_sorted_logs = self.activity_log_storage.sorted_by["created_at"]

        discrete_differences_results = []
        for i in xrange(-half_window/time_interval, half_window/time_interval+1, 1):
            # compute the new datetime that we care about
            time_delta = datetime.timedelta(days=time_interval*i)
            print "    Current time delta: " + str(time_delta)
            new_datetime = commit.datetime + time_delta
    
            # compute the moving averages for each user in the set
            moving_averages = self.moving_averages.get_moving_averages(time_interval, new_datetime, users, controllers=[self.controller])
            
            # add the moving_avgs information to create some discrete
            # difference logs
            difference_logs = self.create_ddl_from_averages(moving_averages, new_datetime, commit, i, time_interval, ba_user_set)
            discrete_differences_results.extend(difference_logs)
        return discrete_differences_results

    def create_ddl_from_averages(self, averages, date_of_average, commit, days_after_commit, moving_avg_timewindow, ba_user_set):
        ddl_logs = []
        for user_account_id, averages_hash in averages.iteritems():
            new_ddl_log = self.create_discrete_difference_log(date_of_average, commit, days_after_commit, user_account_id, moving_avg_timewindow, ba_user_set, averages_hash) 
            ddl_logs.append(new_ddl_log)
        return ddl_logs

    def create_discrete_difference_log(self, date, commit, days_after_commit, user_account_id, moving_avg_timewindow, ba_user_set, averages_hash):
        attributes = [
            ("user_account_id", user_account_id),
            ("controller", self.controller),
            ("days_after_commit", days_after_commit),
            ("datetime", date),
            ("commit_id", commit.commit_id),
            ("commit_datetime", commit.datetime),
            ("commit_quality", commit.get_quality()),
            ("commit_files_changed", commit.num_files_changed),
            ("commit_insertions", commit.num_insertions),
            ("commit_deletions", commit.num_deletions),
            ("commit_files_changed_percentile", self.commit_storage.get_commit_percentile("num_files_changed", commit)),
            ("commit_insertions_percentile", self.commit_storage.get_commit_percentile("num_insertions", commit)),
            ("commit_deletions_percentile", self.commit_storage.get_commit_percentile("num_deletions", commit)),
            ("actions_total_moving_avg", averages_hash["actions_total"]),
            ("sessions_total_moving_avg", averages_hash["sessions_total"]),
            ("actions_controller_moving_avg", averages_hash["actions_" + self.controller]),
            ("sessions_controller_moving_avg", averages_hash["sessions_" + self.controller]),
            ("moving_avg_timewindow", moving_avg_timewindow),
            ("ba_user_set", ba_user_set)
        ]
        difference_log = DiscreteDifferenceLog(attributes, self.settings_obj)
        return difference_log 

class MovingAverages:
    def __init__(self, activity_log_storage, settings_obj):
        self.activity_log_storage = activity_log_storage
        self.settings_obj = settings_obj
        if self.settings_obj.get("check_assertions"):
            self.allowable_averages = sets.Set(["actions","sessions"])

    def _convert_averages_to_single_hash(self, averages_output):
        output_hash = {}
        for average_type, average_hash in averages_output.iteritems():
            for item_subname, item_value in average_hash.iteritems():
                new_name = average_type + "_" + item_subname
                output_hash[new_name] = item_value
        return output_hash

    # this method provides a batched way to get moving averages 
    # a certain length of days before a datetime. Works best for 
    # large number of users (large enough that the user set 
    # constitutes a non-trivial number of the total users in that
    # time period.
    def get_moving_averages_batched(self, time_lag, end_time, users):
        raise Exception("Not yet implemented.")

    # Returns moving averages of actions performed in a given 
    # time period. The  time period is specified with an end_time 
    # and a time_lag before that. The number of actions or distinct 
    # sessions occuring inside of the time period will be returned. 
    #
    # One can also specify controllers to focus in on. 
    # If only_controllers is true, then only the actions and 
    # sessions for the controller will be presented.
    def get_moving_averages(self, time_lag, end_time, users, average_types={"actions","sessions"}, controllers=[]):
        if self.settings_obj.get("check_assertions"):
            self.check_average_types(average_types)
        sorted_user_logs = self.activity_log_storage.get_clustered_by("user_account_id", "created_at")
        averages = {}
        for user_account_id in users:
            current_sorted = sorted_user_logs[user_account_id]
            end_time_index = binary_search_on_attribute(current_sorted, end_time, 0, len(current_sorted)-1, "created_at")        
            time_lag_index = binary_search_on_attribute(current_sorted, end_time-datetime.timedelta(days=time_lag), 0, end_time_index, "created_at")
            user_results = {}
            windowed_logs = current_sorted[time_lag_index:(end_time_index+1)]
            if "actions" in average_types:
                user_results["actions"] = self._get_actions(windowed_logs, controllers)
            if "sessions" in average_types:
                user_results["sessions"] = self._get_sessions(windowed_logs, controllers)
            averages[user_account_id] = self._convert_averages_to_single_hash(user_results)
        return averages

    # Submethod which takes a list of logs in particular window, 
    # and returns the number of actions overall and for each 
    # controller.
    def _get_actions(self, windowed_logs, controllers=[]):
        total_actions = len(windowed_logs)
        output = {"total": total_actions}
        if controllers:
            for controller in controllers:
                output[controller] = 0
            for log in windowed_logs:
                current_controller = log.get("controller")
                if current_controller in output:
                    output[current_controller] += 1
        return output

    def _get_sessions(self, windowed_logs, controllers=[]):
        session_sets = {"total": sets.Set()}
        for controller in controllers:
            session_sets[controller] = sets.Set()
        for log in windowed_logs:
            current_controller = log.get("controller")
            session_id = log.get("session_id")
            session_sets["total"].add(session_id)
            if current_controller in session_sets:
                session_sets[current_controller].add(session_id)

        # convert the sets into a count of the number of 
        # distinct sessions
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
        self.get_moving_avg(time_lag, "actions", "user_account_id", "created_at", self._actions_moving_average_subprod)

    def get_moving_average_sessions(self, time_lag):
        self.get_moving_avg(time_lag, "sessions", "user_account_id", "created_at", self._sessions_moving_average_subprod)

    def get_total_actions(self):
        self.get_total_count("actions", "user_account_id", "created_at", False)

    def get_total_sessions(self):
        self.get_total_count("sessions", "user_account_id", "created_at", True)

    def get_total_count(self, attribute_name, cluster_val, secondary_sort_val, function_to_call, sessions=False):
        user_accounts = self.activity_log_storage.get_clustered_by(cluster_val, secondary_sort_val)
        attribute_to_update = attribute_name + "_total_count"
        for log in self.logs_to_augment:
            ua_id = log.get("user_account_id")
            logs_to_check = user_accounts[ua_id]
            if sessions:
                session_ids = sets.Set()
                for current_log in logs_to_check:
                    session_ids.add(current_log.get("session_id"))
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
        ua_id = log.get("user_account_id")
        current_created_at = log.get("created_at")
        logs_to_check = user_accounts[ua_id]

        # get the indices corresponding to current log and also the
        # log time_lag days into the future
        end_index = binary_search_on_attribute(logs_to_check, current_created_at, 0, len(logs_to_check)-1, 
                "created_at")
        start_index = binary_search_on_attribute(logs_to_check, current_created_at-datetime.timedelta(days=time_lag), 0, end_index, "created_at")
        actions_in_window = end_index - start_index + 1
        return actions_in_window

    def _sessions_moving_average_subprod(self, log, user_accounts):
        ua_id = log.get("user_account_id")
        current_created_at = log.get("created_at")
        logs_to_check = user_accounts[ua_id]

        end_index = binary_search_on_attribute(logs_to_check, current_created_at, 0, len(logs_to_check)-1, "created_at")
        start_index = binary_search_on_attribute(logs_to_check, current_created_at-datetime.timedelta(days=time_lag), 0, end_index, "created_at")

        session_ids = sets.Set() 
        for i in xrange(start_index, end_index+1):
            current_log = logs_to_check[i]
            session_ids.add(current_log.get("session_id"))

        # returns distinct session_ids in the time span
        return len(session_ids)

