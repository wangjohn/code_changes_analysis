import datetime

def binary_search_on_attribute(activity_logs, value, start, end, attribute):
    if end <= start:
        return start
    mid = (start+end)/2
    current_attr = activity_logs[mid].get(attribute)
    if current_attr > value:
        return binary_search_on_attribute(activity_logs, value, start, mid-1, attribute)
    elif current_attr < value:
        return binary_search_on_attribute(activity_logs, value, mid+1, end, attribute)
    else:
        return mid

class FindUserSet:
    def __init__(self, activity_log_storage, settings_obj):
        self.activity_log_storage = activity_log_storage
        self.settings_obj = settings_obj

    # Returns a list of user_account_ids. The first result gives the
    # users who saw a controller before and after a commit inside of
    # the commit_window. The second result gives the users who saw
    # a controller only before the commit, and not after.
    # 
    # Uses the settings object to get the commit_half_window, and the
    # the min number of actions required to be in a set.
    def compute_user_sets(self, controller, commit_datetime):
        logs_time_sorted = self.activity_log_storage.get_sorted_by("created_at")
        commit_index = binary_search_on_attribute(logs_time_sorted, commit_datetime, 0, len(logs_time_sorted)-1, "created_at")
        lower_index = binary_search_on_attribute(logs_time_sorted, commit_datetime-datetime.timedelta(days=self.settings_obj.get("commit_half_window")), 0, commit_index, "created_at")
        upper_index = binary_search_on_attribute(logs_time_sorted, commit_datetime+datetime.timedelta(days=self.settings_obj.get("commit_half_window")), commit_index, len(logs_time_sorted)-1, "created_at")
        
        # get the dictionaries containing all users who had activity
        # before a given commit, and before and after a commit
        controller_before_users = self._get_users_in_time_window(logs_time_sorted, lower_index, commit_index, "controller", controller)
        controller_after_users = self._get_users_in_time_window(logs_time_sorted, commit_index, upper_index, "controller", controller, limit_ua_ids=controller_before_users)
        non_controller_after_users = self._get_users_in_time_window(logs_time_sorted, commit_index, upper_index, "controller", controller, negated=True, limit_ua_ids=controller_before_users)

        ba_users = controller_after_users.keys()
        only_before_users = non_controller_after_users.keys()

        return (ba_users, only_before_users)

    def _get_users_in_time_window(self, activity_logs, start_index, end_index, data_attribute, required_attribute_value, negated=False, limit_ua_ids=None):
        users = {}
        for i in xrange(start_index, end_index+1):
            activity_log = activity_logs[i]
            if self._check_data_attribute_validity(activity_log, data_attribute, required_attribute_value, negated):
                self._add_to_dict_with_attribute(users, activity_log, "user_account_id", limit_ua_ids)
        users = self._delete_users_under_min_actions_threshold(users)
        return users

    def _check_data_attribute_validity(self, activity_log, data_attribute, required_attribute_value, negated):
        if negated:
            return activity_log.get(data_attribute) != required_attribute_value
        return activity_log.get(data_attribute) == required_attribute_value

    def _add_to_dict_with_attribute(self, attribute_dict, activity_log, attribute, limit_attributes):
        attribute_id = activity_log.get(attribute)
        if limit_attributes == None or (attribute_id in limit_attributes):
            if attribute_id in attribute_dict:
                attribute_dict[attribute_id].append(activity_log)
            else:
                attribute_dict[attribute_id] = [activity_log]
            
    def _delete_users_under_min_actions_threshold(self, user_log_set):
        new_set = {}
        for user_account_id, logs in user_log_set.iteritems():
            if len(logs) >= self.settings_obj.get("min_actions_threshold"):
                new_set[user_account_id] = logs
        return new_set
