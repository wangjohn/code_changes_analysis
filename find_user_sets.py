import datetime

def binary_search_on_attribute(activity_logs, value, start, end, attribute):
    if end <= start:
        return start
    mid = (start+end)/2
    current_attr = activity_logs[mid].data_attributes[attribute]
    if current_attr > value:
        return binary_search_on_attribute(activity_logs, value, start, mid-1, attribute)
    elif current_attr < value:
        return binary_search_on_attribute(activity_logs, value, mid+1, end, attribute)
    else:
        return mid

class FindUserSet:
    def __init__(self, activity_log_storage, settings):
        self.activity_log_storage = activity_log_storage
        self.settings = settings

    # Returns a list of user_account_ids. The first result gives the
    # users who saw a controller before and after a commit inside of
    # the commit_window. The second result gives the users who saw
    # a controller only before the commit, and not after.
    # 
    # Uses the settings object to get the commit_half_window
    def compute_user_sets(self, controller, commit_datetime):
        ba_users_after, ba_users_before, only_before_users_before = self.compute_user_log_sets(controller, commit_datetime)
        ba_user_account_ids = ba_users_after.keys()
        only_before_user_account_ids = only_before_users_before.keys()
        return (ba_user_acount_ids, only_before_user_account_ids)

    # Returns a list of action_logs. The first result gives the action
    # logs corresponding to the users who saw a controller both before
    # and after a commit, and gives the action logs before the commit.
    # The second result gives the action logs from that set of users
    # after the commit. The third result gives the action logs from
    # the users who only saw the controller before the commit.
    def compute_user_log_sets(self, controller, commit_datetime):
        logs_time_sorted = self.activity_log_storage.get_sorted_by("created_at")
        commit_index = binary_search_on_attribute(logs_time_sorted, commit_datetime, 0, len(logs_time_sorted)-1, "created_at")
        lower_index = binary_search_on_attribute(logs_time_sorted, commit_datetime-datetime.timedelta(days=self.settings.get("commit_half_window")), 0, commit_index, "created_at")
        upper_index = binary_search_on_attributes(logs_time_sorted, commit_datetime+datetime.timedelta(days=self.settings.get("commit_half_window")), commit_index, len(logs_time_sorted)-1, "created_at")
        
        # get the dictionaries containing all users who had activity
        # before a given commit, and before and after a commit
        all_before_users = self._get_users_in_time_window(logs_time_sorted, lower_index, commit_index, "controller", controller)
        ba_users_after = self._get_users_in_time_window(logs_time_sorted, commit_index, upper_index, "controller", controller, all_before_users)
        ba_users_before = self._get_users_in_time_window(logs_time_sorted, lower_index, commit_index, "controller", controller, self.ba_users_after)

        # construct the set of users who had activity only before a 
        # given commit, but not after
        only_before_users_before = self._get_logs_not_in_set(self.all_before_users, self.ba_users_before)
        return (ba_users_after, ba_users_before, only_before_users_before)

    def _get_users_in_time_window(self, activity_logs, start_index, end_index, data_attribute, required_attribute_value, limit_ua_ids=None):
        users = {}
        for i in xrange(start_index, end_index+1):
            activity_log = activity_logs[i]
            if activity_log.data_attributes[data_attribute] == required_attribute_value:
                self._add_to_dict_with_attribute(users, activity_log, "user_account_id", limit_ua_ids)
        return users

    def _add_to_dict_with_attribute(self, attribute_dict, activity_log, attribute, limit_attributes):
        attribute_id = activity_log.data_attributes[attribute]
        if limit_attributes == None or (attribute_id in limit_attributes):
            if attribute_id in attribute_dict:
                attribute_dict[attribute_id].append(activity_log)
            else:
                attribute_dict[attribute_id] = [activity_log]
            
    def _get_logs_not_in_set(self, activity_logs_dict, rejection_dict):
        new_dict = {}
        for key in activity_logs_dict.iterkeys():
            if key not in rejection_dict:
                new_dict[key] = activity_logs_dict[key]
        return new_dict
        
