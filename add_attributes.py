
class AttributeFactory:
    def __init__(self, logs_to_augment, activity_log_storage):
        self.logs_to_augment = logs_to_augment
        self.activity_log_storage = activity_log_storage

    def get_lagged_activity(self, time_lag):
        user_accounts = self.activity_log_storage.get_clustered_by(
                "user_account_id", "created_at")
        attribute_to_update = "actions_next_" + time_lag + "_days"
        for log in logs_to_augment:
            # get the relevant info about this activity log
            ua_id = log.data_attributes["user_account_id"]
            current_created_at = log.data_attributes["created_at"]
            logs_to_check = user_accounts[ua_id]

            # get the indices corresponding to current log and also the
            # log time_lag days into the future
            start_index = binary_search_on_attribute(logs_to_check, 
                    current_created_at, 0, len(logs_to_check)-1, 
                    "created_at")
            end_index = binary_search_on_attribute(logs_to_check,
                    current_created_at+datetime.timedelta(days=
                    time_lag), start_index, len(logs_to_check)-1,
                    "created_at")
            actions_in_window = end_index - start_index + 1

            # update attributes for this activity_log
            log.add_attribute(attribute_to_update, actions_in_window)


            
        

