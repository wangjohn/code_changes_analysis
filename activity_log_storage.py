from dateutil import parser

class ActivityLogStorage:
    def __init__(self, activity_logs, header_object):
        self.activity_logs = activity_logs
        self.header_obj = header_object
        self.sorted_by = {}
        self.clustered_by = {}

    def get_sorted_by(self, sorting_value):
        if sorting_value in self.sorted_by:
            return self.sorted_by[sorting_value]
        self.sorted_by[sorting_value] = sorted(self.activity_logs, 
                key = lambda k : k.data_attributes[sorting_value])
        return self.sorted_by[sorting_value]

    def get_clustered_by(self, cluster_value):
        if cluster_value in self.clustered_by:
            return self.clustered_by[cluster_value]
        new_cluster = {}
        for log in self.activity_logs:
            cluster_value_key = log.data_attributes[cluster_value]
            if cluster_value_key in new_cluster:
                new_cluster[cluster_value_key].append(log)
            else:
                new_cluster[cluster_value_key] = [log]
        self.clustered_by[cluster_value] = new_cluster
        return self.clustered_by[cluster_value]


class ActivityLog:
    def __init__(self, input_lines, header_object):
        self.header_obj = header_object
        self.data_attributes = {}
        self.added_attributes = {}
        self._convert_to_hash(input_lines)

    def _convert_to_hash(self, input_lines):
        for header in self.header_obj.get_headers():
            self.data_attributes[header] = self.header_obj.
                    get_attribute_from_header(header, input_lines)

    def add_attribute(self, attribute_header, value):
        if attribute_header not in self.header_obj.extra_attributes:
            raise "Attempting to add an undefined attribute."
        self.added_attribute[attribute_header] = value

    def convert_to_row(self):
        row = []
        for data_header in self.header_obj.data_headers.keys():
            row.append(self.data_attributes[data_header])
        for added_attribute in self.header_obj.extra_attributes.keys():
            row.append(self.added_attributes[added_attribute])
        return row

class HeaderObject:
    # unused input lines are:
    #    "model_id": 4,
    #    "status": 5,
    #    "query_params": 7,
    #    "next_profile_activity_log_id": 9,
    #    "impersonated": 11
    def __init__(self):
        self.data_headers = {
            "id": 0,
            "user_account_id": 1,
            "controller": 2,
            "action": 3,
            "created_at": 6,
            "ip_address": 8,
            "session_id": 10,
        }

        self.extra_attributes = {}

    def get_headers(self):
        return self.data_headers.keys

    def get_attribute_from_header(self, header, input_lines):
        raw_string_val = input_lines[self.data_headers[header]]
        if header == "id" or header == "user_account_id":
            return int(raw_string_val)
        if header == "created_at":
            return parser.parse(raw_string_val).replace(tzinfo=None)
        return raw_string_val



