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
        self.sorted_by[sorting_value] = sorted(self.activity_logs, key = lambda k : k.data_attributes[sorting_value])
        return self.sorted_by[sorting_value]

    def get_clustered_by(self, cluster_value, secondary_sort_value):
        key_tuple = (cluster_value, secondary_sort_value)
        if key_tuple in self.clustered_by:
            return self.clustered_by[key_tuple]
        new_cluster = {}
        for log in self.activity_logs:
            cluster_value_key = log.data_attributes[cluster_value]
            if cluster_value_key in new_cluster:
                new_cluster[cluster_value_key].append(log)
            else:
                new_cluster[cluster_value_key] = [log]
        sorted_new_cluster = {}
        for key, logs in new_cluster.iteritems():
            sorted_new_cluster[key] = sorted(logs, key = lambda k : k.data_attributes[secondary_sort_value])
        self.clustered_by[key_tuple] = sorted_new_cluster
        return self.clustered_by[key_tuple]


class Log:
    def __init__(self, header_object):
        self.header_obj = header_object
        self.data_attributes = {}

    def set_data_attributes(self, data_attributes):
        self.data_attributes = data_attributes
        self.check_data_attributes()

    def add_attribute(self, attribute_header, value):
        if not self.header_obj.check_extra_headers_presence(attribute_header):
            raise Exception("Attempting to add an undefined extra attribute: " + attribute_header)
        self.data_attributes[attribute_header] = value

    def check_data_attributes(self):
        for attribute_header in self.data_attributes.iterkeys():
            if not (self.header_obj.check_data_headers_presence(attribute_header) or self.header_obj.check_extra_headers_presence(attribute_header)):
                raise Exception("There exists an undefined data attribute: " + attribute_header)
        
    def convert_to_row(self):
        row = []
        for data_header in self.header_obj.output_headers:
            if data_header in self.data_attributes:
                row.append(self.data_attributes[data_header])
            else:
                row.append('')
        return row

    def get_header(self):
        return self.header_obj


class ActivityLog(Log):
    def __init__(self, input_lines, header_object):
        Log.__init__(self, header_object)
        self._convert_to_hash(input_lines)

    def _convert_to_hash(self, input_lines):
        for header in self.header_obj.get_headers():
            self.add_attribute(header, Log.get_header(self).get_attribute_from_header(header, input_lines))

    def add_attribute(self, attribute_header, value):
        Log.add_attribute(self, attribute_header, value)

    def convert_to_row(self):
        return Log.convert_to_row(self)

    def get_header(self):
        return Log.get_header(self)

class DiscreteDifferenceLog(Log):
    def __init__(self, data_attributes, header_object):
        Log.__init__(self, header_object)
        Log.set_data_attributes(self, data_attributes) 

    def add_attributes_from_hash(self, attributes):
        for attribute_header, value in attributes.iteritems():
            self.add_attribute(attribute_header, value)

    def add_attribute(self, attribute_header, value):
        Log.add_attribute(self, attribute_header, value)

    def convert_to_row(self):
        return Log.convert_to_row(self)

    def get_header(self):
        return Log.get_header(self)

class HeaderObject:
    def __init__(self):
        self.data_headers = {}
        self.extra_headers = {}
        self.output_headers = []

    def set_headers(self, data_headers, extra_headers, output_headers):
        self.data_headers = data_headers
        self.extra_headers = extra_headers
        self.output_headers = output_headers

    def check_extra_headers_presence(self, attribute):
        return (attribute in self.extra_headers)

    def check_data_headers_presence(self, attribute):
        return (attribute in self.data_headers)

    def get_headers(self):
        return self.data_headers.keys()

    def get_attribute_from_header(self, header, input_lines):
        return input_lines[self.data_headers[header]]


class DiscreteDifferenceHeader(HeaderObject):
    def __init__(self):
        HeaderObject.__init__(self)
        data_headers = {
        }
        extra_headers = {
            "user_account_id",
            "controller",
            "days_after_commit",
            "datetime",
            "commit_id",
            "commit_datetime",
            "commit_quality",
            "commit_files_changed",
            "commit_insertions",
            "commit_deletions",
            "commit_files_changed_percentile",
            "commit_insertions_percentile",
            "commit_deletions_percentile",
            "actions_total_moving_avg",
            "sessions_total_moving_avg",
            "actions_controller_moving_avg",
            "sessions_controller_moving_avg",
            "moving_avg_timewindow",
            "ba_user_set"
        }
        output_headers = list(extra_headers)
        HeaderObject.set_headers(self, data_headers, extra_headers, output_headers)

    def check_extra_headers_presence(self, attribute):
        return HeaderObject.check_extra_headers_presence(self, attribute)

    def check_data_headers_presence(self, attribute):
        return (attribute in self.data_headers)

    def get_headers(self):
        return HeaderObject.get_headers(self)

    def get_attribute_from_header(self, header, input_lines):
        return HeaderObject.get_attribute_from_header(self, header, input_lines)

class ActivityLogHeader(HeaderObject):
    def __init__(self):
        HeaderObject.__init__(self)
        data_headers = {
            "id": 0,
            "user_account_id": 1,
            "controller": 2,
            "action": 3,
            "model_id": 4,
            "status": 5,
            "created_at": 6,
            "query_params": 7,
            "ip_address": 8,
            "next_profile_activity_log_id": 9,
            "session_id": 10,
            "impersonated": 11
        }
        extra_headers = {}
        output_headers = [
            "id",
            "user_account_id",
            "controller",
            "action",
            "created_at",
            "ip_address",
            "session_id"
        ]
        HeaderObject.set_headers(self, data_headers, extra_headers, output_headers)
    
    def check_extra_headers_presence(self, attribute):
        return HeaderObject.check_extra_headers_presence(self, attribute)

    def check_data_headers_presence(self, attribute):
        return (attribute in self.data_headers)

    def get_headers(self):
        return HeaderObject.get_headers(self)

    def get_attribute_from_header(self, header, input_lines):
        raw_string_val = HeaderObject.get_attribute_from_header(self, header, input_lines)
        if header == "id" or header == "user_account_id":
            return int(raw_string_val)
        if header == "created_at":
            return parser.parse(raw_string_val).replace(tzinfo=None)
        return raw_string_val


if __name__ == '__main__':
    ddh = DiscreteDifferenceHeader()
    data_attributes = {"user_account_id":5, "controller":2}
    a = DiscreteDifferenceLog(data_attributes, ddh)
    a.add_attribute("session_id", 3)
    print a.convert_to_row()
