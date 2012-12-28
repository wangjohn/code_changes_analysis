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
            cluster_value_key = log.get(cluster_value)
            if cluster_value_key in new_cluster:
                new_cluster[cluster_value_key].append(log)
            else:
                new_cluster[cluster_value_key] = [log]
        sorted_new_cluster = {}
        for key, logs in new_cluster.iteritems():
            sorted_new_cluster[key] = sorted(logs, key = lambda k : k.get(secondary_sort_value))
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
        if not (self.header_obj.check_extra_headers_presence(attribute_header) or self.header_obj.check_data_headers_presence(attribute_header)):
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

class ActivityLog:
    def __init__(self, input_lines, settings_obj):
        self.settings_obj = settings_obj
        self.unparsed_integer = 0 
        self._convert_input_lines(input_lines, settings_obj)

    def _convert_input_lines(self, input_lines, settings_obj):
        for attribute, index in settings_obj.get("csv_unchanged_headers"):
            setattr(self, attribute, input_lines[index])
        for attribute, index in settings_obj.get("csv_integer_headers"):
            val = input_lines[index]
            if val:
                setattr(self, attribute, int(input_lines[index]))
            else:
                self.unparsed_integer += 1
                setattr(self, attribute, None)
        for attribute, index in settings_obj.get("csv_date_headers"):
            setattr(self, attribute, parser.parse(input_lines[index]).replace(tzinfo=None))

    def get(self, attribute):
        return getattr(self, attribute)

    def convert_to_row(self):
        output = []
        for attribute in self.settings_obj.get("csv_output_row_headers"):
            output.append(self.get(attribute))
        return output

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

if __name__ == '__main__':
    ddh = DiscreteDifferenceHeader()
    data_attributes = {"user_account_id":5, "controller":2}
    a = DiscreteDifferenceLog(data_attributes, ddh)
    a.add_attribute("session_id", 3)
    print a.convert_to_row()
