from dateutil import parser

class ActivityLogStorage:
    def __init__(self, activity_logs):
        self.activity_logs = activity_logs
        self.sorted_by = {}
        self.clustered_by = {}

    def get_sorted_by(self, sorting_value):
        if sorting_value in self.sorted_by:
            return self.sorted_by[sorting_value]
        self.sorted_by[sorting_value] = sorted(self.activity_logs, key = lambda k : k.get(sorting_value))
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

class DiscreteDifferenceLog:
    def __init__(self, attribute_list, settings_obj):
        self.settings_obj = settings_obj
        self.add_attributes_from_tuple_list(attribute_list)

    def add_attributes_from_tuple_list(self, attribute_list):
        for attribute_header, value in attribute_list:
            if self.settings_obj.get("check_assertions") and attribute not in self.settings_obj.get("data_output_headers"):
                raise Exception("Attempting to add an undefined attribute.")
            setattr(self, attribute_header, value)

    def get_settings_obj(self):
        return self.settings_obj

    def get_header(self):
        return self.settings_obj.get("data_output_headers")

    def convert_to_row(self):
        output = []
        for header in self.settings_obj.get("data_output_headers"):
            output.append(getattr(self, header))
        return output 
