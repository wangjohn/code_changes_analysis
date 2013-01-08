import csv
import parse_date
import read_csv_data

class PartitionCSV:
    def __init__(self, original_filename, temp_directory, settings_obj, max_logs=3500000, contains_header=True):
        self.original_filename = original_filename
        self.max_logs = max_logs
        self.temp_directory = temp_directory
        self.settings_obj = settings_obj
        self.contains_header = contains_header

        self.sort_index = read_csv_data.find_created_at_index(settings_obj)

    def partition_original_file(self):
        with open(self.original_filename, 'rb') as f:
            reader = csv.reader(f, delimiter=',')
            header_row = None
            if self.contains_header:
                header_row = reader.next()
            file_count = 0
            current_rows = []
            for row in reader:
                current_rows.append(row)
                if len(current_rows) >= self.max_logs:
                    self._write_rows(self, current_rows, file_count, header_row)
                    file_count += 1
                    current_rows = []

    def _write_rows(self, current_rows, file_count, header_row=None):
        with open(self.temp_directory + self.additional_base_file_name + str(file_count), 'wb') as write_file:
            writer = csv.writer(f, delimiter=',')
            if header_row != None:
                writer.writerow(header_row)

            sorted_current_rows = self._sort_current_rows_by_index(current_rows, self.sort_index)
            writer.writerows(sorted_current_rows)

    def _sort_current_rows_by_index(self, current_rows, index, attribute_processing_function=parse_date.parse_to_datetime):
        return sorted(current_rows, key = lambda k : attribute_processing_function(k[index]))


class SortCSVFiles:
    def __init__(self, original_filenames, temp_directory, settings_obj, max_logs=3500000, contains_header=True):
        self.original_filenames = original_filenames
        self.max_logs = max_logs
        self.temp_directory = temp_directory
        self.settings_obj = settings_obj
        self.contains_header = contains_header

        self.sort_index = read_csv_data.find_created_at_index(settings_obj)
