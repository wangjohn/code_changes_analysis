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
    def __init__(self, original_filenames, directory_path, settings_obj, max_logs=3500000, contains_header=True):
        self.original_filenames = original_filenames
        self.max_logs = max_logs
        self.directory_path = directory_path
        self.settings_obj = settings_obj
        self.contains_header = contains_header

        self.sort_index = read_csv_data.find_created_at_index(settings_obj)

    # Take two filesets and merge them together into a new fileset
    # Filesets should be lists of filenames (including the path)
    #
    def merge(self, fileset1, fileset2, new_fileset, header):
        new_file_rows = []
        fileset_counter1 = 0
        fileset_counter2 = 0
        new_file_counter = 0
        reader1 = self._read_new_file(self, filset1, fileset_counter1)
        reader2 = self._read_new_file(self, filset2, fileset_counter2)
        current_log1 = reader1.next()
        current_log2 = reader2.next()

        while fileset_counter1 < len(fileset1) or fileset_counter2 < len(fileset2):
            counter = 0
            new_file_rows = []
            while counter < self.max_logs:
                counter += 1
                if (current_log1 != None and self._compare_logs(current_log1, current_log2)) or current_log2 == None:
                    current_log1, fileset_counter1, reader1 = self._append_then_repopulate_reader(current_log1, reader1, new_file_rows, fileset1, fileset_counter1)
                else:
                    current_log2, fileset_counter2, reader2 = self._append_then_repopulate_reader(current_log2, reader2, new_file_rows, fileset2, fileset_counter2)

            # write the new rows to the new file
            with open(self.directory_path + new_fileset[new_file_counter], 'wb') as f:
                writer = csv.writer(f, delimiter=',')
                if self.contains_header and header:
                    writer.writerow(header)
                writer.writerows(new_file_rows)

            new_file_counter += 1

    def _compare_logs(self, log1, log2):
        return (log1[self.sort_index] < log2[self.sort_index])
            

    def _read_new_file(self, fileset, fileset_counter):
        if fileset_counter < len(fileset):
            new_file = open(self.directory_path + fileset[fileset_counter], 'rb')
            reader = csv.reader(new_file, 'rb')
            if self.contains_header:
                header = reader.next()
            return reader
        else:
            return None

    def _append_then_repopulate_reader(self, current, reader, new_file_rows, fileset, fileset_counter):
        new_file_rows.append(current)
        try:
            new_current = reader.next()
        except StopIteration:
            fileset_counter += 1
            reader = self._read_new_file(fileset, fileset_counter)
            if reader:
                new_current = reader.next()
            else:
                new_current = None
        return (new_current, fileset_counter, reader)

