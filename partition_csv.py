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

    def merge(filename1, filename2, new_filename1, new_filename2, headers=True):
        new_file_rows = []
        with open(filename1, 'rb') as f1:
            with open(filename2, 'rb') as f2:
                reader1 = csv.reader(f1, delimiter=',')
                reader2 = csv.reader(f2, delimiter=',')

                # check for headers
                if headers:
                    headers = reader1.next()
                    headers = reader2.next()

                # walk down the file 
                self._populate_csv_file(new_filename1, reader1, reader2, headers)
                self._populate_csv_file(new_filename2, reader1, reader2, headers)

    def _populate_csv_file(new_filename, reader1, reader2, header):
        counter = 0
        current1 = reader1.next()
        current2 = reader2.next()
        new_file_rows = []
        while counter < self.max_logs and (current1 != None or current2 != None):
            counter += 1
            if (current1 != None and current1 < current2) or current2 == None:
                current1 = self._append_to_files(current1, reader1, new_file_rows)
            else:
                current2 = self._append_to_files(current2, reader2, new_file_rows)

        # write the new rows to the new file
        with open(new_filename, 'wb') as f:
            writer = csv.writer(f, delimiter=',')
            if header:
                writer.writerow(header)
            writer.writerows(new_file_rows)

    def _append_then_repopulate_reader(current, reader, new_file_rows):
        new_file_rows.append(current)
        try:
            new_current = reader.next()
        except StopIteration:
            new_current = None
        return new_current

