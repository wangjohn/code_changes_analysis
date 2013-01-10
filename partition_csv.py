import csv
import parse_date
import read_csv_data
import re
import settings

class PartitionCSV:
    def __init__(self, original_filename, temp_directory, settings_obj, max_logs=3500000, contains_header=True):
        self.original_filename = original_filename
        self.max_logs = max_logs
        self.directory_path = directory_path
        self.settings_obj = settings_obj
        self.contains_header = contains_header

        self.sort_index = read_csv_data.find_created_at_index(settings_obj)

    def partition_original_file(self):
        new_filenames = []
        with open(self.directory_path + self.original_filename, 'rb') as f:
            reader = csv.reader(f, delimiter=',')
            header_row = None
            if self.contains_header:
                header_row = reader.next()
            file_count = 0
            current_rows = []
            for row in reader:
                current_rows.append(row)
                if len(current_rows) >= self.max_logs:
                    filename = self._write_rows(self, current_rows, file_count, header_row)
                    print "  Created new file: {0}".format(filename)
                    new_filenames.append(filename)
                    file_count += 1
                    current_rows = []
        return new_filenames

    def _write_rows(self, current_rows, file_count, header_row=None):
        new_filename = modify_filename(self.original_filename, str(file_count) + "_split")
        with open(self.directory_path + new_filename, 'wb') as write_file:
            writer = csv.writer(f, delimiter=',')
            if header_row != None:
                writer.writerow(header_row)

            sorted_current_rows = self._sort_current_rows_by_index(current_rows, self.sort_index)
            writer.writerows(sorted_current_rows)
        return new_filename

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

    def sort(self):
        return self._sort(self.original_filenames, 0)

    def _sort(self, filenames_set, depth):
        print "  Starting to sort at depth {0}.".format(str(depth))
        print "  Current filenames_set: " + ",".join(filenames_set)
        files_per_set = 2**depth
        if len(filenames_set) <= files_per_set:
            return (filenames_set, depth)

        grouped_filenames_set = []
        current_fileset = [] 

        # Group the files together into sets that we will merge
        for filename in filenames_set:
            if len(new_fileset) < files_per_set:
                current_fileset.append(filename)
            else:
                grouped_filenames_set.append(current_fileset)
                current_fileset = [filename]

        # The new_filenames_set is the set of files that the newly
        # written files will be named
        new_filenames_set = [modify_filename(filenames_set[i], depth) for i in xrange(len(filenames_set)-len(current_fileset))]
        if current_fileset:
            new_filenames_set.extend(current_fileset)

        # Physically merge the files now
        counter = 0
        while counter < len(new_filenames_set):
            fileset1 = group_filenames_set[counter]
            fileset2 = group_filenames_set[counter+1]

            print "    Starting to merge filesets:"
            print "      1: " + ",".join(fileset1)
            print "      2: " + ",".join(fileset2)

            self.merge(fileset1, fileset2, new_filenames_set[(counter*files_per_set):((counter+2)*files_per_set)], self.settings_obj.get("csv_output_row_headers"))

            counter += 2

        print "  Finished sorting at depth {0}.".format(str(depth))

        # Recurse with a higher depth
        return self._sort(new_filenames_set, depth+1)

    # Take two filesets and merge them together into a new fileset
    # Filesets should be lists of filenames and the new_fileset 
    # should be the size of the sum of the two old filesets
    # 
    # This method will write new files with the names according
    # to names from the new_fileset array, with logs in sorted order
    # from the two old filesets
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

def modify_filename(filename, appended_modifier, filename_regex=re.compile("^(.*?)\d*.csv$")):
    match = filename_regex.match(filename)
    if match:
        filename_root = match.group(1)
        return filename_root + str(appended_modifier) + ".csv"
    else:
        raise Exception("Filename in the wrong format (must be csv).")

def run_partition(verbose=True)
    settings_obj = settings.PartitionSettings()
    original_filename = settings_obj.get("csv_data_filename")
    directory = settings_obj.get("csv_data_directory_path")

    partition_csv_obj = PartitionCSV(original_filename, directory, settings_obj)
    print "Beginning to partition the original csv file."
    new_filenames = partition_csv_obj.partition_original_file()
    print "Finished partitioning the original csv file."
    
    sort_csv_files_obj = SortCSVFiles(new_filenames, directory, settings_obj)
    print "Beginning to sort the split csv files."
    sort_csv_files_obj.sort()
    print "Finished sorting the split csv files."
     

if __name__ == '__main__':
    run_partition()
