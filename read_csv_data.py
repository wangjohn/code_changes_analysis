import csv
import activity_log_storage
import parse_date

def read_csv_to_activity_log_storage(filename, settings_obj, start_time, end_time, contains_header=True, verbose=True, old_activity_logs=None):
    header_row = None
    activity_log_counter = 0
    csv_row_counter = 0
    created_at_index = find_created_at_index(settings_obj)
    activity_logs = []
    with open(filename, 'rb') as f:
        reader = csv.reader(f, delimiter=',')
        if contains_header:
            header_row = reader.next()
        for row in reader:
            if is_unparsed_time_in_window(row[created_at_index], start_time, end_time):
                new_activity_log = activity_log_storage.ActivityLog(row, settings_obj)
                activity_logs.append(new_activity_log)
                activity_log_counter += 1
                if verbose and activity_log_counter % 100000 == 0:
                    print "  Created activity log " + str(counter)
            csv_row_counter += 1
            if verbose and csv_row_counter % 100000 == 0:
                print "  Read through row {0} of csv data".format(str(csv_row_counter))
    if verbose:
        print "Finished creating {1} Activity Logs".format(str(activity_log_counter))
        unparsed_integers = 0
        for log in activity_logs:
            unparsed_integers += log.unparsed_integer
        print "There were " + str(unparsed_integers) + " unparsed integers."
    if old_activity_logs != None:
        activity_logs.extend(old_activity_logs)
    return activity_log_storage.ActivityLogStorage(activity_logs)
    
def find_created_at_index(settings_obj):
    for attribute, index in settings_obj.get("csv_date_headers"):
        if attribute == "created_at":
            return index
    raise Exception("The created_at attribute was not defined under the csv_date_headers in the settings object.")

def is_unparsed_time_in_window(unparsed_time, start_time, end_time):
    parsed_time = parse_date.parse_to_datetime(unparsed_time)
    return (parsed_time <= end_time and parsed_time >= start_time)

if __name__ == '__main__':
    read_csv_data("/home/john/activity_log_out.csv")

