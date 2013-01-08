import csv
import activity_log_storage

def read_csv_data(filename, contains_header=True, verbose=True):
    all_rows = []
    header_row = None
    counter = 0
    with open(filename, 'rb') as f:
        reader = csv.reader(f, delimiter=",")
        if contains_header:
            header_row = reader.next()
        for row in reader:
            all_rows.append(row)
            counter += 1
            if verbose and counter % 100000 == 0:
                print "  Imported line " + str(counter) + " from csv."

        return all_rows

def find_created_at_index(settings_obj):
    for attribute, index in settings_obj.get("csv_date_headers"):
        if attribute == "created_at":
            return index
    raise Exception("The created_at attribute was not defined under the csv_date_headers in the settings object.")

def is_unparsed_time_in_window(unparsed_time, start_time, end_time):
    parsed_time = parser.parse(unparsed_time).replace(tzinfo=None)
    return (parsed_time <= end_time and parsed_time >= start_time)

def convert_to_activity_logs(all_rows, settings_obj, start_time, end_time, old_activity_logs=None):
    activity_logs = []
    print "Creating Activity Logs"
    counter = 0
    created_at_index = find_created_at_index(settings_obj)
    for row in all_rows:
        if is_unparsed_time_in_window(row[created_at_index], start_time, end_time):
            new_activity_log = activity_log_storage.ActivityLog(row, settings_obj)
            activity_logs.append(new_activity_log)
            counter += 1
            if counter % 100000 == 0:
                print "  Created activity log " + str(counter)
    print "Finished creating {1} Activity Logs".format(str(counter))
    unparsed_integers = 0
    for log in activity_logs:
        unparsed_integers += log.unparsed_integer
    print "There were " + str(unparsed_integers) + " unparsed integers."
    if old_activity_logs != None:
        activity_logs.extend(old_activity_logs)
    return activity_log_storage.ActivityLogStorage(activity_logs)

if __name__ == '__main__':
    read_csv_data("/home/john/activity_log_out.csv")

