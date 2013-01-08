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

def convert_to_activity_logs(all_rows, settings_obj, old_activity_logs=None):
    activity_logs = []
    print "Creating Activity Logs"
    counter = 0
    for row in all_rows:
        new_activity_log = activity_log_storage.ActivityLog(row, settings_obj)
        activity_logs.append(new_activity_log)
        counter += 1
        if counter % 100000 == 0:
            print "  Created activity log " + str(counter)
    print "Finished creating Activity Logs"
    unparsed_integers = 0
    for log in activity_logs:
        unparsed_integers += log.unparsed_integer
    print "There were " + str(unparsed_integers) + " unparsed integers."
    if old_activity_logs != None:
        activity_logs.extend(old_activity_logs)
    return activity_log_storage.ActivityLogStorage(activity_logs)

if __name__ == '__main__':
    read_csv_data("/home/john/activity_log_out.csv")

