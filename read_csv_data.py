import csv

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
                print "Imported line " + str(counter) + " from csv."

        return all_rows

def convert_to_activity_logs(all_rows):
    activity_logs = []
    header_obj = HeaderObject()
    for row in all_rows:
        activity_logs.append(ActivityLog(row, header_obj))
    return ActivityLogStorage(activity_logs, header_obj)

if __name__ == '__main__':
    read_csv_data("/home/john/activity_log_out.csv")

