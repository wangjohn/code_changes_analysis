# TODO: create a class that writes discrete_difference_logs to a csv output, note that the class should be able to convert a list of logs to a list of objs easily, using a header obj.
import csv

class WriteCSV:
    def __init__(self):
        
    def convert_to_data(self, difference_logs):
        data = []
        for log in difference_logs:
            data.append(log.convert_to_row())
        return data

    # This method assumes that the header for all the discrete 
    # difference logs is the same. 
    def convert_to_csv(self, difference_logs, filename):
        csv_data = self.convert_to_data(difference_logs)
        header_obj = difference_logs[0].get_header()
        headers = header_obj.get_headers()
        
        with open(filename, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(csv_data)

    

