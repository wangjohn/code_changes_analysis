import csv

class WriteCSV:
    def __init__(self, filename, settings_obj):
        self.filename = filename
        self.settings_obj = settings_obj
        self.add_headers_to_csv(filename, settings_obj)

    def convert_to_data(self, logs):
        data = []
        for log in logs:
            data.append(log.convert_to_row())
        return data

    def add_headers_to_csv(self, filename, settings_obj): 
        headers = [header for header in settings_obj.get("data_output_headers")]
        with open(filename, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(headers)

    # This method assumes that the header for all the discrete 
    # difference logs is the same. 
    def add_logs_to_csv(self, logs):
        csv_data = self.convert_to_data(logs)
        
        with open(self.filename, 'a') as f:
            writer = csv.writer(f)
            writer.writerows(csv_data)

    

