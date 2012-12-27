import csv

class WriteCSV:
    def __init__(self):
        pass

    def convert_to_data(self, logs):
        data = []
        for log in logs:
            data.append(log.convert_to_row())
        return data

    # This method assumes that the header for all the discrete 
    # difference logs is the same. 
    def convert_to_csv(self, logs, filename):
        csv_data = self.convert_to_data(logs)
        header_obj = logs[0].get_header()
        headers = header_obj.get_headers()
        
        with open(filename, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(csv_data)

    

