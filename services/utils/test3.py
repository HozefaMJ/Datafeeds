import json
from datetime import date

my_date = date.today()
date_string = my_date.strftime("%Y-%m-%d")

json_data = json.dumps(date_string)
print(json_data)
