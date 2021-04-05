import requests,json
from datetime import datetime
d=datetime.strftime(datetime.now(),'%Y-%m-%d')
r=requests.get(url='https://api.exchangeratesapi.io/history?start_at=2018-01-01&end_at=' + d)
data=r.json()
with open('/Users/condenast/Documents/data_pipeline/input2.json','w') as f:
    json.dumps(data)
    f.close()