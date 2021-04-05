import json
f=open('/Users/condenast/Documents/data_pipeline/input.json')
data = json.load(f)
currency_filter = ('EUR', 'USD', 'JPY', 'CAD', 'GBP', 'NZD','IND')
filtered_data = {dt: {currency: data.get("rates").get(dt).get(currency) for currency in data.get("rates").get(dt) if currency in currency_filter} for dt in data.get("rates")}

data["rates"] = filtered_data
f.close()

with open('/Users/condenast/Documents/data_pipeline/filtered_data.json','w') as f:
    json.dump(data,f)
