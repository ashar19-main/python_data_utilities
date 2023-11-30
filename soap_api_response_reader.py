import zeep
from pandas import DataFrame

def flatten(dictionary, parent_key='', sep='_'):
    items = []
    for k, v in dictionary.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# Replace 'your_soap_service_url' and 'your_soap_method' with your actual SOAP service URL and method
client = zeep.Client(wsdl='your_soap_service_url')
response = client.service.your_soap_method()

# Assuming the response is a dictionary of dictionaries
flattened_data = [flatten(item) for item in response]

# Convert to DataFrame
df = DataFrame(flattened_data)
