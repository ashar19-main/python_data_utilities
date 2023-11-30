import zeep
from pandas import DataFrame
from concurrent.futures import ThreadPoolExecutor, as_completed

def flatten(dictionary, parent_key='', sep='_'):
    items = []
    for k, v in dictionary.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def process_batch(batch):
    return [flatten(item) for item in batch]

def batch_process(response, batch_size=1000):
    for i in range(0, len(response), batch_size):
        yield response[i:i + batch_size]

# Replace 'your_soap_service_url' and 'your_soap_method' with your actual SOAP service URL and method
client = zeep.Client(wsdl='your_soap_service_url')
response = client.service.your_soap_method()

# Assuming the response is a list of dictionaries
flattened_data = []
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_batch, batch) for batch in batch_process(response)]
    for future in as_completed(futures):
        flattened_data.extend(future.result())

# Convert to DataFrame
df = DataFrame(flattened_data)
