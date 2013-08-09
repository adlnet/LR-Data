import requests

slice_url = "https://node01.public.learningregistry.net/slice?any_tags=3dr"

data = requests.get(slice_url).json()

queue = [data]

while len(queue) > 0:
	for doc in data['documents']