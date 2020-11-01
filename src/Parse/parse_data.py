import json
import pandas as pd
import urllib.request

class Parse:
    def __init__(self, file, topic):
        self.file = file
        self.topic = topic

    def parse_and_filter(self):
        a_url = "https://healthdata.gov/data.json"
        urllib.request.urlretrieve(a_url, "sample.json")

        #load the json data
        with open("sample.json", "r") as read_it:
            data = json.load(read_it)
        dataset = data.get("dataset")
        columns_names = ['description', 'publisher', 'title', 'theme']
        # create a dataframe for dataset
        df = pd.DataFrame(dataset)
        df = df[columns_names]
        # filter by topic from description column
        df = df[df['description'].str.contains(self.topic)]
        return df