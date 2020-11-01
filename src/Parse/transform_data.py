"""
Author: Bhargav <lbhargav2020@gmail.com>
Last-Revision: 2020-10-29
Script that transforms the given transforms given pandas dataframe.
"""
from abc import ABC, abstractmethod
import logging

class Command(ABC):
    @abstractmethod
    def execute(self):
        raise NotImplementedError("Please implement in subclass")

class TransformCovidData():
    def __init__(self, data):
        self._data = data
        self.df = data.copy(deep=True)

    def transform_covid_data(self):
        self.df['publisher name'] = self.df['publisher'].apply(lambda row: row.get('name'))
        self.df['publisher type'] = self.df['publisher'].apply(lambda row: row.get('@type'))
        del self.df['publisher']
        logging.info("posts are loaded and transformed")
        print("posts are loaded and transformed")
        return self.df

class CovidData(Command):
    def __init__(self, csv_file):
        self.posts = TransformCovidData(csv_file)

    def execute(self):
        return self.posts.transform_covid_data()

class CommandInvoker():
    def __init__(self, topic, data):
        self.type = topic
        self.commands = {}
        self.commands["COVID"] = CovidData(data)

    def transform(self):
        try:
            print("The given topic is " + self.type)
            logging.info("The given topic is " + self.type)
            return self.commands.get(self.type).execute()
        except Exception as e:
            logging.error(e)