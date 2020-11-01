from src.Parse.parse_data import Parse
from src.Parse.transform_data import CommandInvoker
from src.PostGresMapper.persist_data import DBConnection

if __name__ == "__main__":
    parse = Parse("scratch.json", "COVID")
    dataframe = parse.parse_and_filter()
    transformed_data = CommandInvoker("COVID", dataframe).transform()
    db_conn = DBConnection("localhost", "CovidReports", "postgres", "admin", "5432")
    conn = db_conn.connect()
    db_conn.upsert(conn, transformed_data, "CovidReports")