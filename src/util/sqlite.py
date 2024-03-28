import sqlite3
import pandas as pd


class SQLite:
    def __init__(self, db_name=":memory:"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def to_sql(self, df=None, table_name=None):
        df.to_sql(table_name, self.conn, if_exists="replace", index=False)

    def query(self, sql=None):
        self.cursor.execute(sql)
        query_result = self.cursor.fetchall()

        result_df = pd.DataFrame(
            query_result, columns=[col[0] for col in self.cursor.description]
        )

        return result_df

    def close(self):
        self.cursor.close()
        self.conn.close()
