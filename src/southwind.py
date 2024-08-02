import time

from .config import Config
from .db_connector import DBConnector
from .exporter import Exporter


class SouthWind:
    def __init__(self, config_path: str):

        self.cnf = Config(config_path)
        self.db = DBConnector(self.cnf.db_path)
        self.exporter = Exporter(self.cnf.output_path)
        self.tables = self.cnf.load_datasets()

    def execute(self):

        self.cnf.create_output_folders([table.table_name for table in self.tables])

        max_change_token_values = {}
        for table in self.tables:
            result = self.db.execute_sql(
                f"select count(1) as cnt from information_schema.tables where table_name = '{table.table_name}'",
                "cnt",
            )
            if result == "0":
                self.db.execute_sql(table.genereate_create_table_str())
            max_change_token_values[table.table_name] = self.db.get_max_change_token(
                table.table_name
            )

        def handle_change(table_name: str):
            max_change_token_value = self.db.get_latest_rows(table_name)
            if max_change_token_value != max_change_token_values[table_name]:
                max_change_token_values[table_name] = max_change_token_value
                latest_row = self.db.get_latest_rows(table_name)
                self.exporter.export(table_name, latest_row, self.cnf.output_format)

            if self.cnf.delete_behaviour == "HARD":
                for table in self.tables:
                    self.db.execute_sql(
                        f"delete from {table.table_name} where change_type = 'D'"
                    )

        while True:
            for table in self.tables:
                self.db.execute(table.perform_action())
                handle_change(table.table_name)
                time.sleep(self.cnf.inter_action_delay)
