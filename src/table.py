from typing import List, Union
import logging
import random

from .exceptions import InvalidValueError

from .field import Field
from .imposter import (
    ImposterResult,
    ImposterDirectResult,
    ImposterLookupResult,
    ImposterIncrementResult,
)
from .action import Action, Set, Create, Remove
from .db_connector import Statement, SQLStatement, DirectStatement


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Table:
    def __init__(self, table_name: str, fields: List[Field], actions: List[Action]):
        self.table_name = table_name
        self.fields = fields + [
            Field(
                "change_token", "int", "increment"
            ),  # used as a cdc token to track changes
            Field(
                "change_type", "string", 'static("I")'
            ),  # used as a flag to track change type (D, U, I)
        ]
        self.actions = actions

        print(self.genereate_create_table_str())

    def generate_count_str(self, table: str) -> str:
        return f"""select count(*) as cnt from {table} where change_type != 'D';"""

    def generate_random_lookup_str(
        self, table: str, field: str, default_val: str
    ) -> str:
        return f"""select {field} from {table} where change_type != 'D' using sample 1 union all (select 1 as {field} order by {field} desc)"""  # handles for empty table and filters deleted records

    def generate_increment_str(self, table: str, field: str) -> str:
        return f"""select coalesce((max({field}) + 1), 1) as inc from {table};"""  # not filtering out deleted records as we don't want to reuse the deleted record's id

    def genereate_create_table_str(self):
        return f"""
        CREATE TABLE if not exists {self.table_name}(
            {', '.join(' '.join([field.name, field.type]) + (' primary key' if field.is_pk else '') for field in self.fields)});
        """

    def pass_imposter(self, field: Field) -> Statement:
        result = field.evaluate()

        if isinstance(result, ImposterDirectResult):
            return DirectStatement(result.value)

        elif isinstance(result, ImposterLookupResult):
            return SQLStatement(
                self.generate_random_lookup_str(
                    result.table, result.field, result.default_val
                ),
                result.field,
            )

        elif isinstance(result, ImposterIncrementResult):
            return SQLStatement(
                self.generate_increment_str(self.table_name, field.name), "inc"
            )

        else:
            import pdb

            pdb.set_trace()
            raise InvalidValueError("Invalid value")

    def generate_insert(self) -> List[Statement]:
        result_values = []
        for field in self.fields:
            result_values.append(DirectStatement(f"'"))
            result_values.append(self.pass_imposter(field))
            result_values.append(
                DirectStatement("', " if field != self.fields[-1] else "'")
            )
        return (
            [DirectStatement(f"INSERT INTO {self.table_name} VALUES (")]
            + result_values
            + [DirectStatement(");")]
        )

    def generate_set(self, action: Set) -> List[Statement]:

        result_values = [
            DirectStatement(f"UPDATE {self.table_name} set {action.field} = '")
        ]
        result_values.append(self.pass_imposter(action.value))

        if action.where_clause is not None:
            return result_values + [
                DirectStatement(
                    f"', change_token = (select max(change_token) + 1 from {self.table_name}), change_type = 'U' where {action.where_table}.{action.where_field} {action.where_condition} '"
                ),
                self.pass_imposter(action.where_value),
                DirectStatement("' and change_type != 'D';"),
            ]
        else:
            return result_values + [
                DirectStatement(
                    f"', change_token = (select max(change_token) + 1 from {self.table_name}), change_type = 'U' where change_type != 'D';"
                )
            ]

    def generate_delete(self, action: Remove) -> List[Statement]:

        return [
            DirectStatement(
                f"update {self.table_name} set change_token = (select max(change_token) + 1 from {self.table_name}), change_type = 'D' where {action.where_table}.{action.where_field} {action.where_condition} '"
            ),
            self.pass_imposter(action.where_value),
            DirectStatement(
                "' and change_type != 'D';"
            ),  # probably not need, but will leave in for now
        ]

    def perform_action(self) -> List[Statement]:
        selected_action = random.choices(
            self.actions, [action.frequency for action in self.actions]
        )[0]

        if isinstance(selected_action, Set):
            return self.generate_set(selected_action)
        elif isinstance(selected_action, Create):
            return self.generate_insert()
        elif isinstance(selected_action, Remove):
            return self.generate_delete(selected_action)
        else:
            raise NotImplementedError()

    def get_field_by_name(self, field_name: str) -> Field:
        for field in self.fields:
            if field.name == field_name:
                return field
        return None

    def __str__(self):
        return f"{self.table_name} {self.fields} {self.actions}"

    def __repr__(self):
        return f"{type(self).__name__}({self.__dict__})"
