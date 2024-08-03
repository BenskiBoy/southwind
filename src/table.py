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
    """Table class to represent a table in the database, with fields and actions to perform on the table"""

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

    def generate_count_str(self, table: str) -> str:
        """Generate a count query for a table

        Args:
            table (str): table name

        Returns:
            str: SQL query
        """
        return f"""select count(*) as cnt from {table} where change_type != 'D';"""

    def generate_random_lookup_str(
        self, table: str, field: str, default_val: str = "1"
    ) -> str:
        """Generate a random lookup query for a table and field, with a default value of 1
            Will also handle for empty table and deleted records

        Args:
            table (str): table name
            field (str): field to perform random look of
            default_val (str, optional): default value if table is empty. Defaults to ""

        Returns:
            str: SQL query
        """
        return f"""select {field} from {table} where change_type != 'D' using sample 1 union all (select {default_val} as {field} order by {field} desc)"""  # handles for empty table and filters deleted records

    def generate_increment_str(self, table: str, field: str) -> str:
        """Gets the max of a field and increments it by 1, used for auto incrementing fields

        Args:
            table (str): table name
            field (str): field to get autoincrement of.

        Returns:
            str: SQL query
        """
        return f"""select coalesce((max({field}) + 1), 1) as inc from {table};"""  # not filtering out deleted records as we don't want to reuse the deleted record's id

    def genereate_create_table_str(self) -> str:
        """generate DDL

        Returns:
            str: SQL query
        """
        return f"""
        CREATE TABLE if not exists {self.table_name}(
            {', '.join(' '.join([field.name, field.type]) + (' primary key' if field.is_pk else '') for field in self.fields)});
        """

    def evaluate_imposter(self, field: Field) -> Statement:
        """Evaluate the imposter field and return the appropriate Statement type

        Args:
            field (Field): _description_

        Raises:
            InvalidValueError: _description_

        Returns:
            Statement: _description_
        """
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
            raise InvalidValueError("Invalid value")

    def generate_insert(self) -> List[Statement]:
        """Generate List of statements for insert
        Returns:
            List[Statement]: List of Statemet objects
        """
        result_values = []
        for field in self.fields:
            result_values.append(DirectStatement(f"'"))
            result_values.append(self.evaluate_imposter(field))
            result_values.append(
                DirectStatement("', " if field != self.fields[-1] else "'")
            )
        return (
            [DirectStatement(f"INSERT INTO {self.table_name} VALUES (")]
            + result_values
            + [DirectStatement(");")]
        )

    def generate_set(self, action: Set) -> List[Statement]:
        """Generate List of statements for set
        Args:
            action (Set): Action to perform
        Returns:
            List[Statement]: List of Statement objects
        """

        result_values = [
            DirectStatement(f"UPDATE {self.table_name} set {action.field} = '")
        ]
        result_values.append(self.evaluate_imposter(action.value))

        if action.where_clause is not None:
            return result_values + [
                DirectStatement(
                    f"', change_token = (SELECT MAX(change_token) + 1 FROM {self.table_name}), change_type = 'U' WHERE {action.where_table}.{action.where_field} {action.where_condition} '"
                ),
                self.evaluate_imposter(action.where_value),
                DirectStatement("' AND change_type != 'D';"),
            ]
        else:
            return result_values + [
                DirectStatement(
                    f"', change_token = (SELECT MAX(change_token) + 1 FROM {self.table_name}), change_type = 'U' WHERE change_type != 'D';"
                )
            ]

    def generate_delete(self, action: Remove) -> List[Statement]:
        """Generate List of statements for delete
        Args:
            action (Remove): Action to perform
        Returns:
            List[Statement]: List of Statement objects
        """

        return [
            DirectStatement(
                f"UPDATE {self.table_name} SET change_token = (SELECT MAX(change_token) + 1 FROM {self.table_name}), change_type = 'D' WHERE {action.where_table}.{action.where_field} {action.where_condition} '"
            ),
            self.evaluate_imposter(action.where_value),
            DirectStatement(
                "' AND change_type != 'D';"
            ),  # probably not need, but will leave in for now
        ]

    def perform_action(self) -> List[Statement]:
        """Perform a random action on a table

        Raises:
            NotImplementedError: if action is not implemented

        Returns:
            List[Statement]: List of Statement objects to be executed on the database
        """
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
