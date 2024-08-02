from typing import Dict, Union, List
from .exceptions import InvalidValueError, validate_keys
from .field import Field
from .imposter import Imposter

EQUALITIES = ["==", ">", "<", ">=", "<=", "!="]


class Action:
    def __init__(self, name: str, frequency: float, arguments: List[str | int] = []):
        self.name = name
        self.frequency = frequency
        self.arguments = arguments

    def generate_sql(self):
        raise NotImplementedError()

    @classmethod
    def get_type(self, attribs: Dict):
        if Create.is_type(attribs):
            return Create
        elif Remove.is_type(attribs):
            return Remove
        elif Set.is_type(attribs):
            return Set
        else:
            raise NotImplementedError()

    @classmethod
    def is_type(self, attribs: Dict):
        raise NotImplementedError()

    def __str__(self):
        return f"{self.name}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.__dict__})"


class Create(Action):
    def __init__(
        self,
        name: str,
        frequency: float,
    ):
        super().__init__(name, frequency)

    @classmethod
    def is_type(self, attribs: Dict, table_name: str = ""):
        if attribs["action"].lower() != "create":
            return False
        validate_keys(
            dictionary=attribs,
            required_keys=["name", "frequency", "action"],
            optional_keys=[],
            additional_context=f"Create action requires 'name' and 'frequency' - Table {table_name} - Action `{attribs.get('name', '')}`",
        )
        if not (attribs["frequency"] > 0 and attribs["frequency"] <= 1):
            raise InvalidValueError("Frequency must be between 0 and 1")
        return True


class Remove(Action):
    def __init__(
        self,
        name: str,
        frequency: float,
        where_clause: str,
    ):
        super().__init__(name, frequency)

        self.where_clause = where_clause

        if where_clause:
            self.pass_where_clause(self.where_clause)

    def pass_where_clause(self, where_clause: str):
        tokens = None
        for condition in EQUALITIES:
            if condition in where_clause:
                tokens = where_clause.split(condition)
                self.where_condition = condition
                break
        if not tokens:
            raise InvalidValueError(f"Invalid where condition - {self.where_condition}")

        self.where_table = tokens[0].split(".")[0].replace(" ", "")
        self.where_field = tokens[0].split(".")[1].replace(" ", "")
        self.where_value = Imposter(tokens[1].replace(" ", ""))

    @classmethod
    def is_type(self, attribs: Dict, table_name: str) -> bool:
        if attribs["action"].lower() != "remove":
            return False
        validate_keys(
            dictionary=attribs,
            required_keys=["name", "frequency", "action"],
            optional_keys=["where_condition"],
            additional_context=f"Remove action requires 'name' and 'frequency' and an optional 'where_condition' - Table {table_name} - Action `{attribs.get('name', '')}`",
        )
        if not (attribs["frequency"] > 0 and attribs["frequency"] <= 1):
            raise InvalidValueError("Frequency must be between 0 and 1")
        return True


class Set(Action):

    def __init__(
        self,
        name: str,
        field: Field,
        value: Imposter,
        where_clause: str = None,
        frequency: float = 0.0,
        arguments: List[str | int] = [],
    ) -> None:
        super().__init__(name, frequency)
        self.field = field
        self.value = value
        self.where_clause = where_clause
        self.arguments = arguments
        if where_clause:
            self.pass_where_clause(self.where_clause)

    @classmethod
    def is_type(self, attribs: Dict, table_name: str = "") -> bool:
        if attribs["action"].lower() != "set":
            return False
        validate_keys(
            dictionary=attribs,
            required_keys=["name", "action", "field", "value", "frequency"],
            optional_keys=["where_condition", "arguments"],
            additional_context=f"Set action requires 'name', 'field', 'value' and 'frequency' and an optional 'where_condition' - Table {table_name} - Action `{attribs.get('name', '')}`",
        )
        if not (attribs["frequency"] > 0 and attribs["frequency"] <= 1):
            raise InvalidValueError("Frequency must be between 0 and 1")

        return True

    def pass_where_clause(self, where_clause: str):
        tokens = None
        for condition in EQUALITIES:
            if condition in where_clause:
                tokens = where_clause.split(condition)
                self.where_condition = condition
                break
        if not tokens:
            raise InvalidValueError(f"Invalid where condition - {self.where_condition}")

        self.where_table = tokens[0].split(".")[0].replace(" ", "")
        self.where_field = tokens[0].split(".")[1].replace(" ", "")
        self.where_value = Imposter(tokens[1].replace(" ", ""))

    def __str__(self):
        return f"{self.name} {self.frequency} {self.field} {self.value} {self.where_condition}"

    def __repr__(self):
        return f"{type(self).__name__}({self.__dict__})"
