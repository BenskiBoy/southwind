from typing import Dict, Union, List
from .exceptions import InvalidValueError, validate_keys
from .field import Field
from .imposter import Imposter


class Action:
    """Parent class for all actions. Action is performs an action on a table"""

    REQUIRED_CONFIG_KEYS = ["name", "action", "frequency"]
    OPTIONAL_CONFIG_KEYS = []
    EQUALITIES = ["==", ">", "<", ">=", "<=", "!="]

    def __init__(self, name: str, frequency: float, arguments: List[str | int] = []):
        self.name = name
        self.frequency = frequency
        self.arguments = arguments

    def _pass_where_clause(self, where_clause: str):
        tokens = None
        for condition in Action.EQUALITIES:
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
    def get_type(self, attribs: Dict):
        if Create.is_valid(attribs):
            return Create
        elif Remove.is_valid(attribs):
            return Remove
        elif Set.is_valid(attribs):
            return Set
        else:
            raise NotImplementedError()

    @classmethod
    def is_valid(self, attribs: Dict):
        raise NotImplementedError()

    def __str__(self):
        return f"{self.name}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.__dict__})"


class Create(Action):
    """Create action to create a new record in the table"""

    REQUIRED_CONFIG_KEYS = Action.REQUIRED_CONFIG_KEYS
    OPTIONAL_CONFIG_KEYS = Action.OPTIONAL_CONFIG_KEYS

    def __init__(
        self,
        name: str,
        frequency: float,
    ):
        super().__init__(name, frequency)

    @classmethod
    def is_valid(self, attribs: Dict, table_name: str = "") -> bool:
        if attribs["action"].lower() != "create":
            return False
        validate_keys(
            dictionary=attribs,
            required_keys=Create.REQUIRED_CONFIG_KEYS,
            optional_keys=Create.OPTIONAL_CONFIG_KEYS,
            additional_context=f"Create action requires {' and '.join(Create.REQUIRED_CONFIG_KEYS)} - Table {table_name} - Action `{attribs.get('name', '')}`",
        )
        if not (attribs["frequency"] > 0 and attribs["frequency"] <= 1):
            raise InvalidValueError("Frequency must be between 0 and 1")
        return True


class Remove(Action):
    """Remove action to remove a record from the table"""

    REQUIRED_CONFIG_KEYS = Action.REQUIRED_CONFIG_KEYS
    OPTIONAL_CONFIG_KEYS = Action.OPTIONAL_CONFIG_KEYS + ["where_condition"]

    def __init__(
        self,
        name: str,
        frequency: float,
        where_clause: str,
    ):
        super().__init__(name, frequency)

        self.where_clause = where_clause

        if where_clause:
            self._pass_where_clause(self.where_clause)

    @classmethod
    def is_valid(self, attribs: Dict, table_name: str) -> bool:
        if attribs["action"].lower() != "remove":
            return False
        validate_keys(
            dictionary=attribs,
            required_keys=Remove.REQUIRED_CONFIG_KEYS,
            optional_keys=Remove.OPTIONAL_CONFIG_KEYS,
            additional_context=f"Remove action requires [{','.join(Create.REQUIRED_CONFIG_KEYS)}] and an optional [{','.join(Create.OPTIONAL_CONFIG_KEYS)}] - Table {table_name} - Action `{attribs.get('name', '')}`",
        )
        if not (attribs["frequency"] > 0 and attribs["frequency"] <= 1):
            raise InvalidValueError("Frequency must be between 0 and 1")
        return True


class Set(Action):
    """Set action to set a field to a value in the table"""

    REQUIRED_CONFIG_KEYS = Action.REQUIRED_CONFIG_KEYS + ["field", "value"]
    OPTIONAL_CONFIG_KEYS = Action.OPTIONAL_CONFIG_KEYS + [
        "where_condition",
        "arguments",
    ]

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
            self._pass_where_clause(self.where_clause)

    @classmethod
    def is_valid(self, attribs: Dict, table_name: str = "") -> bool:
        if attribs["action"].lower() != "set":
            return False
        validate_keys(
            dictionary=attribs,
            required_keys=Set.REQUIRED_CONFIG_KEYS,
            optional_keys=Set.OPTIONAL_CONFIG_KEYS,
            additional_context=f"Set action requires {' and '.join(Set.REQUIRED_CONFIG_KEYS)} and an optional [{','.join(Set.OPTIONAL_CONFIG_KEYS)}] - Table {table_name} - Action `{attribs.get('name', '')}`",
        )
        if not (attribs["frequency"] > 0 and attribs["frequency"] <= 1):
            raise InvalidValueError("Frequency must be between 0 and 1")

        return True

    def __str__(self):
        return f"{self.name} {self.frequency} {self.field} {self.value} {self.where_condition}"

    def __repr__(self):
        return f"{type(self).__name__}({self.__dict__})"
