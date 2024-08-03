from typing import List

import yaml
from pathlib import Path

from .exceptions import InvalidConfigSettingError
from .table import Table
from .field import Field
from .imposter import Imposter, ImposterType
from .action import Create, Remove, Set


class Config:
    """
    Config class to load the config file and generate table objects
    """

    DELETE_BEHAVIOURS = ["HARD", "SOFT"]

    def __init__(self, config_path: str):
        """Init class variables and load the config file

        Args:
            config_path (str): path to config file

        Raises:
            ValueError: _description_
        """
        with open(config_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)
            self.db_path = self.config["db_path"]
            self.output_format = self.config["output"]["format"]
            self.output_path = self.config["output"]["path"]
            self.delete_behaviour = self.config["delete_behaviour"].upper()
            self.inter_action_delay = self.config["inter_action_delay"]

            if self.delete_behaviour not in Config.DELETE_BEHAVIOURS:
                raise InvalidConfigSettingError(
                    "Invalid delete behaviour, either 'HARD' or 'SOFT'"
                )

    def create_output_folders(self, table_names: List[str]):
        """Generate the output folders for the tables

        Args:
            table_names (List[str]): List of table names
        """
        for table_name in table_names:
            Path(f"{self.output_path}/{table_name}").mkdir(parents=True, exist_ok=True)

    def load_datasets(self) -> List[Table]:
        """Load the datasets from the config file

        Returns:
            List[Table]: List of Table objects
        """

        if "tables" not in self.config:
            raise InvalidConfigSettingError(
                "'tables' not found in config, consult README for sample config"
            )

        tables = []
        for table in self.config["tables"]:
            fields = []
            actions = []
            table_name = table.get("name", None)
            if table_name is None:
                raise InvalidConfigSettingError("Table name missing in config")

            if "fields" not in table:
                raise InvalidConfigSettingError(
                    f"'fields' not found in table {table_name}, consult README for sample config"
                )

            for field in table["fields"]:
                if field.get("name", None) is None:
                    raise InvalidConfigSettingError(
                        f"Field name not found in table `{table_name}`"
                    )
                if Field.is_valid(field, table_name):
                    fields.append(
                        Field(
                            field["name"],
                            field["type"],
                            field["value"],
                            field.get("is_pk", False),
                            table_name,  # passed so as to provide better error messages
                            field.get("arguments", []),
                        )
                    )

            for action in table["actions"]:
                if Create.is_valid(action, table_name):
                    actions.append(
                        Create(
                            action.get("name", None),
                            action.get("frequency", None),
                        ),
                    )
                elif Remove.is_valid(action, table_name):
                    actions.append(
                        Remove(
                            action.get("name", None),
                            action.get("frequency", None),
                            action.get("where_condition", None),
                        )
                    )
                elif Set.is_valid(action, table_name):

                    actions.append(
                        Set(
                            action.get("name", None),
                            action.get("field", None),
                            Imposter(
                                action.get("value", None), action.get("arguments", [])
                            ),
                            action.get("where_condition", None),
                            action.get("frequency", None),
                        )
                    )

            tables.append(Table(table.get("name", None), fields, actions))

        self._validate_table_config(tables)

        return tables

    def _validate_table_config(self, tables: List[Table]):

        def _get_table_by_name(table_name: str):
            for table in tables:
                if table.table_name == table_name:
                    return table
            return None

        # validate all fields metioned in where statements and actions match the fields in the tables

        for table in tables:
            for field in table.fields:
                if field.imposter.imposter_type == ImposterType.TABLE_RANDOM:
                    fields = (
                        field.imposter.value.split("(")[1]
                        .replace(")", "")
                        .replace(" ", "")
                        .split(",")
                    )
                    if len(fields) != 3:
                        raise InvalidConfigSettingError(
                            f"where condition `{field.imposter.value}` is invalid"
                        )
                    if not _get_table_by_name(fields[0]):
                        raise InvalidConfigSettingError(
                            f"Table `{fields[0]}` not found in config for field value `{field.imposter.value}`"
                        )
                    if not _get_table_by_name(fields[0]).get_field_by_name(fields[1]):
                        raise InvalidConfigSettingError(
                            f"Field `{fields[1]}` not found in table `{fields[0]}` for field value `{field.imposter.value}`"
                        )

            for action in table.actions:
                if (
                    isinstance(action, Set) or isinstance(action, Remove)
                ) and action.where_clause is not None:
                    if _get_table_by_name(action.where_table) is None:
                        raise InvalidConfigSettingError(
                            f"Table `{action.where_table}` not found in config"
                        )
                    if (
                        _get_table_by_name(action.where_table).get_field_by_name(
                            action.where_field
                        )
                        is None
                    ):
                        raise InvalidConfigSettingError(
                            f"Field `{action.where_field}` not found in table `{action.where_table}`"
                        )
                    if action.where_value.imposter_type == ImposterType.TABLE_RANDOM:
                        fields = (
                            action.where_clause.split("(")[1]
                            .replace(")", "")
                            .replace(" ", "")
                            .split(",")
                        )
                        if len(fields) != 3:
                            raise InvalidConfigSettingError(
                                f"where condition {action.where_value} is invalid"
                            )
                        if not _get_table_by_name(fields[0]):
                            raise InvalidConfigSettingError(
                                f"Table `{fields[0]}` not found in config for where condition `{action.where_value}`"
                            )
                        if not _get_table_by_name(fields[0]).get_field_by_name(
                            fields[1]
                        ):
                            raise InvalidConfigSettingError(
                                f"Field `{fields[1]}` not found in table `{fields[0]}` for where condition `{action.where_value}`"
                            )

        return True
