# https://github.com/mrin9/northwind/blob/master/NorthwindModel_Small.png

import logging
import click

from src.southern_gale import SouthernGale

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@click.command()
@click.option(
    "--config",
    prompt="path to config",
    help="path to yaml config file",
    type=click.Path(),
    default="config.yaml",
)
def northwind(config: str):
    click.echo(f"Loading config from {config}")
    SouthernGale(config).execute()


if __name__ == "__main__":
    northwind()
