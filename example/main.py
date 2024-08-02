import logging
import click

from src.southwind import SouthWind

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
def southwind(config: str):
    click.echo(f"Loading config from {config}")
    SouthWind(config).execute()


if __name__ == "__main__":
    southwind()
