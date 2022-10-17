# Craig Tomkow
# 2022-08-03

# local imports
import exceptions

# python imports
import configparser
from abc import ABC, abstractmethod

# 3rd party imports


class Parser(ABC):

    def __init__(self, conf_locations: list) -> None:

        self.config = configparser.ConfigParser()

        try:
            self.filename = self.config.read(conf_locations)
        except configparser.ParsingError:
            raise

        if not self.filename:
            raise exceptions.ConfigFileNotFoundError(conf_locations)

        self.parse()

    @abstractmethod
    def parse(self):
        """Implement this method to parse the configuration file specifics.
           Access the configuration file elements with self.config"""
        pass


class Gluetube(Parser):

    def parse(self) -> None:

        try:
            self.pipeline_dir = self.config['gluetube']['PIPELINE_DIR']
            self.database_dir = self.config['gluetube']['DATABASE_DIR']
        except KeyError as e:
            raise exceptions.ConfigFileParseError(f"Failed to lookup key, {e}", self.filename) from e
