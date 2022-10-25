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
        except configparser.ParsingError as e:
            raise exceptions.ConfigFileParseError("Config file reading error.", self.filename) from e

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
            self.pipeline_scan_interval = self.config['gluetube']['PIPELINE_SCAN_INTERVAL']
            self.database_dir = self.config['gluetube']['DATABASE_DIR']
            self.socket_file = self.config['gluetube']['SOCKET_FILE']
            self.pid_file = self.config['gluetube']['PID_FILE']
        except KeyError as e:
            raise exceptions.ConfigFileParseError(f"Failed to lookup key, {e}", self.filename) from e
