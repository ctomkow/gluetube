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

    @abstractmethod
    def parse(self):
        """Implement this method to parse the configuration file specifics.
           Access the configuration file elements with self.config"""
        pass


class Systems(Parser):

    def parse(self) -> None:
        # TODO: netglue system config file
        pass
