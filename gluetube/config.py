# Craig Tomkow
# 2022-08-03

# local imports
import exception

# python imports
import configparser
from pathlib import Path


class Gluetube:

    cfg_path = Path

    def __init__(self, conf_locations: list) -> None:

        self.config = configparser.ConfigParser()

        try:
            filename = self.config.read(conf_locations)
        except configparser.ParsingError as e:
            raise exception.ConfigFileParseError(f"Config file reading error {conf_locations}") from e

        if not filename:
            raise exception.ConfigFileNotFoundError(conf_locations)

        self.cfg_path = Path(filename.pop())

    def parse(self) -> None:

        try:
            self.pipeline_dir = self.config['gluetube']['PIPELINE_DIR']
            self.pipeline_scan_interval = self.config['gluetube']['PIPELINE_SCAN_INTERVAL']
            self.sqlite_dir = self.config['gluetube']['SQLITE_DIR']
            self.sqlite_app_name = self.config['gluetube']['SQLITE_APP_NAME']
            self.sqlite_kv_name = self.config['gluetube']['SQLITE_KV_NAME']
            self.sqlite_token = self.config['gluetube']['SQLITE_TOKEN']
            self.socket_file = self.config['gluetube']['SOCKET_FILE']
            self.pid_file = self.config['gluetube']['PID_FILE']
            self.gluetube_log_file = self.config['gluetube']['GLUETUBE_LOG_FILE']
            self.http_proxy = self.config['gluetube']['HTTP_PROXY']
            self.https_proxy = self.config['gluetube']['HTTPS_PROXY']
        except KeyError as e:
            raise exception.ConfigFileParseError(f"Failed to lookup key, {e}, in config file") from e

    def write(self) -> None:

        with open(self.cfg_path.resolve().as_posix(), 'w') as configfile:
            self.config.write(configfile)
