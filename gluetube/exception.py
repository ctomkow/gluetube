# Craig Tomkow
# 2022-08-03

# config file errors


class ConfigFileBaseError(Exception):

    """ Base config file exception. Call a more specific exception that inherits this one"""

    def __init__(self, msg) -> None:
        super().__init__(msg)


class ConfigFileNotFoundError(ConfigFileBaseError):

    """ Raise for the configuration file not being found"""

    def __init__(self, cfg_file_locations: list) -> None:
        msg = "The configuration file could not be found in any of the " \
              f"following os path locations: {cfg_file_locations}"
        super().__init__(msg)


class ConfigFileParseError(ConfigFileBaseError):

    """ Raise for errors when parsing the configuration file"""

    def __init__(self, msg: str) -> None:
        msg = f"{msg}. Failed to parse config file"
        super().__init__(msg)

# Pipeline specific errors


class PipelineError(Exception):

    """ Base pipeline exception. Call a more specific exception that inherits this one"""

    def __init__(self, msg) -> None:
        super().__init__(msg)


class PipelineInitializationError(PipelineError):

    """ Raise for pipeline initialization error. Common use cause would be when the pipeline __init__ fails"""

    def __init__(self) -> None:
        msg = "The pipeline failed to start"
        super().__init__(msg)


class PipelineExtractionError(PipelineError):

    """ Raise for pipeline data extraction failing"""

    def __init__(self) -> None:
        msg = "The pipeline extraction failed"
        super().__init__(msg)


class PipelineTransformationError(PipelineError):

    """ Raise for pipeline data transformation failing"""

    def __init__(self) -> None:
        msg = "The pipeline data transformation failed"
        super().__init__(msg)


class PipelineValidationError(PipelineError):

    """ Raise for pipeline data validation failing"""

    def __init__(self) -> None:
        msg = "The pipeline data validation failed"
        super().__init__(msg)


class PipelineLoadingError(PipelineError):

    """ Raise for pipeline data loading failing"""

    def __init__(self) -> None:
        msg = "The pipeline data loading failed"
        super().__init__(msg)

# runner exceptions


class RunnerError(Exception):

    """ Base runner exception. Call a more specific exception that inherits this one"""

    def __init__(self, msg) -> None:
        super().__init__(msg)

# db exceptions


class dbError(Exception):

    """ Base database exception. Call a more specific exception that inherits this one"""

    def __init__(self, msg) -> None:
        super().__init__(msg)

# daemon exceptions


class DaemonError(Exception):

    """ Base daemon exception. Call a more specific exception that inherits this one"""

    def __init__(self, msg) -> None:
        super().__init__(msg)

# rpc exceptions


class rpcError(Exception):

    """ Base RPC exception. Call a more specific exception that inherits this one"""

    def __init__(self, msg) -> None:
        super().__init__(msg)

# auto-discovery execeptions


class AutodiscoveryError(Exception):

    """ Base autodiscovery exception. Call a more specific exception that inherits this one"""

    def __init__(self, msg) -> None:
        super().__init__(msg)
