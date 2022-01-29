import logging
import os
import warnings
from typing import Any, Tuple, Union


class InvalidLoggerLevelName(UserWarning):
    """Raised when a logger level name is not valid."""

    ...


class LoggingMixin:
    """A mixin class to add logging interface."""

    SUDO_ROOT_LOGGER_NAME = "oib"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[call-arg]
        self._init_sudo_root_logger(
            logger_name=self.SUDO_ROOT_LOGGER_NAME,
            env_var_name_of_level="STREAM_SERVING_LOGGER_LEVEL",
            default_level=logging.DEBUG,
        )

    def _init_sudo_root_logger(
        self,
        logger_name: str,
        env_var_name_of_level: str = "",
        default_level: Union[int, str] = logging.DEBUG,
    ) -> None:
        """Initialize the customized root logger in the given logger name
        to separate the logging from the root logger.

        Parameters
        ----------
        logger_name : str
            The name of the logger.
        env_var_name_of_level : str
            The name of the environment variable to read the logger level from.
            If "", the default level is used.
        default_level : Union[int, str]
            The default level of the logger.
        """
        assert isinstance(logger_name, str)

        sudo_root_logger = logging.getLogger(logger_name)

        if not sudo_root_logger.hasHandlers():
            levelname, levelname_from = self._logger_levelname_from_env(
                env_var_name=env_var_name_of_level,
                default=default_level,
            )

            formatter = logging.Formatter(
                fmt="[{asctime}.{msecs:03.0f}] "
                "{levelname:>8}:{name:<10} "
                "{filename} {funcName}:{lineno} - {message}",
                datefmt="%Y-%m-%d %H:%M:%S",
                style="{",
            )

            stream_handler = logging.StreamHandler()  # stream=sys.stderr by default
            stream_handler.setLevel(levelname)
            stream_handler.setFormatter(formatter)

            sudo_root_logger.setLevel(levelname)
            sudo_root_logger.addHandler(stream_handler)
            sudo_root_logger.propagate = (
                False  # to use this logger as a sudo-root logger
            )

            sudo_root_logger.info("Logger `%s` is initialized.", logger_name)
            sudo_root_logger.info(
                "Level of the logger `%s` "
                "and its handler is set to %s from %s settings",
                logger_name,
                levelname,
                levelname_from,
            )

    def _set_logger_level_from_env(
        self,
        logger: logging.Logger,
        default_level: Union[int, str],
        env_var_name_of_level: str = "",
    ) -> None:
        """Set the level of the given logger to the level from the environment variable.
        If the environment variable is not set, use the default level.

        Parameters
        ----------
        logger : logging.Logger
            The logger to set the level.
        default_level : Union[int, str]
            The default level of the logger.
        env_var_name_of_level : str
            The name of the environment variable to read the logger level from.
            If "", the default level is used.
        """
        assert isinstance(logger, logging.Logger)

        levelname, levelname_from = self._logger_levelname_from_env(
            env_var_name=env_var_name_of_level,
            default=default_level,
        )

        logger.setLevel(levelname)

        logger.info(
            "Level of the logger `%s` is set to %s from %s settings",
            logger.name,
            levelname,
            levelname_from,
        )

    # pylint: disable=protected-access
    @staticmethod
    def _logger_levelname_from_env(
        env_var_name: str,
        default: Union[int, str],
    ) -> Tuple[str, str]:
        """
        get the logger level from the environment variable.
        if the environment variable is not set, use the default level.

        Parameters
        ----------
        env_var_name : str
            the name of the environment variable for the logger level
        default : Union[int, str]
            the default value for logger level if the environment variable is not set
            - one of "NOTSET", "DEBUG", "INFO", "WARNING",
                     "WARN", "ERROR", "CRITICAL", "FATAL".

        Returns
        -------
        Tuple[int, str]
            (logger levelname, where the levelname is from)
        """
        assert isinstance(env_var_name, str)
        assert isinstance(default, (int, str))

        if isinstance(default, str):
            assert default in logging._nameToLevel, f"Invalid {default=}"
            default_name = default
        elif isinstance(default, int):  # if `default` is a logger level number.
            assert default in [0, 10, 20, 30, 40, 50], f"Invalid {default=}"
            default_name = logging.getLevelName(default)

        levelname_env = os.environ.get(env_var_name, None)
        levelname = default_name
        levelname_from = "default"

        if levelname_env in logging._nameToLevel:
            levelname = levelname_env
            levelname_from = "env"
        elif levelname_env is not None:
            warnings.warn(
                f"`{levelname_env}` is not a valid logger level. "
                f"The level is set to {default_name=} instead.",
                InvalidLoggerLevelName,
            )

        return levelname, levelname_from

    @property
    def sudo_root_logger(self) -> logging.Logger:
        """Return the sudo-root logger."""
        return logging.getLogger(self.SUDO_ROOT_LOGGER_NAME)

    def module_logger(self, module_name: str) -> logging.Logger:
        """Return the module-level logger, which is child of the sudo-root logger."""
        assert isinstance(module_name, str)
        return self.sudo_root_logger.getChild(module_name)

    @property
    def logger(self) -> logging.Logger:
        """Return the class-level logger, which is child of the sudo-root logger."""
        return self.sudo_root_logger.getChild(self.__class__.__qualname__)
