import logging
import os
import sys


class LoggerFactory:
    """Utility class to configure and retrieve named loggers with separate log files."""

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Instance a logging entity."""
        log_dir = os.getenv("LOG_DIR", "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"{name}.log")

        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        # Get or create logger
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        # Avoid re-adding handlers
        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            logger.propagate = False

        return logger
