from unittest.mock import patch

from app.logger import setup_logging


def test_setup_logging_json_format():
    """Тест настройки логирования в JSON формате"""
    with patch("app.logger.settings") as mock_settings:
        mock_settings.log_format = "json"
        mock_settings.log_level = "INFO"
        setup_logging()


def test_setup_logging_console_format():
    """Тест настройки логирования в консольном формате"""
    with patch("app.logger.settings") as mock_settings:
        mock_settings.log_format = "console"
        mock_settings.log_level = "DEBUG"
        setup_logging()
