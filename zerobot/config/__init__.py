"""Configuration module for zerobot."""

from zerobot.config.loader import load_config, get_config_path
from zerobot.config.schema import Config

__all__ = ["Config", "load_config", "get_config_path"]
