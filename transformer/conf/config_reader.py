import os

import paramiko
import yaml


class ConfigReader:

    @staticmethod
    def read_config():
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            config["network"]["private_key"] = paramiko.RSAKey.from_private_key_file(
                os.path.join(os.path.dirname(__file__), config["network"]["private_key"]))
            return config

    config = read_config()
