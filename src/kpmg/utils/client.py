import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class Client:
    config_path: str = "config/config.yaml"
    config: Dict[str, Any] = field(init=False)

    def __post_init__(self):
        self.config = self._load_config()

    
    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        if not Path(self.config_path).exists():
            raise FileNotFoundError(f"Configuration file: {self.config_path} not found.")
    
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)
    

    def getConfig(self, key: str) ->Any:
        """Retrieve a configuration value using a dot-separated key."""
        keys = key.split(".")
        value = self.config

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            raise KeyError(f"Invalid configuration key: {key}")


# if __name__ == "__main__":
    # Example usage:
    # client = Client()
    # print(client.getConfig("subjects.credit_card.column_sequence"))
    # print(client.getConfig("subjects.credit_card.DataType.TransactionID"))
    # print(client.getConfig("subjects.credit_card.Constraints.TransactionAmount"))