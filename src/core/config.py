import yaml
from src.core.logger import get_logger

logger = get_logger(__name__)

def load_config(path_config:str="config/app_config.yaml") -> dict:
    return dict(1,2)

def load_mapping(table_name:str, mapping_dir:str="config/mapping") -> dict:
    