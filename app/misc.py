from __future__ import annotations

import yaml


def read_yaml(filepath: str) -> dict:
    with open(filepath) as yaml_file:
        try:
            yaml_data = yaml.safe_load(yaml_file)
            return yaml_data
        except yaml.YAMLError as exc:
            print(exc)


def hex_to_string(hex_string: str) -> str:
    try:
        asset_name = bytearray.fromhex(hex_string)
        asset_name = asset_name.replace(b'\x00', b' ')
        asset_name = asset_name.replace(b"'", b"''")
        asset_name = asset_name.decode()
    except UnicodeDecodeError:
        asset_name = hex_string
    return asset_name
