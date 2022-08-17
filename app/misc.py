from __future__ import annotations

import os
import re

import yaml


def read_yaml(filepath: str) -> dict:
    def path_constructor(loader, node):
        # Extract the matched value, expand env variable, and replace the match
        value = node.value
        match = path_matcher.match(value)
        env_var = match.group()[2:-1]
        return os.environ.get(env_var) + value[match.end():]

    path_matcher = re.compile(r'\${([^}^{]+)}')
    yaml.add_implicit_resolver('!path', path_matcher, None, yaml.SafeLoader)
    yaml.add_constructor('!path', path_constructor, yaml.SafeLoader)

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
