from __future__ import annotations

import logging

from db import Db
from settings import settings

logger = logging.getLogger('pantasia-db-sync')


# Class to maintain an index of reference_keys (natural keys) to primary keys
class IdIndex:
    def __init__(self, table_name: str, reference_key: str, database: Db) -> None:
        # Get config to determine whether to use full in_memory_index
        self.config = settings.in_memory_index
        self.table_name = table_name
        self.reference_key = reference_key
        self.db = database
        if self.config is True:
            # Load full index of keys from the database
            self.id_index = self._pantasia_load_id_map()
        else:
            # Init an empty dictionary, we still need a temporary index
            # for every period to check for duplicates, otherwise we'll
            # get duplicate error within the same bulk transaction
            self.id_index = {}

    def _pantasia_load_id_map(self) -> dict:
        # Load all IDs and reference key values from the database
        d_result = {}

        logger.info(f'Loading {self.table_name} data......')

        self.db.pantasia_cur.execute(
            f'SELECT id, {self.reference_key} FROM {self.table_name} ORDER BY id ASC',
        )
        self.db.pantasia_conn.commit()
        results = self.db.pantasia_cur.fetchall()

        for result in results:
            d_result[result[self.reference_key]] = result['id']

        logger.info(
            f'Load {self.table_name} data, '
            f'reference natural key: {self.reference_key}, '
            f'{len(results)} items found and loaded',
        )

        return d_result

    def clear_index(self) -> None:
        # Reset the index to empty dict
        self.id_index = {}

    def get(self, reference_value: any) -> int | None:
        # Get ID from index, returns None if not found
        index_id = self.id_index.get(reference_value)

        # If In-Memory Index is turned on, return the ID or None
        if self.config is True:
            return index_id
        # If In-Memory Index is turned off, return the ID from index if found
        elif self.config is False and index_id is not None:
            return index_id
        # If In-Memory Index is turned off, and ID not found, try to get from DB
        elif self.config is False and index_id is None:
            if type(reference_value) is str:
                reference_value = f"'{reference_value}'"
            self.db.pantasia_cur.execute(
                f'SELECT id FROM {self.table_name} '
                f'WHERE {self.reference_key} '
                f'IN ({reference_value})',
            )
            result = self.db.pantasia_cur.fetchone()
            if result is not None:
                return result['id']
            else:
                return None

    def set(self, index_id: int, reference_value: any) -> None:
        # Set the ID mapped to reference key value in the index
        self.id_index[reference_value] = index_id
