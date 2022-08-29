from __future__ import annotations

import logging.config
import os
import traceback
from pathlib import PurePath
from signal import SIGINT
from signal import signal
from signal import SIGTERM
from time import sleep
from time import time
from typing import Callable

from cardano import get_staking_address
from db import Db
from db import IdIndex
from misc import hex_to_string
from misc import read_yaml
from psycopg2 import DataError
from psycopg2 import IntegrityError
from psycopg2 import InternalError
from psycopg2.extras import Json
from settings import settings


def run(database):
    # Initialize and load data from Pantasia DB
    d_asset_id_x_fingerprint = IdIndex('asset', 'fingerprint', database)
    d_wallet_id_x_address = IdIndex('wallet', 'address', database)
    d_collection_id_x_policy_id = IdIndex('collection', 'policy_id', database)
    d_asset_id_x_asset_ext = IdIndex('asset_ext', 'asset_id', database)

    # Get latest index (id) numbers for each table
    index_asset = database.pantasia_get_last_index('asset')
    index_asset_mint_tx = database.pantasia_get_last_index('asset_mint_tx')
    index_asset_tx = database.pantasia_get_last_index('asset_tx')
    index_collection = database.pantasia_get_last_index('collection')
    index_wallet = database.pantasia_get_last_index('wallet')

    from_datetime = None
    period_list = [database.pantasia_tip]

    while True:
        database.get_latest_cardano_tip()
        database.get_latest_pantasia_tip()

        # Create periods of length $time_interval
        if database.cardano_tip != database.old_cardano_tip:
            period_list = database.create_period_list(period_list)
            database.old_cardano_tip = database.cardano_tip
        else:
            # Pause 10 seconds so that Postgres doesn't get spammed
            sleep(10)

        initial_len = len(period_list)

        while len(period_list) > 1:
            start_time = time()

            if settings.in_memory_index is False:
                # Clear index dictionaries
                d_asset_id_x_fingerprint.clear_index()
                d_wallet_id_x_address.clear_index()
                d_collection_id_x_policy_id.clear_index()
                d_asset_id_x_asset_ext.clear_index()

            # Init lists as containers for data values to be inserted to Pantasia DB
            values_insert_wallet = []
            values_insert_collection = []
            values_insert_asset_mint_tx = []
            values_insert_asset_tx = []
            values_insert_asset = []
            values_insert_asset_ext = []
            values_update_asset_ext_latest_mint_tx_id = []
            values_update_asset_ext_latest_tx_id = []
            values_update_asset_current_wallet_id = []

            # If new element from period_list not the same as the previous,
            # then move the index and get records
            if period_list[0] != from_datetime:
                from_datetime = period_list.pop(0)
                to_datetime = period_list[0]

                # Some math for completion time estimation
                current_count = initial_len - len(period_list)

                logger.info(
                    f'period - {current_count}/{initial_len - 1} '
                    f'| FROM: {from_datetime} | TO: {to_datetime}',
                )

                # Retrieve records from Cardano DB
                time_started = time()
                records = database.pantasia_get_records(to_datetime, from_datetime)
                time_elapsed = time()
                logger.debug(
                    '{execute} running time is {s} seconds for retrieving {rows} rows.'
                    .format(
                        execute='main_query',
                        s=round(time_elapsed - time_started, 4),
                        rows=len(records),
                    ),
                )

                logger.info(f'Processing {len(records)} rows......')

                time_started = time()
                # Loop through records and process them
                for record in records:
                    # Add address to wallet table
                    r_address = record['address']

                    if r_address is not None:
                        # Get staking address from payment address if not return None
                        r_stake_address = get_staking_address(r_address)

                        if r_stake_address is None:
                            # Get index of payment address if already existing in bidict
                            address_index = d_wallet_id_x_address.get(
                                r_address,
                            )

                            # Add new row if can't find in bidict
                            if address_index is None:
                                # Assign new index number,
                                # update bidict and add to values
                                # to insert new row in wallet table
                                address_index = index_wallet
                                d_wallet_id_x_address.set(
                                    address_index, r_address,
                                )
                                r_address_type = 'ENTERPRISE'
                                values_insert_wallet.append(
                                    (address_index, r_address, r_address_type),
                                )

                                # Increment index number for next record
                                index_wallet = index_wallet + 1

                        else:
                            # Get index of stake address if already existing in bidict
                            address_index = d_wallet_id_x_address.get(
                                r_stake_address,
                            )

                            # Add new row if can't find in bidict
                            if address_index is None:
                                # Assign new index number,
                                # update bidict and add to values
                                # to insert new row in wallet table
                                address_index = index_wallet
                                d_wallet_id_x_address.set(
                                    address_index, r_stake_address,
                                )
                                r_address_type = 'STAKE'
                                values_insert_wallet.append(
                                    (address_index, r_stake_address, r_address_type),
                                )

                                # Increment index number for next record
                                index_wallet = index_wallet + 1
                    else:
                        # Assign null value to address index,
                        # this is expected for burn tx (mint tx with negative quantity)
                        address_index = 'Null'

                    # Add policy id to collection table
                    r_policy_id = record['policy_id']

                    # Get index of policy id if already existing in bidict
                    policy_index = d_collection_id_x_policy_id.get(
                        r_policy_id,
                    )

                    # Add new row if can't find in bidict
                    if policy_index is None:
                        # Assign new index number,
                        # update bidict and add to values
                        # to insert new row in collection table
                        policy_index = index_collection
                        d_collection_id_x_policy_id.set(
                            policy_index, r_policy_id,
                        )
                        values_insert_collection.append(
                            (policy_index, r_policy_id),
                        )

                        # Increment index number for next record
                        index_collection = index_collection + 1

                    # Process asset, asset_mint_tx and asset_tx
                    is_mint_tx = record['is_mint_tx']
                    # Get index of asset if already existing in bidict
                    asset_fingerprint_index = d_asset_id_x_fingerprint.get(
                        record['asset_fingerprint'],
                    )

                    # Process asset_mint_tx
                    if is_mint_tx is True:
                        # Get index of asset_mint_tx for a new row
                        asset_mint_tx_index = index_asset_mint_tx

                        # Add new row if can't find in bidict
                        if asset_fingerprint_index is None:
                            # Assign new index number,
                            # update bidict and add to values
                            # to insert new row in asset table
                            asset_fingerprint_index = index_asset
                            d_asset_id_x_fingerprint.set(
                                asset_fingerprint_index,
                                record['asset_fingerprint'],
                            )
                            values_insert_asset.append((
                                asset_fingerprint_index,
                                policy_index,
                                f"{record['policy_id']}."
                                f"{str(record['asset_name_hash'])}",
                                hex_to_string(str(record['asset_name_hash'])),
                                record['asset_fingerprint'],
                                address_index,
                            ))

                            # Increment index number for next record
                            index_asset = index_asset + 1

                        # Update latest_mint_tx_id in asset
                        # if it is a mint tx, except burn tx
                        if record['quantity'] > 0:
                            if d_asset_id_x_asset_ext.get(
                                    asset_fingerprint_index,
                            ) is not None:
                                # Update asset entry with latest_mint_tx_id
                                values_update_asset_ext_latest_mint_tx_id.append(
                                    (asset_fingerprint_index, asset_mint_tx_index),
                                )
                            else:
                                # Add to values to insert new row in asset_ext table
                                values_insert_asset_ext.append(
                                    (
                                        asset_fingerprint_index,
                                        asset_fingerprint_index,
                                        asset_mint_tx_index,
                                        'Null',
                                    ),
                                )
                                d_asset_id_x_asset_ext.set(
                                    asset_fingerprint_index, asset_fingerprint_index,
                                )

                        # Add to values to insert new row in asset_mint_tx table
                        values_insert_asset_mint_tx.append(
                            (
                                asset_mint_tx_index,
                                asset_fingerprint_index,
                                address_index,
                                record['quantity'],
                                record['tx_hash'],
                                record['tx_time'],
                                record['image'],
                                Json(record['metadata']),
                                Json(record['files']),
                            ),
                        )

                        # Increment index number for next record
                        index_asset_mint_tx = index_asset_mint_tx + 1

                    # Process asset_tx
                    else:
                        # Get index of asset_tx for a new row
                        asset_tx_index = index_asset_tx

                        # Add new row if can't find in bidict
                        if asset_fingerprint_index is None:
                            # Assign new index number, update bidict
                            # and add to values to insert new row in asset table
                            asset_fingerprint_index = index_asset
                            d_asset_id_x_fingerprint.set(
                                asset_fingerprint_index, record['asset_fingerprint'],
                            )
                            values_insert_asset.append((
                                asset_fingerprint_index, policy_index,
                                f"{record['policy_id']}.{record['asset_name_hash']}",
                                hex_to_string(str(record['asset_name_hash'])),
                                record['asset_fingerprint'], address_index,
                            ))

                            # Increment index number for next record
                            index_asset = index_asset + 1
                        else:
                            # Update asset entry with current_wallet_id
                            values_update_asset_current_wallet_id.append(
                                (asset_fingerprint_index, address_index),
                            )

                        if d_asset_id_x_asset_ext.get(
                                asset_fingerprint_index,
                        ) is not None:
                            # Update asset entry with latest_tx_id
                            values_update_asset_ext_latest_tx_id.append(
                                (asset_fingerprint_index, asset_tx_index),
                            )
                        else:
                            # Add to values to insert new row in asset_ext table
                            values_insert_asset_ext.append(
                                (
                                    asset_fingerprint_index,
                                    asset_fingerprint_index,
                                    'Null',
                                    asset_tx_index,
                                ),
                            )
                            d_asset_id_x_asset_ext.set(
                                asset_fingerprint_index, asset_fingerprint_index,
                            )

                        # Add to values to insert new row in asset_tx table
                        values_insert_asset_tx.append(
                            (
                                asset_tx_index,
                                asset_fingerprint_index,
                                address_index,
                                record['quantity'],
                                record['tx_hash'],
                                record['tx_time'],
                            ),
                        )

                        # Increment index number for next record
                        index_asset_tx = index_asset_tx + 1
                time_elapsed = time()
                logger.debug(
                    '{execute} running time is {s} seconds for processing {rows} rows.'
                    .format(
                        execute='Processing',
                        s=round(time_elapsed - time_started, 4),
                        rows=len(records),
                    ),
                )

                # Batch insert values into tables
                if len(values_insert_wallet) > 0:
                    database.pantasia_insert_wallet(
                        values=values_insert_wallet,
                    )
                if len(values_insert_collection) > 0:
                    database.pantasia_insert_collection(
                        values=values_insert_collection,
                    )
                if len(values_insert_asset) > 0:
                    database.pantasia_insert_asset(
                        values=values_insert_asset,
                    )
                if len(values_insert_asset_mint_tx) > 0:
                    database.pantasia_insert_asset_mint_tx(
                        values=values_insert_asset_mint_tx,
                    )
                if len(values_insert_asset_tx) > 0:
                    database.pantasia_insert_asset_tx(
                        values=values_insert_asset_tx,
                    )
                if len(values_insert_asset_ext) > 0:
                    database.pantasia_insert_asset_ext(
                        values=values_insert_asset_ext,
                    )
                if len(values_update_asset_ext_latest_mint_tx_id) > 0:
                    database.pantasia_update_asset_ext_latest_mint_tx_id(
                        values=values_update_asset_ext_latest_mint_tx_id,
                    )
                if len(values_update_asset_ext_latest_tx_id) > 0:
                    database.pantasia_update_asset_ext_latest_tx_id(
                        values=values_update_asset_ext_latest_tx_id,
                    )
                if len(values_update_asset_current_wallet_id) > 0:
                    database.pantasia_update_asset_current_wallet_id(
                        values=values_update_asset_current_wallet_id,
                    )

                database.pantasia_conn.commit()

                logger.info(f'{len(records)} rows updated in database.')

                time_difference = time() - start_time
                count_difference = len(records)
                proc_rate = count_difference / time_difference
                logger.debug(
                    f'{round(proc_rate, 2):.2f} record(s)/s',
                )


class GracefulKiller:
    def __init__(self, func: Callable):
        self.func = func
        signal(SIGINT, self.exit_gracefully)
        signal(SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.func()
        quit()


if __name__ == '__main__':
    # Get absolute path of this file
    current_dir = PurePath(__file__).parent.parent
    log_config_path = os.path.join(current_dir, 'logging.yaml')

    # Read logging config
    log_config = read_yaml(log_config_path)
    log_config['loggers']['pantasia-db-sync']['level'] = settings.log_level
    logging.config.dictConfig(log_config)

    # Create logger
    logger = logging.getLogger('pantasia-db-sync')

    logger.info(f'pantasia-db-sync ({settings.environment}) is starting...')

    # Initialize Db connections to Cardano DB and Pantasia DB
    db = Db(settings)

    killer = GracefulKiller(db.close_connections)
    try:
        run(db)
    except (IntegrityError, DataError, InternalError, TypeError, MemoryError, OSError):
        logger.exception(traceback.format_exc())
        db.close_connections()
