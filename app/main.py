from __future__ import annotations

import logging.config
from time import sleep
from time import time

from psycopg2.extras import Json

from app.cardano import get_staking_address
from app.db import Db
from app.misc import hex_to_string
from app.misc import read_yaml


if __name__ == '__main__':
    # Read logging config
    logging.config.dictConfig(read_yaml('../logging.yaml'))

    # Create logger
    logger = logging.getLogger('pantasia-db-sync')

    # Read DB Config
    db_config = read_yaml('../dbconfig.yaml')

    # Initialize Db conections to Cardano DB and Pantasia DB
    db = Db(db_config)

    # Initialize and load data from Pantasia DB
    bd_asset_id_x_fingerprint = db.pantasia_load_id_map('asset', 'fingerprint')
    bd_wallet_id_x_address = db.pantasia_load_id_map('wallet', 'address')
    bd_collection_id_x_policy_id = db.pantasia_load_id_map(
        'collection', 'policy_id',
    )
    d_asset_id_x_asset_ext = db.pantasia_load_asset_ext_asset_id()

    # Get latest index (id) numbers for each table
    index_asset = db.pantasia_get_last_index('asset')
    index_asset_mint_tx = db.pantasia_get_last_index('asset_mint_tx')
    index_asset_tx = db.pantasia_get_last_index('asset_tx')
    index_collection = db.pantasia_get_last_index('collection')
    index_wallet = db.pantasia_get_last_index('wallet')

    is_startup = True
    from_datetime = None
    period_list = [db.pantasia_tip]

    while True:
        # Pause 10 seconds so that Postgres doesn't get spammed
        sleep(10)

        # If the service just started, rollback to prevent duplicates
        if is_startup:
            # Rollback last period to prevent duplicates
            db.pantasia_rollback()
            is_startup = False
        else:
            db.get_latest_cardano_tip()
            db.get_latest_pantasia_tip()

        # Create periods of length $time_interval
        if db.cardano_tip != db.old_cardano_tip:
            period_list = db.create_period_list(period_list)
            db.old_cardano_tip = db.cardano_tip

        initial_len = len(period_list)
        start_count = 0
        start_time = time()

        while len(period_list) > 1:

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

                time_difference = time() - start_time
                if time_difference > 15:
                    start_time = time()
                    count_difference = current_count - start_count
                    proc_rate = count_difference / time_difference
                    start_count = current_count
                    time_left = len(period_list) / proc_rate
                    logger.debug(
                        f'{round(proc_rate, 2):.2f} period/s - '
                        f'Estimated {int(time_left)} seconds to go',
                    )

                logger.info(
                    f'period_list_len - {current_count}/{initial_len - 1} '
                    f'| FROM: {from_datetime} | TO: {to_datetime}',
                )

                # Retrieve records from Cardano DB
                time_started = time()
                records = db.pantasia_get_records(to_datetime, from_datetime)
                time_elapsed = time()
                logger.debug(
                    '{execute} running time is {s} seconds for retrieving {rows} rows.'
                    .format(
                        execute='main_query',
                        s=round(time_elapsed - time_started, 4),
                        rows=len(records),
                    ),
                )

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
                            address_index = bd_wallet_id_x_address.inverse.get(
                                r_address,
                            )

                            # Add new row if can't find in bidict
                            if address_index is None:
                                # Assign new index number,
                                # update bidict and add to values
                                # to insert new row in wallet table
                                address_index = index_wallet
                                bd_wallet_id_x_address.put(
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
                            address_index = bd_wallet_id_x_address.inverse.get(
                                r_stake_address,
                            )

                            # Add new row if can't find in bidict
                            if address_index is None:
                                # Assign new index number,
                                # update bidict and add to values
                                # to insert new row in wallet table
                                address_index = index_wallet
                                bd_wallet_id_x_address.put(
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
                    policy_index = bd_collection_id_x_policy_id.inverse.get(
                        r_policy_id,
                    )

                    # Add new row if can't find in bidict
                    if policy_index is None:
                        # Assign new index number,
                        # update bidict and add to values
                        # to insert new row in collection table
                        policy_index = index_collection
                        bd_collection_id_x_policy_id.put(
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
                    asset_fingerprint_index = bd_asset_id_x_fingerprint.inverse.get(
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
                            bd_asset_id_x_fingerprint.put(
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
                            d_asset_id_x_asset_ext[asset_fingerprint_index] = False

                            # Increment index number for next record
                            index_asset = index_asset + 1

                        # Update latest_mint_tx_id in asset
                        # if it is a mint tx, except burn tx
                        if record['quantity'] > 0:
                            if d_asset_id_x_asset_ext[asset_fingerprint_index] is True:
                                # Update asset entry with latest_mint_tx_id
                                values_update_asset_ext_latest_mint_tx_id.append(
                                    (asset_fingerprint_index, asset_mint_tx_index),
                                )
                            else:
                                # Add to values to insert new row in asset_ext table
                                values_insert_asset_ext.append(
                                    (
                                        asset_fingerprint_index,
                                        asset_mint_tx_index, 'Null',
                                    ),
                                )
                                d_asset_id_x_asset_ext[asset_fingerprint_index] = True

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
                            bd_asset_id_x_fingerprint.put(
                                asset_fingerprint_index, record['asset_fingerprint'],
                            )
                            values_insert_asset.append((
                                asset_fingerprint_index, policy_index,
                                f"{record['policy_id']}.{record['asset_name_hash']}",
                                hex_to_string(str(record['asset_name_hash'])),
                                record['asset_fingerprint'], address_index,
                            ))
                            d_asset_id_x_asset_ext[asset_fingerprint_index] = False

                            # Increment index number for next record
                            index_asset = index_asset + 1
                        else:
                            # Update asset entry with current_wallet_id
                            values_update_asset_current_wallet_id.append(
                                (asset_fingerprint_index, address_index),
                            )

                        if d_asset_id_x_asset_ext[asset_fingerprint_index] is True:
                            # Update asset entry with latest_tx_id
                            values_update_asset_ext_latest_tx_id.append(
                                (asset_fingerprint_index, asset_tx_index),
                            )
                        else:
                            # Add to values to insert new row in asset_ext table
                            values_insert_asset_ext.append(
                                (asset_fingerprint_index, 'Null', asset_tx_index),
                            )
                            d_asset_id_x_asset_ext[asset_fingerprint_index] = True

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
                    db.pantasia_insert_wallet(
                        values=values_insert_wallet,
                    )
                if len(values_insert_collection) > 0:
                    db.pantasia_insert_collection(
                        values=values_insert_collection,
                    )
                if len(values_insert_asset) > 0:
                    db.pantasia_insert_asset(
                        values=values_insert_asset,
                    )
                if len(values_insert_asset_mint_tx) > 0:
                    db.pantasia_insert_asset_mint_tx(
                        values=values_insert_asset_mint_tx,
                    )
                if len(values_insert_asset_tx) > 0:
                    db.pantasia_insert_asset_tx(
                        values=values_insert_asset_tx,
                    )
                if len(values_insert_asset_ext) > 0:
                    db.pantasia_insert_asset_ext(
                        values=values_insert_asset_ext,
                    )
                if len(values_update_asset_ext_latest_mint_tx_id) > 0:
                    db.pantasia_update_asset_ext_latest_mint_tx_id(
                        values=values_update_asset_ext_latest_mint_tx_id,
                    )
                if len(values_update_asset_ext_latest_tx_id) > 0:
                    db.pantasia_update_asset_ext_latest_tx_id(
                        values=values_update_asset_ext_latest_tx_id,
                    )
                if len(values_update_asset_current_wallet_id) > 0:
                    db.pantasia_update_asset_current_wallet_id(
                        values=values_update_asset_current_wallet_id,
                    )

                db.pantasia_conn.commit()
