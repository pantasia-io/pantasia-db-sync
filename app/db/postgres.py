from __future__ import annotations

import logging
from datetime import datetime
from datetime import timedelta
from time import time
from typing import Callable

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger('pantasia-db-sync')


class Db:
    def __init__(self, config) -> None:
        self.config = config
        # Connect to Cardano and Pantasia postgres DB
        logger.debug(
            f'Connecting to {config.cdb_name} '
            f'at {config.cdb_host}:{config.cdb_port}',
        )
        self.cardano_conn = psycopg2.connect(
            dbname=config.cdb_name,
            user=config.cdb_user,
            password=config.cdb_pass,
            host=config.cdb_host,
            port=config.cdb_port,
        )
        logger.debug('Connection successful')
        logger.debug(
            f'Connecting to {config.db_name} '
            f'at {config.db_host}:{config.db_port}',
        )
        self.pantasia_conn = psycopg2.connect(
            dbname=config.db_name,
            user=config.db_user,
            password=config.db_pass,
            host=config.db_host,
            port=config.db_port,
        )
        logger.debug('Connection successful')

        # Open cursors to perform database operations
        self.cardano_cur = self.cardano_conn.cursor(
            cursor_factory=RealDictCursor,
        )
        self.pantasia_cur = self.pantasia_conn.cursor(
            cursor_factory=RealDictCursor,
        )

        # Get Cardano DB tip in datetime
        self.cardano_tip = self.get_latest_cardano_tip()
        self.old_cardano_tip = None

        # Create tables if not yet existing
        self.pantasia_create()

        # Get Pantasia DB tip in datetime
        self.pantasia_tip = self.get_latest_pantasia_tip()

    @staticmethod
    def _measure_time(func: Callable) -> Callable:
        def time_it(*args: any, **kwargs: any) -> None:
            time_started = time()
            func(*args, **kwargs)
            time_elapsed = time()
            values = kwargs.get('values')

            if values is not None:
                logger.debug(
                    '{execute} running time is {s} seconds for inserting {rows} rows.'
                    .format(
                        execute=func.__name__,
                        s=round(
                            time_elapsed - time_started, 4,
                        ),
                        rows=len(values),
                    ),
                )

        return time_it

    def close_connections(self) -> None:
        logger.info('Canceling pending transactions and closing connections......')
        self.cardano_conn.cancel()
        self.cardano_cur.close()
        self.cardano_conn.close()
        self.pantasia_conn.cancel()
        self.pantasia_cur.close()
        self.pantasia_conn.close()
        logger.info('Database connections have been closed.')

    def pantasia_create_tables(self) -> None:
        query = """
                CREATE TABLE IF NOT EXISTS "user" (
                id serial4 PRIMARY KEY,
                pfp_asset_id int8,
                alias varchar (16) UNIQUE NOT NULL,
                created_on timestamp NOT NULL,
                modified timestamp NOT NULL,
                last_login timestamp
                );

                CREATE TABLE IF NOT EXISTS wallet (
                id serial8 PRIMARY KEY,
                address varchar UNIQUE NOT NULL,
                address_type varchar (16) NOT NULL,
                user_id int4
                );

                CREATE TABLE IF NOT EXISTS collection (
                id serial4 PRIMARY KEY,
                policy_id varchar UNIQUE NOT NULL,
                name varchar UNIQUE
                );

                CREATE TABLE IF NOT EXISTS asset (
                id serial8 PRIMARY KEY,
                collection_id int4 NOT NULL,
                hash varchar UNIQUE NOT NULL,
                name varchar NOT NULL,
                fingerprint varchar UNIQUE NOT NULL,
                current_wallet_id int8
                );

                CREATE TABLE IF NOT EXISTS asset_tx (
                id serial8 PRIMARY KEY,
                asset_id int8 NOT NULL,
                wallet_id int8 NOT NULL,
                quantity numeric (20,0) NOT NULL,
                tx_hash varchar NOT NULL,
                tx_time timestamp NOT NULL
                );

                CREATE TABLE IF NOT EXISTS asset_mint_tx (
                id serial8 PRIMARY KEY,
                asset_id int8 NOT NULL,
                wallet_id int8,
                quantity numeric (20,0) NOT NULL,
                tx_hash varchar NOT NULL,
                tx_time timestamp NOT NULL,
                image varchar,
                metadata jsonb,
                files jsonb
                );

                CREATE TABLE IF NOT EXISTS asset_ext (
                id serial8 PRIMARY KEY,
                asset_id int8 UNIQUE NOT NULL,
                latest_mint_tx_id int8,
                latest_tx_id int8
                );
                """
        self.pantasia_cur.execute(query)
        self.pantasia_conn.commit()

    def pantasia_add_foreign_key(
            self,
            table_name: str,
            foreign_key: str,
            reference_table_name: str,
            reference_key: str,
    ) -> None:
        constraint_name = f'fk_{table_name}' \
                          f'_{foreign_key}' \
                          f'_{reference_table_name}'
        query = f"""
                    DO $$
                BEGIN

                  BEGIN
                    ALTER TABLE public."%s"
                    ADD CONSTRAINT %s
                    FOREIGN KEY (%s)
                    REFERENCES "%s"(%s);
                  EXCEPTION
                    WHEN duplicate_object
                    THEN RAISE NOTICE
                    'Table constraint {constraint_name} already exists';
                  END;

                END $$;
                """ % (
            table_name,
            constraint_name,
            foreign_key,
            reference_table_name,
            reference_key,
        )
        self.pantasia_cur.execute(query)

    def pantasia_remove_foreign_key(
            self,
            table_name: str,
            foreign_key: str,
            reference_table_name: str,
    ) -> None:
        constraint_name = f'fk_{table_name}' \
                          f'_{foreign_key}' \
                          f'_{reference_table_name}'
        query = """
                    DO $$
                BEGIN

                  BEGIN
                    ALTER TABLE {} DROP CONSTRAINT {};
                  END;

                END $$;
                """.format(table_name, constraint_name)
        self.pantasia_cur.execute(query)

    def pantasia_create_fk(self) -> None:
        # Create Foreign Keys
        self.pantasia_add_foreign_key(
            'asset', 'collection_id', 'collection', 'id',
        )
        self.pantasia_add_foreign_key(
            'asset', 'current_wallet_id', 'wallet', 'id',
        )

        self.pantasia_add_foreign_key(
            'asset_mint_tx', 'asset_id', 'asset', 'id',
        )
        self.pantasia_add_foreign_key(
            'asset_mint_tx', 'wallet_id', 'wallet', 'id',
        )

        self.pantasia_add_foreign_key('asset_tx', 'asset_id', 'asset', 'id')
        self.pantasia_add_foreign_key('asset_tx', 'wallet_id', 'wallet', 'id')

        self.pantasia_add_foreign_key(
            'asset_ext', 'asset_id', 'asset', 'id',
        )
        self.pantasia_add_foreign_key(
            'asset_ext', 'latest_mint_tx_id', 'asset_mint_tx', 'id',
        )
        self.pantasia_add_foreign_key(
            'asset_ext', 'latest_tx_id', 'asset_tx', 'id',
        )

        self.pantasia_add_foreign_key('wallet', 'user_id', 'user', 'id')
        self.pantasia_conn.commit()

    def pantasia_create(self) -> None:
        # Create Pantasia DB if not exist
        self.pantasia_create_tables()
        self.pantasia_create_fk()

    def pantasia_get_last_index(self, table_name: str) -> int:
        self.pantasia_cur.execute(
            f'SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1',
        )
        self.pantasia_conn.commit()
        result = self.pantasia_cur.fetchone()
        if result is None:
            return 1
        else:
            return result['id'] + 1

    def get_latest_cardano_tip(self) -> datetime:
        # Get latest block time
        self.cardano_cur.execute("""SELECT b.time AS cardano_tip
            FROM block b
            ORDER BY b.time DESC
            LIMIT 1
            OFFSET 3""")
        self.pantasia_conn.commit()

        # cardano_tip delayed 2 minutes as a buffer
        # to allow cardano_db_sync to complete insertions
        cardano_tip = self.cardano_cur.fetchone(
        )['cardano_tip']
        logger.info(f'Cardano DB Tip is at {cardano_tip}')

        self.cardano_tip = cardano_tip
        return cardano_tip

    def get_latest_pantasia_tip(self) -> datetime:
        # Get latest Pantasia tx time
        self.pantasia_cur.execute("""WITH at_tip AS (
            SELECT at2.tx_time
            FROM asset_tx at2
            ORDER BY at2.id DESC
            LIMIT 1
            ),
        amt_tip AS (
            SELECT amt.tx_time
            FROM asset_mint_tx amt
            ORDER BY amt.id DESC
            LIMIT 1
        )
        SELECT att.tx_time AS pantasia_tip FROM at_tip att
        UNION ALL
        SELECT amtt.tx_time AS pantasia_tip FROM amt_tip amtt
        ORDER BY pantasia_tip DESC
        LIMIT 1""")
        self.pantasia_conn.commit()

        pantasia_tip = self.pantasia_cur.fetchone()

        if pantasia_tip is not None:
            pantasia_tip = pantasia_tip['pantasia_tip']
        else:
            # Genesis - First block containing native assets
            logger.info('pantasia_tip not found, starting from Genesis')
            pantasia_tip = datetime.fromisoformat('2021-03-01 21:47:00.000')

        logger.info(f'Pantasia DB Tip is at {pantasia_tip}')
        self.pantasia_tip = pantasia_tip
        return pantasia_tip

    def create_period_list(self, period_list: list) -> list:
        new_tip = self.pantasia_tip

        while new_tip < self.cardano_tip:
            new_tip = new_tip + timedelta(minutes=self.config.time_interval)

            if new_tip > self.cardano_tip:
                new_tip = self.cardano_tip

            period_list.append(new_tip)

        return period_list

    def pantasia_get_records(
            self,
            target_datetime: datetime,
            from_datetime: datetime,
    ) -> list:
        query = """
                WITH all_ma_tx AS
                (SELECT mtm.ident AS ma_id,
                      encode(ma.policy::bytea, 'hex'::text) AS policy_id,
                      encode(ma.name::bytea, 'escape'::text) AS asset_name,
                      encode(ma.name::bytea, 'hex'::text) AS asset_name_hash,
                      ma.fingerprint AS asset_fingerprint,
                      mtm.quantity,
                      mtm.tx_id,
                      NULL AS address,
                      NULL AS stake_address
                FROM ma_tx_mint mtm
                JOIN tx t ON t.id = mtm.tx_id
                JOIN block b ON b.id = t.block_id
                JOIN multi_asset ma ON ma.id = mtm.ident
                WHERE mtm.quantity < 0
                 AND b."time" > %s
                 AND b."time" <= %s
                UNION ALL SELECT mto.ident AS ma_id,
                                encode(ma2.policy::bytea, 'hex'::text) AS policy_id,
                                encode(ma2.name::bytea, 'escape'::text) AS asset_name,
                                encode(ma2.name::bytea, 'hex'::text) AS asset_name_hash,
                                ma2.fingerprint,
                                mto.quantity,
                                to2.tx_id,
                                to2.address,
                                sa."view" AS stake_address
                FROM ma_tx_out mto
                JOIN tx_out to2 ON mto.tx_out_id = to2.id
                JOIN tx t2 ON to2.tx_id = t2.id
                JOIN block b2 ON t2.block_id = b2.id
                JOIN multi_asset ma2 ON ma2.id = mto.ident
                LEFT OUTER JOIN stake_address sa ON to2.stake_address_id = sa.id
                WHERE b2."time" > %s
                 AND b2."time" <= %s )
                SELECT policy_id,
                   asset_fingerprint,
                   asset_name,
                   asset_name_hash,
                   encode(t3.hash, 'hex') AS tx_hash,
                   quantity,
                   address,
                   stake_address,
                   is_mint_tx,
                   b3."time" AS tx_time,
                   image,
                   files,
                   metadata
                FROM all_ma_tx amt
                LEFT JOIN LATERAL
                (SELECT true AS is_mint_tx,
                    tm."key",
                    tm.json -> amt.policy_id -> amt.asset_name ->> 'image' AS image,
                    tm.json -> amt.policy_id -> amt.asset_name AS metadata,
                    tm.json -> amt.policy_id -> amt.asset_name -> 'files' AS files
                FROM ma_tx_mint mtm2
                LEFT OUTER JOIN tx_metadata tm ON tm.tx_id = amt.tx_id
                AND tm."key" = 721
                WHERE (mtm2.ident = amt.ma_id
                 AND mtm2.tx_id = amt.tx_id)) label_mint_tx ON true
                JOIN tx t3 ON amt.tx_id = t3.id
                JOIN block b3 ON t3.block_id = b3.id
                ORDER BY b3.time asc
                """
        values = (
            from_datetime, target_datetime,
            from_datetime, target_datetime,
        )
        self.cardano_cur.execute(query, values)
        self.cardano_conn.commit()
        return self.cardano_cur.fetchall()

    @_measure_time
    def pantasia_insert_wallet(self, values: list) -> None:
        argument_string = ','.join(
            f"({a}, '{b}', '{c}')" for (a, b, c) in values
        )
        query_str = 'INSERT INTO wallet (id, address, address_type) VALUES' + \
                    argument_string
        self.pantasia_cur.execute(query_str)

    @_measure_time
    def pantasia_insert_collection(self, values: list) -> None:
        argument_string = ','.join(
            f"({a}, '{b}')"
            for (a, b) in values
        )
        query_str = 'INSERT INTO collection (id, policy_id) VALUES' + \
                    argument_string
        self.pantasia_cur.execute(query_str)

    @_measure_time
    def pantasia_insert_asset_mint_tx(self, values: list) -> None:
        argument_string = ','.join(
            f"({a}, {b}, {c}, {d}, '{e}', TIMESTAMP '{f}', $${g}$$, {h}, {i})" for
            (a, b, c, d, e, f, g, h, i) in
            values
        )
        query_str = 'INSERT INTO asset_mint_tx ' \
                    '(id, asset_id, wallet_id, quantity, ' \
                    'tx_hash, tx_time, image, metadata, files) ' \
                    'VALUES' + argument_string
        self.pantasia_cur.execute(query_str)

    @_measure_time
    def pantasia_insert_asset_tx(self, values: list) -> None:
        argument_string = ','.join(
            f"({a}, {b}, {c}, {d}, '{e}', TIMESTAMP '{f}')" for (a, b, c, d, e, f) in
            values
        )
        query_str = 'INSERT INTO asset_tx ' \
                    '(id, asset_id, wallet_id, ' \
                    'quantity, tx_hash, tx_time) ' \
                    'VALUES' + argument_string
        self.pantasia_cur.execute(query_str)

    @_measure_time
    def pantasia_insert_asset(self, values: list) -> None:
        argument_string = ','.join(
            f"({a}, {b}, '{c}', '{d}', '{e}', {f})" for (a, b, c, d, e, f)
            in values
        )
        query_str = 'INSERT INTO asset ' \
                    '(id, collection_id, hash, name, ' \
                    'fingerprint, current_wallet_id) ' \
                    'VALUES' + argument_string
        self.pantasia_cur.execute(query_str)

    @_measure_time
    def pantasia_insert_asset_ext(self, values: list) -> None:
        argument_string = ','.join(
            f'({a}, {b}, {c}, {d})' for (a, b, c, d)
            in values
        )
        query_str = 'INSERT INTO asset_ext ' \
                    '(id, asset_id, latest_mint_tx_id, latest_tx_id) ' \
                    'VALUES' + argument_string
        self.pantasia_cur.execute(query_str)

    @_measure_time
    def pantasia_update_asset_ext_latest_mint_tx_id(self, values: list) -> None:
        argument_string = ','.join(
            f'({a}, {b})' for (a, b) in values
        )
        query_str = f"""UPDATE asset_ext AS ae
        SET latest_mint_tx_id = v.latest_mint_tx_id
        FROM (VALUES{argument_string}) AS v(asset_id, latest_mint_tx_id)
        WHERE ae.asset_id = v.asset_id"""
        self.pantasia_cur.execute(query_str)

    @_measure_time
    def pantasia_update_asset_ext_latest_tx_id(self, values: list) -> None:
        argument_string = ','.join(
            f'({a}, {b})' for (a, b) in values
        )
        query_str = f"""UPDATE asset_ext AS ae
        SET latest_tx_id = v.latest_tx_id
        FROM (VALUES{argument_string}) AS v(asset_id, latest_tx_id)
        WHERE ae.asset_id = v.asset_id"""
        self.pantasia_cur.execute(query_str)

    @_measure_time
    def pantasia_update_asset_current_wallet_id(self, values: list) -> None:
        argument_string = ','.join(
            f'({a}, {b})' for (a, b) in values
        )
        query_str = f"""UPDATE asset AS a
        SET current_wallet_id = v.current_wallet_id
        FROM (VALUES{argument_string}) AS v(id, current_wallet_id)
        WHERE a.id = v.id"""
        self.pantasia_cur.execute(query_str)
