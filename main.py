import psycopg2
from psycopg2.extras import RealDictCursor, Json
from datetime import datetime, timedelta
from pycardano import Address, Network
from time import time, sleep
import bidict as bd
import logging.config
import yaml


def read_yaml(filepath):
    with open(filepath, "r") as yaml_file:
        try:
            yaml_data = yaml.safe_load(yaml_file)
            return yaml_data
        except yaml.YAMLError as exc:
            print(exc)


def get_staking_address(address):
    # Check if address is from Shelley Era
    if address.startswith('addr'):
        # Instantiate Address object
        address_obj = Address.from_primitive(address)

        # Return staking address if staking part exists else return None
        if address_obj.staking_part is None:
            return None
        else:
            return Address(staking_part=address_obj.staking_part,
                           network=Network.MAINNET).encode()
    else:
        return None


class Db:
    def __init__(self, config):
        self.config = config

        # Connect to Cardano and Pantasia postgres DB
        self.cardano_conn = psycopg2.connect(dbname=config['cardano']['dbname'], user=config['cardano']['user'],
                                             password=config['cardano']['password'], host=config['cardano']['host'],
                                             port=config['cardano']['port'])
        self.pantasia_conn = psycopg2.connect(dbname=config['pantasia']['dbname'], user=config['pantasia']['user'],
                                              password=config['pantasia']['password'], host=config['pantasia']['host'],
                                              port=config['pantasia']['port'])

        # Open cursors to perform database operations
        self.cardano_cur = self.cardano_conn.cursor(cursor_factory=RealDictCursor)
        self.pantasia_cur = self.pantasia_conn.cursor(cursor_factory=RealDictCursor)

        # Get Cardano DB tip in datetime
        self.cardano_tip = self.get_latest_cardano_tip()
        self.old_cardano_tip = None

        # Create tables if not yet existing
        self.pantasia_create()

        # Get Pantasia DB tip in datetime
        self.pantasia_tip = self.get_latest_pantasia_tip()
        self.old_pantasia_tip = None

    @staticmethod
    def measure_time(func):
        def time_it(*args, **kwargs):
            time_started = time()
            func(*args, **kwargs)
            time_elapsed = time()
            values = kwargs.get('values')

            if values is not None:
                logger.debug("""{execute} running time is {sec} seconds for inserting {rows} rows."""
                             .format(execute=func.__name__, sec=round(time_elapsed - time_started, 4),
                                     rows=len(values)))

        return time_it

    def close_connections(self):
        self.cardano_cur.close()
        self.cardano_conn.close()
        self.pantasia_cur.close()
        self.pantasia_conn.close()

    def pantasia_create_tables(self):
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

    def pantasia_add_foreign_key(self, table_name, foreign_key, reference_table_name, reference_key):
        constraint_name = f"fk_{table_name}_{foreign_key}_{reference_table_name}"
        query = f"""
                    DO $$
                BEGIN

                  BEGIN
                    ALTER TABLE public."%s" ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES "%s"(%s);
                  EXCEPTION
                    WHEN duplicate_object THEN RAISE NOTICE 'Table constraint {constraint_name} already exists';
                  END;

                END $$;
                """ % (table_name, constraint_name, foreign_key, reference_table_name, reference_key)
        self.pantasia_cur.execute(query)

    def pantasia_remove_foreign_key(self, table_name, foreign_key, reference_table_name):
        constraint_name = f"fk_{table_name}_{foreign_key}_{reference_table_name}"
        query = f"""
                    DO $$
                BEGIN

                  BEGIN
                    ALTER TABLE %s DROP CONSTRAINT %s;
                  END;

                END $$;
                """ % (table_name, constraint_name)
        self.pantasia_cur.execute(query)

    def pantasia_create_fk(self):
        # Create Foreign Keys
        self.pantasia_add_foreign_key("asset", "collection_id", "collection", "id")
        self.pantasia_add_foreign_key("asset", "current_wallet_id", "wallet", "id")

        self.pantasia_add_foreign_key("asset_mint_tx", "asset_id", "asset", "id")
        self.pantasia_add_foreign_key("asset_mint_tx", "wallet_id", "wallet", "id")

        self.pantasia_add_foreign_key("asset_tx", "asset_id", "asset", "id")
        self.pantasia_add_foreign_key("asset_tx", "wallet_id", "wallet", "id")

        self.pantasia_add_foreign_key("asset_ext", "latest_mint_tx_id", "asset_mint_tx", "id")
        self.pantasia_add_foreign_key("asset_ext", "latest_tx_id", "asset_tx", "id")

        self.pantasia_add_foreign_key("wallet", "user_id", "user", "id")
        self.pantasia_conn.commit()

    def pantasia_create(self):
        # Create Pantasia DB if not exist
        self.pantasia_create_tables()
        self.pantasia_create_fk()

    def pantasia_get_last_index(self, table_name):
        self.pantasia_cur.execute(f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1")
        result = self.pantasia_cur.fetchone()
        if result is None:
            return 1
        else:
            return result['id'] + 1

    def pantasia_load_id_map(self, table_name, natural_key):
        # Load a bidirectional map of primary key to/from natural key
        bd_result = bd.bidict()

        logger.info(f"Loading {table_name} data......")

        self.pantasia_cur.execute(
            f"SELECT id, {natural_key} FROM {table_name} ORDER BY id ASC")
        results = self.pantasia_cur.fetchall()

        for result in results:
            bd_result.put(result['id'], result[natural_key], bd.ON_DUP_RAISE)

        logger.info(
            f"Load {table_name} data, reference natural key: {natural_key}, {len(results)} items found and loaded")
        return bd_result

    def pantasia_load_asset_ext_asset_id(self):
        # Load dictionary to map asset id to existence of corresponding asset_ext record
        d_result = {}

        logger.info(f"Loading asset_ext data...... ")

        self.pantasia_cur.execute(
            f"""SELECT a.id, ae.asset_id FROM asset a 
                LEFT JOIN asset_ext ae ON a.id = ae.asset_id 
                ORDER BY id ASC""")
        results = self.pantasia_cur.fetchall()

        for result in results:
            if result['asset_id'] is not None:
                d_result[result['id']] = True
            else:
                d_result[result['id']] = False

        logger.info(
            f"Loading asset_ext data, reference natural key: asset_id, {len(results)} items found and loaded")
        return d_result

    def get_latest_cardano_tip(self):
        # Get latest block time
        self.cardano_cur.execute("""SELECT b.time AS cardano_tip
            FROM block b
            ORDER BY b.time DESC
            LIMIT 1""")

        # cardano_tip delayed 2 minutes as a buffer to allow cardano_db_sync to complete insertions
        cardano_tip = self.cardano_cur.fetchone()["cardano_tip"] - timedelta(minutes=2)
        logger.info(f"Cardano DB Tip is at {cardano_tip}")

        self.cardano_tip = cardano_tip
        return cardano_tip

    def get_latest_pantasia_tip(self):
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
        pantasia_tip = self.pantasia_cur.fetchone()

        if pantasia_tip is not None:
            pantasia_tip = pantasia_tip["pantasia_tip"]
        else:
            # Genesis - First block containing native assets
            logger.info("pantasia_tip not found, starting from Genesis")
            pantasia_tip = datetime.fromisoformat('2021-03-01 21:47:37.000')

        logger.info(f"Pantasia DB Tip is at {pantasia_tip}")
        self.pantasia_tip = pantasia_tip
        return pantasia_tip

    def pantasia_rollback(self):
        # Prevent duplicates by deleting entries in asset_mint_tx and asset_tx from pantasia_tip to pantasia_tip + time_interval
        logger.info("Rolling back to prevent duplicates...")
        time_interval = timedelta(hours=self.config["time_interval"])

        logger.info(
            f"""Deleting from asset_tx and asset_mint_tx from {self.pantasia_tip} to {self.pantasia_tip + time_interval}""")

        self.pantasia_cur.execute(
            f"""WITH select_asset_ids AS (
                SELECT asset_id FROM asset_tx at2
                WHERE at2.tx_time >= TIMESTAMP '{self.pantasia_tip}'
                    AND at2.tx_time < TIMESTAMP '{self.pantasia_tip + time_interval}'
                UNION ALL
                SELECT asset_Id FROM asset_mint_tx amt
                WHERE amt.tx_time >= TIMESTAMP '{self.pantasia_tip}'
                    AND amt.tx_time < TIMESTAMP '{self.pantasia_tip + time_interval}'
                )
                DELETE FROM asset_ext ae
                WHERE ae.asset_id IN
                (
                SELECT asset_id FROM select_asset_ids sai
                )"""
        )
        self.pantasia_cur.execute(
            f"""DELETE FROM asset_tx at2 WHERE at2.tx_time >= TIMESTAMP '{self.pantasia_tip}' 
            AND at2.tx_time < TIMESTAMP '{self.pantasia_tip + time_interval}'""")

        self.pantasia_cur.execute(
            f"""DELETE FROM asset_mint_tx atm WHERE atm.tx_time >= TIMESTAMP '{self.pantasia_tip}' 
            AND atm.tx_time < TIMESTAMP '{self.pantasia_tip + time_interval}'""")
        self.pantasia_conn.commit()

        logger.info(
            f"""Delete from asset_tx, asset_mint_tx and asset_ext from {self.pantasia_tip} to {self.pantasia_tip + time_interval} complete""")

    def create_period_list(self, period_list):
        new_tip = self.pantasia_tip

        while new_tip < self.cardano_tip:
            new_tip = new_tip + timedelta(hours=self.config["time_interval"])

            if new_tip > self.cardano_tip:
                new_tip = self.cardano_tip

            period_list.append(new_tip)

        return period_list

    def pantasia_get_records(self, target_datetime, from_datetime):
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
                 AND b."time" >= %s
                 AND b."time" < %s
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
                join multi_asset ma2 ON ma2.id = mto.ident
                LEFT JOIN stake_address sa ON to2.stake_address_id = sa.id
                WHERE b2."time" >= %s
                 AND b2."time" < %s )
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
                      tm.json -> amt.policy_id -> amt.asset_name ->> 'image' AS image,
                      tm.json -> amt.policy_id -> amt.asset_name AS metadata,
                      tm.json -> amt.policy_id -> amt.asset_name -> 'files' AS files
                FROM ma_tx_mint mtm2
                LEFT OUTER JOIN tx_metadata tm ON tm.tx_id = amt.tx_id
                WHERE mtm2.ident = amt.ma_id
                 AND mtm2.tx_id = amt.tx_id) label_mint_tx ON true
                JOIN tx t3 ON amt.tx_id = t3.id
                JOIN block b3 ON t3.block_id = b3.id
                ORDER BY b3.time asc
                """
        values = (from_datetime, target_datetime, from_datetime, target_datetime)
        self.cardano_cur.execute(query, values)
        return self.cardano_cur.fetchall()

    @measure_time
    def pantasia_insert_wallet(self, values):
        argument_string = ",".join("(%s, '%s', '%s')" % (a, b, c) for (a, b, c) in values)
        query_str = """INSERT INTO wallet (id, address, address_type) VALUES""" + argument_string
        self.pantasia_cur.execute(query_str)

    @measure_time
    def pantasia_insert_collection(self, values):
        argument_string = ",".join("(%s, '%s')" % (a, b) for (a, b) in values)
        query_str = """INSERT INTO collection (id, policy_id) VALUES""" + argument_string
        self.pantasia_cur.execute(query_str)

    @measure_time
    def pantasia_insert_asset_mint_tx(self, values):
        argument_string = ",".join(
            "(%s, %s, %s, %s, '%s', TIMESTAMP '%s', $$%s$$, %s, %s)" % (a, b, c, d, e, f, g, h, i) for
            (a, b, c, d, e, f, g, h, i) in
            values)
        query_str = """INSERT INTO asset_mint_tx (id, asset_id, wallet_id, quantity, tx_hash, tx_time, image, metadata, files) 
    VALUES""" + argument_string
        self.pantasia_cur.execute(query_str)

    @measure_time
    def pantasia_insert_asset_tx(self, values):
        argument_string = ",".join(
            "(%s, %s, %s, %s, '%s', TIMESTAMP '%s')" % (a, b, c, d, e, f) for (a, b, c, d, e, f) in
            values)
        query_str = """INSERT INTO asset_tx (id, asset_id, wallet_id, quantity, tx_hash, tx_time) 
    VALUES""" + argument_string
        self.pantasia_cur.execute(query_str)

    @measure_time
    def pantasia_insert_asset(self, values):
        argument_string = ",".join(
            "(%s, %s, '%s', '%s', '%s', %s)" % (a, b, c, d, e, f) for (a, b, c, d, e, f)
            in values)
        query_str = """INSERT INTO asset (id, collection_id, hash, name, fingerprint, current_wallet_id) 
    VALUES""" + argument_string
        self.pantasia_cur.execute(query_str)

    @measure_time
    def pantasia_insert_asset_ext(self, values):
        argument_string = ",".join(
            "(%s, %s, %s)" % (a, b, c) for (a, b, c)
            in values)
        query_str = """INSERT INTO asset_ext (asset_id, latest_mint_tx_id, latest_tx_id) 
    VALUES""" + argument_string
        self.pantasia_cur.execute(query_str)

    @measure_time
    def pantasia_update_asset_ext_latest_mint_tx_id(self, values):
        argument_string = ",".join(
            "(%s, %s)" % (a, b) for (a, b) in values)
        query_str = f"""UPDATE asset_ext AS ae SET latest_mint_tx_id = v.latest_mint_tx_id FROM (VALUES{argument_string}) AS v(asset_id, latest_mint_tx_id) WHERE ae.asset_id = v.asset_id"""
        self.pantasia_cur.execute(query_str)

    @measure_time
    def pantasia_update_asset_ext_latest_tx_id(self, values):
        argument_string = ",".join(
            "(%s, %s)" % (a, b) for (a, b) in values)
        query_str = f"""UPDATE asset_ext AS ae SET latest_tx_id = v.latest_tx_id FROM (VALUES{argument_string}) AS v(asset_id, latest_tx_id) WHERE ae.asset_id = v.asset_id"""
        self.pantasia_cur.execute(query_str)

    @measure_time
    def pantasia_update_asset_current_wallet_id(self, values):
        argument_string = ",".join(
            "(%s, %s)" % (a, b) for (a, b) in values)
        query_str = f"""UPDATE asset AS a SET current_wallet_id = v.current_wallet_id FROM (VALUES{argument_string}) AS v(id, current_wallet_id) WHERE a.id = v.id"""
        self.pantasia_cur.execute(query_str)


def hex_to_string(hex_string):
    try:
        asset_name = bytearray.fromhex(hex_string)
        asset_name = asset_name.replace(b'\x00', b' ')
        asset_name = asset_name.replace(b"'", b"''")
        asset_name = asset_name.decode()
    except UnicodeDecodeError:
        asset_name = hex_string
    return asset_name


if __name__ == '__main__':
    # Read logging config
    logging.config.dictConfig(read_yaml("logging.yaml"))

    # Create logger
    logger = logging.getLogger('pantasia-db-sync')

    # Read DB Config
    db_config = read_yaml("dbconfig.yaml")

    # Initialize Db conections to Cardano DB and Pantasia DB
    db = Db(db_config)

    # Initialize and load data from Pantasia DB
    bd_asset_id_x_fingerprint = db.pantasia_load_id_map("asset", "fingerprint")
    bd_wallet_id_x_address = db.pantasia_load_id_map("wallet", "address")
    bd_collection_id_x_policy_id = db.pantasia_load_id_map("collection", "policy_id")
    d_asset_id_x_asset_ext_exists = db.pantasia_load_asset_ext_asset_id()

    # Get latest index (id) numbers for each table
    index_asset = db.pantasia_get_last_index("asset")
    index_asset_mint_tx = db.pantasia_get_last_index("asset_mint_tx")
    index_asset_tx = db.pantasia_get_last_index("asset_tx")
    index_collection = db.pantasia_get_last_index("collection")
    index_wallet = db.pantasia_get_last_index("wallet")

    is_startup = True
    from_datetime = None
    period_list = [db.pantasia_tip]

    while True:
        # Pause so that Postgres doesn't get spammed
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


            # If new element from period_list not the same as the previous, then move the index and get records
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
                    logger.debug(f"{int(proc_rate)} period/s - Estimated {int(time_left)} seconds to go")

                logger.info(
                    f'period_list_len - {current_count}/{initial_len - 1} | FROM: {from_datetime} | TO: {to_datetime}')

                # Retrieve records from Cardano DB
                time_started = time()
                records = db.pantasia_get_records(to_datetime, from_datetime)
                time_elapsed = time()
                logger.debug("""{execute} running time is {sec} seconds for retrieving {rows} rows."""
                             .format(execute="main_query", sec=round(time_elapsed - time_started, 4),
                                     rows=len(records)))

                time_started = time()
                for record in records:
                    # Add address to wallet table
                    r_address = record['address']

                    if r_address is not None:
                        # Get staking address from payment address if not return None
                        r_stake_address = get_staking_address(r_address)

                        if r_stake_address is None:
                            # Get index of payment address if already existing in bidict
                            address_index = bd_wallet_id_x_address.inverse.get(r_address)

                            # Add new row if can't find in bidict
                            if address_index is None:
                                # Assign new index number, update bidict and add to values to insert new row in wallet table
                                address_index = index_wallet
                                bd_wallet_id_x_address.put(address_index, r_address)
                                r_address_type = "ENTERPRISE"
                                values_insert_wallet.append((address_index, r_address, r_address_type))

                                # Increment index number for next record
                                index_wallet = index_wallet + 1

                        else:
                            # Get index of stake address if already existing in bidict
                            address_index = bd_wallet_id_x_address.inverse.get(r_stake_address)

                            # Add new row if can't find in bidict
                            if address_index is None:
                                # Assign new index number, update bidict and add to values to insert new row in wallet table
                                address_index = index_wallet
                                bd_wallet_id_x_address.put(address_index, r_stake_address)
                                r_address_type = "STAKE"
                                values_insert_wallet.append((address_index, r_stake_address, r_address_type))

                                # Increment index number for next record
                                index_wallet = index_wallet + 1
                    else:
                        # Assign null value to address index, this is expected for burn tx (mint tx with negative quantity)
                        address_index = 'Null'

                    # Add policy id to collection table
                    r_policy_id = record['policy_id']

                    # Get index of policy id if already existing in bidict
                    policy_index = bd_collection_id_x_policy_id.inverse.get(r_policy_id)

                    # Add new row if can't find in bidict
                    if policy_index is None:
                        # Assign new index number, update bidict and add to values to insert new row in collection table
                        policy_index = index_collection
                        bd_collection_id_x_policy_id.put(policy_index, r_policy_id)
                        values_insert_collection.append((policy_index, r_policy_id,))

                        # Increment index number for next record
                        index_collection = index_collection + 1

                    # Process asset, asset_mint_tx and asset_tx
                    is_mint_tx = record['is_mint_tx']
                    # Get index of asset if already existing in bidict
                    asset_fingerprint_index = bd_asset_id_x_fingerprint.inverse.get(record['asset_fingerprint'])

                    # Process asset_mint_tx
                    if is_mint_tx is True:
                        # Get index of asset_mint_tx for a new row
                        asset_mint_tx_index = index_asset_mint_tx

                        # Add new row if can't find in bidict
                        if asset_fingerprint_index is None:
                            # Assign new index number, update bidict and add to values to insert new row in asset table
                            asset_fingerprint_index = index_asset
                            bd_asset_id_x_fingerprint.put(asset_fingerprint_index, record['asset_fingerprint'])
                            values_insert_asset.append((asset_fingerprint_index, policy_index,
                                                        f"{record['policy_id']}.{str(record['asset_name_hash'])}",
                                                        hex_to_string(str(record['asset_name_hash'])),
                                                        record['asset_fingerprint'], address_index))
                            d_asset_id_x_asset_ext_exists[asset_fingerprint_index] = False

                            # Increment index number for next record
                            index_asset = index_asset + 1

                        # Update latest_mint_tx_id in asset if it is a mint tx, except burn tx
                        if record['quantity'] > 0:
                            if d_asset_id_x_asset_ext_exists[asset_fingerprint_index] is True:
                                # Update asset entry with latest_mint_tx_id
                                values_update_asset_ext_latest_mint_tx_id.append(
                                    (asset_fingerprint_index, asset_mint_tx_index))
                            else:
                                # Add to values to insert new row in asset_ext table
                                values_insert_asset_ext.append((asset_fingerprint_index, asset_mint_tx_index, 'Null'))
                                d_asset_id_x_asset_ext_exists[asset_fingerprint_index] = True

                        # Add to values to insert new row in asset_mint_tx table
                        values_insert_asset_mint_tx.append(
                            (asset_mint_tx_index, asset_fingerprint_index, address_index, record['quantity'],
                             record['tx_hash'], record['tx_time'], record['image'], Json(record['metadata']),
                             Json(record['files'])))

                        # Increment index number for next record
                        index_asset_mint_tx = index_asset_mint_tx + 1

                    # Process asset_tx
                    else:
                        # Get index of asset_tx for a new row
                        asset_tx_index = index_asset_tx

                        # Add new row if can't find in bidict
                        if asset_fingerprint_index is None:
                            # Assign new index number, update bidict and add to values to insert new row in asset table
                            asset_fingerprint_index = index_asset
                            bd_asset_id_x_fingerprint.put(asset_fingerprint_index, record['asset_fingerprint'])
                            values_insert_asset.append((asset_fingerprint_index, policy_index,
                                                        f"{record['policy_id']}.{record['asset_name_hash']}",
                                                        hex_to_string(str(record['asset_name_hash'])),
                                                        record['asset_fingerprint'], address_index))
                            d_asset_id_x_asset_ext_exists[asset_fingerprint_index] = False

                            # Increment index number for next record
                            index_asset = index_asset + 1
                        else:
                            # Update asset entry with current_wallet_id
                            values_update_asset_current_wallet_id.append((asset_fingerprint_index, address_index))

                        if d_asset_id_x_asset_ext_exists[asset_fingerprint_index] is True:
                            # Update asset entry with latest_tx_id
                            values_update_asset_ext_latest_tx_id.append((asset_fingerprint_index, asset_tx_index))
                        else:
                            # Add to values to insert new row in asset_ext table
                            values_insert_asset_ext.append((asset_fingerprint_index, 'Null', asset_tx_index))
                            d_asset_id_x_asset_ext_exists[asset_fingerprint_index] = True

                        # Add to values to insert new row in asset_tx table
                        values_insert_asset_tx.append(
                            (asset_tx_index, asset_fingerprint_index, address_index, record['quantity'],
                             record['tx_hash'], record['tx_time']))

                        # Increment index number for next record
                        index_asset_tx = index_asset_tx + 1
                time_elapsed = time()
                logger.debug("""{execute} running time is {sec} seconds for processing {rows} rows."""
                             .format(execute="Processing", sec=round(time_elapsed - time_started, 4),
                                     rows=len(records)))

                if len(values_insert_wallet) > 0:
                    db.pantasia_insert_wallet(values=values_insert_wallet)
                if len(values_insert_collection) > 0:
                    db.pantasia_insert_collection(values=values_insert_collection)
                if len(values_insert_asset) > 0:
                    db.pantasia_insert_asset(values=values_insert_asset)
                if len(values_insert_asset_mint_tx) > 0:
                    db.pantasia_insert_asset_mint_tx(values=values_insert_asset_mint_tx)
                if len(values_insert_asset_tx) > 0:
                    db.pantasia_insert_asset_tx(values=values_insert_asset_tx)
                if len(values_insert_asset_ext) > 0:
                    db.pantasia_insert_asset_ext(values=values_insert_asset_ext)
                if len(values_update_asset_ext_latest_mint_tx_id) > 0:
                    db.pantasia_update_asset_ext_latest_mint_tx_id(values=values_update_asset_ext_latest_mint_tx_id)
                if len(values_update_asset_ext_latest_tx_id) > 0:
                    db.pantasia_update_asset_ext_latest_tx_id(values=values_update_asset_ext_latest_tx_id)
                if len(values_update_asset_current_wallet_id) > 0:
                    db.pantasia_update_asset_current_wallet_id(values=values_update_asset_current_wallet_id)

                db.pantasia_conn.commit()
