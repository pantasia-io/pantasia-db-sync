from __future__ import annotations

import argparse
import datetime
import gzip
import logging
import os
import shutil
import subprocess

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from app.settings import settings


def list_available_backups(manager_config):
    key_list = []
    try:
        backup_folder = manager_config.get('LOCAL_BACKUP_PATH')
        backup_list = os.listdir(backup_folder)
    except FileNotFoundError:
        print(
            f'Could not found {backup_folder} when searching for backups. '
            f'Check your settings',
        )
        exit(1)

    for bckp in backup_list:
        key_list.append(bckp)
    return key_list


def list_postgres_databases(host, database_name, port, user, password):
    try:
        process = subprocess.Popen(
            [
                'psql',
                f'--dbname=postgresql://'
                f'{user}:{password}@{host}:{port}/{database_name}',
                '--list',
            ],
            stdout=subprocess.PIPE,
        )
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print(f'Command failed. Return code : {process.returncode}')
            exit(1)
        return output
    except Exception as e:
        print(e)
        exit(1)


def backup_postgres_db(host, database_name, port, user, password, dest_file, verbose):
    """
    Backup postgres db to a file.
    """
    if verbose:
        try:
            process = subprocess.Popen(
                [
                    'pg_dump',
                    f'--dbname=postgresql://'
                    f'{user}:{password}@{host}:{port}/{database_name}',
                    '-Fc',
                    '-f', dest_file,
                    '--compress=9'
                    '-v',
                ],
                stdout=subprocess.PIPE,
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print(f'Command failed. Return code : {process.returncode}')
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)
    else:

        try:
            process = subprocess.Popen(
                [
                    'pg_dump',
                    f'--dbname=postgresql://'
                    f'{user}:{password}@{host}:{port}/{database_name}',
                    '-f', dest_file,
                ],
                stdout=subprocess.PIPE,
            )
            output = process.communicate()[0]
            if process.returncode != 0:
                print(f'Command failed. Return code : {process.returncode}')
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)


def compress_file(src_file):
    compressed_file = f'{str(src_file)}.gz'
    with open(src_file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    return compressed_file


def extract_file(src_file):
    extracted_file, extension = os.path.splitext(src_file)

    with gzip.open(src_file, 'rb') as f_in:
        with open(extracted_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    return extracted_file


def restore_postgres_db(db_host, db, port, user, password, backup_file, verbose):
    """Restore postgres db from a file."""
    try:
        subprocess_params = [
            'pg_restore',
            '--no-owner',
            '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                user,
                password,
                db_host,
                port,
                db,
            ),
        ]

        if verbose:
            subprocess_params.append('-v')

        subprocess_params.append(backup_file)
        process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
        output = process.communicate()[0]

        if int(process.returncode) != 0:
            print(f'Command failed. Return code : {process.returncode}')

        return output
    except Exception as e:
        print(f'Issue with the db restore : {e}')


def create_db(db_host, database, db_port, user_name, user_password):
    try:
        con = psycopg2.connect(
            dbname='postgres', port=db_port,
            user=user_name, host=db_host,
            password=user_password,
        )

    except Exception as e:
        print(e)
        exit(1)

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        cur.execute(
            'SELECT pg_terminate_backend( pid ) '
            'FROM pg_stat_activity '
            'WHERE pid <> pg_backend_pid( ) '
            "AND datname = '{}'".format(database),
        )
        cur.execute(f'DROP DATABASE IF EXISTS {database} ;')
    except Exception as e:
        print(e)
        exit(1)
    cur.execute(f'CREATE DATABASE {database} ;')
    cur.execute(f'GRANT ALL PRIVILEGES ON DATABASE {database} TO {user_name} ;')
    return database


def swap_after_restore(
        db_host,
        restore_database,
        new_active_database,
        db_port,
        user_name,
        user_password,
):
    try:
        con = psycopg2.connect(
            dbname='postgres', port=db_port,
            user=user_name, host=db_host,
            password=user_password,
        )
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute(
            'SELECT pg_terminate_backend( pid ) '
            'FROM pg_stat_activity '
            'WHERE pid <> pg_backend_pid( ) '
            "AND datname = '{}'".format(new_active_database),
        )
        cur.execute(f'DROP DATABASE IF EXISTS {new_active_database}')
        cur.execute(
            f'ALTER DATABASE "{restore_database}" RENAME TO "{new_active_database}";',
        )
    except Exception as e:
        print(e)
        exit(1)


def move_to_local_storage(comp_file, filename_compressed, manager_config):
    """ Move compressed backup into {LOCAL_BACKUP_PATH}. """
    backup_folder = manager_config.get('LOCAL_BACKUP_PATH')
    try:
        os.listdir(backup_folder)
    except FileNotFoundError:
        os.mkdir(backup_folder)
    shutil.move(
        comp_file, '{}{}'.format(
            manager_config.get('LOCAL_BACKUP_PATH'), filename_compressed,
        ),
    )


def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    args_parser = argparse.ArgumentParser(description='Postgres database management')
    args_parser.add_argument(
        '--action',
        metavar='action',
        choices=['list', 'list_dbs', 'restore', 'backup'],
        required=True,
    )
    args_parser.add_argument(
        '--dest-db',
        metavar='dest_db',
        default=None,
        help='Name of the new restored database',
    )
    args_parser.add_argument(
        '--verbose',
        default=False,
        help='Verbose output',
    )
    args = args_parser.parse_args()

    postgres_host = settings.db_host
    postgres_port = settings.db_port
    postgres_db = settings.db_name
    postgres_restore = f'{postgres_db}_restore'
    postgres_user = settings.db_user
    postgres_password = settings.db_pass
    timestr = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = f'pantasia-db-{timestr}-{postgres_db}.dump'
    filename_compressed = f'{filename}.gz'
    restore_filename = 'tmp/restore.dump.gz'
    restore_uncompressed = './tmp/restore.dump'
    local_storage_path = settings.db_backup_path

    manager_config = {
        'BACKUP_PATH': f'{os.getcwd()}/tmp/',
        'LOCAL_BACKUP_PATH': local_storage_path,
    }
    try:
        os.listdir(manager_config.get('BACKUP_PATH'))
    except FileNotFoundError:
        os.mkdir(manager_config.get('BACKUP_PATH'))
    local_file_path = '{}{}'.format(manager_config.get('BACKUP_PATH'), filename)

    # list task
    if args.action == 'list':
        backup_objects = sorted(list_available_backups(manager_config), reverse=True)
        for key in backup_objects:
            logger.info(f'Key : {key}')
    # list databases task
    elif args.action == 'list_dbs':
        result = list_postgres_databases(
            postgres_host,
            postgres_db,
            postgres_port,
            postgres_user,
            postgres_password,
        )
        for line in result.splitlines():
            logger.info(line)
    # backup task
    elif args.action == 'backup':
        logger.info(f'Backing up {postgres_db} database to {local_file_path}')
        result = backup_postgres_db(
            postgres_host,
            postgres_db,
            postgres_port,
            postgres_user,
            postgres_password,
            local_file_path, args.verbose,
        )
        if args.verbose:
            for line in result.splitlines():
                logger.info(line)

        logger.info('Backup complete')
        logger.info(f'Compressing {local_file_path}')
        comp_file = compress_file(local_file_path)
        os.remove(local_file_path)
        logger.info(f'Moving {comp_file} to local storage...')
        move_to_local_storage(comp_file, filename_compressed, manager_config)
        shutil.rmtree(manager_config.get('BACKUP_PATH'))
        logger.info(
            'Moved to {}{}'.format(
                manager_config.get(
                    'LOCAL_BACKUP_PATH',
                ), filename_compressed,
            ),
        )

    # restore task
    elif args.action == 'restore':
        try:
            os.remove(restore_filename)
            os.remove(restore_uncompressed)
        except Exception:
            pass
        all_backup_keys = list_available_backups(manager_config)
        if all_backup_keys:
            print('Found the following backups :')
            for ix, v in enumerate(all_backup_keys):
                print(f'{ix + 1}: {v}')
            try:
                selected_backup = int(
                    input('Select a backup by its index and press Enter:'),
                ) - 1
                logger.info(
                    f'Restoring {all_backup_keys[selected_backup]} from local storage',
                )
            except ValueError:
                logger.error('Error parsing integer. Please enter an integer.')
            except IndexError:
                logger.error('Please enter an index number from the following list')
                logger.info('Available backups : ')
                for ix, v in enumerate(all_backup_keys):
                    logger.info(f'{ix + 1}: {v}')
        else:
            logger.error(f'No match found for backups with date : {args.date}')
            logger.info(f'Available backups : {[s for s in all_backup_keys]}')
            exit(1)

        shutil.copy(
            '{}/{}'.format(
                manager_config.get('LOCAL_BACKUP_PATH'),
                all_backup_keys[selected_backup],
            ),
            restore_filename,
        )
        logger.info('Fetch complete')

        logger.info(f'Extracting {restore_filename}')
        ext_file = extract_file(restore_filename)
        os.remove(restore_filename)
        logger.info(f'Extracted to : {ext_file}')
        logger.info(f'Creating temp database for restore : {postgres_restore}')
        tmp_database = create_db(
            postgres_host,
            postgres_restore,
            postgres_port,
            postgres_user,
            postgres_password,
        )
        logger.info(f'Created temp database for restore : {tmp_database}')
        logger.info('Restore starting')
        result = restore_postgres_db(
            postgres_host,
            postgres_restore,
            postgres_port,
            postgres_user,
            postgres_password,
            restore_uncompressed,
            args.verbose,
        )
        if args.verbose:
            for line in result.splitlines():
                logger.info(line)
        logger.info('Restore complete')
        if args.dest_db is not None:
            restored_db_name = args.dest_db
            logger.info(
                'Switching restored database with new one : {} > {}'.format(
                    postgres_restore, restored_db_name,
                ),
            )
        else:
            restored_db_name = postgres_db
            logger.info(
                'Switching restored database with active one : {} > {}'.format(
                    postgres_restore, restored_db_name,
                ),
            )

        swap_after_restore(
            postgres_host,
            postgres_restore,
            restored_db_name,
            postgres_port,
            postgres_user,
            postgres_password,
        )
        os.remove(ext_file)
        logger.info('Database restored and active.')
    else:
        logger.warning('No valid argument was given.')
        logger.warning(args)


if __name__ == '__main__':
    main()
