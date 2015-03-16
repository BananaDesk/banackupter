#!/usr/bin/env python
# This is the bananadesk superhero backup script.
# Long live the banackupter!

import os
import baker
import shutil
import blinker
import logging
import tempfile

from datetime import date
from datetime import datetime


# settings
BACKUPTER_ROOT_DIR = '/tmp/backupter'
BACKUPTER_REGISTRY = os.path.join(BACKUPTER_ROOT_DIR, 'backupter.registry')
BACKUPS_LOG_DIR = os.path.join(BACKUPTER_ROOT_DIR, 'logs')
BACKUPS_STORE_DIR = os.path.join(BACKUPTER_ROOT_DIR, 'backups')

# create backupter dirs if not exists
for dir_ in [BACKUPTER_ROOT_DIR, BACKUPS_LOG_DIR, BACKUPS_STORE_DIR]:
    if not os.path.isdir(dir_):
        os.makedirs(dir_)

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    filename=os.path.join(BACKUPS_LOG_DIR, date.today().strftime('%Y-%m-%d'))
    )
logger = logging.getLogger('BANACKUPTER')

# signals
BackupterStart = blinker.signal('BackupterStart')
BackupterError = blinker.signal('BackupterError')
BackupterEnd = blinker.signal('BackupterEnd')
WeeklyBackupError = blinker.signal('WeeklyBackupError')
WeeklyBackupStart = blinker.signal('WeeklyBackupStart')
WeeklyBackupEnd = blinker.signal('WeeklyBackupEnd')
DailyBackupError = blinker.signal('DailyBackupError')
DailyBackupStart = blinker.signal('DailyBackupStart')
DailyBackupEnd = blinker.signal('DailyBackupEnd')
HourlyBackupError = blinker.signal('HourlyBackupError')
HourlyBackupStart = blinker.signal('HourlyBackupStart')
HourlyBackupEnd = blinker.signal('HourlyBackupEnd')

# handlers
def log_backupter_start(statement):
    msg = 'Going to execute a new backup doing: {statement}'.format(statement=statement)
    logger.info(msg)


def log_backupter_end(filename):
    msg = 'Finished backup at {filename} with a size of {filesize} bytes.'.format(
        filename=filename, filesize=get_file_size(filename))
    logger.info(msg)


def sync_to_amazon_s3(s3_destination, rootdir=BACKUPS_STORE_DIR):
    # sr_destination should be a string of format 's3://your-bucket-name/'
    cmd = 's3cmd sync {src} {dst}'.format(src=rootdir, dst=s3_destination)
    logger.info('syncing {src} with amazon simple storage service...')
    logger.info('executing {cmd}'.format(cmd=cmd))
    os.system(cmd)
    logger.info('sync has finished.')

# connect handlers with signals
BackupterStart.connect(log_backupter_start)
BackupterEnd.connect(log_backupter_end)
DailyBackupEnd.connect(sync_to_amazon_s3)
HourlyBackupEnd.connect(sync_to_amazon_s3)
WeeklyBackupEnd.connect(sync_to_amazon_s3)


# core from backupter
def get_file_size(filename):
    """Returns the file size from filename.
    """
    return os.stat(filename).st_size


def get_database_backup_statement(filename, dbname, as_username='postgres'):
    """Returns the line to be executed to create a database
    dump file from postgresql.
        Arguments:
            filename: Name of the file to create.
            dbname: Name from the database.
            as_username: Name from the username to use when connecting
                         to postgresql
    """
    now = datetime.now()
    statement = 'sudo -u {username} pg_dump {dbname} > {filename}'.format(
        username=as_username, dbname=dbname, filename=filename
        )
    return statement


def execute_pgdump(dbname, as_username='postgres'):
    """Create a dump database file calling pg_dump.
       Returns the path  to the created file.
    """

    filedescriptor, filename = tempfile.mkstemp()
    statement = get_database_backup_statement(filename, dbname, as_username)
    BackupterStart.send(statement)
    os.system(statement)
    BackupterEnd.send(filename)

    return filename


@baker.command
def daily(dbname, as_username='postgres'):
    """Create a daily backup.
    """

    filename = '{dbname}-{indate}.dump.sql'.format(
        dbname=dbname, indate=datetime.now().strftime('%Y-%m-%d'))
    backup_daily_dir = os.path.join(BACKUPS_STORE_DIR, 'daily')
    if not os.path.isdir(backup_daily_dir):
        os.makedirs(backup_daily_dir)

    dumpfile = execute_pgdump(dbname, as_username)
    dst = os.path.join(backup_daily_dir, filename)
    logger.info('moving {src} into {dst}'.format(src=dumpfile, dst=dst))
    shutil.move(dumpfile, dst)
    logger.info('{dst} has a size of {size} bytes.'.format(
        dst=dst, size=get_file_size(dst)))


@baker.command
def weekly(dbname, as_username='postgres'):
    """Create a weekly backup.
    """

    filename = '{dbname}-{indate}.dump.sql'.format(
        dbname=dbname, indate=datetime.now().strftime('%Y-%m-%d'))
    backup_weekly_dir = os.path.join(BACKUPS_STORE_DIR, 'weekly')
    if not os.path.isdir(backup_weekly_dir):
        os.makedirs(backup_weekly_dir)

    dumpfile = execute_pgdump(dbname, as_username)
    filename = os.path.join(backup_weekly_dir, filename)
    logger.info('moving {src} into {dst}'.format(src=dumpfile, dst=filename))
    shutil.move(dumpfile, filename)
    logger.info('{dst} has a size of {size} bytes.'.format(
        dst=filename, size=get_file_size(filename)))


baker.run()
