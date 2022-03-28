# -*- coding: utf-8 -*-


"""
meta_generator.py - v4.4
Generate manifest for a directory, its sub-directory and all files recursively.
1. Support zip file hashing without extracting to file system
2. Support auto-reconciliation between folder and file manifests
3. Support timeout, manual stop and resume
4. Support batch processing for multiple directories using csv
5. Optimized for large volumn scan
"""

# build-in libs
import os
import re
import time
import hashlib
import zipfile
import multiprocessing as mp
import concurrent.futures as futures
import argparse
import logging
from datetime import datetime
from shutil import copy2
from typing import List
# thrid-party libs
import pandas as pd
from sqlalchemy import create_engine, func, Column, String, Integer, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL

__author__ = "Aaron Su, Elliot Sun"
__copyright__ = "Copyright 2021, EY Technology Consulting"
__credits__ = ["Chhitesh Shrestha"]

# **********Config section**********
config = {
    # General Configs
    'general': {
        'hash_algorithm': 'SHA256', # Choose from ['MD5', 'SHA256']
        'batch_size': 100, # define number of file to generate hash for each batch
        'batch_timeout': 600 # in second
    },
    # DB Configs
    'db': {
        'type': 'sqlite',
        'server': '',
        'username': '',
        'password': '',
        'database': 'meta.db',
        'table_name': 'file_meta'
    }
}

options = {
    'postprocessing':{
        'multiple':{
            'list_col': 'Reference Folder Path',
            'rm_col': 'Remove',
            'process':[
            {
                'type': 'file',
                'option': 1, # remove cols
                'targets': [
                    {'column_name': 'FULL_PATH'},
                    {'column_name': 'CREATION_TIME'},
                    {'column_name': 'COMPRESSED'},
                    {'column_name': 'PROCESSED'},
                    {'column_name': 'ERROR'},
                    {'column_name': 'BATCH NO.'}
                ]
            },
            {
                'type': 'folder',
                'option': 1, # remove cols
                'targets': [
                    {'column_name': 'ROOT_FILE_SIZE'},
                    {'column_name': 'ROOT_FILE_COUNT'},
                    {'column_name': 'ROOT_SUBFOLDER_COUNT'}
                ]
            },
        ]
        }
    }
}
# **********************************

# output logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Create handlers
console_logger = logging.StreamHandler()
file_logger = logging.FileHandler('meta.log')
# console_logger.setLevel(logging.DEBUG)
file_logger.setLevel(logging.WARNING)
# Create formatters and add it to handlers
concole_format = logging.Formatter('[%(levelname)s] %(message)s')
file_format = logging.Formatter('%(asctime)s - [%(levelname)s] %(message)s')
console_logger.setFormatter(concole_format)
file_logger.setFormatter(file_format)
# Add handlers to the logger
logger.addHandler(console_logger)
logger.addHandler(file_logger)

# database definition
Base = declarative_base()

class FileMeta(Base):
    __tablename__ = 'file_meta'

    full_path = Column('FULL_PATH', String, primary_key=True)
    file_name = Column('FILE_NAME', String)
    folder_path = Column('DIRECTORY', String)
    size = Column('SIZE_IN_BYTES', Integer)
    extension = Column('EXTENSION', String)
    creation_time = Column('CREATION_TIME', DateTime)
    modified_time = Column('LAST_MODIFIED_TIME', DateTime)
    hash_algorithm = Column('HASH_ALGORITHM', String)
    hash_value = Column('HASH_VALUE', String)
    is_compressed = Column('COMPRESSED', Boolean)
    is_processed = Column("PROCESSED", Boolean)
    has_error = Column('ERROR', Boolean)
    batch = Column('BATCH NO.', Integer)

# function timeout decorator
def timeout(timelimit):
    def decorator(func):
        def decorated(*args, **kwargs):
            with futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    result = future.result(timelimit)
                except futures.TimeoutError:
                    # print('Timedout!')
                    raise TimeoutError from None
                else:
                    pass
                executor._threads.clear()
                futures.thread._threads_queues.clear()
                return result
        return decorated
    return decorator

# ------------------------------------
# Helper Functions
# get folder metadata including all subfolders and files within root folder
def get_folder_meta(root_path, excl_tmp=False):
    total_size = 0
    total_file_count = 0
    total_folder_count = 0
    for dirpath, dirnames, filenames in os.walk(root_path):
        total_folder_count += len(dirnames)
        total_file_count += len(filenames)
        for f in filenames:
            if excl_tmp:
                if '~$' in f or 'thumbs.db' in f:
                    total_file_count -= 1
                    continue
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return {'size': total_size, 'n_file': total_file_count, 'n_folder': total_folder_count}

def get_file_category(file_path):
    cat = os.path.splitext(file_path)[-1].strip().lower()
    return cat

def get_file_name(file_path: str) -> str:
    fn = os.path.splitext(os.path.basename(file_path))[0].strip()
    return fn

def not_tempfile(file_path: str) -> bool:
        fn: str = get_file_name(file_path)
        if fn.startswith('~$'):
            return False
        elif fn.lower() == 'thumbs.db':
            return False
        else:
            return True

def get_file_hash(file_path):
    if config['general']['hash_algorithm'] == 'MD5':
        hash_f = hashlib.md5()
    elif config['general']['hash_algorithm'] == 'SHA256':
        hash_f = hashlib.sha256()
    else:
        raise Exception("[Error] Unsupported hash algorithm") 

    try:
        with open(file_path, "rb") as f:
        # for memory efficiency
            for chunk in iter(lambda: f.read(4096), b""):
                hash_f.update(chunk)
    except IOError as e:
        logger.error(e)

        
    return hash_f.hexdigest()

def get_zipfile_hash(entry):
    if config['general']['hash_algorithm'] == 'MD5':
        hash_f = hashlib.md5()
    elif config['general']['hash_algorithm'] == 'SHA256':
        hash_f = hashlib.sha256()
    else:
        raise Exception("[Error] Unsupported hash algorithm") 
        
    while True:
        block = entry.read(1024**2) #1M chunks
        if not block:
            break
        hash_f.update(block)
    return hash_f.hexdigest()

# ------------------------------------
# Multiprocessing functions
def populate_hash(fp):
    generated_hash = get_file_hash(fp)
    new_entry = {
        'Full Path': fp,
        'Hash Value': generated_hash
    }
    return new_entry

# ------------------------------------
# Meta Generator Class
class MetaGenerator():
    def __init__(self, output_dir='', multiprocess=False, include_compressed=False, excl_tmp=False, verbose=1):
        # validate path
        self.config = config
        self.options = options
        self.multiprocess = multiprocess
        self.include_compressed = include_compressed
        self.excl_tmp = excl_tmp
        self.verbose = verbose
        self.recon_check = True
        self.top_dir = ''
        self.loaded = False
        self.output_dir = output_dir

        db_config = self.config['db']
        db_type = db_config['type']

        if self.verbose >= 2:
            console_logger.setLevel(logging.DEBUG)
        elif self.verbose == 1:
            console_logger.setLevel(logging.INFO)
        else:
            console_logger.setLevel(logging.WARNING)

        cwd = os.getcwd()
        logger.info(f'Executing from {cwd}')

        # initialize db engine
        if db_type == 'sqlserver':
            db_config['connection_string'] = "DRIVER={SQL Server};" + f"SERVER={db_config['server']};DATABASE={db_config['database']};UID={db_config['username']};PWD={db_config['password']}"
            connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": db_config['connection_string']})
            self.engine = create_engine(connection_url)
        else:
            connection_url = 'sqlite:///' + db_config['database']
            self.engine = create_engine(connection_url)

        try:
            # migrate database
            Base.metadata.create_all(bind=self.engine)
        except:
            logger.error(f'please make sure you have r/w access to this location: {cwd}')
            raise

        # initialize session factory
        self.Session = sessionmaker(bind=self.engine)

        # Create folder dataframe titles
        folder_cols = ['FOLDER_NAME', 'SIZE_IN_BYTES', 'ROOT_FILE_SIZE', 'FILE_COUNT', 'ROOT_FILE_COUNT', 'SUB_FOLDER_COUNT', 'ROOT_SUBFOLDER_COUNT', 'DIRECTORY_PATH', 'FOLDER_DESCRIPTION']
        self.folder_df = pd.DataFrame(columns = folder_cols)

    def has_records(self):
        with self.Session.begin() as session:
            record = session.query(FileMeta).first()
        if record:
            return True
        else:
            return False

    def has_error_records(self):
        with self.Session.begin() as session:
            record = session.query(FileMeta).filter(FileMeta.has_error == True).first()
        if record:
            return True
        else:
            return False

    def has_match(self, file_name, hash_value):
        with self.Session.begin() as session:
            record = session.query(FileMeta) \
                .filter(FileMeta.file_name == file_name) \
                .filter(FileMeta.hash_value == hash_value) \
                .first()
        if record:
            return True
        else:
            return False

    def clean_table(self):
        with self.Session.begin() as session:
            session.query(FileMeta).delete()

    def clean_error(self):
        with self.Session.begin() as session:
            session.query(FileMeta) \
                .filter(FileMeta.has_error == True) \
                .update({'has_error': None})

    def update_hash_value(self, entries, error=False):
        with self.Session.begin() as session:
            if not error:
                # list of dict
                for entry in entries:
                    session.query(FileMeta) \
                        .filter(FileMeta.full_path == entry['Full Path']) \
                        .update({'hash_value': entry['Hash Value']})
                logger.debug('hash values updated in db')
            else:
                # list of string
                for entry in entries:
                    session.query(FileMeta) \
                        .filter(FileMeta.full_path == entry) \
                        .update({'has_error': True})
                logger.warning('error status updated in db')

    def update_zipfile_status(self, entry):
        with self.Session.begin() as session:
            session.query(FileMeta) \
                .filter(FileMeta.full_path == entry['Full Path']) \
                .update({'is_processed': entry['Processed']})
        # print('zipfile status updated in db')

    def retrieve_files_meta(self):
        with self.Session.begin() as session:
            df = pd.read_sql(session.query(FileMeta).statement, session.bind)
        return df

    # efficient count
    def count_files_to_hash(self):
        with self.Session.begin() as session:
            q = session.query(FileMeta).filter(FileMeta.hash_value == None)
            count_q = q.statement.with_only_columns([func.count()])
            count = q.session.execute(count_q).scalar()
        return count

    def count_zipfiles_to_process(self):
        with self.Session.begin() as session:
            q = session.query(FileMeta) \
                .filter(FileMeta.extension.in_(('.zip','.7z'))) \
                .filter(FileMeta.is_processed == None) \
                .filter(FileMeta.is_compressed == None)
            count_q = q.statement.with_only_columns([func.count()])
            count = q.session.execute(count_q).scalar()
        return count

    def count_zipfiles(self):
        with self.Session.begin() as session:
            q = session.query(FileMeta) \
                .filter(FileMeta.extension.in_(('.zip','.7z'))) \
                .filter(FileMeta.hash_value == None)
            count_q = q.statement.with_only_columns([func.count()])
            count = q.session.execute(count_q).scalar()
        return count

    def count_files_hashed(self, dirpath):
        with self.Session.begin() as session:
            q = session.query(FileMeta) \
                .filter(FileMeta.folder_path == dirpath) \
                .filter(FileMeta.is_compressed == None) \
                .filter(FileMeta.hash_value != None)
            count_q = q.statement.with_only_columns([func.count()])
            count = q.session.execute(count_q).scalar()
        return count

    def get_files_to_hash(self):
        #check if there are empty hash
        with self.Session.begin() as session:
            files_to_hash = session.query(FileMeta) \
                .filter(FileMeta.hash_value == None) \
                .filter(FileMeta.has_error == None) \
                .limit(self.config['general']['batch_size']) \
                .all()
            fp_to_hash = [f.full_path for f in files_to_hash]
        return fp_to_hash

    def get_zipfiles_to_process(self):
        #check if there are empty hash
        with self.Session.begin() as session:
            files_to_hash = session.query(FileMeta) \
                .filter(FileMeta.extension.in_(('.zip','.7z'))) \
                .filter(FileMeta.is_processed == None) \
                .filter(FileMeta.is_compressed == None) \
                .limit(self.config['general']['batch_size']) \
                .all()
            zf_to_process = [f.full_path for f in files_to_hash]
        return zf_to_process

    def get_zipfile_processed(self):
        #check if there are empty hash
        with self.Session.begin() as session:
            files_to_hash = session.query(FileMeta) \
                .filter(FileMeta.is_processed != None) \
                .all()
            zf_to_process = [(f.file_name, f.folder_path) for f in files_to_hash]
        return zf_to_process

    def generate_zipfiles_meta(self, no_hash=False):
        batch_number = 1
        n_extracted = 0
        try:
            while True:
                zipfiles_to_process = self.get_zipfiles_to_process()
                if not zipfiles_to_process:
                    break
                
                logger.info(f'batch {batch_number} loaded {len(zipfiles_to_process)} compressed files to hash...')
                batch_start_time = time.time()
                # for all zipfiles that need to be processed
                for zf in zipfiles_to_process:
                    # open the zip file
                    with zipfile.ZipFile(zf, 'r') as archive:
                        # start a db session
                        with self.Session.begin() as session:
                            # retrieve and update meta from the zip package 
                            # for each compressed file/folder (all levels) within the package
                            for elem in archive.infolist():
                                # extract internal file path
                                ifp = elem.filename
                                # get a unique path by combining external and internal path
                                fp = os.path.normpath(os.path.join(zf, ifp))
                                try:
                                    # if it is a directory
                                    if elem.is_dir():
                                        # skip
                                        continue

                                    # instantiate a db record
                                    fm = FileMeta()

                                    # if it's a file then stream the binary through hashing
                                    if no_hash:
                                        pass
                                    else:
                                        with archive.open(ifp) as entry:
                                            hash_value = get_zipfile_hash(entry)
                                        fm.hash_value = hash_value

                                    # add the record into the master table and mark as a compressed file
                                    fm.full_path = fp
                                    fm.file_name = os.path.basename(ifp)
                                    fm.folder_path = os.path.dirname(fp)
                                    fm.size = elem.file_size
                                    fm.extension = get_file_category(ifp)
                                    # fm.creation_time = '' # Cannot get creation time directly
                                    fm.modified_time = datetime(*elem.date_time)
                                    fm.hash_algorithm = self.config['general']['hash_algorithm']
                                    fm.is_compressed = True
                                    # fm.has_error = False
                                    # insert or update this entry
                                    session.merge(fm)
                                except:
                                    logger.error(f'Error occurred trying to update record for {fp} ')
                                    raise

                                # TODO: need to be modified based on the structure of extracts
                                # if there is manifest within the zip
                                # extract it out to output folder
                                if get_file_name(ifp) == get_file_name(zf):
                                    try:
                                        file_output_path = os.path.join(self.output_dir, get_file_name(zf))
                                        logger.info('found a compressed meta data file')
                                        logger.info(f'extracting {ifp} to {file_output_path}...')
                                        archive.extract(ifp, file_output_path)
                                        n_extracted += 1
                                    except:
                                        logger.error(f'failed to extract {ifp}')
                                        raise
                    # update the record for this zipfile
                    zipfile_entry = {
                        'Full Path': zf,
                        'Processed': True
                    }
                    self.update_zipfile_status(zipfile_entry)
                    logger.info(f'{os.path.basename(zf)} has been processed')

                logger.info(f'hash generated for zipfile batch {batch_number}. time used: {time.time()-batch_start_time}')
                batch_number += 1
            logger.info(f'extracted {n_extracted} compressed meta file(s)')
        except:
            logger.error(f'Exception occurred during hashing zipfile batch {batch_number}')
            raise
        logger.info('all zipfiles processed')
        return


    def generate_tree(self, top_dir):
        if not top_dir:
            logger.error('Source direcotry is not valid')
            raise Exception()
        elif not os.path.exists(top_dir):
            logger.error('Directory does not exist or is not connected, please verify input path')
            raise Exception()
        self.top_dir = top_dir
        logger.debug(f'Generating directory tree for {os.path.basename(top_dir)}')

        # Step 1: get files and folders meta (without total count and size)
        # begin() perform auto-commit at exit
        with self.Session.begin() as session:
            n_file_scanned = 0
            # pre-populate folder entries
            for dirpath, dirnames, filenames in os.walk(top_dir):
                # clean mapping buffer
                file_meta_mappings: List[dict] = []

                # initialise total root files size
                root_file_size = 0

                # skip temp file if required
                if self.excl_tmp:
                    filtered_filenames = [fn for fn in filenames if not_tempfile(fn)]
                else:
                    filtered_filenames = filenames

                # All files under the top dir
                for filename in filtered_filenames:
                    try:
                        fp = os.path.join(dirpath, filename)
                        fsize = os.path.getsize(fp)
                        file_meta_mappings.append(
                            dict(
                                full_path = fp,
                                file_name = filename,
                                folder_path = os.path.dirname(fp),
                                size = fsize,
                                extension = get_file_category(fp),
                                creation_time = datetime.fromtimestamp(os.path.getctime(fp)),
                                modified_time = datetime.fromtimestamp(os.path.getmtime(fp)),
                                hash_algorithm = self.config['general']['hash_algorithm']
                            )
                        )
                    except:
                        logger.error(f'Error occurred trying to update record for {fp}')
                    else:
                        root_file_size += fsize
                    finally:
                        n_file_scanned += 1
                        if n_file_scanned % 1000 == 0:
                            logger.info(f'{n_file_scanned} files scanned...')

                # update session at each folder, skip ORM for efficiency
                session.bulk_insert_mappings(FileMeta, file_meta_mappings)

                new_entry = {
                    'FOLDER_NAME': os.path.basename(dirpath),
                    'ROOT_FILE_COUNT': len(filtered_filenames),
                    'ROOT_SUBFOLDER_COUNT': len(dirnames),
                    'ROOT_FILE_SIZE': root_file_size,
                    'DIRECTORY_PATH': dirpath,
                    'FOLDER_DESCRIPTION': ''
                    }
                self.folder_df = self.folder_df.append(new_entry,ignore_index = True)

        # Step 2: Calculate total count and size using bottom-up (reverse iteration) approach
        logger.info('consolidating folder manifest...')

        path_series: pd.Series = self.folder_df['DIRECTORY_PATH']
        # use itertuples as the most efficient way to iterate row with named value
        # https://stackoverflow.com/questions/7837722/what-is-the-most-efficient-way-to-loop-through-dataframes-with-pandas
        for folder_record in self.folder_df[::-1].itertuples(index=True, name='FolderManifestRecord'):
            child_size = 0
            child_file_count = 0
            child_folder_count = 0
            index = folder_record.Index
            # convert the path to raw string as input to regex
            #path_regex = folder_record.DIRECTORY_PATH.encode('unicode_escape').decode()
            path_regex = re.escape(folder_record.DIRECTORY_PATH)
            # match child folder paths
            direct_child_folders: pd.DataFrame = self.folder_df[path_series.str.match(fr'^({path_regex})[\\\/][^\\\/]+$')]
            # iterate through child folder record
            for child_folder_record in direct_child_folders.itertuples(index=False, name='FolderManifestRecord'):
                child_size += child_folder_record.SIZE_IN_BYTES
                child_file_count += child_folder_record.FILE_COUNT
                child_folder_count += child_folder_record.SUB_FOLDER_COUNT

            # update folder meta
            self.folder_df.loc[index, 'SIZE_IN_BYTES'] = folder_record.ROOT_FILE_SIZE + child_size
            self.folder_df.loc[index, 'FILE_COUNT'] = folder_record.ROOT_FILE_COUNT + child_file_count
            self.folder_df.loc[index, 'SUB_FOLDER_COUNT'] = folder_record.ROOT_SUBFOLDER_COUNT + child_folder_count
        return

    @timeout(config['general']['batch_timeout'])
    def hash_batch(self, files_to_hash):
        results = map(populate_hash, files_to_hash)
        return list(results)

    # hash all files (single process)
    def hash_all(self):
        batch_number = 1
        retry = 1
        files_to_hash = []
        while True: 
            try:
                files_to_hash = self.get_files_to_hash()
                if not files_to_hash:
                    break

                logger.info(f'batch {batch_number} loaded {len(files_to_hash)} files to generate...') 
                batch_start_time = time.time()
                results = self.hash_batch(files_to_hash)
                logger.info(f'hash generated. time used for batch {batch_number}: {time.time() - batch_start_time}')
                self.update_hash_value(results)
            except TimeoutError:
                if retry <= 3:
                    logger.info(f'Timeout executing batch {batch_number}, retrying {retry} out of 3...')
                    retry += 1
                else:
                    logger.warning(f'Maximum retries reached, marking batch {batch_number} as error and skipping over')
                    self.update_hash_value(files_to_hash, error=True)
                    retry = 1
            except:
                logger.error(f'Exception occurred during hashing batch {batch_number}')
                raise
            else:
                retry = 1
                batch_number += 1
        logger.info('hashing completed')
        return

    # hash all files (multi-process)
    # TODO: implement mp timeout
    def hash_all_mp(self):
        batch_number = 1
        n_proc = mp.cpu_count() - 1
        with mp.Pool(n_proc) as pool:
            logger.info(f'spawned {n_proc} processes')
            try:
                while True:        
                    files_to_hash = self.get_files_to_hash()
                    if not files_to_hash:
                        break

                    logger.info(f'batch {batch_number} loaded {len(files_to_hash)} files to generate...') 
                    batch_start_time = time.time()
                    results = pool.map(populate_hash, files_to_hash)
                    logger.info(f'hash generated. time used for batch {batch_number}: {time.time()-batch_start_time}')
                    self.update_hash_value(results)
                    batch_number += 1
            except:
                logger.error(f'Exception occurred during hashing batch {batch_number}')
                raise

        logger.info('hashing completed')
        return

    # Mains
    def generate_meta(self, top_dir, no_hash=False):
        # clean entire table
        clean = False
        # retry failed records
        retry = False
        # regenerate folder hierarchy
        regen = True
        
        if self.has_records():
            if self.count_files_to_hash() <= 0 and self.count_zipfiles_to_process() <= 0:
                restart = input('[INPUT REQUIRED] Scan has been completed previously, clean table and start again (Y/N)? ')
                if restart.lower() in ['y', 'yes']:
                    clean = True
            else:
                resume = input('[INPUT REQUIRED] found unfinished record(s), resume (1) or start over (0)? ')
                if resume == '0':
                    clean = True
                else:
                    if self.has_error_records():
                        retry = input('[INPUT REQUIRED] found failed record(s), retry (Y/N)? ')
                        if retry.lower() in ['yes','y']:
                            retry = True
                    if not self.loaded:
                        refresh = input('[INPUT REQUIRED] Do you want to refresh the tree for modified files (Y/N)? ')
                        if refresh.lower() in ['no','n']:
                            regen = False
        
        if clean:
            self.clean_table()
            logger.info('cleaned existing records and starting over')
        elif retry:
            self.clean_error()
            logger.info('cleaned error status for records')

        if self.loaded:
            logger.info('Using loaded directory tree')
        elif regen:
            logger.info(f'Exploring {top_dir} ...')
            self.generate_tree(top_dir)

        if no_hash:
            logger.info('skipped hashing')
            pass
        else:
            logger.info(f'found {self.count_files_to_hash()} normal files to be hashed, from which {self.count_zipfiles()} are zipfiles')
            logger.info('start generating hash for normal files...')
            if self.multiprocess:
                self.hash_all_mp()
            else:
                self.hash_all()

        if self.include_compressed:
            logger.info('start generating meta for zipfile content...')
            # generate meta for zipfiles marked with unprocessed & uncompressed
            self.generate_zipfiles_meta(no_hash)
        return

    def reconcile(self, mode=0):
        logger.debug(f'Reconciliation mode {mode}')
        DEFAULT = 1
        ZIP_MODE = 2
        ZIP_MODE_EXT = 3
        self.recon_check = True
        skip = False
        if mode == 0:
            skip = True
        elif mode == DEFAULT:
            for index, row in self.folder_df.iterrows():
                source_count = row['ROOT_FILE_COUNT']
                processed_count = self.count_files_hashed(row['DIRECTORY_PATH'])
                self.folder_df.loc[index, 'FILES_HASHED'] = processed_count
                if source_count != processed_count:
                    self.recon_check = False
                    fn = row['FOLDER_NAME']
                    logger.info(f'{fn} has failed reconciliation, source count: {source_count}, processed count {processed_count}')
        elif mode in [ZIP_MODE, ZIP_MODE_EXT]:
            source_list = self.get_zipfile_processed()
            if not source_list:
                skip = True
                logger.info(f'This folder does not contain zip file')
            try:
                n_done = 0
                n_total = len(source_list)
                for (zf_name, dir_path) in source_list:
                    file_name = get_file_name(zf_name)
                    if mode == ZIP_MODE:
                        logger.info(f'reconciling {file_name}...')
                        manifest_df = pd.read_csv(os.path.join(dir_path, file_name + '.csv'))
                        n_not_found = 0
                        for index, row in manifest_df.iterrows():
                            content_file_name = row['File Name']
                            if not self.has_match(content_file_name, row['Hash Value']):
                                n_not_found += 1
                                self.recon_check = False
                                manifest_df.loc[index, 'Unmatched'] = True
                                logger.debug(f'{content_file_name} did not find a match')
                        if n_not_found > 0:
                            output_fp = os.path.join(self.output_dir, 'recon_log_' + file_name + '.csv')
                            logger.info(f'Saving recon log to {output_fp}...')
                            manifest_df.to_csv(output_fp, index = False, header=True)
                        n_done += 1
                        logger.info(f'reconcilied {index+1} records, {n_not_found} unmatched')
                        logger.info(f'reconciled {n_done} out of {n_total} zipfiles')
                    elif mode == ZIP_MODE_EXT:
                        skip = True
                        if self.output_dir:
                            fp = os.path.join(dir_path, file_name + '.csv')
                            output_subdir = os.path.join(self.output_dir, file_name)
                            if not os.path.exists(output_subdir):
                                os.makedirs(output_subdir)
                            logger.info(f'exporting {file_name} to {self.output_dir}')
                            copy2(fp, output_subdir)
                            n_done += 1
                        else:
                            logger.info('output directory is not specified, skipped export')
                logger.info(f'reconciled/copied {n_done} out of {n_total} zipfiles')
            except:
                logger.error('failed to reconcile')
                raise

        if skip:
            logger.info('skipped reconciliation')
        elif self.recon_check:
            logger.info('Reconciliation passed')
        else:
            logger.warning('Reconciliation has failed. Check output csv.')
        return

    def postprocess(self, df, option, targets=[]):
        """
        Post-process the result df from generation inplace
        option:
        1. remove content in chosen columns
        2. remove chosen columns

        targets must be a list of dict containing the column name and content
        e.g. to remove \\\\AU1111.com.au in the DIRECTORY column, use:
        targets = [
            {
                'column_name': 'DIRECTORY', (str)
                'content': '\\\\AU1111.com.au', (str)
                ...
            }
        ]
        """
        REMOVE_COLS = 1
        REMOVE_COTENT = 2
        
        final_df = df # pointer to the same object/dataframe
        if option == REMOVE_COLS:
            for target in targets:
                column_name = target['column_name']
                if column_name in final_df.columns:
                    final_df.drop(column_name, inplace=True, axis=1)
                    logger.debug(f'removed column: {column_name}')
        elif option == REMOVE_COTENT:
            for target in targets:
                column_name = target['column_name']
                content = target['content']
                if column_name in final_df.columns:
                    # TODO: replace iterrows
                    for index, row in df.iterrows():
                        final_df.at[index, column_name] = row[column_name].replace(content, '', 1)
                    logger.debug(f'removed {content} in column: {column_name}')
        return final_df

    def output_zipfile_meta(self, output_dir=''):
        if not output_dir:
            output_dir = self.output_dir
            logger.info(f'output folder default to {output_dir}')
        with self.Session.begin() as session:
            folders = session.query(FileMeta.folder_path) \
                .distinct() \
                .filter(FileMeta.is_compressed == True) \
                .all()
            n_total = len(folders)
            logger.info(f'outputing {n_total} zipfile manifests')
            n_output = 0
            for f in folders:
                fp = f.folder_path
                fn = get_file_name(fp)
                logger.info(f'saving zipfile manifest for {fn} in {output_dir}...')
                df = pd.read_sql(session.query(FileMeta).filter(FileMeta.folder_path == fp).statement, session.bind)
                output_subdir = f'{output_dir}\{fn}'
                if not os.path.exists(output_subdir):
                    os.makedirs(output_subdir)
                df.to_csv(f'{output_subdir}\{fn}_SDPS.csv',index = False, header=True)
                n_output += 1
            logger.info(f'done outputing {n_output} out of {n_total} zipfile manifests')

    # output manifest for a SINGLE directory
    def output_meta(self, top_dir, output_dir='', recon_mode=0, no_hash=False, post_processing=[], rm_substring=''):
        if not output_dir:
            output_dir = self.output_dir
            logger.info(f'output folder default to {output_dir}')

        self.top_dir = top_dir
        # generate meta to the db
        self.generate_meta(top_dir, no_hash=no_hash)

        # retrieve the files result from db
        file_df = self.retrieve_files_meta()
        # reconcile file and folder records
        self.reconcile(mode=recon_mode)

        for process in post_processing:
            # file meta postprocessing
            if process['type'] == 'file':
                # continue with postprocessing
                self.postprocess(file_df, process['option'], targets=process['targets'])
            elif process['type'] == 'folder':
                # folder meta postprocessing
                self.postprocess(self.folder_df, process['option'], targets=process['targets'])

        if not os.path.exists(output_dir):
            logger.info('output direcotry did not exist, creating one at the specified location')
            os.makedirs(output_dir)
        logger.info(f'saving to {output_dir} ...')
        
        folder_name = os.path.basename(top_dir)
        # user defined output name format
        if rm_substring:
            folder_name = top_dir.replace(rm_substring, '', 1)
            folder_name = folder_name.replace('\\', '_')

        datetime_stamp = str(datetime.today().strftime('%Y%m%d%H%M%S')) 
        folder_m_path = os.path.join(output_dir, f'{folder_name}_folder_metadata_{datetime_stamp}.csv')
        file_m_path = os.path.join(output_dir, f'{folder_name}_file_metadata_{datetime_stamp}.csv')
        suffix = 0
        while True:
            try:
                if os.path.exists(folder_m_path):
                    suffix += 1
                    folder_m_path = os.path.join(output_dir, f'{folder_name}_folder_metadata_{suffix}.csv' )
                    file_m_path = os.path.join(output_dir, f'{folder_name}_file_metadata_{suffix}.csv' )
                    continue
                else:
                    self.folder_df.to_csv(folder_m_path, index = False, header=True)
                    file_df.to_csv(file_m_path, index = False, header=True)
                    logger.info(f'saved the manifest for {folder_name} in {folder_m_path}')
                    break
            except PermissionError:
                input('[INPUT REQUIRED] Permission Error occurred, please close the relevant files and press ENTER...')
        return

    # output manifests for MULTIPLE directory
    def output_manifests(self, list_path, output_dir='', recon_mode=0, no_hash=False):
        if not output_dir:
            output_dir = self.output_dir
            logger.info(f'output folder default to {output_dir}')

        if not os.path.isfile(list_path):
            logger.error(f'{list_path} does not exist, please verify input path')
            raise Exception()
        else:
            fl_df = pd.read_csv(list_path)

        cols = fl_df.columns

        list_col = self.options['postprocessing']['multiple']['list_col']
        rm_col = self.options['postprocessing']['multiple']['rm_col']

        if not list_col in cols:
            logger.error(f'could not find the reference column {list_col} in the csv')
            return

        unfinished_df = pd.DataFrame(columns = cols)

        folder_cols = ['FOLDER_NAME', 'SIZE_IN_BYTES', 'FILE_COUNT', 'ROOT_FILE_COUNT', 'SUB_FOLDER_COUNT', 'DIRECTORY_PATH', 'FOLDER_DESCRIPTION']

        # TODO: replace iterrows
        for index, row in fl_df.iterrows():
            try:
                self.clean_table()
                top_dir = row[list_col]

                logger.debug(f'Generating manifest for {top_dir}...')

                # reset folder dataframe
                self.folder_df = pd.DataFrame(columns = folder_cols)
                
                post_processing = self.options['postprocessing']['multiple']['process']

                rm_substring = ''

                if rm_col in cols:
                    # post-processing directory path cleanse
                    rm_substring = str(row[rm_col])
                    if not rm_substring or rm_substring == 'nan':
                        rm_substring = ''
                        logger.warning(f'remove string is not specified for row {index + 2}: {top_dir}')
                    else:
                        post_processing.extend([
                            {
                                'type': 'file',
                                'option': 2, # remove content
                                'targets': [
                                    {
                                        'column_name': 'DIRECTORY',
                                        'content': rm_substring
                                    }
                                ]
                            },
                            {
                                'type': 'folder',
                                'option': 2, # remove content
                                'targets': [
                                    {
                                        'column_name': 'DIRECTORY_PATH',
                                        'content': rm_substring
                                    }
                                ]
                            },
                        ])

                self.output_meta(top_dir, output_dir, recon_mode=recon_mode, post_processing=post_processing, rm_substring=rm_substring, no_hash=no_hash)
                logger.info(f'-------------------------------------------------- folder {index+1} processed')
            except Exception as e:
                logger.error(str(e))
                logger.error(f'failed to generate manifest for {row[list_col]}, skipping...')
                unfinished_df = unfinished_df.append(row, ignore_index=True)
                logger.info(f'-------------------------------------------------- folder {index+1} failed')
        n_unsuccessful = len(unfinished_df.index)
        if n_unsuccessful > 0:
            output_path = os.path.join(output_dir, 'unfinished.csv')
            unfinished_df.to_csv(output_path, index = False, header=True)
            logger.error(f'failed to scan {n_unsuccessful} directory')
            logger.warning(f'unsuccessful directory scan(s) has been recorded in {output_path}')
        return

    def combine(self, pattern, header=[], output_dir='', output_name=''):
        if not output_dir:
            output_dir = self.output_dir
            logger.info(f'output folder default to {output_dir}')
        dir_list = [f.path for f in os.scandir(output_dir) if f.is_dir()]
        combined_df = pd.DataFrame()
        logger.info(f'combining files end with {pattern} over {len(dir_list)} output subfolders...')
        for _dir in dir_list:
            entity_name = os.path.basename(_dir)
            if pattern.endswith('.txt'):
                # default to skip the first row in a txt file
                temp_df = pd.read_csv(os.path.join(_dir, f'{entity_name}{pattern}'), header=None, skiprows=[0])
                temp_df.iloc[-1, 11] = entity_name
            else:
                temp_df = pd.read_csv(os.path.join(_dir, f'{entity_name}{pattern}'))
            combined_df = pd.concat([combined_df, temp_df], ignore_index=True, sort=False)
        if header:
            combined_df.columns = header
        if output_name:
            combined_df.to_csv(f'{output_dir}\{output_name}', index = False, header=True)
        else:
            combined_df.to_csv(f'{output_dir}\combined{pattern}', index = False, header=True)
        logger.info(f'combined file has been saved in {output_dir}')

    def load_tree(self, file_tree, batches=[]):
        file_df = pd.read_csv(file_tree)
        if batches:
            file_df = file_df[file_df['BATCH NO.'].isin(batches)]
        file_df.to_sql('file_meta', con=self.engine, if_exists='replace', index=False)
        self.loaded = True

if __name__ == '__main__':
    # CLI arguments
    parser = argparse.ArgumentParser(description='Generate metadata for folders and files')
    parser.add_argument('-s', '--source', type=str, metavar='', help='Source directory where the manifest will be generated from')
    parser.add_argument('-o', '--output_dir', type=str, metavar='', help='Manifest output directory, default to CWD')
    parser.add_argument('-fl', '--folder_list', type=str, metavar='', help='The csv contain the folder list to generate manifest from')
    parser.add_argument('--recon_mode', type=int, metavar='', help='Choose a reconciliation mode', choices=[1,2,3])
    parser.add_argument('-v', '--verbose', type=int, metavar='', help='Set the verbose level for CLI output')
    parser.add_argument('-mp', '--multiprocess', action='store_true', help='Enable multiprocessing for hash generation')
    parser.add_argument('--inc_zip', action='store_true', help='Generate manifest for zipfile content')
    parser.add_argument('--excl_tmp', action='store_true', help='Ignore temporary files (e.g. ~$data.xlsx, thumbs.db)')
    parser.add_argument('--no_hash', action='store_true', help='Skip generating hash for all files')
    args = parser.parse_args()

    if args.source or args.folder_list:
        startTime = time.time()

        verbose = args.verbose if args.verbose else 1

        # if not specified, default to CWD
        output_dir = args.output_dir if args.output_dir else os.getcwd()

        generator = MetaGenerator(
            output_dir=output_dir, \
            multiprocess=args.multiprocess, \
            include_compressed=args.inc_zip, \
            excl_tmp=args.excl_tmp, \
            verbose=verbose \
        )

        # if flagged folder_list, generate manifest for list of folders
        if args.folder_list:
            generator.output_manifests(args.folder_list, recon_mode=args.recon_mode, no_hash=args.no_hash)
        elif args.source:
            generator.output_meta(args.source, recon_mode=args.recon_mode, no_hash=args.no_hash)
        logger.info('Done, total time elapsed: %s', time.time() - startTime)
    else:
        logger.info('Source directory or folder list location (CLI arguments) not specified, try run the script with "--help"')