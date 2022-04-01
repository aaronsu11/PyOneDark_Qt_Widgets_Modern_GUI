# Meta Manager core logics
# libraries
import os
import re
import time
import json
import logging
import pandas as pd
from typing import List, Dict
# scheduler
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
# core
from .meta_generator import MetaGenerator

# 1. Function to generate meta data ( filename, path, size, hash) for a file
# 2. Wrapper function to walk through files recursively in a folder and generate a meta data csv file
# 3. Function to compare 2 versions of meta data csv file and generate one report in Excel with multiple sheets
# 4. Main function ( main entry point) to read a list of folders from a config file and run step 2 and 3
# 5. Schedule the main function in task manager or a long running application
# 6. Design of config file

# output logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Create handlers
console_logger = logging.StreamHandler()
file_logger = logging.FileHandler('meta.log')
# console_logger.setLevel(logging.DEBUG)
file_logger.setLevel(logging.WARNING)
# Create formatters and add it to handlers
concole_format = logging.Formatter('%(asctime)s - [%(levelname)s] %(message)s')
file_format = logging.Formatter('%(asctime)s - [%(levelname)s] %(message)s')
console_logger.setFormatter(concole_format)
file_logger.setFormatter(file_format)
# Add handlers to the logger
logger.addHandler(console_logger)
logger.addHandler(file_logger)

class MetaManager:
    
    # default paths
    config_path: str = os.path.join(os.getcwd(), 'config.json')
    output_dir: str = os.path.join(os.getcwd(), 'manifest')
    # scheduler
    scheduler: BaseScheduler = None
    # default config for loading csv
    config: dict = {
        # https://docs.trifacta.com/display/DP/Supported+Time+Zone+Values
        'timezone': '',
        # https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html#module-apscheduler.triggers.cron
        'jobs': [],
        'csv': {
            'target_col': 'Reference Folder Path',
            'rm_col': 'Remove',
            'output_col': 'Manifest Output',
            'delta_output_col': 'Delta Output',
            'baseline_col': 'Manifest Baseline Version',
            'schedule_col': 'Schedule Crontab'
        }, 
        'options': {
            'postprocessing': [
                {
                    'type': 'file',
                    'option': 1,
                    'targets': [
                        {'column_name': 'FULL_PATH'},
                        {'column_name': 'COMPRESSED'},
                        {'column_name': 'PROCESSED'},
                        {'column_name': 'ERROR'},
                        {'column_name': 'BATCH NO.'}
                    ]
                },
                {
                    'type': 'folder',
                    'option': 1,
                    'targets': [
                        {'column_name': 'ROOT_FILE_SIZE'},
                        {'column_name': 'ROOT_FILE_COUNT'},
                        {'column_name': 'ROOT_SUBFOLDER_COUNT'}
                    ]
                }
            ]
        }      
    }

    @classmethod
    def read_csv_config(cls, fp: str) -> None:
        csv_config = pd.read_csv(fp, index_col=None).fillna('')

        target_col: str = cls.config['csv']['target_col']
        rm_col: str = cls.config['csv']['rm_col']
        manifest_output_col: str = cls.config['csv']['output_col']
        delta_output_col: str = cls.config['csv']['delta_output_col']
        baseline_col: str = cls.config['csv']['baseline_col']
        schedule_col: str = cls.config['csv']['schedule_col']

        if not target_col in csv_config.columns:
            raise ValueError('No folder path provided under "Reference Folder Path" column, exit...')

        custom_remove: bool = rm_col in csv_config.columns
        custom_output: bool = manifest_output_col in csv_config.columns
        custom_delta_output: bool = delta_output_col in csv_config.columns
        custom_baseline: bool = baseline_col in csv_config.columns
        custom_schedule: bool = schedule_col in csv_config.columns

        job_config: List[dict] = []

        for i, row in csv_config.iterrows():
            # target directory
            directory = row[target_col]
            if not directory:
                continue

            # output config
            output_dir = cls.output_dir
            if custom_output and row[manifest_output_col]:
                output_dir = row[manifest_output_col]

            delta_output_dir = os.path.join(output_dir, 'delta')
            if custom_delta_output and row[delta_output_col]:
                delta_output_dir = row[delta_output_col]

            baseline = ''
            if custom_baseline and row[baseline_col]:
                baseline = row[baseline_col]
                if not isinstance(baseline, str):
                    baseline = str(int(baseline))

            remove = ''
            if custom_remove and row[rm_col]:
                remove = row[rm_col]

            schedule = ''
            if custom_schedule and row[schedule_col]:
                schedule = row[schedule_col]

            job_config.append(
                dict(
                    target=directory,
                    output_dir=output_dir,
                    delta_output_dir=delta_output_dir,
                    baseline=baseline,
                    mask=remove,
                    schedule=schedule
                )
            )
        # save config to manager
        cls.config['jobs'] = job_config

    @classmethod
    def read_json_config(cls, fp: str='') -> None:
        if not fp:
            fp = cls.config_path

        with open(fp, 'r') as f:
            custom_config: dict = json.load(f)

        for key, value in custom_config.items():
            if key in cls.config:
                cls.config[key] = value
            else:
                logger.warning(f'unsupported field "{key}" in custom config')

    @classmethod
    def save_json_config(cls, fp: str='', save_jobs: bool=False) -> None:
        if not fp:
            fp = cls.config_path

        output_config = cls.config.copy()
        if not save_jobs:
        # reset temporary jobs field
            output_config['jobs'] = [
                {
                    "target": "<full directory path>",
                    "output_dir": "<full manifest output path>",
                    "delta_output_dir": "<full delta output path>",
                    "baseline": "<baseline manifest timestamp>",
                    "mask": "<substring to remove in path>",
                    "schedule": {
                        "year": "*", 
                        "month": "*", 
                        "day": "*", 
                        "week": "*/2", 
                        "day_of_week": "sat", 
                        "hour": 4, 
                        "minute": 0
                    }
                }
            ]
        # save config
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(output_config, f, ensure_ascii=False, indent=4)

    @classmethod
    def generate_manifest(cls, source_dir: str, output_dir: str, mask: str='', post_processing=[]) -> str:
        # Instantiate the generator object
        generator = MetaGenerator(
            # set the manifest output location
            output_dir=output_dir if output_dir else cls.output_dir,
            # option to exclude temp file (e.g. ~$data.xlsx, thumbs.db) during scanning
            excl_tmp=False,
            # verbosity of log output
            verbose=0
        )
        generator.clean_table()
        file_meta_fp, folder_meta_fp = generator.output_meta(
            # choose the directory for scan
            source_dir,
            # custom postprocessing steps
            post_processing=post_processing, 
            # mask sub string for paths
            rm_substring=mask
        )
        return file_meta_fp

    @staticmethod
    def compare_manifests(source: str, target: str) -> Dict[str, pd.DataFrame]:
        # read source and target manifest
        df_source = pd.read_csv(
            source, 
            usecols=['FILE_NAME', 'DIRECTORY', 'CREATION_TIME', 'HASH_VALUE'], 
            dtype={
                'FILE_NAME': str,
                'DIRECTORY': str,
                'CREATION_TIME': str,
                'HASH_VALUE': str
            }, 
            index_col=None,
            low_memory=True
        ).reset_index()
        df_target = pd.read_csv(
            target,
            usecols=['FILE_NAME', 'DIRECTORY', 'CREATION_TIME', 'HASH_VALUE'], 
            dtype={
                'FILE_NAME': str,
                'DIRECTORY': str,
                'CREATION_TIME': str,
                'HASH_VALUE': str
            }, 
            index_col=None,
            low_memory=True
        ).reset_index()

        # identify differences
        df_diff = df_source.merge(df_target, how='outer', on=['FILE_NAME', 'DIRECTORY', 'CREATION_TIME', 'HASH_VALUE'], suffixes=('_OLD', '_NEW'), indicator=True)
        df_matched: pd.DataFrame = df_diff.loc[lambda x : x['_merge']=='both'].drop(columns=['_merge'])
        df_source_only: pd.DataFrame = df_diff.loc[lambda x : x['_merge']=='left_only'].drop(columns=['index_NEW', '_merge']).rename(columns = {'index_OLD':'index'})
        df_target_only: pd.DataFrame = df_diff.loc[lambda x : x['_merge']=='right_only'].drop(columns=['index_OLD', '_merge']).rename(columns = {'index_NEW':'index'})

        # adjust the index values
        df_matched['index_OLD'] = df_matched['index_OLD'].add(2).astype(int)
        df_matched['index_NEW'] = df_matched['index_NEW'].add(2).astype(int)
        df_source_only['index'] = df_source_only['index'].add(2).astype(int)
        df_target_only['index'] = df_target_only['index'].add(2).astype(int)

        # identify files that have been renamed
        renamed_df = df_source_only.merge(df_target_only, on=['DIRECTORY', 'HASH_VALUE', 'CREATION_TIME'], suffixes=('_OLD', '_NEW'))
        drop_renamed_old = renamed_df['index_OLD'].to_list()
        drop_renamed_new = renamed_df['index_NEW'].to_list()
        # renamed_df.drop(columns=['_merge_OLD', '_merge_NEW'], inplace=True)

        # mask renamed files
        df_source_only = df_source_only.query('index not in @drop_renamed_old')
        df_target_only = df_target_only.query('index not in @drop_renamed_new')

        # identify files that have been moved
        moved_df = df_source_only.merge(df_target_only, on=['FILE_NAME', 'HASH_VALUE', 'CREATION_TIME'], suffixes=('_OLD', '_NEW'))
        drop_moved_old = moved_df['index_OLD'].to_list()
        drop_moved_new = moved_df['index_NEW'].to_list()
        # moved_df.drop(columns=['_merge_OLD', '_merge_NEW'], inplace=True)

        # mask moved files
        df_source_only = df_source_only.query('index not in @drop_moved_old')
        df_target_only = df_target_only.query('index not in @drop_moved_new')

        # identify files that have been edited
        edited_df = df_source_only.merge(df_target_only, on=['DIRECTORY', 'FILE_NAME', 'CREATION_TIME'], suffixes=('_OLD', '_NEW'))
        drop_edited_old = edited_df['index_OLD'].to_list()
        drop_edited_new = edited_df['index_NEW'].to_list()

        # mask edited files
        df_source_only = df_source_only.query('index not in @drop_edited_old')
        df_target_only = df_target_only.query('index not in @drop_edited_new')

        # rename column name for reporting
        renamed_df.rename(columns = {'index_OLD':'SOURCE_INDEX', 'index_NEW':'TARGET_INDEX', 'FILE_NAME_OLD':'SOURCE_FILE_NAME', 'FILE_NAME_NEW': 'TARGET_FILE_NAME'}, inplace = True)
        moved_df.rename(columns = {'index_OLD':'SOURCE_INDEX', 'index_NEW':'TARGET_INDEX', 'DIRECTORY_OLD':'SOURCE_DIRECTORY', 'DIRECTORY_NEW': 'TARGET_DIRECTORY'}, inplace = True)
        edited_df.rename(columns = {'index_OLD':'SOURCE_INDEX', 'index_NEW':'TARGET_INDEX'}, inplace = True)
        df_matched.rename(columns = {'index_OLD':'SOURCE_INDEX', 'index_NEW':'TARGET_INDEX'}, inplace = True)
        df_source_only.rename(columns = {'index':'SOURCE_INDEX'}, inplace = True)
        df_target_only.rename(columns = {'index':'TARGET_INDEX'}, inplace = True)

        # rearrange column order
        renamed_df = renamed_df[['DIRECTORY', 'SOURCE_INDEX', 'SOURCE_FILE_NAME', 'TARGET_INDEX', 'TARGET_FILE_NAME']]
        moved_df = moved_df[['FILE_NAME', 'SOURCE_INDEX', 'SOURCE_DIRECTORY',  'TARGET_INDEX', 'TARGET_DIRECTORY']]
        edited_df = edited_df[['DIRECTORY', 'FILE_NAME', 'SOURCE_INDEX', 'TARGET_INDEX']]
        df_matched = df_matched[['DIRECTORY', 'FILE_NAME', 'SOURCE_INDEX', 'TARGET_INDEX']]
        df_source_only = df_source_only[['SOURCE_INDEX', 'DIRECTORY', 'FILE_NAME']]
        df_target_only = df_target_only[['TARGET_INDEX', 'DIRECTORY', 'FILE_NAME']]

        return {'matched': df_matched, 'renamed': renamed_df, 'moved': moved_df, 'edited': edited_df, 'deleted': df_source_only, 'new': df_target_only}

    @staticmethod
    def generate_report(comparison_dict: Dict[str, pd.DataFrame], output_fp: str) -> None:
        # write to xlsx report
        writer = pd.ExcelWriter(output_fp)
        # summary page place holder
        pd.DataFrame().to_excel(writer, 'Summary')
        summary_rows = []
        # report each category
        for key, df in comparison_dict.items():
            # get summary stats
            n_record = len(df.index)
            summary_rows.append({'Category': key.upper(), "Count": n_record})
            # skip matched records
            if key == 'matched':
                continue
            # generate excel report
            df.to_excel(writer, key.upper(), index=False)
        # fill in summary page
        pd.DataFrame(summary_rows).to_excel(writer, 'Summary', index=False)
        writer.save()

    @classmethod
    def generate_delta(cls, dir_config: dict, postprocessing: list=[], callback=None) -> None:
        '''generate delta for single directory'''

        # 0. parse config
        if not 'target' in dir_config or not dir_config['target']:
            raise ValueError('target directory is not specified')

        source_dir = dir_config['target']
        dir_name = os.path.basename(source_dir)

        output_dir = cls.output_dir
        if 'output_dir' in dir_config and dir_config['output_dir']:
            output_dir = dir_config['output_dir']

        delta_output_dir: str = os.path.join(output_dir, 'delta')
        if 'delta_output_dir' in dir_config and dir_config['delta_output_dir']:
            delta_output_dir = dir_config['delta_output_dir']

        mask = ''
        if 'mask' in dir_config and dir_config['mask']:
            mask = dir_config['mask']
            # user defined output name format
            dir_name = source_dir.replace(mask, '', 1)
            dir_name = dir_name.replace('\\', '_')
            # additional postprocessing
            postprocessing.extend([
                {
                    'type': 'file', 
                    'option': 2, # remove substring
                    'targets': [{'column_name': 'DIRECTORY','content': mask}]
                },
                {
                    'type': 'folder', 
                    'option': 2, # remove substring
                    'targets': [{'column_name': 'DIRECTORY_PATH', 'content': mask}]
                },
            ])

        # 1. find benchmark file if exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        if 'baseline' in dir_config and dir_config['baseline']:
            fn = dir_config['baseline']
            # if timestamp is provided only
            if fn.isdigit():
                fn = f'{dir_name}_file_metadata_{int(fn)}.csv'
            baseline = os.path.join(output_dir, fn)
            if not os.path.isfile(baseline):
                raise FileNotFoundError('Baseline file manifest not found')
        else:
            file_meta_list = sorted([
                f for f in os.listdir(output_dir) 
                if re.match(dir_name + r'_file_metadata_[0-9]{14}\.csv$', f)
            ])

            if file_meta_list:
                baseline = os.path.join(output_dir, file_meta_list[-1])
            else:
                baseline = ''

        # 2. generate new manifest
        new_file_meta_fp = cls.generate_manifest(source_dir, output_dir, mask, postprocessing)
        logger.info(f'generated new manifest for {source_dir}')

        if baseline:
            # 3. compare manifests
            comparison_dict = cls.compare_manifests(baseline, new_file_meta_fp)
            # 4. generate report
            # create output dir if not exists
            if not os.path.exists(delta_output_dir):
                os.makedirs(delta_output_dir)
            # get report full path
            baseline_timestamp = re.search('([0-9]{14})\.csv$', baseline).group(1)
            new_timestamp = re.search('([0-9]{14})\.csv$', new_file_meta_fp).group(1)
            delta_output_fn = f'{dir_name}_delta_report_{new_timestamp}_to_{baseline_timestamp}.xlsx'
            delta_output_fp = os.path.join(delta_output_dir, delta_output_fn)
            cls.generate_report(comparison_dict, delta_output_fp)
            logger.info(f'generated new delta report for {source_dir}')

    @classmethod
    def schedule_jobs(cls):
        postprocessing: List[dict] = cls.config['options']['postprocessing']
        for job in cls.config['jobs']:
            if not 'schedule' in job:
                # run immediately
                cls.scheduler.add_job(cls.generate_delta, kwargs={'dir_config': job, 'postprocessing': postprocessing})
            else:
                schedule = job['schedule']
                if isinstance(schedule, dict):
                    # unpack dictionary cron config
                    trigger = CronTrigger(**schedule)
                elif isinstance(schedule, str):
                    trigger = CronTrigger.from_crontab(schedule)
                else:
                    continue
                # schedule periodic job
                # TODO: add identifier to job and track them in manager
                cls.scheduler.add_job(cls.generate_delta, trigger=trigger, kwargs={'dir_config': job, 'postprocessing': postprocessing})
        return

    def manage():
        # 1. load config and see any scheduled jobs
        # 2. resume all jobs according to schedule
        pass

    @classmethod
    def shutdown(cls):
        logger.info('shutting down scheduler')
        cls.scheduler.shutdown()

    # CLI METHODS
    # //////////////////////////////////////
    @staticmethod
    def generate_manifest_cli(source: str, folder_list: str, output_dir: str, excl_tmp: bool, no_hash: bool):
        # if not specified, default to CWD
        output_dir = output_dir if output_dir else os.getcwd()

        generator = MetaGenerator(
            output_dir=output_dir,
            excl_tmp=excl_tmp,
        )
        # if flagged folder_list, generate manifest for list of folders
        if folder_list:
            generator.output_manifests(folder_list, no_hash=no_hash)
        elif source:
            generator.output_meta(source, no_hash=no_hash)

    @classmethod
    def compare_manifests_cli(cls, source: str, target: str, output_dir: str):
        # get comparison result
        comparison_dict = cls.compare_manifests(source, target)
        # write to xlsx report
        cls.generate_report(comparison_dict, output_dir)

    @classmethod
    def schedule_jobs_cli(cls, config_path: str):
        # check config is json or csv
        if not config_path:
            logger.error('no config file provided')
            return

        if config_path.endswith('.csv'):
            cls.read_csv_config(config_path)
        elif config_path.endswith('.json'):
            cls.read_json_config(config_path)
        else:
            logger.error('unsupported config type')
            return

        if not 'jobs' in cls.config or not cls.config['jobs']:
            logger.info('no job specified')
            return

        executors = {
            'default': ThreadPoolExecutor(1)
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 1
        }

        # use background scheduler instead of blocking scheduler for faster shutdown time
        if 'timezone' in cls.config and cls.config['timezone']:
            cls.scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults, timezone=cls.config['timezone'])
        else:
            cls.scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults)
        cls.schedule_jobs()
        cls.scheduler.start()

        cls.scheduler.print_jobs()
        logger.info('Press Ctrl+C to exit')
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            cls.shutdown()


if __name__ == '__main__':

    source_dir = r'C:\Users\RN767KA\Projects\test docs\Test Cases\__attachments__'
    directories_list = r'C:\Users\RNXXXXX\folder_list.csv'
    output_dir = r'C:\Users\RN767KA\Projects\Playground\output'

    startTime = time.time()

    # Instantiate the generator object
    generator = MetaGenerator(
        # set the manifest output location
        output_dir=output_dir,
        # multiprocessing can be usefully when scan large volumn of LOCAL files
        multiprocess=False,
        # option to scan content of zipfile without unzipping to disk
        include_compressed=False,
        # option to exclude temp file (e.g. ~$data.xlsx, thumbs.db) during scanning
        excl_tmp=True,
        # verbosity of log output
        verbose=1
    )

    # Common Use Case 1: generate manifest for a single directory
    generator.output_meta(
        # choose the directory for scan
        source_dir,
        # option to override output location
        output_dir=output_dir,
        # reconciliation mode:
        # 1 -> recon between file and folder manifest
        # 2 -> recob between zipfile original and output manifest (files must be in specific locations)
        # 3 -> export zipfile original manifest for external recon (files must be in specific locations)
        recon_mode=0,
        # option to output manifest without generating hash
        # will significantly reduce time if hash is not required
        no_hash=False
    )

    # Common Use Case 2: generate manifests for multiple directories listed in a csv
    # generator.output_manifests(
    #     # choose the csv to use containing the list of directories
    #     directories_list,
    #     # option to override output location
    #     output_dir=output_dir,
    #     # reconciliation mode
    #     recon_mode=0,
    #     # option to output manifest without generating hash
    #     no_hash=True
    # )

    # Other Use Cases:

    # Output zipfile meta for zipfile content independently 
    # generator.output_zipfile_meta(output_dir=output_dir)

    # Combine files in the output directories according to the patterns
    # generator.combine(
    #     # combine files with the name of the folder and ending with .csv
    #     '.csv',
    #     # choose the directories where the output manifest folders are
    #     output_dir=output_dir,
    #     # option to override the combined file name
    #     output_name='combined.csv'
    # )

    # Generate and store metadata within the generator (file meta in db; folder meta in memory) without outputing
    # generator.generate_meta(source_dir, no_hash=False)
    
    print('Done, total time elapsed', time.time() - startTime)