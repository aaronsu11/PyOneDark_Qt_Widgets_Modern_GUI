# Meta Manager core logics

import os
import time
import json
import pandas as pd
from typing import List, Dict

from .meta_generator import MetaGenerator

# 1. Function to generate meta data ( filename, path, size, hash) for a file
# 2. Wrapper function to walk through files recursively in a folder and generate a meta data csv file
# 3. Function to compare 2 versions of meta data csv file and generate one report in Excel with multiple sheets
# 4. Main function ( main entry point) to read a list of folders from a config file and run step 2 and 3
# 5. Schedule the main function in task manager or a long running application
# 6. Design of config file


class MetaManager:
    
    # default config for loading csv
    config: dict = {
        'target_col': 'Reference Folder Path',
        'rm_col': 'Remove',
        'output_col': 'Output',
        'directories': [],
        "options": {}      
    }

    config_path: str = os.path.join(os.getcwd(), 'config.json')
    output_dir: str = os.path.join(os.getcwd(), 'manifests')

    @classmethod
    def read_csv_config(cls, fp: str) -> None:
        csv_config = pd.read_csv(fp, index_col=None)

        target_col: str = cls.config['target_col']
        rm_col: str = cls.config['rm_col']
        output_col: str = cls.config['output_col']

        if not target_col in csv_config.columns:
            raise ValueError('No folder path provided under "Reference Folder Path" column, exit...')

        custom_remove: bool = rm_col in csv_config.columns
        custom_output: bool = output_col in csv_config.columns

        folder_config: List[dict] = []

        for i, row in csv_config.iterrows():
            # target directory
            directory = row[target_col]
            if not directory:
                continue

            # output config
            output_dir = cls.output_dir
            if custom_output and row[output_col]:
                output_dir = row[output_col]

            remove = ''
            if custom_remove:
                remove = row[rm_col]

            folder_config.append(
                dict(
                    target=directory,
                    output_dir=output_dir,
                    mask=remove
                )
            )
        # save config to manager
        cls.config['directories'] = folder_config

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
                print(f'[WARNING] unsupported field "{key}" in custom config')

    @classmethod
    def save_json_config(cls, fp: str='') -> None:
        if not fp:
            fp = cls.config_path

        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(cls.config, f, ensure_ascii=False, indent=4)

    @classmethod
    def generate_manifest(cls, source_dir, output_dir, mask='', post_processing=[]):
        # Instantiate the generator object
        generator = MetaGenerator(
            # set the manifest output location
            output_dir=output_dir if output_dir else cls.output_dir,
            # option to exclude temp file (e.g. ~$data.xlsx, thumbs.db) during scanning
            excl_tmp=False,
            # verbosity of log output
            verbose=0
        )

        generator.output_meta(
            # choose the directory for scan
            source_dir,
            # custom postprocessing steps
            post_processing=post_processing, 
            # mask sub string for paths
            rm_substring=mask
        )

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
        df_source_only: pd.DataFrame = df_diff.loc[lambda x : x['_merge']=='left_only'].drop(columns=['index_NEW', '_merge']).rename(columns = {'index_OLD':'index'})
        df_target_only: pd.DataFrame = df_diff.loc[lambda x : x['_merge']=='right_only'].drop(columns=['index_OLD', '_merge']).rename(columns = {'index_NEW':'index'})

        # adjust the index values
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
        df_source_only.rename(columns = {'index':'SOURCE_INDEX'}, inplace = True)
        df_target_only.rename(columns = {'index':'TARGET_INDEX'}, inplace = True)

        # rearrange column order
        renamed_df = renamed_df[['DIRECTORY', 'SOURCE_INDEX', 'SOURCE_FILE_NAME', 'TARGET_INDEX', 'TARGET_FILE_NAME']]
        moved_df = moved_df[['FILE_NAME', 'SOURCE_INDEX', 'SOURCE_DIRECTORY',  'TARGET_INDEX', 'TARGET_DIRECTORY']]
        edited_df = edited_df[['DIRECTORY', 'FILE_NAME', 'SOURCE_INDEX', 'TARGET_INDEX']]
        df_source_only = df_source_only[['SOURCE_INDEX', 'DIRECTORY', 'FILE_NAME']]
        df_target_only = df_target_only[['TARGET_INDEX', 'DIRECTORY', 'FILE_NAME']]

        return {'renamed': renamed_df, 'moved': moved_df, 'edited': edited_df, 'deleted': df_source_only, 'new': df_target_only}

    def generate_report():
        pass

    @classmethod
    def generate_delta(cls):
        # 1. check benchmark exist or not
        # 2. generate new manifest
        # 3. compare manifests
        # 4. generate report

        if not output_dir:
            output_dir = self.output_dir
            # logger.info(f'output folder default to {output_dir}')

        if not os.path.isfile(list_path):
            # logger.error(f'{list_path} does not exist, please verify input path')
            raise Exception()
        else:
            fl_df = pd.read_csv(list_path)

        cols = fl_df.columns

        list_col = self.options['postprocessing']['multiple']['list_col']
        rm_col = self.options['postprocessing']['multiple']['rm_col']

        if not list_col in cols:
            # logger.error(f'could not find the reference column {list_col} in the csv')
            return

        unfinished_df = pd.DataFrame(columns = cols)

        folder_cols = ['FOLDER_NAME', 'SIZE_IN_BYTES', 'FILE_COUNT', 'ROOT_FILE_COUNT', 'SUB_FOLDER_COUNT', 'DIRECTORY_PATH', 'FOLDER_DESCRIPTION']

        # TODO: replace iterrows
        for index, row in fl_df.iterrows():
            try:
                self.clean_table()
                top_dir = row[list_col]

                # logger.debug(f'Generating manifest for {top_dir}...')

                # reset folder dataframe
                self.folder_df = pd.DataFrame(columns = folder_cols)
                
                post_processing = self.options['postprocessing']['multiple']['process']

                rm_substring = ''

                if rm_col in cols:
                    # post-processing directory path cleanse
                    rm_substring = str(row[rm_col])
                    if not rm_substring or rm_substring == 'nan':
                        rm_substring = ''
                        # logger.warning(f'remove string is not specified for row {index + 2}: {top_dir}')
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
                # logger.info(f'-------------------------------------------------- folder {index+1} processed')
            except Exception as e:
                # logger.error(str(e))
                # logger.error(f'failed to generate manifest for {row[list_col]}, skipping...')
                unfinished_df = unfinished_df.append(row, ignore_index=True)
                # logger.info(f'-------------------------------------------------- folder {index+1} failed')
        n_unsuccessful = len(unfinished_df.index)
        if n_unsuccessful > 0:
            output_path = os.path.join(output_dir, 'unfinished.csv')
            unfinished_df.to_csv(output_path, index = False, header=True)
            # logger.error(f'failed to scan {n_unsuccessful} directory')
            # logger.warning(f'unsuccessful directory scan(s) has been recorded in {output_path}')
        return

    def schedule_job(cls):
        pass

    def manage():
        # 1. load config and see any scheduled jobs
        # 2. resume all jobs according to schedule
        pass

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
        writer = pd.ExcelWriter(output_dir)
        for key, value in comparison_dict.items():
            # generate excel report
            value.to_excel(writer, key.upper(), index=False)
        writer.save()


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