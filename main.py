import argparse
from app.meta_manager import MetaManager

if __name__ == "__main__":
    # CLI MODE
    parser = argparse.ArgumentParser(description='Generate metadata for folders and files')
    # parser.add_argument('-s', '--source', type=str, metavar='', help='Source directory where the manifest will be generated from')
    # parser.add_argument('-l', '--list', type=str, metavar='', help='The csv contain the directory list to generate manifests from')
    # parser.add_argument('-o', '--output_dir', type=str, metavar='', help='Manifest output directory, default to CWD')
    parser.add_argument('-c', '--config', type=str, metavar='', help='The csv/json that contains the job config to generate manifest/delta from')
    # parser.add_argument('--excl_tmp', action='store_true', help='Ignore temporary files (e.g. ~$data.xlsx, thumbs.db)')
    # parser.add_argument('--no_hash', action='store_true', help='Skip generating hash for all files')
    args = parser.parse_args()

    # MetaManager.generate_delta_cli(args.config)
    MetaManager.generate_delta_cli('job_config.json')