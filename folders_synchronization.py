import argparse
import logging
import os
import shutil
from filecmp import dircmp
import schedule
import time


def sync_folders(source, replica):
    """
    Synchronize two folders including subfolders
    """

    comparison = dircmp(source, replica)

    # Copy files from source to replica and overwrite if already exist
    for file in comparison.left_only + comparison.diff_files:
        src_path = os.path.join(source, file)
        rpl_path = os.path.join(replica, file)

        if os.path.isdir(src_path):
            shutil.copytree(src_path, rpl_path)  # Copy entire directory tree
            logging.info(f"Copied directory from {src_path} to {rpl_path}")
        else:
            shutil.copy2(src_path, rpl_path)  # Copy files and preserve metadata
            logging.info(f"Copied file from {src_path} to {rpl_path}")

    # Recursively sync subdirectories
    for sub_dir in comparison.common_dirs:
        sync_folders(os.path.join(source, sub_dir), os.path.join(replica, sub_dir))

    # Remove files and folders not present in source
    for file in comparison.right_only:
        rpl_path = os.path.join(replica, file)
        if os.path.isdir(rpl_path):
            shutil.rmtree(rpl_path)  # Remove directory tree
            logging.info(f"Removed directory {rpl_path}")
        else:
            os.remove(rpl_path)  # Remove file
            logging.info(f"Removed file {rpl_path}")


def main(source, replica, interval, log_path):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        # Delete following line if milliseconds are needed in function output
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[
                            logging.FileHandler(log_path),
                            logging.StreamHandler()
                        ])

    # Initial synchronization and scheduling periodic sync
    logging.info("Starting initial synchronization.")

    try:
        schedule.every(interval).seconds.do(sync_folders, source, replica)  #Default interval is 3600 secs

        # Check that source and replica folders exist
        if not os.path.exists(source):
            raise FileNotFoundError(f"The source directory {source} does not exist.")
        if not os.path.exists(replica):
            raise FileNotFoundError(f"The replica directory {replica} does not exist.")

        # Check that source and replica folders differ
        if source == replica:
            logging.error(f"The folder {source} is identical to {replica}")
            exit()

    except Exception as e:
        logging.error(f"Error synchronizing {source} to {replica}: {str(e)}")
        exit()

    while True:
        schedule.run_pending()
        time.sleep(1)  # Prevents the loop from consuming excessive CPU resources


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synchronize two folders.")
    parser.add_argument("source", help="Source directory path")
    parser.add_argument("replica", help="Replica directory path")
    parser.add_argument("--interval", type=int, default=3600,
                        help="Time interval in seconds for synchronization. Default value is 3600 seconds")
    parser.add_argument("--log_path", default="sync.log", help="Path to the log file")

    args = parser.parse_args()

    main(args.source, args.replica, args.interval, args.log_path)