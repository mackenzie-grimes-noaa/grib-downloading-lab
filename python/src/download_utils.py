"""Some utilities for download grib research, copied from/based on DAS and idsse utilities"""
# ------------------------------------------------------------------------------
# Created on Thu Sept 5 2024
#
# Copyright (c) 2024 Colorado State University. All rights reserved. (1)
#
# Contributors:
#     Mackenzie Grimes (1)
#
# ------------------------------------------------------------------------------
import logging
from datetime import datetime, timedelta, UTC
from random import randint

from idsse.common.utils import exec_cmd, to_iso

logger = logging.getLogger(__name__)


# based on aws_cp from idsse.common.utils, but supports additional s5cmd arguments
def aws_du(path: str, exclude: str | None = None, humanize = True) -> str | int:
    """Run 'du' command to lookup the file size of an S3 object based on path

    Returns:
        str | int: the file size. String like '2.1MB' if humanize was True,
            otherwise the byte count as an int. Returns -1 on error.
    """
    commands = ['s5cmd', '--no-sign-request', 'du']

    # build optional command args if passed
    if exclude:
        commands += ['--exclude', exclude]
    if humanize:
        commands.append('--humanize')
    commands.append(path)

    try:
        result = exec_cmd(commands)
        logger.debug('s5cmd du result: %s', result)
        # return human readable size (e.g. 2.1MB) as is; return byte count as a number
        file_size_str = result[0].split(' ')[0]  # extract just the number
        return file_size_str if humanize else int(file_size_str)

    except FileNotFoundError:
        logger.error('Failed to execute s5cmd cmd. Is it installed?')
        return -1

def aws_cp(path: str,
           dest: str,
           byte_range: tuple[int] | None = None,
           concurrency: int | None = None,
           part_size: int | None = None,
           disable_cache = False) -> bool:
    """Execute a 'cp' on an AWS s3 bucket. Returns True if copy successful.
    Based on function of same name from DAS, but with more s5cmd customization.

    Args:
        path (str): url to S3 object to copy
        dest (str): local filepath where copy should be saved
        byte_range (optional, tuple[int] | None): the start byte and (optionally) end byte
            location within the path file to downloaded. Default is None (copy entire S3 object).
        concurrency (optional, int | None): Number of concurrent threads to execute download
            command. Useful for speeding up download of single, large file from S3.
            Default is None (s5cmd uses 5).
        part_size (optional, int | None): Size (in MB) to break up large S3 objects so parallel
            s5cmd threads can optimize total download speed. Default is None (s5cmd uses 50 MB).
        disable_cache (optional, bool). Disallow S3 from caching objects that have been previously
            requested. Uses --cache-control header to pass this to s3 API. Default is False.
    """
    commands = ['s5cmd', '--no-sign-request', 'cp']
    # append optional s5cmd flags onto command list, if caller provided them
    if concurrency:
        commands += ['--concurrency', str(concurrency)]
    if part_size:
        commands += ['--part-size', str(part_size)]
    if disable_cache:
        commands += ['--cache-control', 'no-cache']

    commands += [path, dest]

    # Commented out this code because s5cmd doesn't support byte ranges at this time
    # if byte_range:
        # append argument to s5cmd to request only the specified byte range of s3 object
        # range_start = byte_range[0]
        # range_end = '' if len(byte_range) < 2 else byte_range[1]  # default end is end of file

        # TODO does this truly set Range header?
        # commands += ['--metadata', f'bytes={range_start}-{range_end}']

    try:
        result = exec_cmd(commands)
        logger.debug('s5cmd result: %s', result)
        return True
    except FileNotFoundError:
        logger.error('Failed to execute s5cmd cmd. Is it installed?')
        return False


def get_random_issue_and_valid(lead_hours = 6) -> tuple[datetime]:
    """Generate a random issue_dt from the last few months (assuming 1 HR issuances),
    and a valid datetime `lead_hours` hours ahead of that random time

    Returns:
        tuple(datetime): a random issue datetime, and a valid datetime `lead_hours` ahead of that
    """
    # pick a random number of hours in the past (up to max_days_past days ago)
    max_days_past = 30 * 6  # roughly 6 months ago
    random_hour = randint(1, max_days_past * 24)

    # round down the current time to the last top of the hour
    latest_issue_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

    logger.debug('Calculated latest issue datetime: %s, subtracting random number of hours: %s',
                 to_iso(latest_issue_dt), random_hour)
    issue = latest_issue_dt - timedelta(hours=random_hour)
    valid = issue + timedelta(hours=lead_hours)

    return issue, valid
