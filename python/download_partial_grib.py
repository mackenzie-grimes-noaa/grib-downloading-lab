'''Utility script to demonstrate getting a partial GRIB file from AWS S3 dataset'''
# ----------------------------------------------------------------------------------
# Created on Thu Sep 5 2023
#
# Copyright (c) 2024 Colorado State University. All rights reserved.             (1)
#
# Contributors:
#     Mackenzie Grimes (1)
#
# Requirements:
#   - [s5cmd](https://github.com/peak/s5cmd)
#
# Usage:
#    python3 download_partial_grib.py \
#        --product NBM \
#        --issue_dt 2024-09-05T12:00:00Z \
#        --valid_dt 2024-09-05T13:00:00Z \
#        --fields RAIN1HR
#
#   Note that --fields should be one of the strings listed in the constants PRODUCT['field_lookup']
#
# ----------------------------------------------------------------------------------

from datetime import datetime
import logging
import os
import re
from argparse import ArgumentParser

from dateutil.parser import parse as dt_parse

from idsse.common.utils import exec_cmd, to_compact, to_iso
from idsse.common.path_builder import PathBuilder

logger = logging.getLogger(__name__)

# all the weather products, and how they're structured to be fetched from AWS S3
PRODUCTS = {
    'NBM': {
        'basedir': 's3://noaa-nbm-grib2-pds',
        'subdir': 'blend.{issue.year:04d}{issue.month:02d}{issue.day:02d}/{issue.hour:02d}/core/',
        'file_base': 'blend.t{issue.hour:02d}z.core.f{lead.hour:03d}',
        'file_ext': '.grib2',
        'field_lookup': {
            'RAIN1HR': {
                'abbrevName': 'APCP',
                'productDefinitionTemplateNumber': 8,
                'lengthOfTimeRange': 1,
            },
            'RAIN6HR': {
                'abbrevName': 'APCP',
                'productDefinitionTemplateNumber': 8,
                'lengthOfTimeRange': 6,
            },
            'ICE1HR': {
                'abbrevName': 'FICEAC',
                # 'productDefinitionTemplateNumber': 8,
                'lengthOfTimeRange': 1,
            },
            'SNOW1HR': {
                'abbrevName': 'ASNOW',
                'productDefinitionTemplateNumber': 8,
                'lengthOfTimeRange': 1,
            },
            'TEMP': {
                'abbrevName': 'TMP',
                'level': 2,
                'productDefinitionTemplateNumber': 0,
            },
            'APPTEMP': {
                'abbrevName': 'APTMP',
                'level': 2,
                'productDefinitionTemplateNumber': 0,
            },
            'RH': {
                'abbrevName': 'RH',
                'level': 2,
                'productDefinitionTemplateNumber': 0,
            },
            'DEWPOINT': {
                'abbrevName': 'DPT',
                'level': 2,
                'productDefinitionTemplateNumber': 0,
            },
            'WINDSPEED': {
                'abbrevName': 'WIND',
                'level': 10,
                'productDefinitionTemplateNumber': 0,
            },
            'WINDGUST': {
                'abbrevName': 'GUST',
                'level': 10,
                'productDefinitionTemplateNumber': 0,
            },
            # 'WINDDIR': {
            #     'abbrevName': 'WDIR',
            #     'level': 10,
            #     # 'productDefinitionTemplateNumber': 0,
            # },
            'CEILING': {
                'abbrevName': 'CEIL',
                # 'productDefinitionTemplateNumber': 0,
                'typeOfLevel': 'cloudBase'
            },
            'VISIBILITY': {
                'abbrevName': 'VIS',
                # 'productDefinitionTemplateNumber': 0,
            },
            'MAXREF': {
                'abbrevName': 'MAXREF',
                'level': 1000,
                # 'productDefinitionTemplateNumber': 8,
            },
            'VIL': {
                'abbrevName': 'VIL',
                # 'productDefinitionTemplateNumber': 0,
            },
            'ECHOTOP': {
                'abbrevName': 'RETOP',
                # 'productDefinitionTemplateNumber': 0,
            },
            'WAVEHEIGHT': {
                'abbrevName': 'HTSGW',
                'productDefinitionTemplateNumber': 0,
            },
            'PROBTHDR1HR': {
                'abbrevName': 'TSTM',
                # 'productDefinitionTemplateNumber': 8,
                'lengthOfTimeRange': 1,
            },
            'PROBTHDR3HR': {
                'abbrevName': 'TSTM',
                # 'productDefinitionTemplateNumber': 8,
                'lengthOfTimeRange': 3,
            },
            'PROBTHDR6HR': {
                'abbrevName': 'TSTM',
                # 'productDefinitionTemplateNumber': 8,
                'lengthOfTimeRange': 6,
            },
        },
    },
}


def aws_cp(path: str, dest: str, byte_range: tuple[int] | None = None) -> bool:
    """Execute a 'cp' on an AWS s3 bucket. Returns True if copy successful.
    Based on function of same name from DAS.

    Args:
        path (str): url to S3 object to copy
        dest (str): local filepath where copy should be saved
        byte_range (optional, tuple[int] | None): the start byte and (optionally) end byte
            location within the path file to downloaded. Default is None (copy entire S3 object).
    """
    commands = ['s5cmd', '--no-sign-request', 'cp', path, dest]
    if byte_range:
        # append argument to s5cmd to request only the specified byte range of s3 object
        range_start = byte_range[0]
        range_end = '' if len(byte_range) < 2 else byte_range[1]  # default end is end of file

        # TODO does this truly set Range header?
        commands += ['--metadata', f'bytes={range_start}-{range_end}']

    try:
        result = exec_cmd(commands)
        logger.debug('s5cmd result: %s', result)
        return True
    except FileNotFoundError:
        logger.error('Failed to execute s5cmd cmd. Is it installed?')
        return False


class Reader():
    """Reader that can download one GRIB field from the provided AWS S3 product"""
    def __init__(self, product: str, region: str):
        self._product_name = product
        if self._product_name not in PRODUCTS:
            raise RuntimeError(
                f'Product {product} not in supported AWS weather products: {PRODUCTS.keys()}'
            )

        self._product = PRODUCTS[self._product_name]
        self._path_builder = PathBuilder(self._product['basedir'],
                                         self._product['subdir'],
                                         self._product['file_base'],
                                         self._product['file_ext'])
        self._file_ext: str = self._product['file_ext']
        self._region: str
        self.set_region(region)

    def set_region(self, region: str):
        """Change the geographic region, e.g. HI for HAWAII or CONUS for contiguous US"""
        self._region = region
        self._file_ext = f'.{region}{self._product["file_ext"]}'
        # TODO: does S3 path change?

    def download_field(self,
                       issue_dt: str,
                       valid_dt: str,
                       field: str,
                       dest_dir: str) -> str | None:
        """Download a single GRIB file from AWS

        Returns:
            str | None: local path to downloaded file, or None on error
        """
        if field not in self._product['field_lookup']:
            raise ValueError((f'Field {field} not recognized for product {self._product}. '
                             f'Supported fields: {self._product["field_lookup"].keys()}'))

        logger.info('Downloading GRIB field %s for product %s', field, self._product['basedir'])
        issue_datetime = dt_parse(issue_dt)
        valid_datetime = dt_parse(valid_dt)

        # download index file, if needed, and compute where in the GRIB file this field exists
        index_filepath = self._download_index_file(issue_datetime, valid_datetime, dest_dir)
        byte_range = self._get_byterange_from_index(index_filepath, field)
        if not byte_range:
            raise RuntimeError(f'Cannot find field {field} in index file at {index_filepath}')


        # use info from index file to download on the part of GRIB file that has target field
        s3_path = self._path_builder.build_path(issue_datetime, valid_datetime)
        # generate new filepath for downloaded grib file
        dest_filepath = self._get_dest_filepath(issue_datetime, valid_datetime, field)
        dest_path = os.path.join(dest_dir, dest_filepath)

        logger.debug('Copying s3 object from %s to %s with range %s', s3_path, dest_path,
                     byte_range)
        return self._download_file(s3_path, dest_path, byte_range)

    def _get_dest_filepath(self,
                           issue: datetime,
                           valid: datetime,
                           field: str | None = None) -> str:
        """Generate destination filepath for local filesystem

        Args:
            issue (datetime): the product's issuance datetime
            valid (datetime): the product's valid datetime
            field (optional, str | None): weather field, if downloading a specific field.
                Default is None

        Returns:
            str: A local filepath to use
        """
        _path = f'{self._product_name}_{to_compact(issue)}_{to_compact(valid)}'
        # append field name, if one was passed
        if field:
            _path += f'_{field}'
        # _path += f'.{self._region}{self._path_builder.file_ext}'
        _path += f'.{self._region}{self._file_ext}'

        return _path

    def _is_line_matching_field(self, index_line: str, field: str):
        """Determine if field from PRODUCT definitions matches a given line of a GRIB index file

        Example index file line:
            32:3866677:d=2024090523:TMP:2 m above ground:2 hour fcst:ens std dev
        Returns:
            bool: True if line is the field of interest
        """
        field_args = self._product['field_lookup'][field]

        # pylint: disable=unused-variable
        num, starting_byte, date, var_name, level, time_length, math_params = (
            index_line.split(':')[0:7]
        )

        # get the field's level (e.g. 1000 m) and time range (e.g. 3 hr), with null safety
        # because not all product field_lookup definitions specify these
        target_level = field_args['level'] if 'level' in field_args else ''
        target_time_length = (field_args['lengthOfTimeRange']
                                if 'lengthOfTimeRange' in field_args else '')

        # TODO: this is some complex logic that needs to be verified
        return (field_args['abbrevName'] == var_name
                and level.startswith(target_level)
                and time_length.startswith(target_time_length)
                # TODO: may need to actually read math_params, not just ensure it's blank
                and math_params == '')

    def _get_byterange_from_index(self, filepath: str, field: str) -> tuple[int] | None:
        """Read a GRIB index file and extract the byte range where a given field is located

        Returns:
            tuple[int]: The starting, and ending (if not EOF), byte numbers where field exists.
                None if field was not found in index file.
        """
        # expr = re.compile(search_str)
        with open(filepath, 'r', encoding='utf-8') as f:
            file_lines = f.readlines()

        for n, line in enumerate(file_lines, start=1):
            # if expr.search(line):
            if self._is_line_matching_field(line, field):
                starting_byte = line.split(':')[1]
                if n + 1 > len(file_lines):
                    # this was last line of file, so EOF is our ending byte
                    return tuple(starting_byte)

                # grab beginning byte of next field (which is end of our field)
                ending_byte = file_lines[n].split(':')[1]
                return tuple(starting_byte, ending_byte)

        return None

    def _download_index_file(self, issue: datetime, valid: datetime, dest_dir: str) -> str | None:
        """Download an index file object from AWS to local file"""
        # config.file_ext controls the dest filename
        original_config_ext = self._file_ext
        # path_build.file_ext controls the source filename
        original_path_builder_ext = self._path_builder.file_ext
        self._file_ext += '.idx'
        self._path_builder.file_ext += '.idx'

        # build index filepath for S3 location, and destination filepath on local fs
        dest_path = os.path.join(dest_dir, self._get_dest_filepath(issue, valid))
        src_path = self._path_builder.build_path(issue, valid)  # TODO: proper building idx path? yes
        if os.path.exists(dest_path):
            return dest_path  # reuse local index file, if it exists

        try:
            logger.debug('Downloading index file from %s to %s', src_path, dest_path)
            index_file_path = self._download_file(src_path, dest_path)
        finally:
            # reset to original file extensions
            self._file_ext = original_config_ext
            self._path_builder.file_ext = original_path_builder_ext

        return index_file_path if os.path.isfile(index_file_path) else None

    def _download_file(self, src: str, dest: str, byte_range: tuple[int] | None = None) -> str:
        if os.path.exists(dest):
            return dest  # file already exists # TODO: is this problematic for re-downloading file?

        logger.debug('Downloading from AWS (%s)', src)
        aws_cp(src, dest, byte_range)
        return dest


def main():
    """Driver function"""
    parser = ArgumentParser()
    parser.add_argument('--product',
                        dest='product',
                        default='NBM',
                        help='Target weather product in AWS, one of: [NBM]. Default: NBM')
    parser.add_argument('--region',
                        dest='region',
                        default='CONUS',
                        choices=PRODUCTS.keys(),
                        help='Geographic region, such as CONUS, AK, or HI. Default: CONUS')
    parser.add_argument('--issue_dt',
                        dest='issue_dt',
                        help=('The issuance datetime of this product to read, in ISO-8601 format. '
                              'E.g. 2020-01-01T12:00:00Z'))
    parser.add_argument('--valid_dt',
                        dest='valid_dt',
                        help=('The valid datetime of this product to read, in ISO-8601 format. '
                              'E.g. 2020-01-01T12:00:00Z'))
    parser.add_argument('--dest',
                        dest='dest',
                        default='.',
                        help='Filepath destination (directory) where file should be downloaded')
    parser.add_argument('--fields',
                        # TODO: how do make this accept multiple fields?
                        dest='fields',
                        help='List of human-readable weather fields to download from S3')

    args = parser.parse_args()
    logger.info('Running with args: %s', args)

    reader = Reader(args.product, args.region)

    target_fields = args.fields
    logger.info('Created reader %s, now using to fetch fields %s', reader, args.fields)
    issue_dt = args.issue_dt
    valid_dt = args.valid_dt
    dest_dir = args.dest

    for field in target_fields:
        dest_file = reader.download_field(issue_dt, valid_dt, field, dest_dir)
        if dest_file:
            logger.info('Downloaded file: %s', dest_file)
        else:
            logger.error('Failed to download file for field %s', field)

    logger.warning('Done!')

    'bytes=3803605-3866677'

if __name__ == '__main__':
    main()
