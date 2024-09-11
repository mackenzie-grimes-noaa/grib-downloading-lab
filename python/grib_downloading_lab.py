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
#   - [idss-engine-commons library](https://github.com/NOAA-GSL/idss-engine-commons)
#
# Usage:
#    python3 grib_downloading_lab.py \
#        --cleanup
#        --issue_dt 2024-09-05T12:00:00Z \
#        --valid_dt 2024-09-05T13:00:00Z \
#
#
# ----------------------------------------------------------------------------------

from datetime import datetime
import logging
import os
import sys
from argparse import ArgumentParser, BooleanOptionalAction, Namespace

from dateutil.parser import parse as dt_parse

from idsse.common.utils import exec_cmd, to_compact, to_iso
from idsse.common.path_builder import PathBuilder

from src.ThreadTimer import ThreadTimer
from src.download_utils import aws_cp, aws_du, get_random_issue_and_valid

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
        'regions': ['CO', 'HI', 'PR', 'GU', 'AK']
    },
}


class Reader():
    """Reader that can download one GRIB field from the provided AWS S3 product"""
    def __init__(self, product: str, region: str):
        self._product_name = product
        if self._product_name not in PRODUCTS:
            raise ValueError(
                f'Product {product} not one of supported AWS weather products: {PRODUCTS.keys()}'
            )

        self._product = PRODUCTS[self._product_name]
        self._path_builder = PathBuilder(self._product['basedir'],
                                         self._product['subdir'],
                                         self._product['file_base'],
                                         self._product['file_ext'])
        self._file_ext: str = self._product['file_ext']
        self._region: str
        self.set_region(region)

    def __str__(self):
        return (f'name={self._product_name}, region={self._region}, '
                f'PathBuilder={str(self._path_builder)}')

    def set_region(self, region: str):
        """Change the geographic region, e.g. HI for HAWAII or CONUS for contiguous US"""
        if region not in self._product['regions']:
            raise ValueError((f'Region {region} not one of the regions for this AWS product: ',
                              self._product["regions"]))

        self._region = region.lower()
        self._file_ext = f'.{self._region}{self._product["file_ext"]}'  # update destination file
        self._path_builder.file_ext = self._file_ext  # update source file ext

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
        s3_path = self.get_source_path(issue_datetime, valid_datetime)

        # generate new filepath for downloaded grib file
        dest_filepath = self.get_local_filename(issue_datetime, valid_datetime, field=field)
        dest_path = os.path.join(dest_dir, dest_filepath)

        logger.debug('Copying s3 object from %s to %s with range %s', s3_path, dest_path,
                     byte_range)
        return self._download_file(s3_path, dest_path, byte_range)

    def get_local_filename(self,
                          issue: datetime,
                          valid: datetime,
                          region: str | None = None,
                          field: str | None = None) -> str:
        """Generate destination filepath for local filesystem

        Args:
            issue (datetime): the product's issuance datetime
            valid (datetime): the product's valid datetime
            region (optional, str | None): the region (e.g. CONUS), which will overwrite the
                existing region set for this Reader if needed.
            field (optional, str | None): weather field, if downloading a specific field.
                Default is None

        Returns:
            str: A local filepath to use as a cp destination
        """
        if region and region != self._region:
            self.set_region(region)  # overwrite Reader's previous region with this specific one

        _path = f'{self._product_name}_{to_compact(issue)}_{to_compact(valid)}'
        # append field name, if one was passed
        if field:
            _path += f'_{field}'
        # _path += f'.{self._region}{self._path_builder.file_ext}'
        _path += self._file_ext

        return _path

    def get_source_path(self, issue: datetime, valid: datetime, region: str | None = None) -> str:
        """Generate S3 source filepath for the given product reader to download a file.

        Args:
            issue (datetime): the product's issuance datetime
            valid (datetime): the product's valid datetime
            region (optional, str | None): the region (e.g. CONUS), which will overwrite the
                existing region set for this Reader if needed.

        Returns:
            str: An s3 filepath to use as a cp source
        """
        if region and region != self._region:
            self.set_region(region)  # overwrite Reader's previous region with this specific one

        return self._path_builder.build_path(issue, valid)

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
        index_dest_path = os.path.join(dest_dir, self.get_local_filename(issue, valid))
        index_src_path = self.get_source_path(issue, valid)  # TODO: proper building idx path? yes

        try:
            logger.debug('Downloading index file from %s to %s', index_src_path, index_dest_path)
            index_file_path = self._download_file(index_src_path, index_dest_path)
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


# s5cmd benchmark testing
def _benchmark_aws_cp(src_path: str,
                      dest_path: str,
                      file_size: str | int,
                      s5_args: tuple[int]) -> str:
    # make sure local file doesn't already exist. might be unneeded as cp would overwrite
    if os.path.exists(dest_path):
        exec_cmd(['rm', dest_path])
        logger.debug('Deleted local file before running next test: %s', dest_path)

    concurrency, chunk_size = s5_args
    logger.info('Downloading file %s with concurrency: %s, part_size: %s',
                src_path, concurrency, chunk_size)

    # run aws_cp with custom concurrency and part_size, timing result
    timer = ThreadTimer((f'downloaded {file_size}B, concurrency: {concurrency}, '
                         f'part_size: {chunk_size} MB'))
    aws_cp(src_path, dest_path, concurrency=concurrency, part_size=chunk_size,
            disable_cache=True)
    timer.stop()
    logger.warning('Completed: %s', timer.get_result())

    return dest_path


def _build_file_paths(reader: Reader,
                      dest_dir: str,
                      issue: datetime,
                      valid: datetime,
                      humanize = True) -> tuple[str, str, str | int]:
    """Based on a given issue and valid datetime, generate source and destination paths,
    and get the size of the file in S3.

    Returns:
        tuple[str, str, str | int]: source path (s3://...), destination path (/local/file/path),
            and human-readable file size (either string like 2.1MB, or byte count as int)
    """
    dest_path = os.path.join(dest_dir, reader.get_local_filename(issue, valid))
    src_path = reader.get_source_path(issue, valid)
    # lookup size of src file in S3 for reporting purposes (don't count the index file)
    file_size = aws_du(src_path, exclude='*.idx', humanize=humanize)

    return src_path, dest_path, file_size


def _delete_files(filepaths: list[str]):
    logger.info('Now deleting %s downloaded files', len(filepaths))
    for file in filepaths:
        exec_cmd(['rm', file])
        logger.debug('Deleted %s', file)


def test_concurrency(_args: Namespace):
    """Quick test to evaluate how much speed we get from s5cmd concurrency for large GRIB files"""
    reader = Reader(_args.product, _args.region)
    logger.debug('Created Reader: %s', reader)
    dest_dir = _args.dest

    # if issue_dt and valid_dt not provided, these variables will be dynamic on every test run
    src_path: str | None = None
    dest_path: str | None = None
    file_size: str | None = None
    # random issue and valid datetimes will be selected if not pass in command line args
    specific_issue: datetime | None = None
    specific_valid: datetime | None = None

    if (_args.issue_dt and _args.valid_dt):
        # download a specific issue_dt and valid_dt GRIB file
        specific_issue = dt_parse(_args.issue_dt)
        specific_valid = dt_parse(_args.valid_dt)

        src_path, dest_path, file_size = _build_file_paths(reader, dest_dir, specific_issue,
                                                           specific_valid)

    # chunk_sizes = [1, 2, 3, 5, 10, 50]  # chunk size in MB
    chunk_sizes = [1, 2, 3, 5, 10]
    concurrency_threads = [8, 12, 16, 20]
    iteration_count = _args.iterations

    files_created: list[str] = []  # local paths of any generated files, so we can cleanup later

    logger.info('Running s3 download for file concurrency threads: %s, chunk size %s MB',
                concurrency_threads, chunk_sizes)
    for _ in range(0, iteration_count):
        for concurrency in concurrency_threads:
            for chunk_size in chunk_sizes:
                if not specific_issue or not specific_valid:
                    # pick a random issue_dt in the past few months, and a valid time +6 hours
                    issue, valid = get_random_issue_and_valid()
                    logger.debug('Generated random issue %s and valid %s', to_iso(issue),
                                 to_iso(valid))
                    src_path, dest_path, file_size = (
                        _build_file_paths(reader, dest_dir, issue, valid)
                    )

                # run each combination of chunk_size and thread_count {iteration_count} times
                file_created = _benchmark_aws_cp(src_path, dest_path, file_size,
                                                s5_args=(concurrency, chunk_size))
                files_created.append(file_created)

    # if requested, clean up copies of all files downloaded by this script
    if _args.cleanup and len(files_created) > 0:
        _delete_files(files_created)


def main():
    """Driver function"""
    parser = ArgumentParser()
    parser.add_argument('--product',
                        dest='product',
                        default='NBM',
                        choices=PRODUCTS.keys(),
                        help='Target weather product in AWS, one of: [NBM]. Default: NBM')
    parser.add_argument('--region',
                        dest='region',
                        default='CO',
                        help='Geographic region acronym, such as CO, AK, HI. Default: CO (CONUS)')
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
                        default=os.path.dirname(__file__),
                        help='Filepath destination (directory) where file should be downloaded')
    parser.add_argument('--fields',
                        # TODO: how do make this accept multiple fields?
                        default=None,
                        dest='fields',
                        help='List of human-readable weather fields to download from S3')
    parser.add_argument('--loglevel', '--log_level',
                        dest='loglevel',
                        default='INFO',
                        help='Set the logging level, e.g. DEBUG. Default is INFO')
    parser.add_argument('--cleanup',
                        dest='cleanup',
                        action=BooleanOptionalAction,
                        help='Pass this flag to have script delete all downloaded files when done')
    parser.add_argument('--iterations',
                        dest='iterations',
                        default=3,
                        type=int,
                        help='Number of times to test each thread & chunk size combo in s5cmd')
    args = parser.parse_args()

    format_str = ('%(asctime)-15s %(levelname)-8s %(module)s::'
                  '%(funcName)s(line %(lineno)d) %(message)s')
    logging.basicConfig(
        level=args.loglevel,
        format=format_str,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger.info('Running with args: %s', args)

    test_concurrency(args)

    # reader = Reader(args.product, args.region)

    # target_fields = args.fields
    # logger.info('Created reader %s, now using to fetch fields %s', reader, args.fields)
    # issue_dt = args.issue_dt
    # valid_dt = args.valid_dt
    # dest_dir = args.dest

    # for field in target_fields:
    #     dest_file = reader.download_field(issue_dt, valid_dt, field, dest_dir)
    #     if dest_file:
    #         logger.info('Downloaded file: %s', dest_file)
    #     else:
    #         logger.error('Failed to download file for field %s', field)

    logger.warning('Done!')


if __name__ == '__main__':
    main()
