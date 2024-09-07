## GRIB Download experiments
This is an experimental Python script that benchmarks download speed for various s5cmd approaches to optimize downloading of large GRIB files.

For example:
- Customize how many threads are used to download the file
- Customize what chunk size to split up the file

## Install
Requirements:
- Python 3.12+
- [s5cmd](https://github.com/peak/s5cmd) command line tool
  - `brew install s5cmd` on macOS. See the link above for all the other install processes
- [idss-engine-commons]() library
  - Clone the repo in another folder, then run `pip install -e ../idss-engine-commons/python/idsse_common` to install it as a "dynamic" local pip library
- [python-dateutil](https://pypi.org/project/python-dateutil/)
  - `pip install python-dateutil`

Highly recommended that you install all Python libraries within a virtual environment.
```sh
python3 -m venv .venv
```
Then activate the environment with `source .venv/bin/activate` before running any pip install commands.

## Run
```sh
python3 python/grib_downloading_lab.py
```
By default, this will download random GRIB files over the last 6 months from the [NOAA Open Data Dissemination S3](https://registry.opendata.aws/noaa-nbm/) to your local file system (approximately 20-30 files, each 150MB+), experimenting with different s5cmd controls, and log the performance of each file.

Optional command line arguments to change this behavior:
- `--cleanup`: after tests complete, delete all the GRIB files it downloaded. Default is to not do this.
- `--region <str>`: the weather product region abbreviation, e.g. "HI", "AK", or "PR". Default is "CO" (contiguous US)
- `--dest <str>`: path to the local directory where all GRIB files will be saved. Default: same directory as the script.
- `--issue_dt <datetime>` and `--valid_dt <datetime>`: set a specific issue and valid datetime to download the GRIB file for (ISO-8601 format, e.g. 2020-01-01T12:00:00Z). By default, the issue and valid are randomly chosen on each download to ensure more evenly distributed performance and to side-step any AWS caching.
- `--loglevel <str>`: the log level to use, e.g. "DEBUG". The default is "WARN"
- `--product <str>`: some other weather model product. For now the only one supported is the default, "NBM"

We had originally intended for this to experiment with download _partial_ GRIB files based on the weather fields one was interested in, and we added some logic to download individual fields from a GRIB file, since the AWS S3 API supports this by accepting a `Range` http header.

Unfortunately s5cmd does not support this header (as of Sept 2024), so efforts were abandoned for now.