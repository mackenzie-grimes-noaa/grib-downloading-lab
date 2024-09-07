"""Temporary convenience wrapper to track CPU clock time for DAS performance analysis"""
# ----------------------------------------------------------------------------------
# Created on Thu Aug 1 2024
#
# Copyright (c) 2023 Colorado State University. All rights reserved.             (1)
# Copyright (c) 2023 Regents of the University of Colorado. All rights reserved. (2)
#
# Contributors:
#     Mackenzie Grimes (1)
#
# ----------------------------------------------------------------------------------

import time


class ThreadTimer():
    """Lightweight timer to track nanoseconds elapsed between start() and stop()

    Args:
        name (optional, str): The name of the function/operation being timed. Will be used to
            pretty-print when get_result() is called. Default is None.
        auto_start (optional, bool): Begin running the timer as soon as this object is created.
            If False, start() will need to be called manually. Default is True.
    """
    def __init__(self, name: str | None = None, auto_start = True):
        self._name = name
        self._start = 0
        self._end = 0
        self._status = 'NOT_STARTED'  # track the state of this timer

        if auto_start:
            self.start()  # begin tracking time

    def start(self):
        """Start the timer. Will be automatically called on init, unless optional argument
        auto_start=False was passed to ThreadTimer().
        """
        if self._status == 'STARTED':
            raise RuntimeError('Cannot call start() twice on same ThreadTimer.')

        self._start = time.perf_counter_ns()
        self._status = 'STARTED'

    def stop(self) -> float:
        """Stop this timer, and return the elapsed time in nanoseconds.
        Can also pretty-print using `get_result()`.

        Return:
            float: The total elapsed time in nanoseconds
        """
        if self._status == 'STOPPED':
            raise RuntimeError('Cannot call stop() twice on the same ThreadTimer.')

        self._end = time.perf_counter_ns()
        self._status = 'STOPPED'
        return self._end - self._start

    def get_result(self, sep = ': ', time_units = 'ms', precision = 2):
        """Build result of timer, formatted nicely for logs. ThreadTimer must be `STOPPED`.
        Example output: `: 123.45 ms : my_function_name()`

        Args:
            sep (optional, str): Separator to be printed between ThreadTimer attributes.
                Default is `: `
            time_units (optional, str): Units of time to print elapsed time. Supported
                units: `s`, `ms`, `ns`. Can also pass full unit name, e.g. `"nanoseconds"`.
                Default is milliseconds.
            precision (optional, int): Number of decimal places to round the elapsed time.
                Default is 2.

        Raises:
            RuntimeError: if ThreadTime status is not `STOPPED` (`stop()` was previously called).
        """
        if self._status != 'STOPPED':
            raise RuntimeError(
                f'ThreadTimer must be stopped before getting result. Status: {self._status}'
            )

        # format output, converting into the requested time units
        elapsed_ns = self._end - self._start
        elapsed_in_units = -1
        if time_units in ['s', 'seconds']:
            elapsed_in_units = elapsed_ns / 1e9
        elif time_units in ['ms', 'milliseconds']:
            elapsed_in_units = elapsed_ns / 1e6
        elif time_units in ['ns', 'nanoseconds']:
            elapsed_in_units = elapsed_ns  # no conversion to do

        return sep.join([
            f'{elapsed_in_units:0.{precision}f} {time_units}',
            self._name if self._name else '',
        ])
