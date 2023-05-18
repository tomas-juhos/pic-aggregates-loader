"""Helper functions to deal with timeframes."""
from datetime import datetime, timedelta
from typing import List
import logging

logger = logging.getLogger(__name__)


def is_week_end(d: datetime) -> bool:
    """Checks if date is the end of the week."""
    if d.weekday() == 4:
        return True
    else:
        return False


def is_month_end(d: datetime) -> bool:
    """Checks if date is the end of the month."""
    next_day = d + timedelta(days=1)
    if d.month != next_day.month:
        return True
    else:
        return False


def get_week_end(d: datetime) -> datetime:
    """Checks if date is the end of the week."""
    return d + timedelta(days=4 - d.weekday())


def get_month_end(d: datetime) -> datetime:
    """Checks if date is the end of the month."""
    next_month = d.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def generate_weeks(years: List[int]):
    weeks = []
    for year in years:
        temp_d = datetime(year, 1, 1)
        week_start = temp_d
        while temp_d.year == year:
            if temp_d.weekday() == 0:
                week_start = temp_d
            if is_week_end(temp_d):
                weeks.append((week_start, temp_d))
            temp_d = temp_d + timedelta(days=1)

    return weeks


def generate_months(years: List[int]):
    months = []
    for year in years:
        temp_d = datetime(year, 1, 1)
        month_start = temp_d
        while temp_d.year == year:
            if temp_d.day == 1:
                month_start = temp_d
            if is_month_end(temp_d):
                months.append((month_start, temp_d))
            temp_d = temp_d + timedelta(days=1)

    return months


def generate_intervals(years: List[int]):
    intervals = []
    for year in years:
        first_day = datetime(year, 1, 1)
        if first_day.weekday() == 0:
            # first day monday, goes back to get last friday of the year
            intervals = intervals + [
                (first_day - timedelta(days=3), datetime(year, 12, 31))
            ]
        elif first_day.weekday() == 6:
            # first day sunday, goes back to get last friday of the year
            intervals = intervals + [
                (first_day - timedelta(days=2), datetime(year, 12, 31))
            ]
        else:
            # any other weekday has a weekday before
            intervals = intervals + [
                (first_day - timedelta(days=1), datetime(year, 12, 31))
            ]

    return intervals
