"""Aggregates loader."""

from datetime import datetime
import logging
from sys import stdout
import os
from typing import Dict, List

import aggregates_loader.date_helpers as date_helpers
import aggregates_loader.model as model
from aggregates_loader.model.entity import Entity
from aggregates_loader.persistence import source, target
import aggregates_loader.queries as queries
from aggregates_loader.timeframe import TimeFrame

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)

logger = logging.getLogger(__name__)


class Loader:
    """Loader class for cap iq returns."""

    YEARS = [
        2010,
        2011,
        2012,
        2013,
        2014,
        2015,
        2016,
        2017,
        2018,
        2019,
        2020,
        2021,
        2022,
        2023,
    ]

    _entities = {
        "aggregate_base": Entity.aggregate_base,
    }

    _timeframes = {
        "weekly": TimeFrame.weekly,
        "monthly": TimeFrame.monthly,
    }

    _dates_generators = {
        TimeFrame.weekly: date_helpers.generate_weeks,
        TimeFrame.monthly: date_helpers.generate_months,
    }

    _get_timeframe_end = {
        TimeFrame.weekly: date_helpers.get_week_end,
        TimeFrame.monthly: date_helpers.get_month_end,
    }

    _model_type = {
        Entity.aggregate_base: model.AggregateBase,
    }

    _queries = {
        Entity.aggregate_base: queries.AggregateBaseQueries,
    }

    def __init__(self) -> None:
        self.source = source.Source(os.environ.get("SOURCE"))
        self.target = target.Target(os.environ.get("TARGET"))

    def run(self) -> None:
        """Persists records to the cap_iq_returns table."""
        entity = self._entities["aggregate_base"]
        for timeframe in self._timeframes.values():
            logger.info(f"Starting process for {timeframe}_base...")

            time_intervals = self._dates_generators[timeframe](self.YEARS)

            periods_per_year = 1
            if timeframe == "monthly":
                periods_per_year = 12
            if timeframe == "weekly":
                periods_per_year = 52

            date_ranges = []
            for intervals_slice in self.list_slicer(time_intervals, periods_per_year):
                date_ranges.append((intervals_slice[0][0], intervals_slice[-1][-1]))

            last_persisted_date = self.target.get_last_persisted_date(timeframe)
            # daily_base max date is 2023-03-16
            if last_persisted_date:
                date_ranges = [
                    dr
                    for dr in date_ranges
                    if dr[1] >= last_persisted_date and dr[0] < datetime(2023, 3, 16)
                ]

            if not date_ranges:
                logger.info(f"All records for {timeframe}_base have been persisted.")
                continue

            n = len(date_ranges)
            i = 0
            for date_range in date_ranges:
                logger.info(f"Persisted {i}/{n} {timeframe}.")
                logger.debug("Fetching records...")

                raw_records = self.source.get_records(date_range=date_range)

                if raw_records:
                    logger.debug("Building history per gvkey...")
                    history: Dict[int, List] = {}
                    for record in raw_records:
                        if record[1] not in history.keys():
                            history[record[1]] = [record]
                        else:
                            history[record[1]].append(record)

                    logger.debug("Curating records...")
                    records = []
                    for gvkey in history.keys():
                        key_records = history[gvkey]

                        binned_records = {}
                        for record in key_records:
                            period_end = self._get_timeframe_end[timeframe](record[0])
                            if period_end in binned_records.keys():
                                binned_records[period_end].append(record)
                            else:
                                binned_records[period_end] = [record]

                        for d, rb in binned_records.items():
                            if rb:
                                curated_record = self._model_type[entity].build_record(
                                    (d, gvkey), rb
                                )
                                if not curated_record.is_empty:
                                    records.append(curated_record.as_tuple())

                    logger.debug("Persisting records...")
                    upsert_query = self._queries[entity].UPSERT.format(
                        timeframe=timeframe.value
                    )
                    self.target.execute(upsert_query, records)

                    self.target.commit_transaction()

                i += 1

    def cleanup(self):
        """Removes records for companies that are not
        traded a minimum of days (trading days - 3) in that given month."""
        logger.info("Starting table cleanups...")
        months = date_helpers.generate_months(self.YEARS)
        i = 0
        n = len(months)
        for month in months:
            logger.debug(f"Cleaned {i}/{n} months...")
            max_dps = self.target.get_max_dps(month)
            non_traded_gvkeys = self.target.get_non_traded_gvkeys(max_dps-3)

            delete_query = ("DELETE FROM {timeframe}_base "
                            "WHERE (datadate, gvkey) IN (VALUES %s)"
                            "AND datadate BETWEEN {month_start} AND {month_end};")
            for timeframe in self._timeframes:
                query = delete_query.format(timeframe=timeframe, month_start=month[0], month_end=month[1])
                self.target.execute(query, non_traded_gvkeys)
                self.target.commit_transaction()
            i += 1
        logger.debug(f"Cleaned {n}/{n} months...")
        logger.info("Terminating...")

    @staticmethod
    def list_slicer(lst: List, slice_len: int) -> List[List]:
        """Slice list into list of lists.

        Args:
            lst: list to slice.
            slice_len: size of each slice.

        Returns:
            Sliced list.
        """
        res = []
        i = 0
        while i + slice_len < len(lst):
            res.append(lst[i : i + slice_len])  # noqa
            i = i + slice_len
        res.append(lst[i:])
        return res


loader = Loader()
loader.run()
