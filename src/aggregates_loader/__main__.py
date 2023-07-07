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

                raw_records = self.source.get_records(timeframe="daily", date_range=date_range)

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
        traded a minimum of days (trading days - 4) in that given month."""
        logger.info("Starting table cleanups...")
        timeframes = ["monthly", "weekly", "daily"]
        months = date_helpers.generate_months(self.YEARS)
        i = 0
        n = len(months)
        for month in months:
            logger.debug(f"Cleaned {i}/{n} months...")
            max_dps = self.target.get_max_dps(month)
            if max_dps:
                non_traded_gvkeys = self.target.get_non_traded_gvkeys(max_dps-4, month[0], month[1])
                if non_traded_gvkeys:
                    delete_query = ("DELETE FROM {timeframe}_base "
                                    "WHERE (gvkey) IN (VALUES %s) "
                                    "AND datadate BETWEEN {month_start} AND {month_end};")
                    for timeframe in timeframes:
                        query = delete_query.format(timeframe=timeframe, month_start=f"\'{month[0]}\'", month_end=f"\'{month[1]}\'")
                        self.target.execute(query, non_traded_gvkeys)
                        self.target.commit_transaction()
                i += 1
        logger.debug(f"Cleaned {n}/{n} months...")
        logger.info("Terminating...")

    def winsorize_returns(self):
        logger.info('Winsorizing returns...')
        timeframes = ["monthly", "weekly", "daily"]
        date_intervals = date_helpers.generate_intervals(self.YEARS)
        n = len(date_intervals)
        for timeframe in timeframes:
            logger.info(f"Processing {timeframe} records...")
            i = 0
            for date_interval in date_intervals:
                logger.debug(f"Processed {i}/{n} date intervals.")
                raw_records = self.source.get_records(timeframe=timeframe, date_range=date_interval)
                raw_records = [model.BaseData.build_record(r) for r in raw_records]

                if raw_records:
                    logger.debug("Building history per date...")
                    history: Dict[datetime, List[model.BaseData]] = {}
                    for record in raw_records:
                        if record.datadate not in history.keys():
                            history[record.datadate] = [record]
                        else:
                            history[record.datadate].append(record)

                    winsorized_returns = []
                    for d, records in history.items():
                        returns = [(r.datadate, r.gvkey, r.rtn) for r in records]
                        returns.sort(key=lambda r: r[2])
                        # CHANGE WINSORIZE FACTOR BELOW IF NEEDED
                        quantile_index = len(returns) // 20
                        high_value = returns[-quantile_index][2]
                        low_value = returns[quantile_index][2]

                        for r in returns:
                            if r[2] > high_value:
                                winsorized_returns.append((r[0], r[1], high_value))
                            elif r[2] < low_value:
                                winsorized_returns.append((r[0], r[1], low_value))
                            else:
                                winsorized_returns.append(r)
                else:
                    logger.info("No more records to process.")
                    return

                upsert_query = queries.WinsorizedReturnsQueries.UPSERT.format(timeframe=timeframe)
                self.target.execute(upsert_query, winsorized_returns)
                self.target.commit_transaction()

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
# loader.cleanup()
loader.winsorize_returns()
