"""Aggregate base model."""

from datetime import datetime
from decimal import Decimal
import logging
from math import sqrt
from typing import List, Optional, Tuple

from aggregates_loader.model.base import Modeling

logger = logging.getLogger(__name__)


class AggregateBase(Modeling):
    """Aggregate base record object class."""

    datadate: datetime
    gvkey: int

    utilization_pct: Optional[Decimal] = None
    bar: Optional[int] = None
    age: Optional[Decimal] = None
    tickets: Optional[int] = None
    units: Optional[Decimal] = None
    market_value_usd: Optional[Decimal] = None
    loan_rate_avg: Optional[Decimal] = None
    loan_rate_max: Optional[Decimal] = None
    loan_rate_min: Optional[Decimal] = None
    loan_rate_range: Optional[Decimal] = None
    loan_rate_stdev: Optional[Decimal] = None

    market_cap: Optional[Decimal] = None
    shares_out: Optional[Decimal] = None
    volume: Optional[Decimal] = None
    rtn: Optional[Decimal] = None

    dps: int

    @classmethod
    def build_record(
        cls, key: Tuple[datetime, int], records: List[Tuple]
    ) -> "AggregateBase":
        """Builds Aggregate Base record object.

        Args:
            key: datetime, gvkey.
            records: record from ciq market cap table.

        Returns:
            Returns record object.
        """
        res = cls()

        res.datadate = key[0]
        res.gvkey = key[1]

        utilization_pct_values = [r[2] for r in records if r[2]]
        bar_values = [r[3] for r in records if r[3]]
        age_values = [r[4] for r in records if r[4]]
        tickets_values = [r[5] for r in records if r[5]]
        units_values = [r[6] for r in records if r[6]]
        market_value_usd_values = [r[7] for r in records if r[7]]
        loan_rate_avg_values = [r[8] for r in records if r[8]]
        loan_rate_max_values = [r[9] for r in records if r[9]]
        loan_rate_min_values = [r[10] for r in records if r[10]]
        loan_rate_range_values = [r[11] for r in records if r[11]]
        loan_rate_stdev_values = [r[12] for r in records if r[12]]

        market_cap_values = [r[13] for r in records if r[13]]
        shares_out_values = [r[14] for r in records if r[14]]
        volume_values = [r[15] for r in records if r[15]]
        rtn_values = [r[16] for r in records if r[16] is not None]

        # AVG UTILIZATION PERCENT
        res.utilization_pct = (
            Decimal(sum(utilization_pct_values) / len(utilization_pct_values))
            if utilization_pct_values
            else None
        )
        # AVG BAR
        res.bar = Decimal(sum(bar_values) / len(bar_values)) if bar_values else None
        # AVG AGE
        res.age = Decimal(sum(age_values) / len(age_values)) if age_values else None
        # TOTAL TICKETS
        res.tickets = (
            Decimal(sum(tickets_values) / len(tickets_values))
            if tickets_values
            else None
        )
        # TOTAL UNITS
        res.units = (
            Decimal(sum(units_values) / len(units_values)) if units_values else None
        )
        # TOTAL MARKET_VALUE_USD
        res.market_value_usd = (
            Decimal(sum(market_value_usd_values) / len(market_value_usd_values))
            if market_value_usd_values
            else None
        )
        # AVG LOAN RATE AVG
        res.loan_rate_avg = (
            Decimal(sum(loan_rate_avg_values) / len(loan_rate_avg_values))
            if loan_rate_avg_values
            else None
        )
        # MAX LOAN RATE MAX
        res.loan_rate_max = (
            Decimal(sum(loan_rate_max_values) / len(loan_rate_max_values))
            if loan_rate_max_values
            else None
        )
        # MIN LOAN RATE MIN
        res.loan_rate_min = (
            Decimal(sum(loan_rate_min_values) / len(loan_rate_min_values))
            if loan_rate_min_values
            else None
        )
        res.loan_rate_range = (
            Decimal(sum(loan_rate_range_values) / len(loan_rate_range_values))
            if loan_rate_range_values
            else None
        )
        # AGGREGATE STDEV
        res.loan_rate_stdev = (
            sqrt(sum([x**2 for x in loan_rate_stdev_values]))
            if loan_rate_stdev_values
            else None
        )
        res.market_cap = (
            Decimal(sum(market_cap_values) / len(market_cap_values))
            if market_cap_values
            else None
        )
        res.shares_out = (
            Decimal(sum(shares_out_values) / len(shares_out_values))
            if shares_out_values
            else None
        )
        res.volume = (
            Decimal(sum(volume_values) / len(volume_values))
            if volume_values
            else None
        )
        if rtn_values:
            agg_rtn = 1
            for r in rtn_values:
                agg_rtn = agg_rtn * (1 + r)

            res.rtn = agg_rtn - 1

        res.dps = len(records)

        return res

    def as_tuple(self) -> Tuple:
        """Get tuple with object attributes.

        Returns:
            Tuple with object attributes.
        """
        return (
            self.datadate,
            self.gvkey,
            self.utilization_pct,
            self.bar,
            self.age,
            self.tickets,
            self.units,
            self.market_value_usd,
            self.loan_rate_avg,
            self.loan_rate_max,
            self.loan_rate_min,
            self.loan_rate_range,
            self.loan_rate_stdev,
            self.market_cap,
            self.shares_out,
            self.volume,
            self.rtn,
            self.dps
        )

    @property
    def is_empty(self):
        if (
            self.utilization_pct is None
            and self.bar is None
            and self.age is None
            and self.tickets is None
            and self.units is None
            and self.market_value_usd is None
            and self.loan_rate_avg is None
            and self.loan_rate_max is None
            and self.loan_rate_min is None
            and self.loan_rate_range is None
            and self.loan_rate_stdev is None
            and self.market_cap is None
            and self.shares_out is None
            and self.volume is None
            and self.rtn is None
        ):
            return True
        else:
            return False
