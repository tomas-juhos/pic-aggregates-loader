CREATE TABLE weekly_base
(
    datadate                            TIMESTAMP,
    gvkey                               INTEGER,

    utilization_pct                     DECIMAL(14,8),
    bar                                 DECIMAL(14,8),
    age                                 DECIMAL(18,7),
    tickets                             DECIMAL(18,2),
    units                               DECIMAL(18,4),
    market_value_usd                    DECIMAL(18,2),
    loan_rate_avg                       DECIMAL(18,9),
    loan_rate_max                       DECIMAL(18,9),
    loan_rate_min                       DECIMAL(18,9),
    loan_rate_range                     DECIMAL(18,9),
    loan_rate_stdev                     DECIMAL(18,9),

    market_cap                          DECIMAL(30,15),
    shares_out                          DECIMAL(30,4),
    rtn                                 DECIMAL(25,15),

    dps                                 INTEGER,

    PRIMARY KEY (gvkey, datadate)
);