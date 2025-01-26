CREATE TABLE IF NOT EXISTS stocks ( id SERIAL, trade_time TIMESTAMPTZ NOT NULL,
                                                                      close_price FLOAT NOT NULL,
                                                                                        high_price FLOAT NOT NULL,
                                                                                                         low_price FLOAT NOT NULL,
                                                                                                                         num_trades INT NOT NULL,
                                                                                                                                        open_price FLOAT NOT NULL,
                                                                                                                                                         volume INT NOT NULL,
                                                                                                                                                                    vwap FLOAT NOT NULL, -- Default ticker to 'TSLA' for this dataset:
 ticker TEXT NOT NULL, -- TimescaleDB requires the time partition column in the primary key:
 PRIMARY KEY (trade_time,
              id));

SELECT create_hypertable('stocks', 'trade_time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS anomaly (trade_time TIMESTAMPTZ NOT NULL, ticker TEXT NOT NULL, bot TEXT, distance FLOAT, classification TEXT, descr TEXT, 
PRIMARY KEY (trade_time, ticker));

SELECT create_hypertable('anomaly', 'trade_time', if_not_exists => TRUE);