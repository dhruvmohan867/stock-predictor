CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(100),
    sector VARCHAR(50)
);

CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY,
    stock_id INT REFERENCES stocks(id),
    date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT
);
