WITH wash_trading AS (
    SELECT distinct wash_trading_filter_ AS wash_trading_filter
    FROM (
        SELECT CASE WHEN '{{Wash Trading Filter}}' IN ('ON', 'OFF') THEN false END AS wash_trading_filter_
        UNION
        SELECT CASE WHEN '{{Wash Trading Filter}}'='OFF' THEN true END AS wash_trading_filter_
        )
        WHERE wash_trading_filter_ IS NOT NULL
    )
    
, raw_data AS (
    SELECT date_trunc('month', t.block_time) AS time
    , CASE WHEN LENGTH(t.aggregator_name) > 1 THEN LOWER(t.aggregator_name) ELSE LOWER(t.project) END AS marketplace
    , SUM(t.amount_usd) AS total_volume
    FROM nft.trades t
    INNER JOIN nft.wash_trades wt ON wt.block_number=t.block_number
        AND wt.unique_trade_id=t.unique_trade_id
        AND wt.is_wash_trade IN (SELECT wash_trading_filter FROM wash_trading)
    WHERE t.blockchain = 'ethereum'
    AND t.block_time >= date_trunc('month', NOW() - interval '2' year)
    AND t.block_time < date_trunc('month', NOW())
    AND t.tx_hash NOT IN (SELECT tx_hash FROM query_1982559) -- https://dune.com/queries/1982559 (Remove ocasional bad data)
    GROUP BY 1, 2
    )

, eth_price AS (
    SELECT date_trunc('month', minute) AS time
    , AVG(price) AS eth_avg_price
    FROM prices.usd
    WHERE blockchain='ethereum'
    AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    AND minute >= date_trunc('month', NOW() - interval '2' year)
    AND minute < date_trunc('month', NOW())
    GROUP BY 1
    )

, ordering AS (
    SELECT ROW_NUMBER() OVER (ORDER BY total_volume DESC) AS ranking
    , marketplace
    , total_volume
    FROM raw_data
    WHERE time = (SELECT MAX(time) FROM raw_data)
    )
    
SELECT rd.time
, COALESCE(names.name, codename) AS name
, CASE WHEN '{{Currency}}'='USD' THEN rd.total_volume
    WHEN '{{Currency}}'='ETH' THEN rd.total_volume/ep.eth_avg_price
    END AS volume
FROM raw_data rd
LEFT JOIN query_1933124 names ON names.codename=rd.marketplace -- https://dune.com/queries/1933124 For proper marketplace name capitalisations
LEFT JOIN ordering o ON o.marketplace=rd.marketplace
LEFT JOIN eth_price ep ON rd.time=ep.time
ORDER BY o.ranking
