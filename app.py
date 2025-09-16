import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector
from datetime import datetime, timezone

st.set_page_config(page_title="Flow Weekly Stats", page_icon="🟩", layout="wide")

# ---------- Connexió ----------
@st.cache_resource
def get_conn():
    params = dict(
        user=st.secrets[""],
        account=st.secrets[""],
        warehouse=st.secrets[""],
        role=st.secrets[""],
    )
    if "SNOWFLAKE_PASSWORD" in st.secrets:
        params["password"] = st.secrets[""]
    if "SNOWFLAKE_AUTHENTICATOR" in st.secrets:
        params["authenticator"] = st.secrets[""]
    return snowflake.connector.connect(**params)

@st.cache_data(ttl=300)
def run_query(sql: str):
    cur = get_conn().cursor()
    try:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()

def now_local():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")

def download_btn(df, label, fname):
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label=label, data=csv, file_name=fname, mime="text/csv")

# ---------- “Last data week” ----------
SQL_LAST_DATA_WEEK = """
WITH weeks AS (
  SELECT TRUNC(block_timestamp,'WEEK') AS wk FROM flow.core.fact_transactions WHERE block_timestamp < TRUNC(CURRENT_DATE,'WEEK')
  UNION ALL
  SELECT TRUNC(block_timestamp,'WEEK') AS wk FROM flow.core_evm.fact_transactions WHERE block_timestamp < TRUNC(CURRENT_DATE,'WEEK')
)
SELECT MAX(wk) AS last_week FROM weeks;
"""

# ---------- SQLs ----------
SQL_TX_NUMBERS = """
WITH final AS (
  SELECT
    TRUNC(block_timestamp,'WEEK') AS week,
    CASE WHEN tx_succeeded='true' THEN 'Succeeded' ELSE 'Failed' END AS type,
    COUNT(DISTINCT tx_id) AS total_transactions,
    SUM(COUNT(DISTINCT tx_id)) OVER (
      PARTITION BY CASE WHEN tx_succeeded='true' THEN 'Succeeded' ELSE 'Failed' END
      ORDER BY TRUNC(block_timestamp,'WEEK')
    ) AS cum_transactions
  FROM (
    SELECT DISTINCT tx_id, block_timestamp, tx_succeeded FROM flow.core.fact_transactions
    UNION ALL
    SELECT DISTINCT tx_hash AS tx_id, block_timestamp, tx_succeeded FROM flow.core_evm.fact_transactions
  ) x
  WHERE TRUNC(block_timestamp,'WEEK') < TRUNC(CURRENT_DATE,'WEEK')
  GROUP BY 1,2
)
SELECT * FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY type ORDER BY week DESC) <= 2
ORDER BY week ASC, type ASC;
"""

SQL_TX_OVER_TIME = """WITH previous_week_transactions AS (
    SELECT
        trunc(block_timestamp,'week') as week,
        count(distinct tx_id) as total_transactions
    FROM
        flow.core.fact_transactions
    WHERE
        block_timestamp<trunc(current_date,'week')-1
    GROUP BY
        1
    UNION ALL
    SELECT
        trunc(block_timestamp,'week') as week,
        count(distinct tx_hash) as total_transactions
    FROM
        flow.core_evm.fact_transactions
    WHERE
        block_timestamp<trunc(current_date,'week')-1
    GROUP BY
        1
),
aggregated_previous_week AS (
    SELECT
        week,
        sum(total_transactions) as total_transactions
    FROM
        previous_week_transactions
    GROUP BY
        week
),
current_week_transactions AS (
    SELECT
        trunc(block_timestamp,'week') as week,
        count(distinct tx_id) as total_transactions
    FROM
        flow.core.fact_transactions
    WHERE
        block_timestamp<trunc(current_date,'week')
    GROUP BY
        1
    UNION ALL
    SELECT
        trunc(block_timestamp,'week') as week,
        count(distinct tx_hash) as total_transactions
    FROM
        flow.core_evm.fact_transactions
    WHERE
        block_timestamp<trunc(current_date,'week')
    GROUP BY
        1
),
aggregated_current_week AS (
    SELECT
        week,
        sum(total_transactions) as total_transactions
    FROM
        current_week_transactions
    GROUP BY
        week
)
SELECT
    current_week.week,
    current_week.total_transactions,
    CONCAT(current_week.total_transactions, ' (', current_week.total_transactions - previous_week.total_transactions, ')') as transactions_diff,
    ((current_week.total_transactions - previous_week.total_transactions) / previous_week.total_transactions) * 100 as pcg_diff,
    SUM(current_week.total_transactions) OVER (ORDER BY current_week.week) as cum_transactions
FROM
    aggregated_current_week current_week
LEFT JOIN
    aggregated_previous_week previous_week
ON
    dateadd(week, -1, current_week.week) = previous_week.week
WHERE
    current_week.week < trunc(current_date,'week')
ORDER BY
    current_week.week desc
"""
SQL_TX_SUCCESS_FAIL = """
 with txs as (
select distinct tx_id, block_timestamp, tx_succeeded
from flow.core.fact_transactions
UNION
select distinct tx_hash as tx_id, block_timestamp, tx_succeeded
from flow.core_evm.fact_transactions
)
SELECT
trunc(block_timestamp,'week') as week,
case when tx_succeeded='true' then 'Succeeded' else 'Failed' end as type,
count(distinct tx_id) as total_transactions
from txs
where week<trunc(current_date,'week') 
group by 1,2
order by 1 asc """
SQL_AVG_TX_FEE_WEEKLY = """WITH evm_data AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS date,
        COUNT(TX_HASH) AS total_transactions,
        SUM(CASE WHEN tx_succeeded = 'TRUE' THEN 1 ELSE 0 END) AS successful_transactions,
        successful_transactions/total_transactions as success_rate,
        SUM(tx_fee) AS fees,
        AVG(tx_fee) AS avg_tx_fee,
        COUNT(DISTINCT FROM_ADDRESS) AS unique_users,
        avg(DATEDIFF(MINUTE, INSERTED_TIMESTAMP, BLOCK_TIMESTAMP)) AS latency_MINUTEs
    FROM 
        flow.core_evm.fact_transactions
    --WHERE 
    --    BLOCK_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE) and block_timestamp<current_date
    GROUP BY 
        1
),
non_evm_data AS (
    SELECT 
        DATE_TRUNC('day', x.BLOCK_TIMESTAMP) AS date,
        COUNT(distinct x.TX_ID) AS total_transactions,
        SUM(CASE WHEN x.TX_SUCCEEDED = 'TRUE' THEN 1 ELSE 0 END) AS successful_transactions,
        successful_transactions/total_transactions as success_rate,
        SUM(y.event_data:amount) AS fees,
        AVG(y.event_data:amount) AS avg_tx_fee,
        COUNT(DISTINCT x.PAYER) AS unique_users,
        avg(DATEDIFF(MINUTE, x.INSERTED_TIMESTAMP, x.BLOCK_TIMESTAMP)) AS latency_MINUTEs
    FROM 
        flow.core.fact_transactions x
join flow.core.fact_events y on x.tx_id=y.tx_id
    WHERE 
    --    x.BLOCK_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE) and 
event_contract='A.f919ee77447b7497.FlowFees'
and event_Type='FeesDeducted' and x.block_timestamp<current_date
    GROUP BY 
        1
),
flow_price as (
SELECT
trunc(hour,'hour') as hour,
avg(price) as price
from flow.price.ez_prices_hourly
where symbol = 'FLOW'
group by 1 
),
final as (
select date, fees, avg_tx_fee from evm_data union select date, fees, avg_tx_fee from non_evm_data
)
 SELECT
trunc(date,'week') as month,
avg(avg_tx_fee) as avg_tx_fee_flow,
avg(avg_tx_fee*price) as avg_tx_fee_usd
from final x join flow_price y on trunc(date,'week')=trunc(hour,'week')
where month<trunc(current_date,'week')
group by 1 order by 1 asc """
SQL_STAKED_OVER_TIME = """WITH
  staking as (
  SELECT
trunc(block_timestamp,'week') as date,
--case when action in ('DelegatorTokensCommitted','TokensCommitted') then 'Staking',
--when action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn') then 'Unstaking'
--  end as actions,
count(distinct tx_id) as transactions,
sum(transactions) over (order by date) as cum_transactions,
count(distinct delegator) as delegators,
sum(delegators) over (order by date) as cum_delegators,
sum(amount) as volume,
sum(volume) over (order by date) as cum_volume,
avg(amount) as avg_volume,
median(amount) as median_volume,
avg(volume) over (order by date rows between 6 preceding and current row) as avg_7d_ma_volume
from flow.gov.ez_staking_actions  where action in ('DelegatorTokensCommitted','TokensCommitted')
  group by 1 order by 1 asc
  ),
unstaking as (
    SELECT
trunc(block_timestamp,'week') as date,
--case when action in ('DelegatorTokensCommitted','TokensCommitted') then 'Staking',
--when action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn') then 'Unstaking'
--  end as actions,
count(distinct tx_id) as transactions,
sum(transactions) over (order by date) as cum_transactions,
count(distinct delegator) as delegators,
sum(delegators) over (order by date) as cum_delegators,
sum(amount) as volume,
sum(volume) over (order by date) as cum_volume,
avg(amount) as avg_volume,
median(amount) as median_volume,
avg(volume) over (order by date rows between 6 preceding and current row) as avg_7d_ma_volume
from flow.gov.ez_staking_actions  where action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn')
  group by 1 order by 1 asc
)
SELECT
x.date,
x.transactions as staking_transactions,y.transactions as unstaking_transactions,
x.cum_transactions as total_staking_transactions,y.cum_transactions as total_unstaking_transactions,total_staking_transactions-total_unstaking_transactions as net_staking_transactions,
x.delegators as staking_delegators,y.delegators as unstaking_delegators,
x.cum_delegators as total_staking_delegators,y.cum_delegators as total_unstaking_delegators, total_staking_delegators-total_unstaking_delegators as net_staking_delegators,
x.volume as staked_volume, y.volume*(-1) as unstaked_volume, staked_volume+unstaked_volume as net_staked_volume,
x.cum_volume as total_staked_volume, y.cum_volume*(-1) as total_unstaked_volume, total_staked_volume+total_unstaked_volume+2.4e8 as total_net_staked_volume
from staking x
left outer join unstaking y on x.date=y.date 
where x.date<trunc(current_date,'week') and y.date<trunc(current_date,'week')
order by 1 asc 
"""
SQL_STAKERS_SUMMARY = """WITH
  staking as (
  SELECT
delegator,
sum(amount) as volume,
avg(amount) as avg_volume,
median(amount) as median_volume
from flow.gov.ez_staking_actions  where action in ('DelegatorTokensCommitted','TokensCommitted')
  group by 1 order by 1 asc
  ),
unstaking as (
    SELECT
delegator,
sum(amount) as volume,
avg(amount) as avg_volume,
median(amount) as median_volume
from flow.gov.ez_staking_actions  where action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn')
  group by 1 order by 1 asc
),
staking_past_week as (
  SELECT
delegator,
sum(amount) as volume,
avg(amount) as avg_volume,
median(amount) as median_volume
from flow.gov.ez_staking_actions  where action in ('DelegatorTokensCommitted','TokensCommitted')
and block_timestamp<current_date-INTERVAL '1 WEEK'
  group by 1 order by 1 asc
  ),
unstaking_past_week as (
    SELECT
delegator,
sum(amount) as volume,
avg(amount) as avg_volume,
median(amount) as median_volume
from flow.gov.ez_staking_actions  where action in ('UnstakedTokensWithdrawn','DelegatorUnstakedTokensWithdrawn')
and block_timestamp<current_date-INTERVAL '1 WEEK'
 group by 1 order by 1 asc
),
  final as (
SELECT
ifnull(x.delegator,y.delegator) as delegator,
ifnull(x.volume,0) as total_staked_volume, 
ifnull(y.volume*(-1),0) as total_unstaked_volume, 
total_staked_volume+total_unstaked_volume as total_net_staked_volume
from staking x
left outer join unstaking y on  x.delegator=y.delegator
order by 1 asc 
),
  final2 as (
SELECT
ifnull(x.delegator,y.delegator) as delegator,
ifnull(x.volume,0) as total_staked_volume, 
ifnull(y.volume*(-1),0) as total_unstaked_volume, 
total_staked_volume+total_unstaked_volume as total_net_staked_volume
from staking_past_week x
left outer join unstaking_past_week y on  x.delegator=y.delegator
order by 1 asc 
),
totals as (
SELECT
count(distinct x.delegator) as unique_stakers,
count(distinct y.delegator) as unique_stakers_past_week,
unique_stakers-unique_stakers_past_week as users_diff,
((unique_stakers-unique_stakers_past_week)/unique_stakers_past_week)*100 as pcg_diff
from final x,final2 y --where x.total_net_staked_volume>0 and y.total_net_staked_volume>0
)
select 
unique_stakers,
case when users_diff>=0 then CONCAT(unique_stakers, ' (+', users_diff, ')')
when users_diff<0 then CONCAT(unique_stakers, ' (-', users_diff, ')') end as unique_stakers_diff,
pcg_diff from totals
 """
SQL_FLOW_PRICE_WEEK = """with
revv as (
SELECT 
  hour,
  AVG(price) AS price_usd,
  MAX(price) AS high,
  STDDEV_POP(price) AS std
FROM flow.price.ez_prices_hourly
WHERE symbol ILIKE '%flow%'
  AND hour >= CURRENT_DATE - INTERVAL '1 WEEK' and price<0.5
GROUP BY hour
ORDER BY hour ASC 
),
flow as (
 select 
hour,
price_usd,
high,
LAG(price_usd, 1) OVER (ORDER BY hour) AS open,
std
from revv
) 
select
y.hour as recorded_hour,
y.price_usd as flow_price,
(y.high - y.open) / y.open * 100 as flow_price_change_percentage,
y.std as flow_price_volatility
from flow y
order by 1 desc """
SQL_TOKENS_WEEKLY_MOVERS = """SELECT asset_id as token,
  AVG(close) AS avg_price,
  (AVG(close) - AVG(CASE
                      WHEN hour BETWEEN DATEADD(day, -1, GETDATE()) AND GETDATE() THEN close
                      ELSE NULL
                    END)) / AVG(close) AS avg_deviation_weekly_pct
FROM flow.price.fact_prices_ohlc_hourly
WHERE hour BETWEEN DATEADD(day, -7, GETDATE()) AND GETDATE()
GROUP BY 1 having token is not null order by avg_deviation_weekly_pct desc 
"""

# Accounts
SQL_USERS_BY_REGION = """WITH
-- Calculate the debut (first activity) of each user
news AS (
    SELECT
        DISTINCT authorizers[0] AS user,
        MIN(TRUNC(block_timestamp, 'hour')) AS debut
    FROM
        flow.core.fact_transactions
    GROUP BY
        1
),
-- Generate a list of all hours (0 to 23)
all_hours AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS hour_of_day
    FROM
        TABLE(GENERATOR(ROWCOUNT => 24))
),
-- Count the number of transactions for each user per hour
actives AS (
    SELECT
        DISTINCT authorizers[0] AS user,
        EXTRACT(HOUR FROM block_timestamp) AS active_hour,        
        COUNT(DISTINCT tx_id) AS transactions
    FROM
        flow.core.fact_transactions x
    GROUP BY
        1,
        2
),
-- Join the debut and hourly transaction counts for each user
user_activity AS (
    SELECT
        n.user,
        n.debut,
        h.hour_of_day,
        COALESCE(a.transactions, 0) AS transactions,
        RANK() OVER (PARTITION BY n.user ORDER BY COALESCE(a.transactions, 0) DESC) AS hourly_rank
    FROM
        news n
    CROSS JOIN
        all_hours h
    LEFT JOIN
        actives a ON n.user = a.user AND h.hour_of_day = active_hour
),
-- Determine the range of most active and least active hours for each user
user_hourly_ranges AS (
    SELECT
        user,
        debut,
        LISTAGG(hour_of_day, ',') WITHIN GROUP (ORDER BY transactions DESC) AS most_active_hours,
        LISTAGG(hour_of_day, ',') WITHIN GROUP (ORDER BY transactions ASC) AS least_active_hours
    FROM (
        SELECT
            user,
            debut,
            hour_of_day,
            transactions,
            ROW_NUMBER() OVER (PARTITION BY user, debut ORDER BY transactions DESC) AS active_rank,
            ROW_NUMBER() OVER (PARTITION BY user, debut ORDER BY transactions ASC) AS inactive_rank
        FROM
            user_activity
    )
    WHERE
        active_rank <= 6-- OR inactive_rank <= 3
    GROUP BY
        user, debut
),
user_regions AS (
    SELECT
    user,
    debut,
    SUM(CASE WHEN POSITION('08' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('17' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('18' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Europe_Central_count,
    
    SUM(CASE WHEN POSITION('02' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('17' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS US_East_Coast_count,
    
    SUM(CASE WHEN POSITION('08' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('14' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('15' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Asia_count,
    
    SUM(CASE WHEN POSITION('23' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('00' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('07' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('08' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Europe_Western_count,
    
    SUM(CASE WHEN POSITION('17' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('18' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('19' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('02' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS US_Central_count,
    
    SUM(CASE WHEN POSITION('11' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('12' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('13' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('19' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('20' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('21' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS US_West_Coast_count,
    
    SUM(CASE WHEN POSITION('20' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('21' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('22' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('05' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('06' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS South_America_count,

    SUM(CASE WHEN POSITION('00' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('02' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('05' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Africa_count,

    SUM(CASE WHEN POSITION('10' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('11' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('12' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('13' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('14' IN most_active_hours) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('15' IN most_active_hours) > 0 THEN 1 ELSE 0 END) AS Oceania_count
FROM
    user_hourly_ranges
GROUP BY
    user, debut
) --select * from user_regions where user='0xecb50f2955d00a5c'
,
regions as (
SELECT
    user,
    debut,
    CASE
        WHEN Europe_Central_count > US_East_Coast_count AND Europe_Central_count > Asia_count AND Europe_Central_count > Europe_Western_count AND Europe_Central_count > US_Central_count AND Europe_Central_count > US_West_Coast_count AND Europe_Central_count > South_America_count THEN 'Europe Central'
        WHEN US_East_Coast_count > Europe_Central_count AND US_East_Coast_count > Asia_count AND US_East_Coast_count > Europe_Western_count AND US_East_Coast_count > US_Central_count AND US_East_Coast_count > US_West_Coast_count AND US_East_Coast_count > South_America_count THEN 'United States (East Coast)'
        WHEN Asia_count > Europe_Central_count AND Asia_count > US_East_Coast_count AND Asia_count > Europe_Western_count AND Asia_count > US_Central_count AND Asia_count > US_West_Coast_count AND Asia_count > South_America_count THEN 'Asia'
        WHEN Europe_Western_count > Europe_Central_count AND Europe_Western_count > US_East_Coast_count AND Europe_Western_count > Asia_count AND Europe_Western_count > US_Central_count AND Europe_Western_count > US_West_Coast_count AND Europe_Western_count > South_America_count THEN 'Europe Western'
        WHEN US_Central_count > Europe_Central_count AND US_Central_count > US_East_Coast_count AND US_Central_count > Asia_count AND US_Central_count > Europe_Western_count AND US_Central_count > US_West_Coast_count AND US_Central_count > South_America_count THEN 'United States (Central)'
        WHEN US_West_Coast_count > Europe_Central_count AND US_West_Coast_count > US_East_Coast_count AND US_West_Coast_count > Asia_count AND US_West_Coast_count > Europe_Western_count AND US_West_Coast_count > US_Central_count AND US_West_Coast_count > South_America_count THEN 'United States (West Coast)'
        WHEN South_America_count > Europe_Central_count AND South_America_count > US_East_Coast_count AND South_America_count > Asia_count AND South_America_count > Europe_Western_count AND South_America_count > US_Central_count AND South_America_count > US_West_Coast_count then 'South America'
        WHEN Africa_count > Europe_Central_count AND Africa_count > US_East_Coast_count AND Africa_count > Asia_count AND Africa_count > Europe_Western_count AND Africa_count > US_Central_count AND Africa_count > US_West_Coast_count AND Africa_count > South_America_count AND Africa_count > Oceania_count THEN 'Africa'
        WHEN Oceania_count > Europe_Central_count AND Oceania_count > US_East_Coast_count AND Oceania_count > Asia_count AND Oceania_count > Europe_Western_count AND Oceania_count > US_Central_count AND Oceania_count > US_West_Coast_count AND Oceania_count > South_America_count AND Oceania_count > Africa_count THEN 'Oceania'
        
else 'Unknown Region' end as geographical_region
from user_regions
)
SELECT
trunc(block_timestamp,'week') as "Week",
case when geographical_region in ('United States (East Coast)','United States (Central)','United States (West Coast)') then 'US' 
when geographical_region in ('Europe Central','Europe Western') then 'Europe'
else geographical_region end as "Geographical Region",
count(distinct authorizers[0]) as "Active Accounts"
from flow.core.fact_transactions x join regions y on x.authorizers[0]=user
--where ""Week""<trunc(current_date,'week')
group by 1,2 order by 1 asc, 3 desc  
"""
SQL_USERS_BY_REGION_OVER_TIME = """WITH
-- Calculate the debut (first activity) of each user
news AS (
    SELECT
        DISTINCT authorizers[0] AS user,
        MIN(TRUNC(block_timestamp, 'hour')) AS debut
    FROM
        flow.core.fact_transactions
    GROUP BY
        1
),
-- Generate a list of all hours (0 to 23)
all_hours AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS hour_of_day
    FROM
        TABLE(GENERATOR(ROWCOUNT => 24))
),
-- Count the number of transactions for each user per hour
actives AS (
    SELECT
        DISTINCT authorizers[0] AS user,
        EXTRACT(HOUR FROM block_timestamp) AS active_hour,        
        COUNT(DISTINCT tx_id) AS transactions
    FROM
        flow.core.fact_transactions x
    GROUP BY
        1,
        2
),
-- Join the debut and hourly transaction counts for each user
user_activity AS (
    SELECT
        n.user,
        n.debut,
        h.hour_of_day,
        COALESCE(a.transactions, 0) AS transactions,
        RANK() OVER (PARTITION BY n.user ORDER BY COALESCE(a.transactions, 0) DESC) AS hourly_rank
    FROM
        news n
    CROSS JOIN
        all_hours h
    LEFT JOIN
        actives a ON n.user = a.user AND h.hour_of_day = active_hour
),
-- Determine the range of most active and least active hours for each user
user_hourly_ranges AS (
    SELECT
        user,
        debut,
        LISTAGG(hour_of_day, ',') WITHIN GROUP (ORDER BY transactions DESC) AS most_count,
        LISTAGG(hour_of_day, ',') WITHIN GROUP (ORDER BY transactions ASC) AS least_count
    FROM (
        SELECT
            user,
            debut,
            hour_of_day,
            transactions,
            ROW_NUMBER() OVER (PARTITION BY user, debut ORDER BY transactions DESC) AS active_rank,
            ROW_NUMBER() OVER (PARTITION BY user, debut ORDER BY transactions ASC) AS inactive_rank
        FROM
            user_activity
    )
    WHERE
        active_rank <= 6-- OR inactive_rank <= 3
    GROUP BY
        user, debut
),
user_regions AS (
    SELECT
    user,
    debut,
    SUM(CASE WHEN POSITION('08' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('17' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('18' IN most_count) > 0 THEN 1 ELSE 0 END) AS Europe_Central_count,
    
    SUM(CASE WHEN POSITION('02' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('17' IN most_count) > 0 THEN 1 ELSE 0 END) AS US_East_Coast_count,
    
    SUM(CASE WHEN POSITION('08' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('10' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('14' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('15' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('16' IN most_count) > 0 THEN 1 ELSE 0 END) AS Asia_count,
    
    SUM(CASE WHEN POSITION('23' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('00' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('07' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('08' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('09' IN most_count) > 0 THEN 1 ELSE 0 END) AS Europe_Western_count,
    
    SUM(CASE WHEN POSITION('17' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('18' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('19' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('02' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_count) > 0 THEN 1 ELSE 0 END) AS US_Central_count,
    
    SUM(CASE WHEN POSITION('11' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('12' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('13' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('19' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('20' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('21' IN most_count) > 0 THEN 1 ELSE 0 END) AS US_West_Coast_count,
    
    SUM(CASE WHEN POSITION('20' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('21' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('22' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('05' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('06' IN most_count) > 0 THEN 1 ELSE 0 END) AS South_America_count,

    SUM(CASE WHEN POSITION('00' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('01' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('02' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('03' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('04' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('05' IN most_count) > 0 THEN 1 ELSE 0 END) AS Africa_count,

    SUM(CASE WHEN POSITION('10' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('11' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('12' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('13' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('14' IN most_count) > 0 THEN 1 ELSE 0 END +
        CASE WHEN POSITION('15' IN most_count) > 0 THEN 1 ELSE 0 END) AS Oceania_count
FROM
    user_hourly_ranges
GROUP BY
    user, debut
) --select * from user_regions where user='0xecb50f2955d00a5c'
,
regions as (
SELECT
    user,
    debut,
    CASE
        WHEN Europe_Central_count > US_East_Coast_count AND Europe_Central_count > Asia_count AND Europe_Central_count > Europe_Western_count AND Europe_Central_count > US_Central_count AND Europe_Central_count > US_West_Coast_count AND Europe_Central_count > South_America_count THEN 'Europe Central'
        WHEN US_East_Coast_count > Europe_Central_count AND US_East_Coast_count > Asia_count AND US_East_Coast_count > Europe_Western_count AND US_East_Coast_count > US_Central_count AND US_East_Coast_count > US_West_Coast_count AND US_East_Coast_count > South_America_count THEN 'United States (East Coast)'
        WHEN Asia_count > Europe_Central_count AND Asia_count > US_East_Coast_count AND Asia_count > Europe_Western_count AND Asia_count > US_Central_count AND Asia_count > US_West_Coast_count AND Asia_count > South_America_count THEN 'Asia'
        WHEN Europe_Western_count > Europe_Central_count AND Europe_Western_count > US_East_Coast_count AND Europe_Western_count > Asia_count AND Europe_Western_count > US_Central_count AND Europe_Western_count > US_West_Coast_count AND Europe_Western_count > South_America_count THEN 'Europe Western'
        WHEN US_Central_count > Europe_Central_count AND US_Central_count > US_East_Coast_count AND US_Central_count > Asia_count AND US_Central_count > Europe_Western_count AND US_Central_count > US_West_Coast_count AND US_Central_count > South_America_count THEN 'United States (Central)'
        WHEN US_West_Coast_count > Europe_Central_count AND US_West_Coast_count > US_East_Coast_count AND US_West_Coast_count > Asia_count AND US_West_Coast_count > Europe_Western_count AND US_West_Coast_count > US_Central_count AND US_West_Coast_count > South_America_count THEN 'United States (West Coast)'
        WHEN South_America_count > Europe_Central_count AND South_America_count > US_East_Coast_count AND South_America_count > Asia_count AND South_America_count > Europe_Western_count AND South_America_count > US_Central_count AND South_America_count > US_West_Coast_count then 'South America'
        WHEN Africa_count > Europe_Central_count AND Africa_count > US_East_Coast_count AND Africa_count > Asia_count AND Africa_count > Europe_Western_count AND Africa_count > US_Central_count AND Africa_count > US_West_Coast_count AND Africa_count > South_America_count AND Africa_count > Oceania_count THEN 'Africa'
        WHEN Oceania_count > Europe_Central_count AND Oceania_count > US_East_Coast_count AND Oceania_count > Asia_count AND Oceania_count > Europe_Western_count AND Oceania_count > US_Central_count AND Oceania_count > US_West_Coast_count AND Oceania_count > South_America_count AND Oceania_count > Africa_count THEN 'Oceania'
else 'Unknown Region' end as geographical_region
from user_regions
),
--regions as (
--SELECT
--    user,
--    debut,
--    most_count,
--    CASE
--        WHEN POSITION('11' IN most_count) > 0 OR POSITION('12' IN most_count) > 0 OR POSITION('13' IN most_count) > 0 THEN 'Europe Central'
--        WHEN POSITION('05' IN most_count) > 0 OR POSITION('06' IN most_count) > 0 OR POSITION('07' IN most_count) > 0 THEN 'United States (East Coast)'
--        WHEN POSITION('17' IN most_count) > 0 OR POSITION('18' IN most_count) > 0 OR POSITION('19' IN most_count) > 0 THEN 'Asia'
--        WHEN POSITION('02' IN most_count) > 0 OR POSITION('03' IN most_count) > 0 OR POSITION('04' IN most_count) > 0 THEN 'Europe Western'
--        WHEN POSITION('08' IN most_count) > 0 OR POSITION('09' IN most_count) > 0 OR POSITION('10' IN most_count) > 0 THEN 'United States (Central)'
--        WHEN POSITION('14' IN most_count) > 0 OR POSITION('15' IN most_count) > 0 OR POSITION('16' IN most_count) > 0 THEN 'United States (West Coast)'
--        WHEN POSITION('00' IN most_count) > 0 OR POSITION('01' IN most_count) > 0 OR POSITION('02' IN most_count) > 0 THEN 'South America'
--        ELSE 'Unknown Region'
--   END AS geographical_region
--FROM
--    user_hourly_ranges
--)
region_counts AS (
    SELECT
        trunc(debut,'week') as debut,
        case when geographical_region in ('United States (East Coast)','United States (Central)','United States (West Coast)') then 'US'
        when geographical_region in ('Europe Central','Europe Western') then 'Europe'
 else geographical_region end AS region,
        COUNT(DISTINCT user) AS total_accounts
    FROM
        regions
    GROUP BY
        1,2
),
total_accounts AS (
    SELECT
        trunc(debut,'week') as debut,
        COUNT(DISTINCT user) AS total
    FROM
        regions
group by 1
),
known_accounts as (
SELECT
trunc(debut,'week') as debut,
COUNT(DISTINCT user) AS total3
from regions 
WHERE
        geographical_region <> 'Unknown Region'
group by 1
),
unknown_accounts AS (
    SELECT
        trunc(debut,'week') as debut,
        COUNT(DISTINCT user) AS total2
    FROM
        regions
    WHERE
        geographical_region = 'Unknown Region'
group by 1
),
pcgs as (
SELECT
distinct x.debut, x.region,
total_accounts/total3 as pcg,
total_accounts+total2*pcg as total_def
from region_counts x
left join known_accounts y on x.debut=y.debut 
left join unknown_accounts z on x.debut=z.debut
where x.region <> 'Unknown Region'
)

SELECT
trunc(debut,'week') as "Week",
region as "Geographical Region",
total_def as "Accounts Created"
from pcgs --group by 1,2 
--where ""Week""<trunc(current_date,'week')
order by 1 asc, 3 desc  
"""
SQL_USERS_NUMBERS = """WITH news AS (
    SELECT
        DISTINCT CAST(value AS VARCHAR) AS users,  -- Explicitly casting to VARCHAR
        MIN(trunc(b.block_timestamp, 'week')) AS debut
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY 1

    UNION ALL

    SELECT
        DISTINCT from_address AS users,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM
        flow.core_evm.fact_transactions
    GROUP BY 1
),
news2 AS (
    SELECT
        debut,
        COUNT(DISTINCT users) AS new_users
    FROM
        news
    GROUP BY debut
),
actives AS (
    SELECT
        trunc(b.block_timestamp, 'week') AS week,
        COUNT(DISTINCT CAST(value AS VARCHAR)) AS active_users  -- Explicitly casting to VARCHAR
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY 1

    UNION ALL

    SELECT
        trunc(block_timestamp, 'week') AS week,
        COUNT(DISTINCT from_address) AS active_users
    FROM
        flow.core_evm.fact_transactions
    GROUP BY 1
),
aggregated_actives AS (
    SELECT
        week,
        SUM(active_users) AS active_users
    FROM
        actives
    GROUP BY week
),
final AS (
    SELECT
        a.week,
        a.active_users,
        n.new_users,
        SUM(n.new_users) OVER (ORDER BY a.week) AS unique_users
    FROM
        aggregated_actives a
    LEFT JOIN
        news2 n ON a.week = n.debut
    WHERE
        a.week < trunc(current_date, 'week')
    ORDER BY 1 ASC
),
final2 AS (
    SELECT
        a.week,
        a.active_users,
        n.new_users,
        SUM(n.new_users) OVER (ORDER BY a.week) AS unique_users
    FROM
        aggregated_actives a
    LEFT JOIN
        news2 n ON a.week = n.debut
    WHERE
        a.week < current_date - INTERVAL '2 WEEKS'
    ORDER BY 1 ASC
),
final_week AS (
    SELECT * FROM final ORDER BY week DESC LIMIT 1
),
final_past_week AS (
    SELECT * FROM final2 ORDER BY week DESC LIMIT 1
)
SELECT
    final.*,
    CONCAT(final.unique_users, ' (', final.unique_users - final2.unique_users, ')') AS new_accounts,
    ((final.active_users - final2.active_users) / final2.active_users) * 100 AS pcg_diff
FROM
    final_week AS final
JOIN
    final_past_week AS final2
WHERE
    final.week < trunc(current_date, 'week')"""
SQL_USERS_OVER_TIME = """WITH news AS (
    SELECT
        DISTINCT CAST(value AS VARCHAR) AS users,  -- Explicitly casting to VARCHAR
        MIN(trunc(b.block_timestamp, 'week')) AS debut
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY 1

    UNION ALL

    SELECT
        DISTINCT from_address AS users,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM
        flow.core_evm.fact_transactions
    GROUP BY 1
),
news2 AS (
    SELECT
        debut,
        COUNT(DISTINCT users) AS new_users
    FROM
        news
    GROUP BY debut
),
actives AS (
    SELECT
        trunc(b.block_timestamp, 'week') AS week,
        COUNT(DISTINCT CAST(value AS VARCHAR)) AS active_users  -- Explicitly casting to VARCHAR
    FROM
        flow.core.ez_transaction_actors AS b,
        LATERAL FLATTEN(INPUT => b.actors) AS a  -- Flattening the actors array
    GROUP BY 1

    UNION ALL

    SELECT
        trunc(block_timestamp, 'week') AS week,
        COUNT(DISTINCT from_address) AS active_users
    FROM
        flow.core_evm.fact_transactions
    GROUP BY 1
),
aggregated_actives AS (
    SELECT
        week,
        SUM(active_users) AS active_users
    FROM
        actives
    GROUP BY week
),
final AS (
    SELECT
        a.week,
        a.active_users,
        n.new_users,
        SUM(n.new_users) OVER (ORDER BY a.week) AS unique_users
    FROM
        aggregated_actives a
    LEFT JOIN
        news2 n ON a.week = n.debut
    WHERE
        a.week < trunc(current_date, 'week')
    ORDER BY 1 ASC
)
 SELECT
week as month,
active_users, new_users
from final
order by 1 desc 
 """

# NFTs
SQL_NFT_SALES_NUMBERS = """WITH prices AS (
    SELECT 
        trunc(hour, 'week') AS hour,
        token_address AS token_contract,
        AVG(price) AS price_usd
    FROM flow.price.ez_prices_hourly
    WHERE symbol = 'FLOW'
    GROUP BY 1, 2
),
finalis AS (
    SELECT
        trunc(block_timestamp, 'week') AS week,
        tx_id,
        buyer,
        currency,
        price,
        nft_collection
    FROM flow.nft.ez_nft_sales x
    WHERE tx_succeeded = 'true'

    UNION

    SELECT
        trunc(x.block_timestamp, 'week') AS week,
        x.tx_id,
        z.authorizers[0] AS buyer,
        x.event_data:salePaymentVaultType AS currency,
        x.event_data:salePrice AS price,
        x.event_data:nftType AS nft_collection
    FROM flow.core.fact_events x
    JOIN flow.core.fact_transactions z ON x.tx_id = z.tx_id
    WHERE x.event_contract = 'A.4eb8a10cb9f87357.NFTStorefrontV2'
    AND x.event_type = 'ListingCompleted'
    AND x.event_data:purchased = 'true'
),
final AS (
    SELECT
        week,
        COUNT(DISTINCT tx_id) AS sales,
        COUNT(DISTINCT buyer) AS nft_buyers, --currency,
       -- CASE 
       --     WHEN currency ILIKE '%flow%' THEN SUM(price) * COALESCE(AVG(price_usd), 1)
       --     ELSE SUM(price)
       -- END AS volume,
        sum(price) as volume,
        sum(volume) over (order by week) as total_volume,
        COUNT(DISTINCT nft_collection) AS active_collections
    FROM finalis x
--    LEFT JOIN prices y ON week = hour
    WHERE week < trunc(CURRENT_DATE, 'week')
    GROUP BY week --,currency
),

finalis2 as (
SELECT
trunc(block_timestamp,'week') as week,
tx_id,
buyer,
currency,
price,
--price*avg(price_usd) as volume,
nft_collection
from flow.nft.ez_nft_sales x
where tx_succeeded='true'

union 
 
SELECT
trunc(x.block_timestamp,'week') as week,
x.tx_id,
z.authorizers[0] as buyer,
x.event_data:salePaymentVaultType as currency,
x.event_data:salePrice as price,
x.event_data:nftType as nft_collection
from flow.core.fact_events x
join flow.core.fact_transactions z on x.tx_id=z.tx_id
where x.event_contract='A.4eb8a10cb9f87357.NFTStorefrontV2'
and x.event_type='ListingCompleted' --and event_data:customID='flowverse-nft-marketplace'
and x.event_data:purchased='true'


 ),

final2 AS (
    SELECT
        week,
        COUNT(DISTINCT tx_id) AS sales,
        COUNT(DISTINCT buyer) AS nft_buyers, --currency,
       -- CASE 
       --     WHEN currency ILIKE '%flow%' THEN SUM(price) * COALESCE(AVG(price_usd), 1)
       --     ELSE SUM(price)
       -- END AS volume,
        sum(price) as volume,
        sum(volume) over (order by week) as total_volume,
        COUNT(DISTINCT nft_collection) AS active_collections
    FROM finalis2 x
    --LEFT JOIN prices y ON week = hour
    WHERE week < trunc(CURRENT_DATE, 'week') -1
    GROUP BY week --,currency
),


final_week as (select * from final order by 1 desc limit 1),
final_past_week as (select * from final2 order by 1 desc limit 1)
select 
final.*,concat(final.sales,' (',final.sales-final2.sales,')') as sales_diff, ((final.sales-final2.sales)/final2.sales)*100 as pcg_diff_sales,
 concat(final.volume,' (',final.volume-final2.volume,')') as vol_diff, ((final.volume-final2.volume)/final2.volume)*100 as pcg_diff_vol

from final_week as final join final_past_week as final2
where final.week<trunc(current_date,'week')

"""
SQL_NFT_SALES_OVER_TIME = """with
prices as (
select
trunc(hour,'week') as hour,
token_address as token_contract,
avg(price) as price_usd
from flow.price.ez_prices_hourly
group by 1,2
),
opensea_data AS (
    SELECT 
        tx_hash, 
        from_address, 
        block_timestamp, 
        value AS price
    FROM flow.core_evm.fact_transactions 
    WHERE to_address='0x0000000000000068f116a894984e2db1123eb395' 
        AND tx_succeeded='TRUE'
),
opensea_event_data AS (
    SELECT 
        tx_hash, 
        CASE
      WHEN LTRIM(SUBSTR(data,3), '0') = '' THEN NULL
      ELSE
        /* use TRY_TO_DECIMAL so bad formats become NULL rather than bomb out */
        TRY_TO_DECIMAL(
          LTRIM(SUBSTR(data, 3), '0'),
          REPEAT(
            'X',
            GREATEST(LENGTH(LTRIM(SUBSTR(data,3), '0')), 1)
          )
        ) / POW(10,18)
    END AS event_price
    FROM flow.core_evm.fact_event_logs 
    WHERE topic_0='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
        AND contract_address='0xd3bf53dac106a0290b0483ecbc89d40fcc961f3e'
),
finalis as (
-- major cadence 
SELECT
trunc(block_timestamp,'week') as week,
tx_id,
buyer,
currency,
price,
--price*avg(price_usd) as volume,
nft_collection
from flow.nft.ez_nft_sales x
where tx_succeeded='true' 

union 

-- other cadence
SELECT
trunc(x.block_timestamp,'week') as week,
x.tx_id,
z.authorizers[0] as buyer,
x.event_data:salePaymentVaultType as currency,
x.event_data:salePrice as price,
x.event_data:nftType as nft_collection
from flow.core.fact_events x
join flow.core.fact_transactions z on x.tx_id=z.tx_id
where x.event_contract='A.4eb8a10cb9f87357.NFTStorefrontV2'
and x.event_type='ListingCompleted' --and event_data:customID='flowverse-nft-marketplace'
and x.event_data:purchased='true'

union

-- beezie
select trunc(x.block_timestamp,'week') as week,
x.tx_hash,
x.topics[2] as buyer,
'a.b19436aae4d94622.fiattoken' as currency,
CASE
      WHEN LTRIM(SUBSTR(y.data,3), '0') = '' THEN NULL
      ELSE
        /* use TRY_TO_DECIMAL so bad formats become NULL rather than bomb out */
        TRY_TO_DECIMAL(
          LTRIM(SUBSTR(y.data, 3), '0'),
          REPEAT(
            'X',
            GREATEST(LENGTH(LTRIM(SUBSTR(y.data,3), '0')), 1)
          )
        ) / POW(10,6)
    END as price,
x.origin_from_address as collection
from flow.core_evm.fact_event_logs x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash
where x.origin_function_signature in ('0x052eb226','0x09c56431') and x.contract_address='0xd112634f06902a977db1d596c77715d72f8da8a9' and x.tx_succeeded='TRUE'
and x.data='0x' and x.event_index=12 and y.topics[0]='0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'

union
--mintify
select trunc(x.block_timestamp,'week') as week,
y.tx_hash,
y.origin_from_address as buyer,
'A.1654653399040a61.FlowToken' as currency,
x.value as price,
y.contract_address as collection
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash
where x.origin_function_signature in ('0xe7acab24','0x87201b41') and lower(x.to_address)=lower('0x00000003cf2c206e1fdA7fd032b2f9bdE12Ec6Cc') and x.tx_succeeded=TRUE
and y.origin_function_signature in ('0xe7acab24','0x87201b41') and lower(y.origin_to_address)=lower('0x00000003cf2c206e1fdA7fd032b2f9bdE12Ec6Cc') and y.tx_succeeded='TRUE' and data='0x'

union
-- opensea

SELECT 
    trunc(opensea_data.block_timestamp,'week') as week, 
    opensea_data.tx_hash,
    opensea_data.from_address as buyer,
    'A.1654653399040a61.FlowToken' as currency, 
    COALESCE(NULLIF(opensea_data.price, 0), opensea_event_data.event_price) AS price,
    NULL as collection
FROM opensea_data
LEFT JOIN opensea_event_data 
ON opensea_data.tx_hash = opensea_event_data.tx_hash
having price is not null

 )
SELECT
distinct week,
count(distinct tx_id) as sales,
sum(sales) over (order by week) as total_sales,
count(distinct buyer) as nft_buyers,
sum(price)*avg(price_usd) as volume,
sum(volume) over (order by week) as total_volume,
count(distinct nft_collection) as active_collections
from finalis x
left join prices y on week=hour
and x.currency ilike y.token_contract
where week<trunc(current_date,'week')
group by 1 order by 1 desc  
"""

# Contracts
SQL_CONTRACTS_NUMBERS = """WITH core_news AS (
    SELECT DISTINCT event_contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core.fact_events
    GROUP BY 1
),
evm_news AS (
    SELECT DISTINCT contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM (
select x.block_timestamp, x.from_address as creator,y.contract_address as contract 
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
UNION
select x.block_timestamp, x.from_address as creator, x.tx_hash as contract 
from flow.core_evm.fact_transactions x
where (x.origin_function_signature='0x60c06040' or x.origin_function_signature='0x60806040') and tx_hash not in (select x.tx_hash 
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%')
)
    GROUP BY 1
),
combined_news AS (
    SELECT new_contract, debut FROM core_news
    UNION ALL
    SELECT new_contract, debut FROM evm_news
),
tots as (
select count(distinct new_contract) as total_contracts from combined_news
),
active_contracts AS (
    SELECT
        trunc(x.block_timestamp, 'week') AS date,
        COUNT(DISTINCT event_contract) AS active_contracts
    FROM flow.core.fact_events x
    WHERE x.tx_succeeded = 'true'
    GROUP BY 1
    UNION ALL
    SELECT
        trunc(y.block_timestamp, 'week') AS date,
        COUNT(DISTINCT contract_address) AS active_contracts
    FROM flow.core_evm.fact_event_logs y
    GROUP BY 1
),
current_week_active AS (
    SELECT date, sum(active_contracts) as active_contracts
    FROM active_contracts
    WHERE date = trunc(current_date, 'week') - INTERVAL '1 WEEK' group by 1
),
previous_week_active AS (
    SELECT date, sum(active_contracts) as active_contracts
    FROM active_contracts
    WHERE date = trunc(current_date, 'week') - INTERVAL '2 WEEKS' group by 1
),
current_week_stats AS (
    SELECT 
        trunc(current_date, 'week') - INTERVAL '1 WEEK' AS date,
        COUNT(DISTINCT new_contract) AS new_contractss
    FROM combined_news
    WHERE debut >= trunc(current_date, 'week') - INTERVAL '1 WEEK'
        AND debut < trunc(current_date, 'week') group by 1
),
previous_week_stats AS (
    SELECT 
        trunc(current_date, 'week') - INTERVAL '2 WEEKS' AS date,
        COUNT(DISTINCT new_contract) AS new_contractss
    FROM combined_news
    WHERE debut >= trunc(current_date, 'week') - INTERVAL '2 WEEKS'
        AND debut < trunc(current_date, 'week') - INTERVAL '1 WEEK' group by 1
)

SELECT 
    cw.date AS current_week,
    COALESCE(cw.active_contracts, 0) AS active_contracts,
    COALESCE(pw.active_contracts, 0) AS previous_week_active_contracts,
    COALESCE(pwa.new_contractss, 0) AS new_contracts,
    total_contracts,
    COALESCE(pws.new_contractss, 0) AS previous_week_new_contracts,
    COALESCE((cw.active_contracts - pw.active_contracts) / NULLIF(pw.active_contracts, 0) * 100, 0) AS pct_diff
FROM current_week_active cw
FULL OUTER JOIN previous_week_active pw ON cw.date-INTERVAL '1 WEEK' = pw.date
FULL OUTER JOIN current_week_stats pwa ON cw.date = pwa.date
FULL OUTER JOIN previous_week_stats pws ON cw.date-INTERVAL '1 WEEK' = pws.date
join tots
WHERE cw.date >= '2024-01-01'
ORDER BY current_week DESC"""
SQL_CONTRACTS_ACTIVE_OVER_TIME = """WITH news AS (
    SELECT DISTINCT event_contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core.fact_events
    GROUP BY 1
),
evm_news AS (
    SELECT DISTINCT contract_address AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core_evm.fact_event_logs
    GROUP BY 1
),
combined_news AS (
    SELECT new_contract, debut FROM news
    UNION ALL
    SELECT new_contract, debut FROM evm_news
),
final as (
SELECT
    trunc(x.block_timestamp, 'week') AS date,
    COUNT(DISTINCT x.event_contract) AS active_contracts
FROM flow.core.fact_events x
WHERE x.tx_succeeded = 'true' and date<trunc(current_date,'week')
GROUP BY 1

UNION ALL

SELECT
    trunc(y.block_timestamp, 'week') AS date,
    COUNT(DISTINCT y.contract_address) AS active_contracts
FROM flow.core_evm.fact_event_logs y where date>'2020-01-01' and date<trunc(current_date,'week')
GROUP BY 1
ORDER BY date ASC
)
select date, sum(active_contracts) as active_contracts from final group by 1 order by 1 desc """
SQL_CONTRACTS_NEW_OVER_TIME = """WITH core_news AS (
    SELECT DISTINCT event_contract AS new_contract,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM flow.core.fact_events
    GROUP BY 1
),
evm_news AS (
    SELECT DISTINCT contract AS new_contract, creator,
        MIN(trunc(block_timestamp, 'week')) AS debut
    FROM (
select x.block_timestamp, x.from_address as creator,y.contract_address as contract 
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%'
UNION
select x.block_timestamp, x.from_address as creator, x.tx_hash as contract 
from flow.core_evm.fact_transactions x
where (x.origin_function_signature='0x60c06040' or x.origin_function_signature='0x60806040') and tx_hash not in (select x.tx_hash 
from flow.core_evm.fact_transactions x
join flow.core_evm.fact_event_logs y on x.tx_hash=y.tx_hash 
where y.topics[0] ilike '%0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0%')
)
    GROUP BY 1,2
),
combined_news AS (
    SELECT new_contract, debut, 'Cadence' as source FROM core_news
    UNION ALL
    SELECT new_contract, debut, CASE WHEN creator LIKE '0x0000000000000000000000020000000000000000%' THEN 'COA EVM Contract' else 'Non-COA EVM Contract' end as source FROM evm_news
)
SELECT
    debut AS date, source,
    COUNT(DISTINCT new_contract) AS new_contracts,
    SUM(COUNT(DISTINCT new_contract)) OVER (partition by source ORDER BY debut) AS unique_contracts
FROM combined_news where date>'2020-01-01' and date<trunc(current_date,'week')
GROUP BY debut, source
ORDER BY debut ASC"""

# ---------- UI ----------
st.sidebar.title("Flow Weekly Stats")
ttl = st.sidebar.slider("Cache TTL (s)", 60, 900, 300, step=30)
st.sidebar.caption(f"Now: {now_local()}")
# allow canviar TTL en calent
run_query.clear()
run_query = st.cache_data(ttl=ttl)(run_query.func)

st.title("🟩 Flow Weekly Stats — Dashboard")

# Last data week
try:
    lastwk = run_query(SQL_LAST_DATA_WEEK)
    last_week = pd.to_datetime(lastwk.iloc[0,0]).date() if not lastwk.empty else None
    st.info(f"Last data week available: **{last_week}** (weeks are ISO-truncated).")
except Exception as e:
    st.warning(f"No 'last data week' info: {e}")

tabs = st.tabs([
    "Overview", "Transactions", "Staking",
    "Accounts", "NFTs", "Contracts",
    "Prices & Tokens", "Conclusions"
])

# ---------- Overview ----------
with tabs[0]:
    col1,col2,col3,col4 = st.columns(4)
    try:
        tx_over = run_query(SQL_TX_OVER_TIME)
        if not tx_over.empty:
            tx_over = tx_over.sort_values("WEEK")
            latest = tx_over.tail(1)
            prev = tx_over.tail(2).head(1)
            col1.metric("Weekly transactions", int(latest["TOTAL_TRANSACTIONS"].iloc[0]),
                        delta=f'{int(latest["TOTAL_TRANSACTIONS"].iloc[0] - prev["TOTAL_TRANSACTIONS"].iloc[0]):+}')
            col2.metric("WoW %", f"{latest['PCG_DIFF'].iloc[0]:.2f}%")
    except Exception as e:
        st.warning(f"Overview transactions failed: {e}")

    try:
        fee = run_query(SQL_AVG_TX_FEE_WEEKLY)
        if not fee.empty:
            col3.metric("Avg tx fee (FLOW)", f"{fee.tail(1)['AVG_TX_FEE_FLOW'].iloc[0]:.6f}")
            col4.metric("Avg tx fee (USD)", f"${fee.tail(1)['AVG_TX_FEE_USD'].iloc[0]:.6f}")
    except Exception as e:
        st.warning(f"Overview fees failed: {e}")

    st.markdown("### Weekly Transactions (trend)")
    if 'tx_over' in locals() and not tx_over.empty:
        chart = alt.Chart(tx_over).mark_line().encode(
            x="WEEK:T", y="TOTAL_TRANSACTIONS:Q",
            tooltip=["WEEK","TOTAL_TRANSACTIONS","PCG_DIFF"]
        )
        st.altair_chart(chart, use_container_width=True)
        download_btn(tx_over, "⬇️ Download transactions (CSV)", "transactions_weekly.csv")

# ---------- Transactions ----------
with tabs[1]:
    st.subheader("Numbers (last two weeks by type)")
    df1 = run_query(SQL_TX_NUMBERS)
    st.dataframe(df1, use_container_width=True)
    download_btn(df1, "⬇️ Download table", "tx_numbers_last2.csv")

    st.subheader("Succeeded vs Failed (weekly)")
    df2 = run_query(SQL_TX_SUCCESS_FAIL)
    chart = alt.Chart(df2).mark_bar().encode(
        x="WEEK:T", y="TOTAL_TRANSACTIONS:Q", color="TYPE:N",
        tooltip=["WEEK","TYPE","TOTAL_TRANSACTIONS"]
    )
    st.altair_chart(chart, use_container_width=True)
    download_btn(df2, "⬇️ Download data", "tx_success_failed.csv")

    st.subheader("Cumulative transactions")
    df3 = run_query(SQL_TX_OVER_TIME).sort_values("WEEK")
    chart2 = alt.Chart(df3).mark_line().encode(
        x="WEEK:T", y="CUM_TRANSACTIONS:Q", tooltip=["WEEK","CUM_TRANSACTIONS"]
    )
    st.altair_chart(chart2, use_container_width=True)
    download_btn(df3, "⬇️ Download data", "tx_over_time.csv")

# ---------- Staking ----------
with tabs[2]:
    st.subheader("Staked vs Unstaked — Weekly")
    dfa = run_query(SQL_STAKED_OVER_TIME)
    if not dfa.empty:
        base = alt.Chart(dfa)
        st.altair_chart(base.mark_line().encode(x="DATE:T", y="STAKED_VOLUME:Q"), use_container_width=True)
        st.altair_chart(base.mark_line().encode(x="DATE:T", y="UNSTAKED_VOLUME:Q"), use_container_width=True)
        download_btn(dfa, "⬇️ Download data", "staking_over_time.csv")

    st.subheader("Unique stakers — Summary")
    dfb = run_query(SQL_STAKERS_SUMMARY)
    if not dfb.empty:
        a,b,c,d = st.columns(4)
        a.metric("Unique stakers", int(dfb["UNIQUE_STAKERS"].iloc[0]))
        b.metric("Prev week", int(dfb["UNIQUE_STAKERS_PAST_WEEK"].iloc[0]))
        c.metric("Δ users", int(dfb["USERS_DIFF"].iloc[0]))
        d.metric("WoW %", f"{dfb['PCG_DIFF'].iloc[0]:.2f}%")
        download_btn(dfb, "⬇️ Download summary", "staking_summary.csv")

# ---------- Accounts ----------
with tabs[3]:
    st.subheader("Users — numbers (totals & week)")
    u1 = run_query(SQL_USERS_NUMBERS)
    st.dataframe(u1, use_container_width=True)
    download_btn(u1, "⬇️ Download", "users_numbers.csv")

    st.subheader("Active & New users over time (weekly)")
    u2 = run_query(SQL_USERS_OVER_TIME)
    chart = alt.Chart(u2).transform_fold(
        ["ACTIVE_USERS","NEW_USERS"], as_=['metric','value']
    ).mark_line().encode(x="MONTH:T", y="value:Q", color="metric:N")
    st.altair_chart(chart, use_container_width=True)
    download_btn(u2, "⬇️ Download", "users_over_time.csv")

    st.subheader("Users by region (weekly snapshot)")
    u3 = run_query(SQL_USERS_BY_REGION)
    st.dataframe(u3, use_container_width=True)
    download_btn(u3, "⬇️ Download", "users_by_region.csv")

    st.subheader("Users by region over time")
    u4 = run_query(SQL_USERS_BY_REGION_OVER_TIME)
    st.dataframe(u4, use_container_width=True)
    download_btn(u4, "⬇️ Download", "users_by_region_time.csv")

# ---------- NFTs ----------
with tabs[4]:
    st.subheader("NFT sales — numbers (week vs prev)")
    n1 = run_query(SQL_NFT_SALES_NUMBERS)
    st.dataframe(n1, use_container_width=True)
    download_btn(n1, "⬇️ Download", "nft_sales_numbers.csv")

    st.subheader("NFT sales over time (weekly)")
    n2 = run_query(SQL_NFT_SALES_OVER_TIME)
    st.dataframe(n2, use_container_width=True)
    download_btn(n2, "⬇️ Download", "nft_sales_over_time.csv")

# ---------- Contracts ----------
with tabs[5]:
    st.subheader("Contracts — numbers (week vs prev)")
    c1 = run_query(SQL_CONTRACTS_NUMBERS)
    st.dataframe(c1, use_container_width=True)
    download_btn(c1, "⬇️ Download", "contracts_numbers.csv")

    colA, colB = st.columns(2)
    with colA:
        st.markdown("**Active contracts over time**")
        c2 = run_query(SQL_CONTRACTS_ACTIVE_OVER_TIME)
        st.dataframe(c2, use_container_width=True)
        download_btn(c2, "⬇️ Download", "contracts_active_over_time.csv")
    with colB:
        st.markdown("**New contracts by source (weekly)**")
        c3 = run_query(SQL_CONTRACTS_NEW_OVER_TIME)
        st.dataframe(c3, use_container_width=True)
        download_btn(c3, "⬇️ Download", "contracts_new_over_time.csv")

# ---------- Prices & Tokens ----------
with tabs[6]:
    st.subheader("FLOW price — last week")
    p1 = run_query(SQL_FLOW_PRICE_WEEK).sort_values("RECORDED_HOUR")
    st.altair_chart(alt.Chart(p1).mark_line().encode(x="RECORDED_HOUR:T", y="FLOW_PRICE:Q"), use_container_width=True)
    download_btn(p1, "⬇️ Download", "flow_price_week.csv")

    st.subheader("Top tokens weekly movers")
    p2 = run_query(SQL_TOKENS_WEEKLY_MOVERS)
    st.dataframe(p2.head(50), use_container_width=True)
    download_btn(p2, "⬇️ Download", "tokens_weekly_movers.csv")

# ---------- Conclusions ----------
with tabs[7]:
    st.subheader("Conclusions")
    st.write(
        "- Resume aquí highlights (WoW diffs, picos NFT, variacions de preu, etc.).\n"
        "- Si vols, hi afegim un generador d’insights automàtics."
    )

st.caption(f"Last updated: {now_local()}  •  Cache TTL: {ttl}s")
