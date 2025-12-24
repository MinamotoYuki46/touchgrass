WITH latest_day AS (
    SELECT MAX(local_date) AS day
    FROM screen_time
),
screen_time_today AS (
    SELECT
        SUM(minutes_spent) AS total_minutes
    FROM screen_time
    WHERE local_date = (SELECT day FROM latest_day)
),
latest_location AS (
    SELECT
        latitude,
        longitude
    FROM user_location
    ORDER BY resolved_utc DESC
    LIMIT 1
)
SELECT
    st.total_minutes,
    CASE
        WHEN st.total_minutes >= {{SCREEN_TIME_HIGH}}
            THEN 'high'
        ELSE 'normal'
    END AS screen_time_level,

    w.weather_category,
    (w.weather_category IN ('clear', 'cloudy')) AS weather_ok,

    ll.latitude  AS user_latitude,
    ll.longitude AS user_longitude,
    (ll.latitude IS NOT NULL AND ll.longitude IS NOT NULL) AS location_available
FROM screen_time_today st
CROSS JOIN weather w
CROSS JOIN latest_location ll;
