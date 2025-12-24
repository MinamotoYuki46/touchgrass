SELECT
    location_name,
    location_category,
    address,
    distance_km,
    crowd_level,
    google_maps_link,

    (
        /* Crowd score */
        CASE crowd_level
            WHEN 'low' THEN 3
            WHEN 'medium' THEN 2
            ELSE 0
        END * 0.4

        +

        /* Distance score (behavior-aware) */
        CASE
            WHEN distance_km BETWEEN {{IDEAL_MIN_KM}} AND {{IDEAL_MAX_KM}} THEN 3
            WHEN distance_km <= {{MAX_DISTANCE_KM}} THEN 2
            ELSE 0
        END * 0.35

        +

        /* Activity relevance */
        CASE
            WHEN location_category IN ('park', 'outdoor') THEN 3
            WHEN location_category = 'sports' THEN 2
            ELSE 0
        END * 0.25
    ) AS priority_score

FROM places_enriched
WHERE
    crowd_level IN ('low', 'medium')
    AND distance_km BETWEEN {{MIN_DISTANCE_KM}} AND {{MAX_DISTANCE_KM}}

ORDER BY priority_score DESC
LIMIT {{MAX_RESULTS}};
