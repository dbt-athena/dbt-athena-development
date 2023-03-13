{{ config(
  materialized='table',
  format='json'
) }}

SELECT cast(t.ts AS timestamp(3)) AS ts
FROM
    unnest(
        sequence(
            current_date - INTERVAL '1' DAY,
            cast(REPLACE(cast(current_timestamp as varchar), ' UTC', '') as timestamp(3)),
            INTERVAL '1' MINUTE
        )
    ) AS t(ts)
