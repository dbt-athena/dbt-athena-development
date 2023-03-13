{{ config(
  materialized='table',
  partitioned_by=['hour(ts)'],
  table_type='iceberg',
  table_properties={
    'vacuum_max_snapshot_age_seconds': '86400',
    'optimize_rewrite_delete_file_threshold': '2'
  }
) }}

SELECT cast(t.ts AS timestamp(6)) AS ts
FROM
    unnest(
        sequence(
            current_date - INTERVAL '1' DAY,
            cast(REPLACE(cast(current_timestamp as varchar), ' UTC', '') as timestamp(6)),
            INTERVAL '1' MINUTE
        )
    ) AS t(ts)
