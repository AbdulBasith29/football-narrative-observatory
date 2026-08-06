# Data Quality Strategy (dbt Invariants)

To ensure the platform prevents analytically plausible but incorrect SQL, we enforce strict grain and additivity tests via dbt. These tests mathematically guarantee the boundaries defined in our warehouse design.

## 1. Grain and Uniqueness Tests

### Target-Opinion Uniqueness
Ensures the target opinion table is strictly unique per resolved comment-target pair and model version.
```sql
-- tests/assert_unique_comment_target_model.sql
select
    comment_target_key,
    model_version_key,
    count(*) as row_count
from {{ ref('fact_comment_target_opinion') }}
group by
    comment_target_key,
    model_version_key
having count(*) > 1
```

### Bridge Uniqueness
Ensures a comment cannot resolve to the same target entity multiple times.
```sql
-- tests/assert_unique_comment_target_bridge.sql
select
    comment_key,
    target_entity_key,
    count(*) as row_count
from {{ ref('bridge_comment_target') }}
group by
    comment_key,
    target_entity_key
having count(*) > 1
```

## 2. Decomposition Additivity

Ensures the symmetric decomposition exactly balances out to the total sentiment shift for the shared strata.
```sql
-- tests/assert_symmetric_decomposition_additivity.sql
with validation as (
    select
        player_key,
        event_key,
        baseline_window_key,
        comparison_window_key,
        total_sentiment_shift,
        symmetric_tone_effect,
        symmetric_mix_effect,
        abs(
            symmetric_tone_effect
            + symmetric_mix_effect
            - total_sentiment_shift
        ) as residual
    from {{ ref('mart_channel_shift_decomposition') }}
    where decomposition_status = 'VALID'
)

select *
from validation
where residual > 0.0001
```

## 3. Grain Fan-Out Test

Verifies that assigning likes across targets via fractional attribution does not create or destroy total engagement.
```sql
-- tests/assert_fractional_engagement_conservation.sql
with comment_engagement as (
    select
        sum(like_count) as source_likes
    from {{ ref('fact_comment') }}
    where is_in_analytical_sample
),

allocated_engagement as (
    select
        sum(allocated_like_count) as allocated_likes
    from {{ ref('mart_comment_target_engagement_fractional') }}
)

select
    source_likes,
    allocated_likes,
    abs(source_likes - allocated_likes) as residual
from comment_engagement
cross join allocated_engagement
where abs(source_likes - allocated_likes) > 0.0001
```
*(Note: production SQL should apply this only to universes of comments with at least one resolved target.)*

## 4. Sample Isolation Tests

Ensures auxiliary algorithmic-surfacing comments don't leak into main event analysis.
```sql
-- No relevance observations in primary analytical marts
select *
from {{ ref('mart_target_opinion_daily') }}
where sample_type <> 'CHRONOLOGICAL'
```

Ensures multiple API retrievals do not duplicate canonical comments.
```sql
-- No duplicated canonical comments caused by repeated retrievals
select
    source_comment_id,
    count(*)
from {{ ref('fact_comment') }}
group by source_comment_id
having count(*) > 1
```

Ensures opinions never exist without a valid target bridge.
```sql
-- Every target opinion resolves through the bridge
select o.comment_target_key
from {{ ref('fact_comment_target_opinion') }} o
left join {{ ref('bridge_comment_target') }} b
    on o.comment_target_key = b.comment_target_key
where b.comment_target_key is null
```

## 5. Watermark Completeness Tests

Ensures incomplete scans are never presented as complete.
```sql
-- Identify capped runs
select
    video_key,
    ingestion_run_id
from {{ ref('fact_video_ingestion_run') }}
where comments_retrieved > configured_comment_cap
   or pages_retrieved > configured_page_cap
```

```sql
-- Ensure capped scans aren't falsely labelled complete
select *
from {{ ref('fact_video_ingestion_run') }}
where scan_status = 'COMPLETE_TO_WATERMARK'
  and watermark_reached = false
```
