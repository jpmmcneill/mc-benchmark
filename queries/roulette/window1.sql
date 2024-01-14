-- slower
explain analyze
with setup as (
    select
        samples.generate_series as sample,
        turns.generate_series as turn,
        1000 as value,
    from generate_series(1, 1000) as samples
    cross join generate_series(1, 1000) as turns
),

randoms as (
    select
        *,
        case when floor(random() * (37)) >= 19 then 10 else -10 end as value_diff,
    from setup
),

calc as (
    select
        sample,
        turn,
        value + sum(value_diff) over (partition by sample order by turn) as value
    from randoms
)

select * from calc
;
