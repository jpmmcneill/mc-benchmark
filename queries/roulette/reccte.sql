-- this one is pretty quick!
explain analyze
with recursive casino as (
    select
        cast(generate_series as int) as sample,
        cast(1000 as int) as value,
        cast(0 as int) as turn
    from generate_series(1, 1000)
    union all
    select
        casino.sample,
        case
            when casino.value = 0 then 0
        else casino.value + (
            case
                when floor(random() * (37)) >= 19 then 10
                else -10
            end
        )
        end as value,
        casino.turn + 1 as turn
    from casino
    where value >= 0 and casino.turn < 100
)

select * from casino
;
