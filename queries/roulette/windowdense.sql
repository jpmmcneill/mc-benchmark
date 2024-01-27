-- slowest
explain analyze
select
    samples.generate_series as sample,
    turns.generate_series as turn,
    1000 + sum(
        -- this doesn't account for negative values
        -- but that doesn't matter for speed purposes
        -- as that would be one extra window function!
        case
            when floor(random() * (37)) >= 19
            then 10 else -10 end
    ) over (
        partition by sample order by turn
    ) as value
from generate_series(1, 1000) as samples
cross join generate_series(1, 100) as turns
