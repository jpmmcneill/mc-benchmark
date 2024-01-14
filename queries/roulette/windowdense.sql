-- slowest
explain analyze
select
    samples.generate_series as sample,
    turns.generate_series as turn,
    1000 + sum(
        case when floor(random() * (37)) >= 19 then 10 else -10 end
    ) over (
        partition by sample order by turn
    ) as value
from generate_series(1, 1000) as samples
cross join generate_series(1, 10000) as turns
