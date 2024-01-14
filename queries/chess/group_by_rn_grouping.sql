with groupings as (
    select
        score,
        count(*) as num_in_group
    from players
    group by all
),
    
rankings as materialized (
    select
        r.player_id,
        r.elo,
        r.score,
        random() as _rand,
        row_number() over (partition by r.score order by r.elo desc) as group_rank,
        g.num_in_group
    from players as r
    inner join groupings as g on
        r.score = g.score
),

result as (
    select
        l.player_id as l_player_id,
        r.player_id as r_player_id,
        prob_win(l.elo, r.elo) as pw,
        prob_draw(l.elo, r.elo) as pd,
        case
            when r_player_id is null then 1
            when l._rand < pw then 1
            when l._rand < pw + pd then 0.5
            else 0
        end as l_score_diff
    from rankings as l
    left join rankings as r on
        l.player_id != r.player_id and
        l.score = r.score and
        l.group_rank + ceil(l.num_in_group / 2) = r.group_rank
    where
        l.group_rank <= ceil(l.num_in_group / 2)
),

scores as (
    select
        l_player_id as player_id,
        l_score_diff as score_diff
    from result
    union all
    select
        r_player_id as player_id,
        1 - l_score_diff as score_diff
    from result
    where r_player_id is not null
)

select * from scores
