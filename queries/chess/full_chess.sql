explain analyze
with recursive players as (
    select
        generate_series as player_id,
        800 + random()*400 as elo,
        round(6 * random())/2 as score
    from generate_series(1, 200)
),

rounds as (
    select
        player_id,
        0 as score,
        0 as round
    from players

    union all

    select
        r.player_id,
        r.score + m.score_diff as score,
        r.round + 1 as round
    from rounds as r
    inner join (
        with rankings as materialized (
            select
                *,
                random() as _rand,
                row_number() over (partition by score order by elo desc) as group_rank,
                count(*) over (partition by score) as num_in_group
            from players
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
    ) as m on
        r.player_id = m.player_id and
        r.round < 6
)

select * from rounds
