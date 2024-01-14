with result as (
    select
        score,
        player_id as l_player_id,
        elo as l_elo,
        lead(player_id, ceil(num_in_group / 2)) over (partition by score order by elo desc) as r_player_id,
        lead(elo, ceil(num_in_group / 2)) over (partition by score order by elo desc) as r_elo,
        prob_win(l_elo, r_elo) as pw,
        prob_draw(l_elo, r_elo) as pd,
        case
            when r_player_id is null then 1
            when _rand < pw then 1
            when _rand < pw + pd then 0.5
            else 0
        end as l_score_diff
    from (
        select
            player_id,
            score,
            elo,
            random() as _rand
        from players
    )
    inner join (
        select
            score,
            count(*) as num_in_group
        from players
        group by score
    ) using (score)
    qualify row_number() over (partition by score order by elo desc) <= ceil(num_in_group / 2)
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
