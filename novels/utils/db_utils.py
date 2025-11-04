SELECT_BOOKS = """
    select 
        b.original_id book_id,
        b."name" book_name,
        b.site,
        b.description,
        b.author author_name,
        b.url url,
        b.picture_url,
        b.free_chapters free,
        b.score,
        b.chapters,
        b.created_at,
        b.updated_at,
        (b.score * (CASE WHEN (b.free_chapters + 1) > 0
                         THEN LN((b.free_chapters + 1)) / LN(2)
                         ELSE 0 END)
        )::float AS weighted_score,
        (EXTRACT(EPOCH FROM (NOW() - b.created_at)) / 86400.0)::float AS age_days,
        (EXTRACT(EPOCH FROM (NOW() - b.updated_at)) / 86400.0)::float AS updated_days,
        (
          ( (b.score * (CASE WHEN (b.free_chapters + 1) > 0
                              THEN LN((b.free_chapters + 1)) / LN(2)
                              ELSE 0 END)
             )
            / ( (EXTRACT(EPOCH FROM (NOW() - b.created_at)) / 86400.0) + 1 )
          ) * 1.5
          + 1.0 / ( (EXTRACT(EPOCH FROM (NOW() - b.updated_at)) / 86400.0) + 1 )
        )::float AS freshness_score,
        bt.tags,
        bt.genres,
        bt.fandoms
    from 
        nrml.books_v2 b
    left join (
    	select
    		ta.site,
    		ta.bookid,
            jsonb_agg(ta.name) filter (where ta.type in ('Теги', 'tag')) tags,
            jsonb_agg(ta.name) filter (where ta.type in ('Жанры')) genres,
            jsonb_agg(ta.name) filter (where ta.type in ('Фэндомы')) fandoms
    	from 
    		nrml.tags_association ta
    	join nrml.tags t on t.site = ta.site and t.name = ta.name and t.type = ta.type
        group by 1, 2
    ) bt on (
        bt.site = b.site 
        or bt.site = 'www.novels.com'
        ) and bt.bookid = b.original_id::text
"""

SELECT_TAGS = """
    SELECT 
        e->>'value'					name,
        count(distinct bt.bookid)	book_count
    FROM nrml.book_tags bt
    CROSS JOIN LATERAL jsonb_array_elements(bt.attr) AS e
    WHERE e ? 'value' and e->>'type' in ('Теги', 'tag')
    group by 1
    order by 2 desc
"""

SELECT_GENRES = """
    SELECT 
        e->>'value'					name,
        count(distinct bt.bookid)	book_count
    FROM nrml.book_tags bt
    CROSS JOIN LATERAL jsonb_array_elements(bt.attr) AS e
    WHERE e ? 'value' and e->>'type' in ('Жанры')
    group by 1
    order by 2 desc
"""

SELECT_FANDOMS = """
    SELECT 
        e->>'value'					name,
        count(distinct bt.bookid)	book_count
    FROM nrml.book_tags bt
    CROSS JOIN LATERAL jsonb_array_elements(bt.attr) AS e
    WHERE e ? 'value' and e->>'type' in ('Фэндомы')
    group by 1
    order by 2 desc
"""