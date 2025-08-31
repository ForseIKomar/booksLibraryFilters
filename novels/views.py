# views.py
from django.shortcuts import render
from django.http import JsonResponse
from django.core.paginator import Paginator  # используем только для удобства total_pages, но считаем count SQL-ом
from django.db import connections
from django.utils.timezone import now

PAGE_SIZE = 100

def _fetchall_dict(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def filter_books(request):
    # --- входные параметры ---
    tags            = request.GET.getlist("tag")
    min_chapters    = request.GET.get("min_chapters")
    max_chapters    = request.GET.get("max_chapters")
    min_rating      = request.GET.get("min_rating")
    max_rating      = request.GET.get("max_rating")
    sort_by         = request.GET.get("sort_by", "")
    sort_dir        = request.GET.get("sort_dir", "desc")
    page            = int(request.GET.get("page", 1))
    exclude_tags    = request.GET.getlist("exclude_tag")
    exclude_clusters= request.GET.getlist("exclude_cluster")
    site           = request.GET.getlist("site")
    title_query     = request.GET.get("title", "")
    created_from    = request.GET.get("created_from")
    created_to      = request.GET.get("created_to")
    updated_from    = request.GET.get("updated_from")
    updated_to      = request.GET.get("updated_to")
    #is_chinese      = request.GET.get("is_chinese")  # "yes"/"no"/None

    # --- маппинг сортировки (белый список) ---
    sort_map = {
        "chapters":        "b.free_chapters",
        "bookName":        'b."name"',
        "score":           "b.score",
        "weighted_score":  "weighted_score",
        "created_at":      "b.created_at",
        "updated_at":      "b.updated_at",
        "freshness_score": "freshness_score",
    }
    order_sql = sort_map.get(sort_by, "freshness_score")  # по умолчанию свежесть
    order_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    base_select = """
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
    print(site)
    # --- динамические фильтры ---
    where_clauses = []
    params = []

    # is_chinese
    #if is_chinese == "yes":
    #     where_clauses.append("b.is_chinese = TRUE")
    #elif is_chinese == "no":
    #    where_clauses.append("b.is_chinese = FALSE")

    # exclude_tags: NOT EXISTS на каждый тег (iexact)
    tags = request.GET.getlist("tag")
    if tags:
        ph = ", ".join(["%s"] * len(tags))
        where_clauses.append(f"""
            EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(bt.tags) AS t(val)
                WHERE val IN ({ph})
            )
        """)
        params.extend(tags)

    exclude_tags = request.GET.getlist("exclude_tag")
    if exclude_tags:
        ph = ", ".join(["%s"] * len(exclude_tags))
        where_clauses.append(f"""
            NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(bt.tags) AS t(val)
                WHERE val IN ({ph})
            )
        """)
        params.extend(exclude_tags)

    genres = request.GET.getlist("genre")
    if genres:
        ph = ", ".join(["%s"] * len(genres))
        where_clauses.append(f"""
            EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(bt.genres) AS t(val)
                WHERE val IN ({ph})
            )
        """)
        params.extend(genres)

    exclude_genres = request.GET.getlist("exclude_genre")
    if exclude_genres:
        ph = ", ".join(["%s"] * len(exclude_genres))
        where_clauses.append(f"""
            NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(bt.genres) AS t(val)
                WHERE val IN ({ph})
            )
        """)
        params.extend(exclude_genres)

    fandoms = request.GET.getlist("fandom")
    if fandoms:
        ph = ", ".join(["%s"] * len(fandoms))
        where_clauses.append(f"""
            EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(bt.fandoms) AS t(val)
                WHERE val IN ({ph})
            )
        """)
        params.extend(fandoms)

    exclude_fandoms = request.GET.getlist("exclude_fandom")
    if exclude_fandoms:
        ph = ", ".join(["%s"] * len(exclude_fandoms))
        where_clauses.append(f"""
            NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(bt.fandoms) AS t(val)
                WHERE val IN ({ph})
            )
        """)
        params.extend(exclude_fandoms)

    if site:
        placeholders = ", ".join(["%s"] * len(site))
        where_clauses.append(f"b.site IN ({placeholders})")
        params.extend(site)

    if title_query:
        where_clauses.append("b.name ILIKE %s")
        params.append(f"%{title_query}%")

    if min_rating:
        where_clauses.append("b.score >= %s")
        params.append(float(str(min_rating).replace(",", ".")))
    if max_rating:
        where_clauses.append("b.score <= %s")
        params.append(float(str(max_rating).replace(",", ".")))

    if min_chapters:
        where_clauses.append("b.free_chapters >= %s")
        params.append(int(min_chapters))
    if max_chapters:
        where_clauses.append("b.free_chapters <= %s")
        params.append(int(max_chapters))

    if created_from:
        where_clauses.append("coalesce(b.created_at, '2000-01-01') >= %s")
        params.append(created_from)
    if created_to:
        where_clauses.append("coalesce(b.created_at, '2000-01-01') <= %s")
        params.append(created_to)
    if updated_from:
        where_clauses.append("coalesce(b.created_at, b.created_at) >= %s")
        params.append(updated_from)
    if updated_to:
        where_clauses.append("coalesce(b.created_at, b.created_at) <= %s")
        params.append(updated_to)

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    # --- COUNT(*) для пагинации ---
    count_sql = f"SELECT COUNT(*) FROM ({base_select}{where_sql}) AS q"
    # --- выборка страницы ---
    offset = (page - 1) * PAGE_SIZE
    data_sql = f"""
        {base_select}
        {where_sql}
        ORDER BY {order_sql} {order_dir}, b.book_id DESC
        LIMIT %s OFFSET %s
    """

    # --- запросы ---
    with connections["pgsql"].cursor() as cur:
        # total count
        cur.execute(count_sql, params)
        total_count = cur.fetchone()[0]
        total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)

        # page data
        cur.execute(data_sql, params + [PAGE_SIZE, offset])
        rows = _fetchall_dict(cur)

        # теги (с количеством книг, по убыванию)
        cur.execute("""
            SELECT 
                e->>'value'					name,
                count(distinct bt.bookid)	book_count
            FROM nrml.book_tags bt
            CROSS JOIN LATERAL jsonb_array_elements(bt.attr) AS e
            WHERE e ? 'value' and e->>'type' in ('Теги', 'tag')
            group by 1
            order by 2 desc
        """)
        tags_stat = _fetchall_dict(cur)

        cur.execute("""
            select 
                url name
            from 
                sites s 
        """)
        sites_stats = _fetchall_dict(cur)

        # теги (с количеством книг, по убыванию)
        cur.execute("""
            SELECT 
                e->>'value'					name,
                count(distinct bt.bookid)	book_count
            FROM nrml.book_tags bt
            CROSS JOIN LATERAL jsonb_array_elements(bt.attr) AS e
            WHERE e ? 'value' and e->>'type' in ('Жанры')
            group by 1
            order by 2 desc
        """)
        genres_stat = _fetchall_dict(cur)

        # теги (с количеством книг, по убыванию)
        cur.execute("""
            SELECT 
                e->>'value'					name,
                count(distinct bt.bookid)	book_count
            FROM nrml.book_tags bt
            CROSS JOIN LATERAL jsonb_array_elements(bt.attr) AS e
            WHERE e ? 'value' and e->>'type' in ('Фэндомы')
            group by 1
            order by 2 desc
        """)
        fandoms_stat = _fetchall_dict(cur)

    # --- подготовка JSON данных для фронта ---
    books_data = []
    for r in rows:
        books_data.append({
            "bookId": r["book_id"],
            "bookName": r["book_name"],
            "description": r["description"],
            "site": r["site"],
            "authorName": r["author_name"],
            "url": r["url"],
            "cover_url": r["picture_url"],
            "score": (str(r["score"]).replace(".", ",")) if r["score"] is not None else None,
            "free": r["free"],
            "chapters": r["chapters"],
            "tags": r.get("tags", []) or [],
            "genres": r.get("genres", []) or [],
            "fandoms": r.get("fandoms", []) or [],
            "clusterLabel": r.get("cluster_labels", []) or [],
            # при желании можно отдать и вычисляемые поля
            "weighted_score": r.get("weighted_score"),
            "freshness_score": r.get("freshness_score"),
        })

    # --- AJAX ответ ---
    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({"books": books_data, "total_pages": total_pages}, safe=False)

    # --- Рендер страницы ---
    # Преобразуем статистику тегов/кластеров в удобный формат для шаблона
    return render(request, "novels/index.html", {
        "books": books_data,                 # вместо page_obj — список словарей
        "tags": tags_stat,                   # [{name, book_count}, ...]
        "genres": genres_stat,                   # [{name, book_count}, ...]
        "fandoms": fandoms_stat,                   # [{name, book_count}, ...]
        "sites": sites_stats,                   # [{name, book_count}, ...]
        "page_number": page,
        "total_pages": total_pages,
    })
