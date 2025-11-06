# views.py
from django.shortcuts import render
from django.http import JsonResponse
from django.db import connections
from django.views.decorators.csrf import csrf_exempt
from django.utils.timezone import localtime
import json
from novels.utils.pg_utils import create_book_action, get_books_action_stats
from novels.utils.db_utils import SELECT_BOOKS, SELECT_TAGS, SELECT_GENRES, SELECT_FANDOMS

PAGE_SIZE = 100


def _fetchall_dict(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def format_ts(ts):
    """Форматирует timestamp в читаемый вид"""
    return localtime(ts).strftime("%Y-%m-%d %H:%M") if ts else None


def filter_books(request):
    # --- входные параметры ---
    tags = request.GET.getlist("tag")
    min_chapters = request.GET.get("min_chapters")
    max_chapters = request.GET.get("max_chapters")
    min_rating = request.GET.get("min_rating")
    max_rating = request.GET.get("max_rating")
    sort_by = request.GET.get("sort_by", "")
    sort_dir = request.GET.get("sort_dir", "desc")
    page = int(request.GET.get("page", 1))
    exclude_tags = request.GET.getlist("exclude_tag")
    exclude_clusters = request.GET.getlist("exclude_cluster")
    site = request.GET.getlist("site")
    title_query = request.GET.get("title", "")
    created_from = request.GET.get("created_from")
    created_to = request.GET.get("created_to")
    updated_from = request.GET.get("updated_from")
    updated_to = request.GET.get("updated_to")

    # --- маппинг сортировки ---
    sort_map = {
        "chapters": "b.free_chapters",
        "bookName": 'b."name"',
        "score": "b.score",
        "weighted_score": "weighted_score",
        "created_at": "b.created_at",
        "updated_at": "b.updated_at",
        "freshness_score": "freshness_score",
    }
    order_sql = sort_map.get(sort_by, "freshness_score")
    order_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    base_select = SELECT_BOOKS
    where_clauses = []
    params = []

    # --- динамические фильтры ---
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
    offset = (page - 1) * PAGE_SIZE
    data_sql = f"""
        {base_select}
        {where_sql}
        ORDER BY {order_sql} {order_dir}, b.book_id DESC
        LIMIT %s OFFSET %s
    """

    with connections["pgsql"].cursor() as cur:
        cur.execute(count_sql, params)
        total_count = cur.fetchone()[0]
        total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)

        cur.execute(data_sql, params + [PAGE_SIZE, offset])
        rows = _fetchall_dict(cur)

        cur.execute(SELECT_TAGS)
        tags_stat = _fetchall_dict(cur)

        cur.execute("SELECT url name FROM sites s")
        sites_stats = _fetchall_dict(cur)

        cur.execute(SELECT_GENRES)
        genres_stat = _fetchall_dict(cur)

        cur.execute(SELECT_FANDOMS)
        fandoms_stat = _fetchall_dict(cur)

    print('Filtering books 0')
    # --- добавляем статистику по действиям ---
    book_ids = [r["book_id"] for r in rows]
    actions_stats = get_books_action_stats(book_ids)

    print('Filtering books 1')
    books_data = []
    for r in rows:
        stats = actions_stats.get(str(r["book_id"]), {})
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
            "weighted_score": r.get("weighted_score"),
            "freshness_score": r.get("freshness_score"),
            "last_view": format_ts(stats.get("view")),
            "last_click": format_ts(stats.get("click")),
        })

    print('Filtering books 2')
    # --- AJAX ответ ---
    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({"books": books_data, "total_pages": total_pages}, safe=False)

    return render(request, "novels/index.html", {
        "books": books_data,
        "tags": tags_stat,
        "genres": genres_stat,
        "fandoms": fandoms_stat,
        "sites": sites_stats,
        "page_number": page,
        "total_pages": total_pages,
    })


@csrf_exempt
def api_book_view_batch(request):
    data = json.loads(request.body or "{}")
    print("📘 VIEW_BATCH:", data)
    for book_id in data.get("book_ids", []):
        create_book_action(book_id, "view")
    return JsonResponse({"status": "ok", "count": len(data.get("book_ids", []))})


@csrf_exempt
def api_book_click(request, book_id):
    print("🖱️ CLICK:", book_id)
    create_book_action(book_id, "click")
    return JsonResponse({"status": "ok", "book_id": book_id})
