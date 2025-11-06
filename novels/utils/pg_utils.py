# pg_utils.py
from django.db import connections
from .db_utils import *

PAGE_SIZE = 100


def _fetchall_dict(cur):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

SELECT_SITES = """
    SELECT url AS name
    FROM sites s
"""


# --- вспомогательная функция формирования SQL ---
def build_book_filter_sql(filters, sort_by, sort_dir, page):
    """
    Возвращает готовые SQL-запросы (count_sql, data_sql) и список параметров.
    """
    where_clauses = []
    params = []

    def add_clause(sql, values):
        if values:
            placeholders = ", ".join(["%s"] * len(values))
            where_clauses.append(sql.format(ph=placeholders))
            params.extend(values)

    # --- фильтры ---
    add_clause("""
        EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(bt.tags) AS t(val)
            WHERE val IN ({ph})
        )
    """, filters.get("tags"))

    add_clause("""
        NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(bt.tags) AS t(val)
            WHERE val IN ({ph})
        )
    """, filters.get("exclude_tags"))

    add_clause("""
        EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(bt.genres) AS t(val)
            WHERE val IN ({ph})
        )
    """, filters.get("genres"))

    add_clause("""
        NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(bt.genres) AS t(val)
            WHERE val IN ({ph})
        )
    """, filters.get("exclude_genres"))

    add_clause("""
        EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(bt.fandoms) AS t(val)
            WHERE val IN ({ph})
        )
    """, filters.get("fandoms"))

    add_clause("""
        NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(bt.fandoms) AS t(val)
            WHERE val IN ({ph})
        )
    """, filters.get("exclude_fandoms"))

    # --- одиночные фильтры ---
    if filters.get("site"):
        placeholders = ", ".join(["%s"] * len(filters["site"]))
        where_clauses.append(f"b.site IN ({placeholders})")
        params.extend(filters["site"])

    if filters.get("title_query"):
        where_clauses.append("b.name ILIKE %s")
        params.append(f"%{filters['title_query']}%")

    if filters.get("min_rating"):
        where_clauses.append("b.score >= %s")
        params.append(filters["min_rating"])
    if filters.get("max_rating"):
        where_clauses.append("b.score <= %s")
        params.append(filters["max_rating"])

    if filters.get("min_chapters"):
        where_clauses.append("b.free_chapters >= %s")
        params.append(filters["min_chapters"])
    if filters.get("max_chapters"):
        where_clauses.append("b.free_chapters <= %s")
        params.append(filters["max_chapters"])

    if filters.get("created_from"):
        where_clauses.append("COALESCE(b.created_at, '2000-01-01') >= %s")
        params.append(filters["created_from"])
    if filters.get("created_to"):
        where_clauses.append("COALESCE(b.created_at, '2000-01-01') <= %s")
        params.append(filters["created_to"])
    if filters.get("updated_from"):
        where_clauses.append("COALESCE(b.updated_at, b.created_at) >= %s")
        params.append(filters["updated_from"])
    if filters.get("updated_to"):
        where_clauses.append("COALESCE(b.updated_at, b.created_at) <= %s")
        params.append(filters["updated_to"])

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    # --- сортировка и пагинация ---
    offset = (page - 1) * PAGE_SIZE
    count_sql = f"SELECT COUNT(*) FROM ({SELECT_BOOKS}{where_sql}) AS q"
    data_sql = f"""
        {SELECT_BOOKS}
        {where_sql}
        ORDER BY {sort_by} {sort_dir}, b.book_id DESC
        LIMIT %s OFFSET %s
    """
    params_with_limit = params + [PAGE_SIZE, offset]

    return count_sql, data_sql, params, params_with_limit


# --- единая функция выполнения всех SQL ---
def fetch_books_and_stats(count_sql, data_sql, params, params_with_limit):
    with connections["pgsql"].cursor() as cur:
        cur.execute(count_sql, params)
        total_count = cur.fetchone()[0]
        total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)

        cur.execute(data_sql, params_with_limit)
        rows = _fetchall_dict(cur)

        cur.execute(SELECT_TAGS)
        tags_stat = _fetchall_dict(cur)
        cur.execute(SELECT_GENRES)
        genres_stat = _fetchall_dict(cur)
        cur.execute(SELECT_FANDOMS)
        fandoms_stat = _fetchall_dict(cur)
        cur.execute(SELECT_SITES)
        sites_stats = _fetchall_dict(cur)

    return rows, total_pages, tags_stat, genres_stat, fandoms_stat, sites_stats


def create_book_action(book_id, action_type):
    """
    Записывает действие (просмотр, клик и т.п.) в историю.
    """
    query = """
        INSERT INTO book_actions_history (book_id, action_type, created_at)
        VALUES (%s, %s, NOW());
    """
    with connections["pgsql"].cursor() as cur:
        cur.execute(query, (book_id, action_type))
    return True


def get_books_action_stats(book_ids):
    """
    Возвращает последние даты просмотра и клика для списка book_id.
    Возвращает словарь: {book_id: {"view": dt1, "click": dt2}}
    """
    if not book_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(book_ids))
    query = f"""
        SELECT 
            book_id::text,
            action_type,
            MAX(created_at) AS last_time
        FROM book_actions_history
        WHERE book_id IN ({placeholders}) and created_at < NOW() - interval '1 hours'
        GROUP BY book_id, action_type;
    """
    with connections["pgsql"].cursor() as cur:
        cur.execute(query, book_ids)
        rows = cur.fetchall()
    stats = {}
    for book_id, action_type, last_time in rows:
        if book_id not in stats:
            stats[book_id] = {}
        stats[book_id][action_type] = last_time
    return stats