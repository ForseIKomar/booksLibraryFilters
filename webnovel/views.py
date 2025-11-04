from django.shortcuts import render
from django.http import JsonResponse
from novels.utils.pg_utils import build_book_filter_sql, fetch_books_and_stats, PAGE_SIZE, create_book_action, \
    get_books_action_stats
from django.utils.timezone import now
from django.utils.timezone import localtime, make_aware, is_naive

def filter_books(request):
    # --- входные параметры ---
    filters = {
        "tags": request.GET.getlist("tag"),
        "exclude_tags": request.GET.getlist("exclude_tag"),
        "genres": request.GET.getlist("genre"),
        "exclude_genres": request.GET.getlist("exclude_genre"),
        "fandoms": request.GET.getlist("fandom"),
        "exclude_fandoms": request.GET.getlist("exclude_fandom"),
        "site": request.GET.getlist("site"),
        "title_query": request.GET.get("title", ""),
        "min_rating": request.GET.get("min_rating"),
        "max_rating": request.GET.get("max_rating"),
        "min_chapters": request.GET.get("min_chapters"),
        "max_chapters": request.GET.get("max_chapters"),
        "created_from": request.GET.get("created_from"),
        "created_to": request.GET.get("created_to"),
        "updated_from": request.GET.get("updated_from"),
        "updated_to": request.GET.get("updated_to"),
    }

    sort_map = {
        "chapters": "b.free_chapters",
        "bookName": 'b."name"',
        "score": "b.score",
        "weighted_score": "weighted_score",
        "created_at": "b.created_at",
        "updated_at": "b.updated_at",
        "freshness_score": "freshness_score",
    }
    sort_by = sort_map.get(request.GET.get("sort_by", ""), "freshness_score")
    sort_dir = "DESC" if request.GET.get("sort_dir", "desc").lower() == "desc" else "ASC"
    page = int(request.GET.get("page", 1))

    # --- сбор SQL и выполнение ---
    count_sql, data_sql, params, params_with_limit = build_book_filter_sql(filters, sort_by, sort_dir, page)
    rows, total_pages, tags_stat, genres_stat, fandoms_stat, sites_stats = fetch_books_and_stats(
        count_sql, data_sql, params, params_with_limit
    )

    # --- Получаем статистику по действиям (просмотр, клик)
    book_ids = [r["book_id"] for r in rows]
    action_stats = get_books_action_stats(book_ids)

    books_data = []
    for r in rows:
        book_id = r["book_id"]
        stats = action_stats.get(book_id, {})
        books_data.append({
            "bookId": book_id,
            "bookName": r["book_name"],
            "description": r["description"],
            "site": r["site"],
            "authorName": r["author_name"],
            "url": r["url"],
            "cover_url": r["picture_url"],
            "score": str(r["score"]).replace(".", ",") if r["score"] is not None else None,
            "free": r["free"],
            "chapters": r["chapters"],
            "tags": r.get("tags", []) or [],
            "genres": r.get("genres", []) or [],
            "fandoms": r.get("fandoms", []) or [],
            "weighted_score": r.get("weighted_score"),
            "freshness_score": r.get("freshness_score"),
            "last_view": localtime(stats.get("view")).strftime("%Y-%m-%d %H:%M") if stats.get("view") else None,
            "last_click": localtime(stats.get("click")).strftime("%Y-%m-%d %H:%M") if stats.get("click") else None,
        })

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


def view_book(request, book_id):
    create_book_action(book_id, "view")
    return JsonResponse({"status": "ok", "action": "view", "book_id": book_id})


def click_book(request, book_id):
    create_book_action(book_id, "click")
    return JsonResponse({"status": "ok", "action": "click", "book_id": book_id})

