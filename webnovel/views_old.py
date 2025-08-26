import os
from django.conf import settings
from django.core.paginator import Paginator
from django.shortcuts import render
from django.http import JsonResponse
import pandas as pd
from itertools import chain
import math
import ast

# Загрузка данных
BOOKS_CSV_PATH = os.path.join(settings.BASE_DIR, "books_clustered_named.csv")
books_df = pd.read_csv(BOOKS_CSV_PATH)
books_data = list({b['bookId']: b for b in books_df.to_dict(orient="records")}.values())


all_tags = sorted(set(
    tag.strip()
    for b in books_data
    for tag in str(b.get("tags") or "").split()
))

all_clusters = sorted(set(chain.from_iterable(
    str(b.get("cluster_label", "")).split() for b in books_data
)))


def filter_books(request):
    tags = request.GET.getlist("tag")
    clusters = request.GET.getlist("cluster")

    min_chapters = request.GET.get("min_chapters")
    max_chapters = request.GET.get("max_chapters")
    min_rating = request.GET.get("min_rating")
    max_rating = request.GET.get("max_rating")
    page = int(request.GET.get("page", 1))

    filtered = books_data

    if tags:
        filtered = [
            b for b in filtered
            if all(tag.upper() in str(b.get("tags") or "").upper().split() for tag in tags)
        ]

    if clusters:
        filtered = [
            b for b in filtered
            if any(cl.lower() in str(b.get("cluster_label", "")).lower().split() for cl in clusters)
        ]

    if min_chapters:
        filtered = [b for b in filtered if int(b.get("chapters", 0)) >= int(min_chapters)]

    if max_chapters:
        filtered = [b for b in filtered if int(b.get("chapters", 0)) <= int(max_chapters)]

    if min_rating:
        filtered = [b for b in filtered if float(str(b.get("score", "0")).replace(",", ".")) >= float(min_rating.replace(",", "."))]

    if max_rating:
        filtered = [b for b in filtered if float(str(b.get("score", "0")).replace(",", ".")) <= float(max_rating.replace(",", "."))]

    sort_by = request.GET.get("sort_by", "")
    sort_dir = request.GET.get("sort_dir", "desc")  # "asc" или "desc"

    # Взвешенный рейтинг (если нужен для сортировки)
    for b in filtered:
        try:
            ch = int(b.get("chapters", 0))
            score = float(str(b.get("score", "0")).replace(",", "."))
            b["weighted_score"] = math.log2(ch + 1) * score
        except:
            b["weighted_score"] = 0

    # Сортировка
    if sort_by in {"chapters", "bookName", "score", "weighted_score"}:
        reverse = (sort_dir == "desc")
        filtered.sort(key=lambda b: (
            float(str(b.get(sort_by, 0)).replace(",", ".")) if sort_by != "bookName" else str(
                b.get("bookName", "")).lower()
        ), reverse=reverse)

    paginator = Paginator(filtered, 20)
    page_obj = paginator.get_page(page)

    # Добавляем URL обложек
    for b in page_obj:
        b["coverUrl"] = f"https://book-pic.webnovel.com/bookcover/{b['bookId']}?imageMogr2/thumbnail/600x"

    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({"books": list(page_obj)}, safe=False)

    return render(request, "webnovel/index.html", {
        "books": page_obj,
        "tags": all_tags,
        "clusters": all_clusters,
        "page_number": page,
        "total_pages": paginator.num_pages,
    })