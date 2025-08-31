from webnovel.models import Book, Tag, Cluster, Log2
from django.shortcuts import render
from django.core.paginator import Paginator
from django.http import JsonResponse
from django.db.models import Q, F, ExpressionWrapper
from django.db.models import FloatField
from django.db.models import Count  # Правильный импорт
import math
from django.db.models import DateTimeField, ExpressionWrapper, F, FloatField, DurationField
from django.utils.timezone import now

def filter_books(request):
    tags = request.GET.getlist("tag")
    clusters = request.GET.getlist("cluster")
    min_chapters = request.GET.get("min_chapters")
    max_chapters = request.GET.get("max_chapters")
    min_rating = request.GET.get("min_rating")
    max_rating = request.GET.get("max_rating")
    sort_by = request.GET.get("sort_by", "")
    sort_dir = request.GET.get("sort_dir", "desc")
    page = int(request.GET.get("page", 1))
    exclude_tags = request.GET.getlist("exclude_tag")
    exclude_clusters = request.GET.getlist("exclude_cluster")
    title_query = request.GET.get("title", "")
    created_from = request.GET.get("created_from")
    created_to = request.GET.get("created_to")
    updated_from = request.GET.get("updated_from")
    updated_to = request.GET.get("updated_to")

    # Получаем все книги
    books = Book.objects.all()

    is_chinese = request.GET.get("is_chinese")

    if is_chinese == "yes":
        books = books.filter(is_chinese=True)
    elif is_chinese == "no":
        books = books.filter(is_chinese=False)

    # Исключить теги
    if exclude_tags:
        for tag in exclude_tags:
            books = books.exclude(tags__name__iexact=tag)

    # Исключить кластеры
    if exclude_clusters:
        for cl in exclude_clusters:
            books = books.exclude(cluster__label__icontains=cl)

    # Фильтрация по тегам
    if tags:
        for tag in tags:
            books = books.filter(tags__name__iexact=tag)

    # Фильтрация по кластерам
    if clusters:
        q = Q()
        for cl in clusters:
            q |= Q(cluster__label__icontains=cl)
        books = books.filter(q)

    # Фильтрация по количеству глав
    if min_chapters:
        books = books.filter(chapters__gte=int(min_chapters))

    if max_chapters:
        books = books.filter(chapters__lte=int(max_chapters))

    # Фильтрация по рейтингу
    if min_rating:
        books = books.filter(score__gte=float(min_rating.replace(",", ".")))

    if max_rating:
        books = books.filter(score__lte=float(max_rating.replace(",", ".")))

    if title_query:
        books = books.filter(book_name__icontains=title_query)

    if created_from:
        books = books.filter(created_at__gte=created_from)

    if created_to:
        books = books.filter(created_at__lte=created_to)

    if updated_from:
        books = books.filter(updated_at__gte=updated_from)

    if updated_to:
        books = books.filter(updated_at__lte=updated_to)

    # Добавляем взвешенный рейтинг в аннотацию
    books = books.annotate(
        weighted_score=ExpressionWrapper(
            F("score") * Log2(F("chapters") + 1),
            output_field=FloatField()
        )
    )
    # Дополнительно: разницы во времени
    books = books.annotate(
        weighted_score=ExpressionWrapper(
            F("score") * Log2(F("chapters") + 1),
            output_field=FloatField()
        ),
        age_days=ExpressionWrapper(
            now() - F("created_at"),
            output_field=DurationField()
        ),
        updated_days=ExpressionWrapper(
            now() - F("updated_at"),
            output_field=DurationField()
        )
    )

    # Весовая формула: чем новее и свежее, тем лучше (меньше дней = выше вес)
    books = books.annotate(
        freshness_score=ExpressionWrapper(
            F("weighted_score") / (
                    F("age_days") / 86400 + 1
            ) * 1.5 +
            1 / (F("updated_days") / 86400 + 1),
            output_field=FloatField()
        )
    )

    # Сортировка
    allowed_sort_fields = {
        "chapters": "chapters",
        "bookName": "book_name",
        "score": "score",
        "weighted_score": "weighted_score",
        "created_at": "created_at",
        "updated_at": "updated_at",
        "freshness_score": "freshness_score"
    }

    if sort_by in allowed_sort_fields:
        field_name = allowed_sort_fields[sort_by]
        if sort_dir == "desc":
            field_name = "-" + field_name
        books = books.order_by(field_name)

    # Пагинация
    paginator = Paginator(books, 100)
    page_obj = paginator.get_page(page)

    # Формируем данные для отображения в шаблоне
    books_data = []
    for book in page_obj:
        books_data.append({
            "bookId": book.book_id,
            "bookName": book.book_name,
            "authorName": book.author_name,
            "url": book.url,
            "score": str(book.score).replace(".", ","),
            "chapters": book.chapters,
            "coverUrl": f"https://book-pic.webnovel.com/bookcover/{book.book_id}?imageMogr2/thumbnail/600x",
            "tags": [tag.name for tag in book.tags.all()],  # Предположим, что у книги есть связь с тегами
            "clusterLabel": [cluster.label for cluster in book.cluster.all()],  # Если кластер есть
        })

    # Ответ для Ajax запроса
    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse({"books": books_data, "total_pages": paginator.num_pages}, safe=False)

    # Сортируем теги по количеству книг в убывающем порядке
    tags = Tag.objects.annotate(book_count=Count('books')).order_by('-book_count')

    # Сортируем кластеры по количеству книг в убывающем порядке, исключая кластеры с 0 книгами
    clusters = Cluster.objects.annotate(book_count=Count('books')).filter(book_count__gt=0).order_by('-book_count')

    # Передаем теги и кластеры в контекст шаблона
    return render(request, "novels/index.html", {
        "books": page_obj,
        "tags": tags,  # Теги с количеством книг
        "clusters": clusters,  # Кластеры с количеством книг
        "page_number": page,
        "total_pages": paginator.num_pages,
    })
