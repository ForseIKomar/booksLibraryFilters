from webnovel.models import Book, Tag, Cluster
from django.db import transaction
import csv
import os
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.timezone import make_aware
from datetime import datetime


def parse_timestamp(value):
    try:
        dt = datetime.fromtimestamp(int(value) / 1000)
        return make_aware(dt)  # делает datetime aware (с привязкой к текущей TIME_ZONE из настроек)
    except:
        return None


class Command(BaseCommand):
    help = "Импорт книг с тегами и кластерами, с обновлением по book_id"

    def handle(self, *args, **kwargs):
        csv_path = os.path.join(settings.BASE_DIR, "books_clustered_named.csv")

        if not os.path.exists(csv_path):
            self.stderr.write(self.style.ERROR(f"Файл не найден: {csv_path}"))
            return

        tag_cache = {t.name: t for t in Tag.objects.all()}
        cluster_cache = {c.label: c for c in Cluster.objects.all()}

        tags_to_create = []
        clusters_to_create = []

        count = 0
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            with transaction.atomic():
                for row in reader:
                    book_id = row.get("bookId")
                    if not book_id:
                        continue

                    # Кластеры
                    cluster_labels = str(row.get("cluster_label", "")).split()
                    clusters = []
                    for cluster_name in cluster_labels:
                        cluster_name = cluster_name.strip()
                        if cluster_name:
                            if cluster_name not in cluster_cache:
                                obj = Cluster(label=cluster_name)
                                clusters_to_create.append(obj)
                                cluster_cache[cluster_name] = obj
                            clusters.append(cluster_cache[cluster_name])

                    # Теги
                    tags = []
                    for tag_name in str(row.get("tags", "")).split():
                        tag_name = tag_name.strip()
                        if tag_name:
                            if tag_name not in tag_cache:
                                obj = Tag(name=tag_name)
                                tags_to_create.append(obj)
                                tag_cache[tag_name] = obj
                            tags.append(tag_cache[tag_name])

                    # После сбора тегов и кластеров — создаём их
                if tags_to_create:
                    Tag.objects.bulk_create(tags_to_create)
                    tag_cache.update({t.name: t for t in Tag.objects.filter(name__in=[t.name for t in tags_to_create])})

                if clusters_to_create:
                    Cluster.objects.bulk_create(clusters_to_create)
                    cluster_cache.update({c.label: c for c in Cluster.objects.filter(label__in=[c.label for c in clusters_to_create])})

        # Второй проход — создаём или обновляем книги
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            with transaction.atomic():
                for row in reader:
                    book_id = row.get("bookId")
                    if not book_id:
                        continue

                    book, created = Book.objects.get_or_create(book_id=book_id)
                    book.book_name = row.get("bookName", "")
                    book.author_name = row.get("authorName", "")
                    book.url = row.get("url", "")
                    book.chapters = int(row.get("chapterNum") or 0)
                    book.score = float(str(row.get("totalScore", "0")).replace(",", "."))
                    book.first_chapter = row.get("firstChapterName", "")
                    book.is_chinese = row.get("is_chinese", "")
                    book.created_at = parse_timestamp(row.get("publishTime"))
                    book.updated_at = parse_timestamp(row.get("coverUpdateTime"))
                    book.save()

                    # Обновляем связи
                    cluster_labels = str(row.get("cluster_label", "")).split()
                    cluster_objs = [cluster_cache[label] for label in cluster_labels if label in cluster_cache]
                    book.cluster.set(cluster_objs)

                    tag_names = str(row.get("tags", "")).split()
                    tag_objs = [tag_cache[name] for name in tag_names if name in tag_cache]
                    book.tags.set(tag_objs)

                    count += 1

        self.stdout.write(self.style.SUCCESS(f"✅ Импорт завершён. Обработано книг: {count}"))
