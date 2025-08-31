from django.db import models
from django.db.models import F, ExpressionWrapper, FloatField, Func
from django.utils import timezone

class Log2(Func):
    function = 'LOG'
    arity = 1

    def as_sql(self, compiler, connection):
        # LOG(x, base) = LN(x) / LN(base)
        sql, params = compiler.compile(self.source_expressions[0])
        return f"(LOG({sql}) / LOG(2))", params

class Tag(models.Model):
    name = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.name

    @property
    def count(self):
        return self.books.count()


class Cluster(models.Model):
    label = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.label

    @property
    def count(self):
        return self.books.count()


class Book(models.Model):
    book_id = models.CharField(max_length=30, unique=True)
    book_name = models.CharField(max_length=255)
    author_name = models.CharField(max_length=255, blank=True)
    url = models.URLField(max_length=500)
    chapters = models.IntegerField(default=0)
    score = models.FloatField(default=0.0)
    first_chapter = models.CharField(max_length=255, blank=True)
    tags = models.ManyToManyField(Tag, related_name="books")
    cluster = models.ManyToManyField(Cluster, related_name="books")
    is_chinese = models.BooleanField(default=False, blank=True)
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(default=timezone.now, blank=True)

    def cover_url(self):
        return f"https://book-pic.webnovel.com/bookcover/{self.book_id}?imageMogr2/thumbnail/600x"

    def __str__(self):
        return self.book_name
