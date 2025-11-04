from django.urls import path
from . import views
from .views import api_book_view_batch, api_book_click

urlpatterns = [
    path('', views.filter_books, name='novels_index'),
    path("api/book/view_batch/", api_book_view_batch, name="api_book_view_batch"),
    path("api/book/click/<str:book_id>/", api_book_click, name="api_book_click"),
]
