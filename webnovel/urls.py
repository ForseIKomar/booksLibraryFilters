from django.urls import path
from . import views

urlpatterns = [
    path('', views.filter_books, name='webnovel_index'),
]
