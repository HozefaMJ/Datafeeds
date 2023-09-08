from django.urls import path
from . import views

urlpatterns = [
    path('quilter',views.quilter),
    path('quilterLumpsum',views.quilterLumpsum),
    path('utmost',views.utmost),
    path('utmostLumpsum',views.utmostLumpsum),
    path('rl360',views.rl360),
    path('zurich',views.zurich),
    path('fpi',views.fpi),
    path('hansard',views.hansard),
    path('providence',views.providence),
    path('praemium',views.praemium),
    path('seb',views.seb),
    path('financial-account',views.FinancialAccount.as_view()),
    path('holdings',views.Holdings.as_view()),
]