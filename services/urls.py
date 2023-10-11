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
    path('find_non_unique_entries',views.find_non_unique_entries),
    path('find_missing_policy_numbers',views.find_missing_policy_numbers),
    path('financial-accounts-all',views.map_and_return_data),
    path('holdings-all',views.map_and_return_data_policy),
    path('find_non_unique_entries_excel',views.find_non_unique_entries_excel),
    path('financial-account',views.FinancialAccount.as_view()),
    path('holdings',views.Holdings.as_view()),
]