from django.contrib import admin
from django.urls import path, include
from DjangoServer.apps.auth import views
from django.conf.urls import url

urlpatterns = [
    path('login/', views.MyprojectLoginView.as_view(), name='login_page'),
    path('register/', views.RegisterUserView.as_view(), name='register_page'),
    path('logout/', views.MyProjectLogout.as_view(), name='logout_page'),
]
