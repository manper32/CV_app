"""cv_app_d URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from cv_app.views import excel_CV_COL,csv_CV_Claro\
    ,csv_CV_CarP,csv_CV_FalaJ,csv_CV_FalaC,csv_GesD_Davi,csv_CV_Sant

urlpatterns = [
    path('admin/', admin.site.urls),
    path('excel_CV_Col/', excel_CV_COL),
    path('csv_CV_Claro/', csv_CV_Claro),
    path('csv_CV_CarP/', csv_CV_CarP),
    path('csv_CV_FalaJ/', csv_CV_FalaJ),
    path('csv_CV_FalaC/', csv_CV_FalaC),
    path('csv_CV_Sant/', csv_CV_Sant),
    path('csv_GesD_Davi/', csv_GesD_Davi),
]
