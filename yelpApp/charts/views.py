from django.shortcuts import render
from django.http import HttpResponse

def index(request):
    business = "business.png"
    checkins = "checkins.png"
    return render(request,'index.html',{'img1':business,'img2':checkins})
# Create your views here.
