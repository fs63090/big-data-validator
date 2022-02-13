from django.shortcuts import render
from .forms import InputForm

def handle_uploaded_file(f):
	with open('validator/input/'+f.name, 'wb+') as destination:
		for chunk in f.chunks():
			destination.write(chunk)
# Create your views here.
def home_view(request):
	context = {}
	if request.POST:
		form = InputForm(request.POST, request.FILES)
		if form.is_valid():
			handle_uploaded_file(request.FILES["input_field"])
			handle_uploaded_file(request.FILES["metadata_field"])
	else:
		form = InputForm()
	context['form'] = form
	return render(request, "home.html", context)
