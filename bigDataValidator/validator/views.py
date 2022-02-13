from django.shortcuts import render
from .forms import InputForm

def handle_input_file(f):
	with open('validator/inputs/'+f.name, 'wb+') as destination:
		for chunk in f.chunks():
			destination.write(chunk)

def handle_metadata_file(f):
	with open('validator/metadata/csv/'+f.name, 'wb+') as destination:
		for chunk in f.chunks():
			destination.write(chunk)
# Create your views here.
def home_view(request):
	context = {}
	if request.POST:
		form = InputForm(request.POST, request.FILES)
		if form.is_valid():
			handle_input_file(request.FILES["input_field"])
			handle_metadata_file(request.FILES["metadata_field"])
	else:
		form = InputForm()
	context['form'] = form
	return render(request, "home.html", context)
