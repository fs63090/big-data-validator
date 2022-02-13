from django import forms

class InputForm(forms.Form):
	name = forms.CharField()
	input_field = forms.FileField()
	metadata_field = forms.FileField()
