from django import forms
from django.forms.extras.widgets import SelectDateWidget

class SearchForm(forms.Form):

    EXPERIMENTS = (
        ('-', '-----'),
        ('atl', 'Atlas'),
        ('lhc', 'LHCb'),
        ('oth', 'Other'),
       )

        
    #f_d = forms.DateField(label="From", required = True, widget=SelectDateWidget(years=range(2007,2011)))
    #t_d = forms.DateField(label="To", required = False, widget=SelectDateWidget(years=range(2007,2011)))                
    from_date = forms.CharField(label = "From")
    to_date = forms.CharField(label = "To")
    e = forms.CharField(label = "Experiment", widget=forms.Select(choices=EXPERIMENTS)) 



