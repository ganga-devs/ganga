from django.conf.urls.defaults import *

urlpatterns = patterns('monitoringsite.gangamon.views',
    (r'^gangajobs', 'gangajobs'),
    (r'^gangadetails', 'gangadetails'),
    (r'^dianeruns', 'dianeruns'),
    (r'^dianedetails', 'dianedetails'),
    (r'^get_runs_JSON','get_runs_JSON'),
    (r'^get_tasks_JSON','get_tasks_JSON'),
    (r'^get_users_JSON','get_users_JSON'),
    (r'^get_tasks_statuses','get_tasks_statuses'),
)
