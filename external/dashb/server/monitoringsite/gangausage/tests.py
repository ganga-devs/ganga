"""
This file demonstrates two different styles of tests (one doctest and one
unittest). These will both pass when you run "manage.py test".

Replace these with more appropriate tests for your application.
"""

from django.test import TestCase

class SimpleTest(TestCase):
    def test_basic_addition(self):
        """
        Tests that 1 + 1 always equals 2.
        """
        from gangausage.models import *
        s = GangaSession()
        s.time_start=100
        s.save()

        for s in GangaSession.objects.all():
            print s.__dict__
#            for a in s):
##                print a,getattr(s,a)
#            print
            

__test__ = {"doctest": """
Another way to test that 1 + 1 is equal to 2.

>>> 1 + 1 == 2
True
"""}

