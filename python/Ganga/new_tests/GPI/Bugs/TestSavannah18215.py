from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


def goto_state(j, destination):

    if destination == 'killed':
        states = ['submitting', 'submitted', 'running']
    else:
        states = ['submitting', 'submitted', 'running', 'completing']
    for s in states:
        j._impl.updateStatus(s)
        if s == destination:
            return
    j._impl.updateStatus(destination)


class TestSavannah18215(GangaUnitTest):
    def test_Savannah18215(self):
        from Ganga.GPI import Job

        # test manual failing and removal of jobs stuck in submitting or completing states

        j = Job()
        goto_state(j, 'submitting')
        j.force_status('failed')
        j.remove()

        j = Job()
        goto_state(j, 'completing')
        j.force_status('failed')
        j.remove()

        j = Job()
        goto_state(j, 'completed')
        j.force_status('failed')
        j.remove()

        j = Job()
        goto_state(j, 'failed')
        j.force_status('failed')
        j.remove()

        j = Job()
        goto_state(j, 'killed')
        j.force_status('failed')
        j.remove()
