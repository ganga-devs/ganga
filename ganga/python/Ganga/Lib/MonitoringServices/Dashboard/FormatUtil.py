"""Formatting utilities.

These utilities are not Ganga-specific and may be externalized.

N.B. This code is under development and should not generally be used or relied upon.

"""

DETAILS_DATA_KEY = 'detailsData'
def dictToWlcg(msg, include_microseconds=True):
    """Converts the given dictionary to WLCG message format.
    
    @param msg: A dictionary of key value pairs.
    @param include_microseconds: If microseconds should be included in the date
    format, default True.
    
    The keys and values are converted to strings using str() with the following
    exceptions:
      - if key is 'detailsData' and the value is a list or tuple, then its value
        is converted to a multi-line string using str() for each item and the
        WLCG-specified line separator.
      - if value is None, then it is converted to an empty string.
      - if value is an instance of datetime.datetime, and it is naive (i.e. it
        contains no timezone info), then it is assumed to be UTC and converted
        to the WLCG date format. i.e. YYYY-MM-DDTHH:MM:SS.mmmmmmZ or
        YYYY-MM-DDTHH:MM:SSZ if include_microseconds is False.
        
    Apart from detailsData, which is by definition the final entry, the entries
    are sorted according to natural ordering.
    
    See also:
      - WLCG message format.
        https://twiki.cern.ch/twiki/bin/view/LCG/GridMonitoringProbeSpecification#Message_Formats
      - WLCG date format.
        https://twiki.cern.ch/twiki/bin/view/LCG/GridMonitoringProbeSpecification#Date_Formats
    
    """
    import datetime
    lines = []
    for k, v in msg.iteritems():
        if k != DETAILS_DATA_KEY:
            # if v is UTC datetime then format in WLCG DateFormat 
            if isinstance(v, datetime.datetime) and v.tzinfo is None:
                # remove microseconds
                if not include_microseconds:
                    v = v.replace(microsecond=0)
                v = '%sZ' % v.isoformat()
            if v is None:
                v = ''
            lines.append('%s: %s' % (k, v))
    lines.sort()
    if DETAILS_DATA_KEY in msg:
        dd = msg[DETAILS_DATA_KEY]
        if isinstance(dd, (list, tuple)):
            dd = '\n'.join(dd)
        lines.append('%s: %s' % (DETAILS_DATA_KEY, dd))
    lines.append('EOT\n')
    wlcg = '\n'.join(lines)
    return wlcg


if __name__ == '__main__':
    # test code
    import datetime
    msg = {
        'a':1,
        'b':2,
        'c':datetime.datetime.utcnow(),
        'd':None,
        'detailsData': ['line 1', 'line 2']
        }
    wlcg = dictToWlcg(msg)
    print wlcg
    print
    wlcg = dictToWlcg(msg, include_microseconds=False)
    print wlcg

