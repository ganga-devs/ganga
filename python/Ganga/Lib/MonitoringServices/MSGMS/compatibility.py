# this module provides certain functionalities missing in 2.3 and 2.4
# the interface mimics the standard library in 2.5

# backwards compatibility for < 2.4
def format_exc():
    import traceback
    #FIXME: < 2.4
    import StringIO
    tbm = StringIO.StringIO()
    traceback.print_exc(None,tbm)
    return tbm.getvalue()
    #2.4: return traceback.format_exc()

try: # 2.5
    import uuid as uuid_module
    def uuid():
        return str(uuid_module.uuid4())
except ImportError: # <2.5
    # FIXME: poor's man uuid
    def uuid():
        import random,time
        #return str(int(random.uniform(0,10))) # TESTING multiple worker registration
        return str(random.uniform(0,100000000)+time.time())
