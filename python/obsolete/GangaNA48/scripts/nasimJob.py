
def randomString():
    """Simple method to generate a random string"""
    from random import randint
    from string import ascii_uppercase,join
            
    def addToSample(sample,ascii_length):
        """Basically random.select but python2.2"""
        a = ascii_uppercase[randint(0,ascii_length-1)]
        if not a in sample:
            sample.append(a)
        else:
            #passing by referance
            addToSample(sample,ascii_length)

    ascii_length = len(ascii_uppercase)     
    sample = []
    for _ in range(6):
        addToSample(sample,ascii_length)
    assert(len(sample) == 6)
            
    #seed is set to clock during import
    return join([str(a) for a in sample],'')
        

j = Job()
j.application = Nasim()
j.application.job_file = '/disk/f8b/home/mws/na48/cmc007.job'
j.application.titles_file = '/disk/f8b/home/mws/na48/cmc007user.titles'
j.application.beam = 1
j.application.seed = -1
j.application.run_number = 20410
j.application.num_triggers = 100

j.outputdata = NA48OutputDataset()
j.outputdata.name = 'gridtest_nasim_' + randomString()

j.backend = LCG()
j.backend.requirements.use_blacklist = False
