import os, time

# Finally update the monitoring
runMonitoring()

# Loop over the jobs and remove completed ones
curr_doms = []
avail_doms = []
for j in jobs:
    if j.status == 'completed':
            
        avail_doms += j.application.domains
        if os.environ['SPIDER_PAYLOADOUTPUT'] != '':

            os.system("mkdir -p " + os.environ['SPIDER_LINKDIR'] + '/payload_outputs/%d' % j.id)
            os.system("cp " + j.outputdir + '/' + os.environ['SPIDER_PAYLOADOUTPUT'] + ' ' + \
                      os.environ['SPIDER_LINKDIR'] + '/payload_outputs/%d/' % j.id + \
                      os.environ['SPIDER_PAYLOADOUTPUT'])
            
        j.remove()
        
    elif j.status == 'failed':
        j.resubmit()
        curr_doms += j.application.domains 
    else:
        curr_doms += j.application.domains  
    
# Find the possible domains to spider
dom_file = open( os.environ['SPIDER_DOMAINLISTFILE'], "r")
for dom in dom_file.readlines():
    if (not os.path.exists(os.path.join(dom.strip(), 'queue_list.txt')) or os.path.getsize( os.path.join(dom.strip(), 'queue_list.txt') ) > 4) and not dom.strip() in curr_doms:
        avail_doms.append( dom.strip('\n'))

# remove repeated domains and domains that have been
# found but are being spidered by another job
avail_doms2 = []
for dom in avail_doms:
    if (not os.path.exists(os.path.join(dom.strip(), 'queue_list.txt')) or os.path.getsize( os.path.join(dom.strip(), 'queue_list.txt') ) > 4) and not dom in avail_doms2 and not dom in curr_doms:
        avail_doms2.append(dom)

# now create the jobs
idom = 0
num_doms = eval(os.environ['SPIDER_DOMSPERJOB'])
while idom < len(avail_doms2):
    
    j = Job()
    for input in os.environ['SPIDER_ADDINPUTS'].split():
        j.inputsandbox.append( File( input ) )
        
    j.application = Spider()
    j.application.max_links = eval( os.environ['SPIDER_MAXDAILYHIT'] )
    if idom < (len(avail_doms2) - 2 * num_doms):
        j.application.domains = avail_doms2[idom:idom + num_doms]
        idom += num_doms
    else:
        j.application.domains = avail_doms2[idom:]
        idom += 3 * num_doms
        
    j.application.safe_domains = [ os.environ['SPIDER_SAFEDOMAINS'] ]
    j.application.repository_loc = os.environ['SPIDER_LINKDIR'] 
    j.application.payload = File( os.environ['SPIDER_PAYLOAD'] )
    j.application.payload_output = os.environ['SPIDER_PAYLOADOUTPUT']
    j.backend = LCG()



curr_doms = []

for j in jobs:
    bad = False
    for dom in j.application.domains:
        if dom in curr_doms:
            print "************************************"
            print "ERROR: Muliple Domains specified!!!!"
            print "************************************"
            bad = True
        else:
            curr_doms.append(dom)
            
    if j.status == 'new' and not bad:
        j.submit()

# Finally, assemble results
out = open( os.path.join(os.environ['SPIDER_LINKDIR'], "results.txt"), "w")
total_viewed = 0
total_queued = 0

for dir in os.listdir(os.environ['SPIDER_LINKDIR']):

    if dir != "gangadir" and dir != "payload_outputs" and path.isdir( dir ):

        # find the total viewed, queued links
        num_queued = 0
        num_viewed = 0

        out.write(dir + '\n')
        ul = ""
        i = 0
        while i < len(dir):
            ul += "-"
            i += 1
            
        out.write( ul + '\n' )

        queue_file = os.path.join(os.environ['SPIDER_LINKDIR'] + '/' + dir, "queue_list.txt")
        view_file = os.path.join(os.environ['SPIDER_LINKDIR'] + '/' + dir, "viewed_list.txt")
        
        if os.path.exists( queue_file ):
            f = open( queue_file )
            num_queued = len(f.readlines())
            f.close()

        if os.path.exists( view_file ):
            f = open( view_file )
            num_viewed = len(f.readlines())
            f.close()

        out.write("Number Queued: %d\n" % num_queued)
        out.write("Number Viewed: %d\n\n" % num_viewed)

        total_queued += num_queued
        total_viewed += num_viewed

# Overall stats
out.write("\n\n")
out.write("----------------------------------------------------------\n")
out.write("Total viewed links: %d\n" % total_viewed)
out.write("Total queued links: %d\n\n" % total_queued)

sub = 0
run = 0
for j in jobs:
    if j.status == 'submitted':
        sub += 1
    if j.status == 'running':
        run += 1
        
out.write("%d jobs submitted\n" % sub)
out.write("%d jobs running\n" % run)

out.close()

