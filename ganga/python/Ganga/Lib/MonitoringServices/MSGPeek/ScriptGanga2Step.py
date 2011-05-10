from time import sleep
import filecmp
import difflib
import os

ftmp = open('Temp.log','r')
d = eval(ftmp.read())
ftmp.close()

job_start = min(d)
f = open ('ResultsTests.log','w')

print d
for k in d:
    job_b =  jobs[k]          #"producer"
    aux = d.get(k, None)
    if aux == -1 :
        job_a = None
    else :
        job_a  = jobs[aux]   #"consumer"

    
    if job_a is not None and (job_a.status,job_b.status) == ("completed","completed"):
        b = job_b.outputdir+'stdout'
    #    b1 = open(b,'r')
    #    print b1.read()
    #job_b.peek("stdout","tail -n 5")
    
        a = job_a.outputdir+'stdout'
    #    a1 = open(a,'r')
    #    print a1.read()
    #job_a.peek("stdout","tail -n 5")
        

        if not os.path.exists(a) : 
            f.write("The output for the client %s doesn't exist, problems launching the client\n" %job_a.id)

        elif os.path.getsize(a) == 0 :
            f.write("The size of output of the client %s is 0, increase the number of the lines in the step 1\n" %job_a.id)
        
        elif not filecmp.cmp(a,b) :
            fcollector = open(b,'r')
            fouput = open(a,'r')
            line_co = fcollector.readlines()
            line_out = fouput.readlines()    
    #    for line in difflib.ndiff(line_co, line_out):
    #        print line,
    
            diff = difflib.context_diff(line_co, line_out)
    #    sys.stdout.writelines(diff)
            if diff is None :
                f.write("The job %s and client %s: Test passed\n" % (job_b.id, job_a.id) )
            else :
                f.write("The job %s and client %s: NOT passed, please look into the file Prod_%d-Cons_%d to see the details\n" % (job_b.id, job_a.id, job_b.id, job_a.id)) 

                aux = open('Prod_%d-Cons_%d'%(job_b.id, job_a.id),'w' ) 
                for line in diff: 
                    aux.write(line)
                aux.close()   
            
            fcollector.close()
            fouput.close()
        else:
            f.write("The job %s and client %s: Test passed\n" %(job_b.id, job_a.id)) 
    elif job_a is None:
        f.write('No client associated to the job %s\n' %(job_b.id))
    else :
        f.write("The job %s and client %s: Test not passed because their status are: %s and %s\n" %(job_a.id,job_b.id,job_a.status,job_b.status))


f.close()
