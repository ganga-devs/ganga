#!/usr/bin/env python

# Create server
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8182

def hostname():
  import socket
  try:
    return socket.gethostbyaddr(socket.gethostname())[0]
    # [bugfix #20333]: 
    # while working offline and with an improper /etc/hosts configuration	
    # the localhost cannot be resolved 
  except:
    return 'localhost'

def main():
  import sys,os

  port = DEFAULT_PORT
  host = hostname()
  outputdir = os.path.expanduser('~/gangadir/outputserver')

  def makedirs(path):
    try:
      os.makedirs(path)
    except OSError,x:
      import errno
      if x.errno != errno.EEXIST:
        raise

  makedirs(outputdir)
  
  if len(sys.argv)>1:
    port = int(sys.argv[1])

  from SimpleXMLRPCServer import SimpleXMLRPCServer
    
  server = SimpleXMLRPCServer((host,port))

  #server.register_introspection_functions()

  def send_output(id,name,x):
    jobdir = os.path.join(outputdir,id.replace('.',os.sep))
    makedirs(jobdir)
    fn = os.path.join(jobdir,name)
    print 'job %s received %s saved in %s'%(id,name,fn)
    f = file(fn,'w')
    f.write(x)
    f.close()
    return 1

  server.register_function(send_output, 'send_output')

  print 'Starting output server:',host,port,outputdir

  # Run the server's main loop
  server.serve_forever()

if __name__ == '__main__':
  main()
