import os, system, time, datetime, subprocess
from Ganga.Utility.Config import getConfig

config = getConfig('Configuration')
gangadir = config['gangadir']
config = getConfig('System')
gangapath = config['GANGA_PYTHONPATH']

testdir = os.path.join(gangadir, "TestRobot")
os.chdir(testdir)
f = open('heartbeat.txt')
Data = f.readline()
EqualsPoint = -1
for i in range(len(Data)):
    if (Data[i] == '-'):
        EqualsPoint = i
        
if (EqualsPoint != -1):
    LogTime = Data[:(EqualsPoint-1)].strip()
    ProcessID = int(Data[(EqualsPoint+1):].strip())
    LogTime = datetime.datetime(*(time.strptime(LogTime,"%H:%M:%S %j %y"))[0:5])
    MaxTime = datetime.datetime().now() - datetime.timedelta(minutes=30)
if (LogTime < Maxtime):
    os.kill(ProcessID,"")
    cmd = "ganga --config="+gangapath+"/GangaTest/Lib/TestRobot/TESTROBOT.ini robot run"
    subprocess.popen(cmd)
f.close()
