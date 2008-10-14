import JobInfoParser

jlist = []
for j in jobs['DIANE_4']:
    jlist.append(j)

dataTable = JobInfoParser.getJobInfoTable(jlist,['backend','backend.actualCE','application','status','backend.status'])


from GangaPlotter import *
plotter = GangaPlotter()

plotter.piechart(dataTable,pltColId=4,pltTitle='Pie Chart of backend status')


### Users' Interface
from GangaPlotter import *
plotter = GangaPlotter()

# no dataproc
plotter.piechart(jobs['DIANE_4'],attr='backend.CE',title='Job distribution by queue',output='test.png')

# user defined dataproc
plotter.piechart(jobs['DIANE_4'],attr='backend.CE',title='Job distribution by CE',output='test.png',dataproc=lambda x:x.split(':2119/')[0])

# build in dataproc
plotter.piechart(jobs['DIANE_4'],attr='backend.CE',title='Job distribution by CE',output='test.png',attrext='by_ce')
plotter.piechart(jobs['DIANE_4'],attr='backend.CE',title='Job distribution by queue',output='test.png',attrext='by_queue')
plotter.piechart(jobs['DIANE_4'],attr='backend.CE',title='Job distribution by country',output='test.png',attrext='by_country')
