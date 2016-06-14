#!/usr/bin/env python

wrfile = file('fillrandom.root', 'w')

wrfile.write('this is just a test')
wrfile.close()


wrfile1 = file('fillrandom1.root', 'w')

wrfile1.write('this is another a test')
wrfile1.close()


wrfile2 = file('fillrandom.root1', 'w')

wrfile2.write('this is another a test')
wrfile2.close()


wrfile3 = file('fillrandom1.root1', 'w')

wrfile3.write('this is another a test')
wrfile3.close()
