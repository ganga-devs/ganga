#!/usr/bin/env ganga

import re
import inspect
import GangaPlotHelper

## require matplotlib to produce statistic plots
from pylab import *

import sys
from functools import reduce

from Ganga.Utility.Config import makeConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import isStringLike, canLoopOver

logger = getLogger()
config = makeConfig('GangaPlotter','GangaPlotter configuration parameters')

NOT_APPLICABLE = '*N/A*'

class GetJobAttrException(Exception):

    '''exception for parser error'''

    def __init__(self,msg):
        self.msg = msg
    def __str__(self):
        return self.msg

## this function could be put in Ganga.Utility.util
def isNumeric(myValue):
    '''checks via type() if myVar is numeric or not'''
    if type(myValue)==type(1) or type(myValue)==type(1) or \
       type(myValue)==type(1.1) or type(myValue)==type(1+1j) :
       return 1
    else:
       return 0

def __getJobAttribute__(job,attrName):

    '''method for getting the value of the given attribute from the GANGA job object'''

    attrList  = attrName.split('.')
    attrValue = job

    for attr in attrList:
        if not re.match(r'^\_\_.*\_\_$',attr):
            try:
                attrValue = getattr(attrValue,attr)
            except Exception as x:
                errMsg = 'Cannot select attribute %s: %s!' % (attr,str(x))
                raise GetJobAttrException(errMsg)
        else:
            errMsg = 'Query the attribute %s is not allowed!' % attr 
            raise GetJobAttrException(errMsg)

    attrClass = attrValue.__class__.__name__

    try:
        attrValue = attrValue._impl._name
    except AttributeError:
        pass
    
    return (attrValue, attrClass)

def __makeJobInfo__(job,attrNameList):

    '''make complete jobinfo according to the given list of job attributs'''

    jobInfo = []
    for attrName in attrNameList:
        try:
            if isStringLike(attrName):
                attrValue, attrClass = __getJobAttribute__(job,attrName)
            else:
                attrValue, attrClass = attrName(job), None
            jobInfo.append(attrValue)
        except Exception as x:
            logger.warning('Ignoring job %d: %s'%(job.id,str(x)))
            jobInfo.append(NOT_APPLICABLE)

    return jobInfo 

def __makePlotTitle__(num_jobs,deep,subtitle):

    title = 'Statistics for %d jobs ' % num_jobs

    if(deep):
        title += ' (w/ subjobs): '
    else:
        title += ' (w/o subjobs): '

    return title+subtitle

def getJobInfoTable(jobs,attrNameList,deep=True):

    '''prepare the data table for plots'''
    
    jobInfoTable = []
    jobInfoTable.append(attrNameList)
    id = 0
    try:
        for j in jobs:
            if deep and len(j.subjobs) > 0:
                # get the infotable from subjobs
                # The deep looping will go through all of the subjob levels
                info = getJobInfoTable(j.subjobs,attrNameList,deep)

                # extend the main infotable to include the infotable of the subjobs 
                # the title row should be substracted before merging
                jobInfoTable.extend(info[1:])
                id = id + len(info[1:])
            else:
                # if deep looping is off or there is no subjob inside
                # take the info directly from the job 
                jobInfoTable.append(__makeJobInfo__(j,attrNameList))
                id += 1

    except Exception as e:
        raise e

    return jobInfoTable


class GangaPlotter:

    ''' the object of GANGA job statistical plots '''

    def __init__(self):
        self.figId  = 0

    def __setFigureId__(self):
        self.figId += 1

    def __doPlot__(self):            
        ## savefile if requested
        if self.output != None:
            savefig(self.output)
            logger.info('the plot has been saved in %s' % self.output)

        ## force to plot if interactive mode is not activated
        if not isinteractive():
            show()
            close()

    def __unionLists__(self,lists):

        unionSet = set(lists[0])

        for i in range(1,len(lists)):
            newSet   = set(lists[i])
            unionSet.update(newSet)

        unionList = []
        for i in range(len(unionSet)):
            unionList.append(unionSet.pop())

        return unionList

    def __makeList__(self,element):

        if isStringLike(element):
            myList = [element]
        elif canLoopOver(element):
            myList = element
        else:
            myList = [element]
        return myList

    def __setDataProcessor__(self,attr,attrext,dataproc):

        """ set the data processor for attribute values """

        if attrext != None and dataproc != None:
            logger.info('user defined dataproc %s will be used.' % str(dataproc))
        elif attr in ['backend.CE','backend.actualCE']:
            # the build-in data processors 
            if attrext == 'by_queue':
                dataproc = None
            if attrext == 'by_ce':
                dataproc = lambda x:x.split(':2119/')[0]
            if attrext == 'by_country':
                dataproc = lambda x:GangaPlotHelper.tld_country(x.split(':2119/')[0].split('.')[-1])
        return dataproc

    def __setColormap__(self, pltColorMap, numColors):

        """ setup the color map """

        plt_cmap = []
        if not pltColorMap is None:
            ## load user defined pltColorMap
            logger.info('load user defined color map.')
            for i in range(numColors):
                plt_cmap.append(pltColorMap[i % len(pltColorMap)])
        else:
            #plt_cmap = self.__defaultColormap__(numColors)
            plt_cmap = self.__defaultColormapAlt__(numColors)

        return plt_cmap

    def __defaultColormapAlt__(self,dataBlockNumber):

        '''the default color map definition'''

        c_min = 0.1
        c_max = 0.9
        c_delta_mag = 1.0*(c_max - c_min) / dataBlockNumber

        c_map = []
        for i in range(dataBlockNumber):
            c_mag = c_min + i * c_delta_mag
            c_map.append(GangaPlotHelper.floatRgb(c_mag,c_min,c_max))

        return c_map

    def __defaultColormap__(self,dataBlockNumber):

        '''the default color map definition'''
 
        c_i_min = 400 - 200
        c_i_max = 400 + 100
 
        c_map = []
 
        for i in range(dataBlockNumber):
            ## select the corresponding color for displaying the fraction
            c_i = i % (c_i_max-c_i_min+1)
            if i % 2 == 0:
                c_i = (c_i + c_i_min)
            else:
                c_i = (c_i_max - i)
            c_x = 1.0 * int(c_i/64)
            c_y = 1.0 * int((c_i % 64)/8)
            c_z = 1.0 * c_i - 64*c_x - 8*c_y
            color = (c_x/8.0,c_y/8.0,c_z/8.0)
 
            c_map.append(color)
 
        return c_map

    def __defaultSubtitle__(self,titleAttr):

        """ compose the subtitle according to the given titleAttr. """

        if isStringLike(titleAttr):  ## a string
            return titleAttr
        elif canLoopOver(titleAttr): ## a list
            return self.__defaultSubtitle__(titleAttr[0])
        elif inspect.isfunction(titleAttr): ## other things: function, object ...
            return 'user function %s' % titleAttr.__name__

    def __procPltData__(self,value,pltDataProc):

        """ data processing on the attribute value """

        if not pltDataProc is None:
            try:
                value = pltDataProc(value)
            except Exception as x:
                logger.warning('Ignoring data processing on %s: %s'%(str(value),str(x)))
        else:
            value = value

        return value

    def __makeScatter__(self,dataTable,pltXColId=0,pltYColId=1,pltTitle='Scatter Plot',pltXLabel=None,pltYLabel=None,pltColorMap=None,pltOutput=None,pltXDataProc=None,pltYDataProc=None,pltCDataProc=None):

        """backend scatter plot generator """

        ## the last marker is a ntuple for "star"
        markerList = ['o','s','^','d','+','v','x', (5,1,0)]

        pltAlpha=0.7

        dataHeader = dataTable[0]
        dataTable  = dataTable[1:]

        if len(dataTable) < 1:
            logger.warning('Scatter plot requires at least 2 columns in the data Table, the given contains only %d.') % len(dataTable)
            return

        xdata = {} 
        ydata = {} 

        i = 0
        for data in dataTable:

            logger.info('processing data row %d' % i)

            i+=1

            xval = self.__procPltData__(data[pltXColId],pltXDataProc)
            yval = self.__procPltData__(data[pltYColId],pltYDataProc)
            
            if len(data) >= 3:
                cval = self.__procPltData__(data[2],pltCDataProc)
            else:
                cval = pltTitle

            if cval not in xdata.keys():
               xdata[cval] = []
               ydata[cval] = []

            xdata[cval].append(xval)
            ydata[cval].append(yval)       

        if not pltXLabel:
            pltXLabel = dataHeader[pltXColId]

        if not pltYLabel:
            pltYLabel = dataHeader[pltYColId]

        ##-----
        ## Generating marker and color array
        ##-----
        legend_labels = xdata.keys()
        mycmap = self.__setColormap__( pltColorMap, len(legend_labels) )

        ##-----
        ## Generating the scatter plot
        ##-----
        self.output = pltOutput
        self.__setFigureId__()

        figure(self.figId)
        rc('font',size=8.0)

        i = 0

        charts = []
        for key in legend_labels:

            marker = markerList[ i % len(markerList) ]
            chart = scatter(xdata[key], ydata[key], c=[ mycmap[i] ]*len(xdata[key]), cmap=mycmap, marker=marker, alpha=pltAlpha, label=key)
            charts.append(chart)

            i += 1

        grid(True)
        title(pltTitle)
        ylabel(pltYLabel)
        xlabel(pltXLabel)
        #legend(shadow=True, loc='best')
        legend(charts, legend_labels, shadow=True, loc='best')
        axis('on')

        self.__doPlot__()

    def __makeBarChart__(self,dataTable,pltXColId=0,pltYColIds=[1],pltTitle='Bar Chart',pltXLabel=None,pltYLabel='#',pltColorMap=None,pltOutput=None,pltXDataProc=None,pltYDataProcs=None,stackedBar=True):

        """ backend bar chart generator """

        dataHeader = dataTable[0]
        dataTable  = dataTable[1:]

        if len(dataTable) < 1:
            logger.warning('Barchart requires at least 2 columns in the data Table, the given contains only %d.') % len(dataTable)
            return

        pltYColIds    = self.__makeList__(pltYColIds)
        pltYDataProcs = self.__makeList__(pltYDataProcs)

        for i in range(len(pltYDataProcs),len(pltYColIds)):
            pltYDataProcs.append(None)

        ##----- 
        ## extract the given data for pie chart 
        ##----- 
        # summarize the data of the given column 
        pltData = {}
        for data in dataTable:
            # apply the user defined data processor
            xLabel = self.__procPltData__(data[pltXColId],pltXDataProc)
            # should make sure all the values are represented in String type
            xLabel  = str(xLabel)

            for i in range(len(pltYColIds)):
                yColId = pltYColIds[i]
                yLabel = self.__procPltData__(data[yColId],pltYDataProcs[i])
                if isNumeric(yLabel):
                    # if the label is a numeric value, take it as the value
                    # and use dataHeader as the yLabel
                    yvalue = yLabel
                    yLabel = str(dataHeader[yColId])
                else:
                    # if the label is not a numerical value, take it as the label
                    # and use 1 as yvalue
                    yvalue = 1
                    yLabel = str(yLabel)
             
                if xLabel in pltData:
                    if yLabel in pltData[xLabel]:
                        pltData[xLabel][yLabel] = pltData[xLabel][yLabel] + yvalue
                    else:
                        pltData[xLabel][yLabel] = yvalue
                else:
                    pltData[xLabel] = {}
                    pltData[xLabel][yLabel] = yvalue

        ##----- 
        ## Mapping the extracted data to the columns for bar chart plot 
        ##----- 

        # the xLabels 
        bar_xlabels = pltData.keys()
        bar_xlabels.sort()

        # collecting values for the bars
        bars = {}
        valueLists = []
        for l in bar_xlabels:
            valueLists.append(pltData[l].keys())

        valueLists = self.__unionLists__(valueLists)

        for v in valueLists:
            barValues = []
            for l in bar_xlabels:
                if v in pltData[l]:
                    barValues.append(pltData[l][v])
                else:
                    barValues.append(0)
            bars[v] = barValues

        # the pltColorMap
        bar_cmap = self.__setColormap__( pltColorMap, len(bars.keys()) )

        ##-----
        ## Generating the bar chart 
        ##-----
        self.output = pltOutput
        self.__setFigureId__()

        num_bars  = len(bars) # the number of bars
        num_dbars = num_bars  # the number of basrs displayed on the plots
        if stackedBar:
            num_dbars  = 1
        num_xtics = len(bar_xlabels)

        figure(self.figId)
        rc('font',size=6.0)
        a = axes([0.1,0.3,0.8,0.6])

        bar_width = 0.35
        bar_gap   = 0.3
        bar_org   = map(lambda x:x*(num_dbars*bar_width+bar_gap), arange(num_xtics))
        tic_pos   = map(lambda x:x+num_dbars*bar_width/2.0, bar_org)

        legend_labels = []
        legend_texts  = bars.keys()

        bar_off = map(lambda x:0,bars[legend_texts[0]])
        for i in range(num_bars):
            chart = None
            if stackedBar and i>0:
                bar_off = map(lambda x,y:x+y,bar_off,bars[legend_texts[i-1]])
                chart   = bar(bar_org, bars[legend_texts[i]], width=bar_width, bottom=bar_off, color=bar_cmap[i])
            else:
                bar_pos = map(lambda x:x+bar_width*i,bar_org)
                chart   = bar(bar_pos, bars[legend_texts[i]], width=bar_width, bottom=bar_off, color=bar_cmap[i])

            legend_labels.append(chart[0])

        grid(True)
        title(pltTitle)
        ylabel(pltYLabel)
        if pltXLabel is None:
            pltXLabel = str(dataHeader[0])
        xlabel(pltXLabel)
        xx = xticks(tic_pos, bar_xlabels)
        setp(xx[1], 'rotation', 90, fontsize=8.0)
        legend(legend_labels, legend_texts, shadow=True, loc='best')
        axis('on')

        self.__doPlot__()

    def __makeMultiHistograms__(self, dataTable, pltColIdList, pltDataProcList, pltLabelList, pltTitle='Histogram',\
                                pltNumBins=50,pltRange=(-1,-1),pltXLabel=None,pltColorMap=None,pltNormalize=False,\
                                pltOutput=None):

        """ backend generator for multiple histograms """

        hist_cmap = self.__setColormap__( pltColorMap, len(pltColIdList) )

        # set figure id
        self.__setFigureId__()

        figure(self.figId)
        rc('font',size=8.0)

        # create only the xlabel if only one histogram is plotted
        if not pltXLabel:
            xlabel(dataHeader[pltColId])
        else:
            xlabel(pltXLabel)

        for id in pltColIdList:

            pltFaceColor = hist_cmap[id]
            pltDataProc  = pltDataProcList[id]
            pltLabel     = pltLabelList[id]

            self.__makeHistogram__(dataTable, pltColId=id, pltNumBins=pltNumBins, pltRange=pltRange, \
                                   pltFaceColor=pltFaceColor, pltNormalize=pltNormalize, \
                                   pltLabel=pltLabel, pltOutput=pltOutput, pltDataProc=pltDataProc)

        # making the plot
        self.output = pltOutput

        if pltNormalize:
            ylabel('')
        else:
            ylabel('# jobs')


        title(pltTitle)
        legend()
        grid(True)
        self.__doPlot__()

    def __makeHistogram__(self,dataTable,pltColId=0,pltNumBins=50, pltRange=(-1,-1), \
                          pltFaceColor='green',pltNormalize=False,\
                          pltLabel=None,pltOutput=None,pltDataProc=None):

        """ backend histogram generator """

        pltAlpha=0.7

        ##-----
        ## extract the given data for pie chart
        ##-----
        # summarize the data of the given column
        dataHeader = dataTable[0]
        dataTable  = dataTable[1:]

        pltData = {}
        pltData[dataHeader[pltColId]] = []
        for data in dataTable:
            
            # apply the user defined data processor
            value = self.__procPltData__(data[pltColId],pltDataProc)

            # make sure the value is in floating point
            if not isNumeric(value):
                raise TypeError('histogram data should be a numeric value.')
            else:
                pltData[dataHeader[pltColId]].append(value)
        
        #a = axes([0.05,0.1,0.5,0.7])
        ## disable plot range if -1 is given to the range specification
        if -1 in pltRange:
            pltRange = None

        n, bins, patches = hist(pltData[dataHeader[pltColId]], bins=pltNumBins, range=pltRange, normed=pltNormalize, facecolor=pltFaceColor, label=pltLabel, alpha=pltAlpha)

        ## a trick for remove the dummy labels from the legend
        for p in patches[1:]:
            p.set_label('_nolegend_')
        

    def __makePieChart__(self,dataTable,pltColId=0,pltTitle='Pie Chart',pltColorMap=None,pltOutput=None,pltDataProc=None):

        """ backend pie chart generator """

        ##----- 
        ## extract the given data for pie chart 
        ##----- 
        # summarize the data of the given column
        dataHeader = dataTable[0]
        dataTable  = dataTable[1:]

        pltData = {}
        for data in dataTable:
            # apply the user defined data processor
            value = self.__procPltData__(data[pltColId],pltDataProc)

            # should make sure all the values are represented in String type
            value = str(value)

            if value in pltData:
                pltData[value] = pltData[value] + 1
            else:
                pltData[value] = 1

        ##----- 
        ## Mapping the extracted data to the columns for pie chart plot 
        ##----- 
        # the pltColorMap
        pie_cmap = self.__setColormap__( pltColorMap, len(pltData.keys()) )
 
        # the labels 
        pie_labels = pltData.keys()
        pie_labels.sort()
 
        # the values
        pie_values = []
        for label in pie_labels:
            pie_values.append(pltData[label])
 
        ##----- 
        ## Generating the pie chart 
        ##----- 
        plotLegend = False
        if len(pie_labels) >= 8: plotLegend = True

        self.output = pltOutput

        self.__setFigureId__()

        if plotLegend:

            legend_length    = 1000 # put a very large number will put all lables in one legend 
            legend_panel_num = len(pie_labels) / legend_length
            if len(pie_labels) % legend_length > 0: legend_panel_num += 1

            fig_size_x  = 6.0 + 4.0 * legend_panel_num
            fig_size_y  = 5.0
            fig_rleft   = 0.05 * 10.0 / fig_size_x 
            fig_rbottom = 0.1
            fig_rwidth  = 0.5  * 10.0 / fig_size_x
            fig_rheight = 0.8

            legend_rwidth = 0.3 * 10.0 / fig_size_x

            figure(self.figId,figsize=(fig_size_x,fig_size_y))
            rc('legend',fontsize=8.0)
            rc('font',size=8.0)
            a = axes([fig_rleft,fig_rbottom,fig_rwidth,fig_rheight])
            chart = pie(pie_values,labels=None,colors=pie_cmap,autopct='%1.1f%%',shadow=True)
            # add percentage on the label
            def sumList(L):
                    return reduce(lambda x,y:x+y, L)
            for i in range(len(pie_labels)):
                percentage = '(%1.1f%%) ' % (100.0 * pie_values[i] / sumList(pie_values))
                pie_labels[i] = percentage + pie_labels[i]

            # put labels on legends
            for i in range(legend_panel_num):
                id_beg = i * legend_length
                id_end = id_beg + legend_length
                if id_end > len(pie_labels): id_end = len(pie_labels)
                #legend_loc = (2*fig_rleft + fig_rwidth + i*(legend_rwidth+fig_rleft), 0.5)
                #legend(chart[0][id_beg:id_end],pie_labels[id_beg:id_end],loc='best',labelsep=0.005,handlelen=0.05,pad=0.1,numpoints=2)
                figlegend(chart[0][id_beg:id_end],pie_labels[id_beg:id_end],loc='center right',labelsep=0.005,handlelen=0.05,pad=0.1,numpoints=2)
        else:
            figure(self.figId)
            rc('font',size=8.0)
            #a = axes([0.05,0.1,0.5,0.7])
            chart = pie(pie_values,labels=pie_labels,colors=pie_cmap,autopct='%1.1f%%',shadow=True)

        title(pltTitle)
        axis('off')
        self.__doPlot__()

    def barchart(self,jobs,xattr,yattr,**keywords):
        
        """
        The plotter's interface for generating bar chart.
        
        usage:
            >>> plotter.barchart(jobs,xattr,yattr,**keywords)

            Generate a bar chart presenting the distribution of yattr along xattr. 

        required arguments:
             jobs: A GANGA's job table

            xattr: A string or a user defined function, the corresponding value of which
                   will be extracted as the x value of the bar chart.

            yattr: A string or a user defined function, the corresponding value of which
                   will be extracted as the y value of the bar chart.

        optional arguments:
            colormap: A list of values representing the colors that will be picked to
                      paint the pie chart. The supported description of color can be found 
                      at http://matplotlib.sourceforge.net/matplotlib.pylab.html#-colors

               title: A string specifying the title of the bar chart.

              xlabel: A string specifying the xlabel of the bar chart.

              ylabel: A string specifying the ylabel of the bar chart.

              output: A name of file where the bar chart plot will be exported. The format
                      is auto-determinated by the extension of the given name.

           xdataproc: An user-defined function which will be applied to process the value 
                      of xattr before plotting the bar chart.

           ydataproc: The same as xdataproc; but apply on the value of yattr. 

            xattrext: Trigger the build-in data pre-processor on the value of xattr.
                      It will be override if "xdataproc" argument is also specified.
                      ** Supported attrext for "backend.actualCE" and "backend.CE":
                         - "by_ce": Catagorize CE queues into CE
                         - "by_country": Catagorize CE queues into country 

            yattrext: The same as xattrext; but apply on the value of yattr.

             stacked: True of False for stacking multiple bars in one stick.

                deep: Sets if looping over all the subjob levels. Default is "True"
        """

        # default keyword arguments
        subtitle  = self.__defaultSubtitle__(yattr)      # default subtitle
        xlabel    = None        # xlabel
        ylabel    = '#'         # ylabel
        colormap  = None        # colormap
        output    = None        # the output file of the picture
        xattrext  = None        # trigger the build-in data processing function on xattr
        yattrext  = None        # trigger the build-in data processing function on yattr
        xdataproc = None        # function for xdata processing (cannot work together with 'attrext')
        ydataproc = None        # function for ydata processing (cannot work together with 'attrext')
        stacked   = True        # stack bars in one stick
        deep      = True        # deep looping over all the subjob levels 

        # update keyword arguments with the given values
        if 'title' in keywords:    subtitle   = keywords['title']
        if 'xlabel' in keywords:   xlabel     = keywords['xlabel']
        if 'ylabel' in keywords:   ylabel     = keywords['ylabel']
        if 'colormap' in keywords: colormap   = keywords['colormap']
        if 'output' in keywords:   output     = keywords['output']
        if 'xattrext' in keywords: xattrext   = keywords['xattrext']
        if 'yattrext' in keywords: yattrext   = keywords['yattrext']
        if 'xdataproc' in keywords: xdataproc = keywords['xdataproc']
        if 'ydataproc' in keywords: ydataproc = keywords['ydataproc']
        if 'stacked' in keywords:   stacked   = keywords['stacked']
        if 'deep' in keywords:       deep     = keywords['deep']

        jlist = []
        for j in jobs:
            jlist.append(j)

        xattr_spec = self.__makeList__(xattr)
        yattr_spec = self.__makeList__(yattr)

        # special build-in dataprocs for the CE-based bar chart
        xdataproc = self.__setDataProcessor__(xattr_spec[0],xattrext,xdataproc)
        ydataproc = self.__setDataProcessor__(yattr_spec[0],yattrext,ydataproc)

        dataTable = getJobInfoTable(jlist,[xattr_spec[0],yattr_spec[0]],deep)
        # make the plot title
        title = __makePlotTitle__(len(dataTable[1:]),deep,subtitle)

        # make the plot
        self.__makeBarChart__(dataTable,pltXColId=0,pltYColIds=[1],pltTitle=title,pltXLabel=xlabel,pltYLabel=ylabel,pltColorMap=colormap,pltOutput=output,pltXDataProc=xdataproc,pltYDataProcs=[ydataproc],stackedBar=stacked)

    def scatter(self,jobs,xattr,yattr,**keywords):

        """
        The plotter's interface for generating scatter plot.

        usage:
            >>> plotter.scatter(jobs,xattr,yattr,**keywords)

            Generate a scatter chart presenting the distribution of yattr along xattr.

        required arguments:
             jobs: A GANGA's job table

            xattr: A string or a user defined function, the corresponding value of which
                   will be extracted as the x value of the scatter plot.

            yattr: A string or a user defined function, the corresponding value of which
                   will be extracted as the y value of the scatter plot.

        optional arguments:

               cattr: A String or a user defined function to catagorize the data points into group.
                      Each group will be plotted with the same marker filled with the same color.

            colormap: A list of values representing the colors that will be picked to
                      distinguish different data group categorized by cattr. The supported description of color can be found
                      at http://matplotlib.sourceforge.net/matplotlib.pylab.html#-colors

               title: A string specifying the title.

              xlabel: A string specifying the xlabel.

              ylabel: A string specifying the ylabel.

              output: A name of file where the chart will be exported. The format
                      is auto-determinated by the extension of the given name.

           xdataproc: An user-defined function which will be applied to process the value
                      of xattr before plotting chart.

           ydataproc: The same as xdataproc; but apply on the value of yattr.

           cdataproc: The same as xdataproc; but apply on the value of cattr.

            xattrext: Trigger the build-in data pre-processor on the value of xattr.
                      It will be override if "xdataproc" argument is also specified.
                      ** Supported attrext for "backend.actualCE" and "backend.CE":
                         - "by_ce": Catagorize CE queues into CE
                         - "by_country": Catagorize CE queues into country

            yattrext: The same as xattrext; but apply on the value of yattr.

            cattrext: The same as xattrext; but apply on the value of cattr.

                deep: Sets if looping over all the subjob levels. Default is "True"
        """

        # default keyword arguments
        subtitle  = self.__defaultSubtitle__(yattr)      # default subtitle
        cattr     = None        # catagory attribute
        xlabel    = None        # xlabel
        ylabel    = None        # ylabel
        colormap  = None        # colormap
        output    = None        # the output file of the picture
        xattrext  = None        # trigger the build-in data processing function on xattr
        yattrext  = None        # trigger the build-in data processing function on yattr
        cattrext  = None        # trigger the build-in data processing function on cattr
        xdataproc = None        # function for xdata processing (cannot work together with 'attrext')
        ydataproc = None        # function for ydata processing (cannot work together with 'attrext')
        cdataproc = None        # function for cdata processing (cannot work together with 'attrext')
        deep      = True        # deep looping over all the subjob levels

        # update keyword arguments with the given values
        if 'cattr' in keywords:     cattr     = keywords['cattr']
        if 'title' in keywords:    subtitle   = keywords['title']
        if 'xlabel' in keywords:   xlabel     = keywords['xlabel']
        if 'ylabel' in keywords:   ylabel     = keywords['ylabel']
        if 'colormap' in keywords: colormap   = keywords['colormap']
        if 'output' in keywords:   output     = keywords['output']
        if 'xattrext' in keywords: xattrext   = keywords['xattrext']
        if 'yattrext' in keywords: yattrext   = keywords['yattrext']
        if 'cattrext' in keywords: cattrext   = keywords['cattrext']
        if 'xdataproc' in keywords: xdataproc = keywords['xdataproc']
        if 'ydataproc' in keywords: ydataproc = keywords['ydataproc']
        if 'cdataproc' in keywords: cdataproc = keywords['cdataproc']
        if 'deep' in keywords:       deep     = keywords['deep']

        jlist = []
        for j in jobs:
            jlist.append(j)

        xattr_spec = self.__makeList__(xattr)
        yattr_spec = self.__makeList__(yattr)
        cattr_spec = self.__makeList__(cattr)

        # special build-in dataprocs for the CE-based bar chart
        xdataproc = self.__setDataProcessor__(xattr_spec[0],xattrext,xdataproc)
        ydataproc = self.__setDataProcessor__(yattr_spec[0],yattrext,ydataproc)
        cdataproc = self.__setDataProcessor__(cattr_spec[0],cattrext,cdataproc)

        if cattr_spec[0]:
            dataTable = getJobInfoTable(jlist,[xattr_spec[0],yattr_spec[0],cattr_spec[0]],deep)
        else:
            dataTable = getJobInfoTable(jlist,[xattr_spec[0],yattr_spec[0]],deep)
            
        # make the plot title
        title = __makePlotTitle__(len(dataTable[1:]),deep,subtitle)

        # make the plot
        self.__makeScatter__(dataTable,pltXColId=0,pltYColId=1,pltTitle=title,pltXLabel=xlabel,pltYLabel=ylabel,pltColorMap=colormap,pltOutput=output,pltXDataProc=xdataproc,pltYDataProc=ydataproc,pltCDataProc=cdataproc)

    def piechart(self,jobs,attr,**keywords):
        
        """
        The plotter's interface for generating pie chart.
        
        usage:
            >>> plotter.piechart(jobs,attr,**keywords)

            Generate a pie chart presenting the distribution of th attr among
            the given jobs.

        required arguments:
            jobs: A GANGA's job table

            attr: A string or a user defined function, the corresponding value of which
                  will be extracted from the given jobs to generate the pie chart.

        optional arguments:
            colormap: A list of values representing the colors that will be picked to
                      paint the pie chart. The supported description of color can be found 
                      at http://matplotlib.sourceforge.net/matplotlib.pylab.html#-colors

               title: A string specifying the title of the pie chart.

              output: A name of file where the pie chart plot will be exported. The format
                      is auto-determinated by the extension of the given name.

            dataproc: An user-defined function which will be applied to process the value 
                      of attr before plotting the pie chart.

             attrext: Trigger the build-in data pre-processor on the value of attr.
                      It will be override if "dataproc" argument is also specified.
                      ** Supported attrext for "backend.actualCE" and "backend.CE":
                         - "by_ce": Catagorize CE queues into CE
                         - "by_country": Catagorize CE queues into country 

                deep: Sets if looping over all the subjob levels. Default is "True"
        """

        # default keyword arguments
        subtitle = self.__defaultSubtitle__(attr)       # default subtitle
        colormap = None        # colormap
        output   = None        # the output file of the picture
        attrext  = None        # trigger the build-in data processing function 
        dataproc = None        # function for data processing (cannot work together with 'attrext')
        deep     = True        # deep looping over all the subjob levels 

        # update keyword arguments with the given values
        if 'title' in keywords:    subtitle = keywords['title']
        if 'colormap' in keywords: colormap = keywords['colormap']
        if 'output' in keywords:   output   = keywords['output']
        if 'attrext' in keywords:  attrext  = keywords['attrext']
        if 'dataproc' in keywords: dataproc = keywords['dataproc']
        if 'deep' in keywords:       deep   = keywords['deep']

        jlist = []
        for j in jobs:
            jlist.append(j)

        attr_spec = self.__makeList__(attr)

        # special build-in dataprocs for the CE-based pie chart
        dataproc  = self.__setDataProcessor__(attr,attrext,dataproc)
        dataTable = getJobInfoTable(jlist,attr_spec,deep)

        # make the plot title
        title = __makePlotTitle__(len(dataTable[1:]),deep,subtitle)

        # make the plot
        self.__makePieChart__(dataTable,pltColId=0,pltTitle=title,pltColorMap=colormap,pltOutput=output,pltDataProc=dataproc)

    def histogram(self,jobs,attr,**keywords):

        """
        The plotter's interface for generating one or multiple histograms in one chart.

        usage:
            >>> plotter.histogram(jobs,attr,**keywords)

            Generate a histogram catogorized by the attr among the given jobs.

        required arguments:
            jobs: A GANGA's job table

            attr: A string or a user defined function, the corresponding value of which
                  will be extracted from the given jobs to generate the histogram.

        optional arguments:
            colormap: A list of values representing the colors that will be picked to
                      paint the histogram bars. The supported description of color can be found
                      at http://matplotlib.sourceforge.net/matplotlib.pylab.html#-colors

           normalize: Sets if doing histogram normalization. Default is "False"

             numbins: Sets the number of bins in the histogram. Default is 50

               title: A string specifying the title of the histogram.

              xlabel: A string specifying the xlabel of the histogram.

               label: A string specifying the lable of the histogram displayed on the legend.

                xmin: A number specifying the lower bound of the histogram range.
    
                xmax: A number specifying the upper bound of the histogram range.

              output: A name of file where the pie chart plot will be exported. The format
                      is auto-determinated by the extension of the given name.

            dataproc: An user-defined function which will be applied to process the value
                      of attr before plotting the histogram.

                deep: Sets if looping over all the subjob levels. Default is "True"
        """

        # default keyword arguments
        subtitle  = self.__defaultSubtitle__(attr)       # default subtitle
        xlabel    = self.__defaultSubtitle__(attr)       # default xlabel
        label     = self.__defaultSubtitle__(attr)       # default label
        colormap  = None        # colormap
        output    = None        # the output file of the picture
        attrext   = None        # trigger the build-in data processing function
        dataproc  = None        # function for data processing (cannot work together with 'attrext')
        deep      = True        # deep looping over all the subjob levels
        normalize = False       # histogram normalization
        numbins   = 50          # number of bins in the histogram
        xmin      = -1          # lower bound of the histogram 
        xmax      = -1          # upper bound of the histogram

        # update keyword arguments with the given values
        if 'title' in keywords:    subtitle  = keywords['title']
        if 'colormap' in keywords: colormap  = keywords['colormap']
        if 'xlabel' in keywords:    xlabel   = keywords['xlabel']
        if 'label' in keywords:     label    = keywords['label']
        if 'output' in keywords:   output    = keywords['output']
        if 'dataproc' in keywords: dataproc  = keywords['dataproc']
        if 'deep' in keywords:       deep    = keywords['deep']
        if 'normalize' in keywords: normalize= keywords['normalize']
        if 'numbins' in keywords:   numbins  = keywords['numbins']
        if 'xmin' in keywords:      xmin     = keywords['xmin']
        if 'xmax' in keywords:      xmax     = keywords['xmax']

        jlist = []
        for j in jobs:
            jlist.append(j)

        attr_spec = self.__makeList__(attr)

        logger.debug('attribute specification: %s' % repr(attr_spec))

        # special build-in dataprocs for the CE-based pie chart
        dataproc  = self.__setDataProcessor__(attr,attrext,dataproc)
        dataTable = getJobInfoTable(jlist,attr_spec,deep)

        logger.debug('internal data table: %s' % repr(dataTable))

        # make the plot title
        title = __makePlotTitle__(len(dataTable[1:]),deep,subtitle)

        pltColIdList = range(len(dataTable[0]))

        if not canLoopOver(dataproc):
            dataproc = len( dataTable[0] ) * [dataproc]
        elif len(dataproc) != len(dataTable[0]):
            logger.error('dataproc requires a list with %d elements, %d are given' % (len(dataTable[0]),len(dataproc)) )
            return False

        if (not canLoopOver(label)) or (isStringLike(label)):
            label = len( dataTable[0] ) * [label]
        elif len(label) != len(dataTable[0]):
            logger.error('label requires a list with %d elements, %d are given' % (len(dataTable[0]), len(label)) )
            return False

        # make the plot
        self.__makeMultiHistograms__(dataTable,pltColIdList=pltColIdList,pltDataProcList=dataproc,pltLabelList=label, \
                                     pltTitle=title,pltNumBins=numbins,pltRange=(xmin,xmax), pltXLabel=xlabel, \
                                     pltColorMap=colormap,pltNormalize=normalize, \
                                     pltOutput=output)

