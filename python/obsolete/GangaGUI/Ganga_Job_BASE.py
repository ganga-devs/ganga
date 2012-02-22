# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/Ganga_Job_BASE.ui'
#
# Created: Wed Mar 1 23:12:11 2006
#      by: The PyQt User Interface Compiler (pyuic) 3.14.1
#
# WARNING! All changes made in this file will be lost!


from qt import *


class Ganga_Job_BASE(QWidget):
    def __init__(self,parent = None,name = None,fl = 0):
        QWidget.__init__(self,parent,name,fl)

        if not name:
            self.setName("Ganga_Job_BASE")


        Ganga_Job_BASELayout = QHBoxLayout(self,11,6,"Ganga_Job_BASELayout")

        self.frame_JobNavigator = QFrame(self,"frame_JobNavigator")
        self.frame_JobNavigator.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Expanding,1,0,self.frame_JobNavigator.sizePolicy().hasHeightForWidth()))
        self.frame_JobNavigator.setFrameShape(QFrame.NoFrame)
        self.frame_JobNavigator.setFrameShadow(QFrame.Plain)
        frame_JobNavigatorLayout = QVBoxLayout(self.frame_JobNavigator,0,0,"frame_JobNavigatorLayout")
        Ganga_Job_BASELayout.addWidget(self.frame_JobNavigator)

        self.widgetStack_JobAttributes = QWidgetStack(self,"widgetStack_JobAttributes")
        self.widgetStack_JobAttributes.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Expanding,0,0,self.widgetStack_JobAttributes.sizePolicy().hasHeightForWidth()))

        self.WStackPage_X = QWidget(self.widgetStack_JobAttributes,"WStackPage_X")
        WStackPage_XLayout = QVBoxLayout(self.WStackPage_X,11,6,"WStackPage_XLayout")
        self.widgetStack_JobAttributes.addWidget(self.WStackPage_X,0)
        Ganga_Job_BASELayout.addWidget(self.widgetStack_JobAttributes)

        self.languageChange()

        self.resize(QSize(600,480).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)


    def languageChange(self):
        self.setCaption(self.__tr("Ganga Job"))


    def __tr(self,s,c = None):
        return qApp.translate("Ganga_Job_BASE",s,c)
