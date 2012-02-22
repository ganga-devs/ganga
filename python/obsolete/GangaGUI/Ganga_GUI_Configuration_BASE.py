# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/Ganga_GUI_Configuration_BASE.ui'
#
# Created: Tue Feb 28 12:13:30 2006
#      by: The PyQt User Interface Compiler (pyuic) 3.14.1
#
# WARNING! All changes made in this file will be lost!


from qt import *


class GUI_Configuration_BASE(QDialog):
    def __init__(self,parent = None,name = None,modal = 0,fl = 0):
        QDialog.__init__(self,parent,name,modal,fl)

        if not name:
            self.setName("GUI_Configuration_BASE")

        self.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,QSizePolicy.Preferred,0,0,self.sizePolicy().hasHeightForWidth()))
        self.setModal(1)

        GUI_Configuration_BASELayout = QGridLayout(self,1,1,11,6,"GUI_Configuration_BASELayout")

        self.pushButton_Cancel = QPushButton(self,"pushButton_Cancel")

        GUI_Configuration_BASELayout.addWidget(self.pushButton_Cancel,1,2)

        self.pushButton_Apply = QPushButton(self,"pushButton_Apply")

        GUI_Configuration_BASELayout.addWidget(self.pushButton_Apply,1,1)
        spacer1 = QSpacerItem(182,16,QSizePolicy.Expanding,QSizePolicy.Minimum)
        GUI_Configuration_BASELayout.addItem(spacer1,1,0)

        self.pushButton_Ok = QPushButton(self,"pushButton_Ok")
        self.pushButton_Ok.setDefault(1)

        GUI_Configuration_BASELayout.addWidget(self.pushButton_Ok,1,3)

        self.tabWidget_GUIConfig = QTabWidget(self,"tabWidget_GUIConfig")
        self.tabWidget_GUIConfig.setSizePolicy(QSizePolicy(QSizePolicy.Ignored,QSizePolicy.Expanding,0,0,self.tabWidget_GUIConfig.sizePolicy().hasHeightForWidth()))

        self.tab_General = QWidget(self.tabWidget_GUIConfig,"tab_General")
        self.tabWidget_GUIConfig.insertTab(self.tab_General,QString.fromLatin1(""))

        self.tab_Monitoring = QWidget(self.tabWidget_GUIConfig,"tab_Monitoring")
        self.tabWidget_GUIConfig.insertTab(self.tab_Monitoring,QString.fromLatin1(""))

        self.tab_JobBuilder = QWidget(self.tabWidget_GUIConfig,"tab_JobBuilder")
        self.tabWidget_GUIConfig.insertTab(self.tab_JobBuilder,QString.fromLatin1(""))

        self.tab_Scriptor = QWidget(self.tabWidget_GUIConfig,"tab_Scriptor")
        self.tabWidget_GUIConfig.insertTab(self.tab_Scriptor,QString.fromLatin1(""))

        self.tab_Log = QWidget(self.tabWidget_GUIConfig,"tab_Log")
        self.tabWidget_GUIConfig.insertTab(self.tab_Log,QString.fromLatin1(""))

        self.tab_LogicalFolder = QWidget(self.tabWidget_GUIConfig,"tab_LogicalFolder")
        self.tabWidget_GUIConfig.insertTab(self.tab_LogicalFolder,QString.fromLatin1(""))

        GUI_Configuration_BASELayout.addMultiCellWidget(self.tabWidget_GUIConfig,0,0,0,3)

        self.languageChange()

        self.resize(QSize(611,359).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.pushButton_Apply,SIGNAL("clicked()"),self.slotApply)
        self.connect(self.pushButton_Cancel,SIGNAL("clicked()"),self.reject)
        self.connect(self.tabWidget_GUIConfig,SIGNAL("currentChanged(QWidget*)"),self.slotTabChanged)
        self.connect(self.pushButton_Ok,SIGNAL("clicked()"),self.accept)

        self.setTabOrder(self.tabWidget_GUIConfig,self.pushButton_Apply)
        self.setTabOrder(self.pushButton_Apply,self.pushButton_Cancel)
        self.setTabOrder(self.pushButton_Cancel,self.pushButton_Ok)


    def languageChange(self):
        self.setCaption(self.__tr("GUI Configuration"))
        self.pushButton_Cancel.setText(self.__tr("Cancel"))
        self.pushButton_Apply.setText(self.__tr("Apply"))
        self.pushButton_Ok.setText(self.__tr("Ok"))
        self.tabWidget_GUIConfig.changeTab(self.tab_General,self.__tr("General"))
        self.tabWidget_GUIConfig.changeTab(self.tab_Monitoring,self.__tr("Monitoring"))
        self.tabWidget_GUIConfig.changeTab(self.tab_JobBuilder,self.__tr("Job Builder"))
        self.tabWidget_GUIConfig.changeTab(self.tab_Scriptor,self.__tr("Scriptor"))
        self.tabWidget_GUIConfig.changeTab(self.tab_Log,self.__tr("Log"))
        self.tabWidget_GUIConfig.changeTab(self.tab_LogicalFolder,self.__tr("Logical Folder"))


    def updateConfig(self):
        print "GUI_Configuration_BASE.updateConfig(): Not implemented yet"

    def slotApply(self):
        print "GUI_Configuration_BASE.slotApply(): Not implemented yet"

    def slotTabChanged(self,a0):
        print "GUI_Configuration_BASE.slotTabChanged(QWidget*): Not implemented yet"

    def __tr(self,s,c = None):
        return qApp.translate("GUI_Configuration_BASE",s,c)
