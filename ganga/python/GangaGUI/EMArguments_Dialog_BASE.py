# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/EMArguments_Dialog_BASE.ui'
#
# Created: Thu Nov 23 12:42:01 2006
#      by: The PyQt User Interface Compiler (pyuic) snapshot-20040627
#
# WARNING! All changes made in this file will be lost!


from qt import *


class EMArguments_Dialog_BASE(QDialog):
    def __init__(self,parent = None,name = None,modal = 0,fl = 0):
        QDialog.__init__(self,parent,name,modal,fl)

        if not name:
            self.setName("EMArguments_Dialog_BASE")

        self.setModal(1)

        EMArguments_Dialog_BASELayout = QGridLayout(self,1,1,11,6,"EMArguments_Dialog_BASELayout")

        self.textLabel_FuncDef = QLabel(self,"textLabel_FuncDef")
        self.textLabel_FuncDef.setTextFormat(QLabel.PlainText)

        EMArguments_Dialog_BASELayout.addMultiCellWidget(self.textLabel_FuncDef,0,0,0,2)

        self.listView_Arguments = QListView(self,"listView_Arguments")
        self.listView_Arguments.addColumn(self.__tr("Argument"))
        self.listView_Arguments.header().setClickEnabled(0,self.listView_Arguments.header().count() - 1)
        self.listView_Arguments.header().setResizeEnabled(0,self.listView_Arguments.header().count() - 1)
        self.listView_Arguments.addColumn(self.__tr("Value"))
        self.listView_Arguments.header().setClickEnabled(0,self.listView_Arguments.header().count() - 1)
        self.listView_Arguments.header().setResizeEnabled(0,self.listView_Arguments.header().count() - 1)
        self.listView_Arguments.setAllColumnsShowFocus(1)
        self.listView_Arguments.setItemMargin(4)
        self.listView_Arguments.setResizeMode(QListView.LastColumn)
        self.listView_Arguments.setDefaultRenameAction(QListView.Accept)

        EMArguments_Dialog_BASELayout.addMultiCellWidget(self.listView_Arguments,1,1,0,2)

        self.pushButton_Ok = QPushButton(self,"pushButton_Ok")

        EMArguments_Dialog_BASELayout.addWidget(self.pushButton_Ok,2,2)

        self.pushButton_Cancel = QPushButton(self,"pushButton_Cancel")

        EMArguments_Dialog_BASELayout.addWidget(self.pushButton_Cancel,2,1)
        spacer1 = QSpacerItem(181,20,QSizePolicy.Expanding,QSizePolicy.Minimum)
        EMArguments_Dialog_BASELayout.addItem(spacer1,2,0)

        self.languageChange()

        self.resize(QSize(475,156).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.pushButton_Ok,SIGNAL("clicked()"),self,SLOT("accept()"))
        self.connect(self.pushButton_Cancel,SIGNAL("clicked()"),self,SLOT("reject()"))


    def languageChange(self):
        self.setCaption(self.__tr("Export method argument list"))
        self.textLabel_FuncDef.setText(QString.null)
        self.listView_Arguments.header().setLabel(0,self.__tr("Argument"))
        self.listView_Arguments.header().setLabel(1,self.__tr("Value"))
        self.pushButton_Ok.setText(self.__tr("Ok"))
        self.pushButton_Cancel.setText(self.__tr("Cancel"))


    def __tr(self,s,c = None):
        return qApp.translate("EMArguments_Dialog_BASE",s,c)
