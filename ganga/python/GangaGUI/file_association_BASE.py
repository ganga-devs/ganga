# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/file_association_BASE.ui'
#
# Created: Wed Mar 8 11:30:59 2006
#      by: The PyQt User Interface Compiler (pyuic) 3.14.1
#
# WARNING! All changes made in this file will be lost!


from qt import *


class File_Association_BASE(QWidget):
    def __init__(self,parent = None,name = None,fl = 0):
        QWidget.__init__(self,parent,name,fl)

        if not name:
            self.setName("File_Association_BASE")


        File_Association_BASELayout = QGridLayout(self,1,1,11,6,"File_Association_BASELayout")

        layout1 = QVBoxLayout(None,0,6,"layout1")

        self.pushButton_Add = QPushButton(self,"pushButton_Add")
        layout1.addWidget(self.pushButton_Add)

        self.pushButton_Del = QPushButton(self,"pushButton_Del")
        layout1.addWidget(self.pushButton_Del)
        spacer1 = QSpacerItem(20,81,QSizePolicy.Minimum,QSizePolicy.Expanding)
        layout1.addItem(spacer1)

        File_Association_BASELayout.addLayout(layout1,1,1)

        self.textLabel = QLabel(self,"textLabel")

        File_Association_BASELayout.addWidget(self.textLabel,0,0)

        self.listView = QListView(self,"listView")
        self.listView.addColumn(self.__tr("GUI"))
        self.listView.addColumn(self.__tr("File extension"))
        self.listView.addColumn(self.__tr("Associated external application"))
        self.listView.setAllColumnsShowFocus(1)
        self.listView.setShowSortIndicator(1)

        File_Association_BASELayout.addWidget(self.listView,1,0)

        self.languageChange()

        self.resize(QSize(423,162).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)


    def languageChange(self):
        self.setCaption(self.__tr("File_Association"))
        self.pushButton_Add.setText(self.__tr("Add"))
        self.pushButton_Del.setText(self.__tr("Delete"))
        self.textLabel.setText(self.__tr("File_Association"))
        self.listView.header().setLabel(0,self.__tr("GUI"))
        self.listView.header().setLabel(1,self.__tr("File extension"))
        self.listView.header().setLabel(2,self.__tr("Associated external application"))
        QToolTip.add(self.listView,self.__tr("Check the checkbox to start the external application as a GUI application."))


    def __tr(self,s,c = None):
        return qApp.translate("File_Association_BASE",s,c)
