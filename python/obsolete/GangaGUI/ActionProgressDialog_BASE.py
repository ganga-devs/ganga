# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/ActionProgressDialog_BASE.ui'
#
# Created: Tue May 8 12:38:15 2007
#      by: The PyQt User Interface Compiler (pyuic) 3-snapshot-20061021
#
# WARNING! All changes made in this file will be lost!


from qt import *


class ActionProgressDialog_BASE(QDialog):
    def __init__(self,parent = None,name = None,modal = 0,fl = 0):
        QDialog.__init__(self,parent,name,modal,fl)

        if not name:
            self.setName("ActionProgressDialog_BASE")

        self.setSizePolicy(QSizePolicy(QSizePolicy.Fixed,QSizePolicy.Fixed,0,0,self.sizePolicy().hasHeightForWidth()))
        self.setMinimumSize(QSize(350,280))
        self.setMaximumSize(QSize(350,280))
        self.setSizeGripEnabled(1)
        self.setModal(0)

        ActionProgressDialog_BASELayout = QVBoxLayout(self,11,6,"ActionProgressDialog_BASELayout")

        self.pushButton_Close = QPushButton(self,"pushButton_Close")
        self.pushButton_Close.setSizePolicy(QSizePolicy(QSizePolicy.Maximum,QSizePolicy.Fixed,0,0,self.pushButton_Close.sizePolicy().hasHeightForWidth()))
        self.pushButton_Close.setFocusPolicy(QPushButton.StrongFocus)
        ActionProgressDialog_BASELayout.addWidget(self.pushButton_Close)

        self.languageChange()

        self.resize(QSize(350,280).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)


    def languageChange(self):
        self.setCaption(self.__tr("Action progress"))
        self.pushButton_Close.setText(self.__tr("close"))


    def __tr(self,s,c = None):
        return qApp.translate("ActionProgressDialog_BASE",s,c)
