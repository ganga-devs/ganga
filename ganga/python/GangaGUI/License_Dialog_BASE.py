# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/License_Dialog_BASE.ui'
#
# Created: Mon May 14 17:33:15 2007
#      by: The PyQt User Interface Compiler (pyuic) 3-snapshot-20061021
#
# WARNING! All changes made in this file will be lost!


from qt import *


class License_Dialog_BASE(QDialog):
    def __init__(self,parent = None,name = None,modal = 0,fl = 0):
        QDialog.__init__(self,parent,name,modal,fl)

        if not name:
            self.setName("License_Dialog_BASE")

        self.setModal(1)

        License_Dialog_BASELayout = QGridLayout(self,1,1,11,6,"License_Dialog_BASELayout")

        self.textEdit = QTextEdit(self,"textEdit")
        self.textEdit.setTextFormat(QTextEdit.PlainText)
        self.textEdit.setReadOnly(1)
        self.textEdit.setUndoRedoEnabled(0)
        self.textEdit.setAutoFormatting(QTextEdit.AutoNone)

        License_Dialog_BASELayout.addMultiCellWidget(self.textEdit,0,0,0,2)

        self.pushButton_Ok = QPushButton(self,"pushButton_Ok")
        self.pushButton_Ok.setDefault(1)

        License_Dialog_BASELayout.addWidget(self.pushButton_Ok,1,1)
        spacer2 = QSpacerItem(0,20,QSizePolicy.Expanding,QSizePolicy.Minimum)
        License_Dialog_BASELayout.addItem(spacer2,1,2)
        spacer1 = QSpacerItem(0,20,QSizePolicy.Expanding,QSizePolicy.Minimum)
        License_Dialog_BASELayout.addItem(spacer1,1,0)

        self.languageChange()

        self.resize(QSize(396,416).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.pushButton_Ok,SIGNAL("clicked()"),self.close)

        self.setTabOrder(self.pushButton_Ok,self.textEdit)


    def languageChange(self):
        self.setCaption(self.__tr("GANGA License Information"))
        self.pushButton_Ok.setText(self.__tr("Ok"))


    def __tr(self,s,c = None):
        return qApp.translate("License_Dialog_BASE",s,c)
