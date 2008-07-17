# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/Ganga_Log_BASE.ui'
#
# Created: Tue May 23 12:59:30 2006
#      by: The PyQt User Interface Compiler (pyuic) 3.14.1
#
# WARNING! All changes made in this file will be lost!


from qt import *


class Ganga_Log_BASE(QWidget):
    def __init__(self,parent = None,name = None,fl = 0):
        QWidget.__init__(self,parent,name,fl)

        if not name:
            self.setName("Ganga_Log_BASE")


        Ganga_Log_BASELayout = QVBoxLayout(self,0,0,"Ganga_Log_BASELayout")

        self.textEdit_Log = QTextEdit(self,"textEdit_Log")
        self.textEdit_Log.setTextFormat(QTextEdit.PlainText)
        self.textEdit_Log.setLinkUnderline(0)
        self.textEdit_Log.setReadOnly(1)
        self.textEdit_Log.setUndoRedoEnabled(0)
        self.textEdit_Log.setAutoFormatting(QTextEdit.AutoNone)
        Ganga_Log_BASELayout.addWidget(self.textEdit_Log)

        self.languageChange()

        self.resize(QSize(485,331).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)


    def languageChange(self):
        self.setCaption(self.__tr("Log"))


    def __tr(self,s,c = None):
        return qApp.translate("Ganga_Log_BASE",s,c)
