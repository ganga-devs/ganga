# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/Ganga_Scriptor_BASE.ui'
#
# Created: Mon Jan 30 12:08:19 2006
#      by: The PyQt User Interface Compiler (pyuic) 3.14.1
#
# WARNING! All changes made in this file will be lost!


from qt import *


class Ganga_Scriptor_BASE(QWidget):
    def __init__(self,parent = None,name = None,fl = 0):
        QWidget.__init__(self,parent,name,fl)

        if not name:
            self.setName("Ganga_Scriptor_BASE")

        self.setFocusPolicy(QWidget.StrongFocus)
        self.setAcceptDrops(1)

        Ganga_Scriptor_BASELayout = QVBoxLayout(self,0,0,"Ganga_Scriptor_BASELayout")

        self.splitter_ScriptorVertical = QSplitter(self,"splitter_ScriptorVertical")
        self.splitter_ScriptorVertical.setOrientation(QSplitter.Vertical)

        self.splitter_ScriptorHorizontal = QSplitter(self.splitter_ScriptorVertical,"splitter_ScriptorHorizontal")
        self.splitter_ScriptorHorizontal.setOrientation(QSplitter.Horizontal)

        self.listView_ActiveScripts = QListView(self.splitter_ScriptorHorizontal,"listView_ActiveScripts")
        self.listView_ActiveScripts.addColumn(self.__tr("Script"))
        self.listView_ActiveScripts.addColumn(self.__tr("Description"))
        self.listView_ActiveScripts.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Expanding,1,0,self.listView_ActiveScripts.sizePolicy().hasHeightForWidth()))
        self.listView_ActiveScripts.setFocusPolicy(QListView.StrongFocus)
        self.listView_ActiveScripts.setAcceptDrops(1)
        self.listView_ActiveScripts.setResizePolicy(QScrollView.Manual)
        self.listView_ActiveScripts.setAllColumnsShowFocus(1)
        self.listView_ActiveScripts.setShowSortIndicator(1)
        self.listView_ActiveScripts.setRootIsDecorated(0)
        self.listView_ActiveScripts.setDefaultRenameAction(QListView.Accept)

        self.splitter_ButtonHiding = QSplitter(self.splitter_ScriptorHorizontal,"splitter_ButtonHiding")
        self.splitter_ButtonHiding.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,QSizePolicy.Expanding,2,0,self.splitter_ButtonHiding.sizePolicy().hasHeightForWidth()))
        self.splitter_ButtonHiding.setOrientation(QSplitter.Vertical)

        self.textEdit = QTextEdit(self.splitter_ButtonHiding,"textEdit")
        self.textEdit.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,QSizePolicy.Expanding,0,1,self.textEdit.sizePolicy().hasHeightForWidth()))
        self.textEdit.setCursor(QCursor(4))
        self.textEdit.setFocusPolicy(QTextEdit.StrongFocus)
        self.textEdit.setAcceptDrops(1)
        self.textEdit.setTextFormat(QTextEdit.PlainText)
        self.textEdit.setLinkUnderline(0)
        self.textEdit.setWordWrap(QTextEdit.NoWrap)

        self.pushButton_ExecScript = QPushButton(self.splitter_ButtonHiding,"pushButton_ExecScript")
        self.pushButton_ExecScript.setSizePolicy(QSizePolicy(QSizePolicy.MinimumExpanding,QSizePolicy.Fixed,0,0,self.pushButton_ExecScript.sizePolicy().hasHeightForWidth()))
        Ganga_Scriptor_BASELayout.addWidget(self.splitter_ScriptorVertical)

        self.languageChange()

        self.resize(QSize(632,289).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.listView_ActiveScripts,SIGNAL("doubleClicked(QListViewItem*)"),self.pushButton_ExecScript.animateClick)
        self.connect(self.listView_ActiveScripts,SIGNAL("returnPressed(QListViewItem*)"),self.pushButton_ExecScript.animateClick)


    def languageChange(self):
        self.setCaption(self.__tr("Scriptor"))
        self.listView_ActiveScripts.header().setLabel(0,self.__tr("Script"))
        self.listView_ActiveScripts.header().setLabel(1,self.__tr("Description"))
        QToolTip.add(self.listView_ActiveScripts,self.__tr("'Favourites' script list."))
        QToolTip.add(self.textEdit,self.__tr("Entire code snippet will be run when Execute button is pressed."))
        self.pushButton_ExecScript.setText(self.__tr("Execute"))
        QToolTip.add(self.pushButton_ExecScript,self.__tr("Run script currently in the scriptor window."))


    def __tr(self,s,c = None):
        return qApp.translate("Ganga_Scriptor_BASE",s,c)
