# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/inputlist_widget_BASE.ui'
#
# Created: Wed Aug 8 19:24:41 2007
#      by: The PyQt User Interface Compiler (pyuic) 3-snapshot-20061021
#
# WARNING! All changes made in this file will be lost!


from qt import *


class InputList_Widget_BASE(QWidget):
    def __init__(self,parent = None,name = None,fl = 0):
        QWidget.__init__(self,parent,name,fl)

        if not name:
            self.setName("InputList_Widget_BASE")

        self.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Preferred,0,0,self.sizePolicy().hasHeightForWidth()))

        InputList_Widget_BASELayout = QHBoxLayout(self,11,6,"InputList_Widget_BASELayout")

        self.listView = QListView(self,"listView")
        self.listView.addColumn(self.__tr("List"))
        self.listView.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Expanding,0,0,self.listView.sizePolicy().hasHeightForWidth()))
        self.listView.setFocusPolicy(QListView.StrongFocus)
        self.listView.setAcceptDrops(1)
        self.listView.setFrameShape(QListView.StyledPanel)
        self.listView.setFrameShadow(QListView.Sunken)
        self.listView.setSelectionMode(QListView.Extended)
        self.listView.setShowSortIndicator(1)
        self.listView.setShowToolTips(0)
        self.listView.setResizeMode(QListView.AllColumns)
        self.listView.setDefaultRenameAction(QListView.Accept)
        InputList_Widget_BASELayout.addWidget(self.listView)

        layout4 = QVBoxLayout(None,0,0,"layout4")

        self.pushButton_Insert = QPushButton(self,"pushButton_Insert")
        self.pushButton_Insert.setFocusPolicy(QPushButton.StrongFocus)
        layout4.addWidget(self.pushButton_Insert)

        self.pushButton_Browse = QPushButton(self,"pushButton_Browse")
        layout4.addWidget(self.pushButton_Browse)

        self.pushButton_Edit = QPushButton(self,"pushButton_Edit")
        layout4.addWidget(self.pushButton_Edit)

        self.pushButton_Delete = QPushButton(self,"pushButton_Delete")
        layout4.addWidget(self.pushButton_Delete)

        self.pushButton_Revert = QPushButton(self,"pushButton_Revert")
        layout4.addWidget(self.pushButton_Revert)

        self.pushButton_Custom = QPushButton(self,"pushButton_Custom")
        self.pushButton_Custom.setSizePolicy(QSizePolicy(QSizePolicy.Minimum,QSizePolicy.Fixed,0,0,self.pushButton_Custom.sizePolicy().hasHeightForWidth()))
        layout4.addWidget(self.pushButton_Custom)
        spacer4 = QSpacerItem(20,20,QSizePolicy.Minimum,QSizePolicy.Expanding)
        layout4.addItem(spacer4)
        InputList_Widget_BASELayout.addLayout(layout4)

        self.languageChange()

        self.resize(QSize(212,217).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.pushButton_Insert,SIGNAL("clicked()"),self.slotInsert)
        self.connect(self.pushButton_Delete,SIGNAL("clicked()"),self.slotDelete)
        self.connect(self.pushButton_Revert,SIGNAL("clicked()"),self.slotRevert)
        self.connect(self.pushButton_Custom,SIGNAL("clicked()"),self.slotCustom)
        self.connect(self.pushButton_Browse,SIGNAL("clicked()"),self.slotBrowse)

        self.setTabOrder(self.listView,self.pushButton_Insert)
        self.setTabOrder(self.pushButton_Insert,self.pushButton_Edit)
        self.setTabOrder(self.pushButton_Edit,self.pushButton_Delete)
        self.setTabOrder(self.pushButton_Delete,self.pushButton_Revert)


    def languageChange(self):
        self.setCaption(self.__tr("InputList_Widget_BASE"))
        self.listView.header().setLabel(0,self.__tr("List"))
        self.pushButton_Insert.setText(self.__tr("Add"))
        self.pushButton_Browse.setText(self.__tr("Browse"))
        self.pushButton_Edit.setText(self.__tr("Edit"))
        self.pushButton_Delete.setText(self.__tr("Delete"))
        self.pushButton_Revert.setText(self.__tr("Revert"))
        self.pushButton_Custom.setText(QString.null)


    def slotInsert(self):
        print "InputList_Widget_BASE.slotInsert(): Not implemented yet"

    def slotDelete(self):
        print "InputList_Widget_BASE.slotDelete(): Not implemented yet"

    def slotRevert(self):
        print "InputList_Widget_BASE.slotRevert(): Not implemented yet"

    def slotCustom(self):
        print "InputList_Widget_BASE.slotCustom(): Not implemented yet"

    def slotBrowse(self):
        print "InputList_Widget_BASE.slotBrowse(): Not implemented yet"

    def __tr(self,s,c = None):
        return qApp.translate("InputList_Widget_BASE",s,c)
