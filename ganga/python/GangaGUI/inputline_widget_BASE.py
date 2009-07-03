# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/inputline_widget_BASE.ui'
#
# Created: Tue May 8 12:38:40 2007
#      by: The PyQt User Interface Compiler (pyuic) 3-snapshot-20061021
#
# WARNING! All changes made in this file will be lost!


from qt import *


class InputLine_Widget_BASE(QWidget):
    def __init__(self,parent = None,name = None,fl = 0):
        QWidget.__init__(self,parent,name,fl)

        if not name:
            self.setName("InputLine_Widget_BASE")

        self.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Preferred,0,0,self.sizePolicy().hasHeightForWidth()))

        InputLine_Widget_BASELayout = QVBoxLayout(self,11,6,"InputLine_Widget_BASELayout")

        layout5 = QHBoxLayout(None,0,0,"layout5")

        self.textLabel = QLabel(self,"textLabel")
        layout5.addWidget(self.textLabel)
        spacer2 = QSpacerItem(0,20,QSizePolicy.MinimumExpanding,QSizePolicy.Minimum)
        layout5.addItem(spacer2)

        self.pushButton_Browse = QPushButton(self,"pushButton_Browse")
        layout5.addWidget(self.pushButton_Browse)

        self.pushButton_Edit = QPushButton(self,"pushButton_Edit")
        layout5.addWidget(self.pushButton_Edit)

        self.pushButton_Clear = QPushButton(self,"pushButton_Clear")
        layout5.addWidget(self.pushButton_Clear)

        self.pushButton_Revert = QPushButton(self,"pushButton_Revert")
        layout5.addWidget(self.pushButton_Revert)
        InputLine_Widget_BASELayout.addLayout(layout5)

        layout4 = QHBoxLayout(None,0,0,"layout4")

        self.lineEdit = QLineEdit(self,"lineEdit")
        self.lineEdit.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Fixed,1,0,self.lineEdit.sizePolicy().hasHeightForWidth()))
        self.lineEdit.setCursor(QCursor(0))
        layout4.addWidget(self.lineEdit)

        self.pushButton_Custom = QPushButton(self,"pushButton_Custom")
        layout4.addWidget(self.pushButton_Custom)
        InputLine_Widget_BASELayout.addLayout(layout4)

        self.languageChange()

        self.resize(QSize(384,90).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.pushButton_Clear,SIGNAL("clicked()"),self.slotClear)
        self.connect(self.pushButton_Revert,SIGNAL("clicked()"),self.slotRevert)
        self.connect(self.pushButton_Browse,SIGNAL("clicked()"),self.slotBrowse)
        self.connect(self.pushButton_Custom,SIGNAL("clicked()"),self.slotCustom)

        self.setTabOrder(self.lineEdit,self.pushButton_Browse)
        self.setTabOrder(self.pushButton_Browse,self.pushButton_Edit)
        self.setTabOrder(self.pushButton_Edit,self.pushButton_Clear)
        self.setTabOrder(self.pushButton_Clear,self.pushButton_Revert)


    def languageChange(self):
        self.setCaption(self.__tr("InputLine_Widget_BASE"))
        self.textLabel.setText(self.__tr("Attribute:"))
        self.pushButton_Browse.setText(self.__tr("Browse"))
        self.pushButton_Edit.setText(self.__tr("Edit"))
        self.pushButton_Clear.setText(self.__tr("Clear"))
        self.pushButton_Revert.setText(self.__tr("Revert"))
        self.pushButton_Custom.setText(QString.null)


    def slotRevert(self):
        print "InputLine_Widget_BASE.slotRevert(): Not implemented yet"

    def slotBrowse(self):
        print "InputLine_Widget_BASE.slotBrowse(): Not implemented yet"

    def slotClear(self):
        print "InputLine_Widget_BASE.slotClear(): Not implemented yet"

    def slotCustom(self):
        print "InputLine_Widget_BASE.slotCustom(): Not implemented yet"

    def __tr(self,s,c = None):
        return qApp.translate("InputLine_Widget_BASE",s,c)
