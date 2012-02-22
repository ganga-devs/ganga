# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/inputchoice_widget_BASE.ui'
#
# Created: Tue May 8 12:38:56 2007
#      by: The PyQt User Interface Compiler (pyuic) 3-snapshot-20061021
#
# WARNING! All changes made in this file will be lost!


from qt import *


class InputChoice_Widget_BASE(QWidget):
    def __init__(self,parent = None,name = None,fl = 0):
        QWidget.__init__(self,parent,name,fl)

        if not name:
            self.setName("InputChoice_Widget_BASE")

        self.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Preferred,0,0,self.sizePolicy().hasHeightForWidth()))

        InputChoice_Widget_BASELayout = QGridLayout(self,1,1,11,6,"InputChoice_Widget_BASELayout")
        spacer1 = QSpacerItem(0,20,QSizePolicy.MinimumExpanding,QSizePolicy.Minimum)
        InputChoice_Widget_BASELayout.addItem(spacer1,0,1)

        self.textLabel = QLabel(self,"textLabel")

        InputChoice_Widget_BASELayout.addWidget(self.textLabel,0,0)

        self.comboBox = QComboBox(0,self,"comboBox")
        self.comboBox.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Fixed,0,0,self.comboBox.sizePolicy().hasHeightForWidth()))
        self.comboBox.setAutoCompletion(1)
        self.comboBox.setDuplicatesEnabled(0)

        InputChoice_Widget_BASELayout.addMultiCellWidget(self.comboBox,1,1,0,2)

        self.pushButton_Revert = QPushButton(self,"pushButton_Revert")

        InputChoice_Widget_BASELayout.addWidget(self.pushButton_Revert,0,2)

        self.languageChange()

        self.resize(QSize(174,81).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.pushButton_Revert,SIGNAL("clicked()"),self.slotRevert)


    def languageChange(self):
        self.setCaption(self.__tr("InputChoice_Widget_BASE"))
        self.textLabel.setText(self.__tr("Attribute:"))
        self.pushButton_Revert.setText(self.__tr("Revert"))


    def slotRevert(self):
        print "InputChoice_Widget_BASE.slotRevert(): Not implemented yet"

    def __tr(self,s,c = None):
        return qApp.translate("InputChoice_Widget_BASE",s,c)
