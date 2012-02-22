# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/Credentials_Dialog_BASE.ui'
#
# Created: Sat Feb 24 00:02:44 2007
#      by: The PyQt User Interface Compiler (pyuic) 3.14.1
#
# WARNING! All changes made in this file will be lost!


from qt import *


class Credentials_Dialog_BASE(QDialog):
    def __init__(self,parent = None,name = None,modal = 0,fl = 0):
        QDialog.__init__(self,parent,name,modal,fl)

        if not name:
            self.setName("Credentials_Dialog_BASE")

        self.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Maximum,0,0,self.sizePolicy().hasHeightForWidth()))
        self.setModal(1)

        Credentials_Dialog_BASELayout = QVBoxLayout(self,11,6,"Credentials_Dialog_BASELayout")
        Credentials_Dialog_BASELayout.setResizeMode(QLayout.Fixed)

        layout3 = QHBoxLayout(None,0,6,"layout3")

        self.textLabel_Attention = QLabel(self,"textLabel_Attention")
        self.textLabel_Attention.setPaletteForegroundColor(QColor(250,0,0))
        self.textLabel_Attention.setTextFormat(QLabel.PlainText)
        layout3.addWidget(self.textLabel_Attention)

        self.textLabel_AttentionMsg = QLabel(self,"textLabel_AttentionMsg")
        self.textLabel_AttentionMsg.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,QSizePolicy.Fixed,1,0,self.textLabel_AttentionMsg.sizePolicy().hasHeightForWidth()))
        self.textLabel_AttentionMsg.setTextFormat(QLabel.PlainText)
        layout3.addWidget(self.textLabel_AttentionMsg)
        Credentials_Dialog_BASELayout.addLayout(layout3)

        self.tabWidget_Credentials = QTabWidget(self,"tabWidget_Credentials")
        self.tabWidget_Credentials.setEnabled(1)
        self.tabWidget_Credentials.setSizePolicy(QSizePolicy(QSizePolicy.Expanding,QSizePolicy.Preferred,0,1,self.tabWidget_Credentials.sizePolicy().hasHeightForWidth()))
        self.tabWidget_Credentials.setFocusPolicy(QTabWidget.NoFocus)

        self.tab = QWidget(self.tabWidget_Credentials,"tab")
        tabLayout = QVBoxLayout(self.tab,11,6,"tabLayout")
        self.tabWidget_Credentials.insertTab(self.tab,QString.fromLatin1(""))
        Credentials_Dialog_BASELayout.addWidget(self.tabWidget_Credentials)

        layout8 = QHBoxLayout(None,0,6,"layout8")

        layout7 = QVBoxLayout(None,0,0,"layout7")

        self.checkBox_autorenew = QCheckBox(self,"checkBox_autorenew")
        layout7.addWidget(self.checkBox_autorenew)

        self.checkBox_credMonitoring = QCheckBox(self,"checkBox_credMonitoring")
        self.checkBox_credMonitoring.setChecked(1)
        layout7.addWidget(self.checkBox_credMonitoring)
        layout8.addLayout(layout7)
        spacer5 = QSpacerItem(16,16,QSizePolicy.Expanding,QSizePolicy.Minimum)
        layout8.addItem(spacer5)

        self.pushButton_Destroy = QPushButton(self,"pushButton_Destroy")
        self.pushButton_Destroy.setEnabled(0)
        self.pushButton_Destroy.setSizePolicy(QSizePolicy(QSizePolicy.Minimum,QSizePolicy.Fixed,0,0,self.pushButton_Destroy.sizePolicy().hasHeightForWidth()))
        self.pushButton_Destroy.setAutoDefault(0)
        layout8.addWidget(self.pushButton_Destroy)

        self.pushButton_Renew = QPushButton(self,"pushButton_Renew")
        self.pushButton_Renew.setSizePolicy(QSizePolicy(QSizePolicy.Minimum,QSizePolicy.Fixed,0,0,self.pushButton_Renew.sizePolicy().hasHeightForWidth()))
        layout8.addWidget(self.pushButton_Renew)
        Credentials_Dialog_BASELayout.addLayout(layout8)

        self.line4 = QFrame(self,"line4")
        self.line4.setFrameShape(QFrame.HLine)
        self.line4.setFrameShadow(QFrame.Sunken)
        self.line4.setFrameShape(QFrame.HLine)
        Credentials_Dialog_BASELayout.addWidget(self.line4)

        layout18 = QHBoxLayout(None,0,6,"layout18")

        self.pushButton_Close = QPushButton(self,"pushButton_Close")
        self.pushButton_Close.setSizePolicy(QSizePolicy(QSizePolicy.Minimum,QSizePolicy.Fixed,0,0,self.pushButton_Close.sizePolicy().hasHeightForWidth()))
        layout18.addWidget(self.pushButton_Close)
        spacer3_2 = QSpacerItem(40,20,QSizePolicy.Expanding,QSizePolicy.Minimum)
        layout18.addItem(spacer3_2)
        Credentials_Dialog_BASELayout.addLayout(layout18)

        self.languageChange()

        self.resize(QSize(313,202).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.pushButton_Close,SIGNAL("clicked()"),self.slotClose)
        self.connect(self.pushButton_Renew,SIGNAL("clicked()"),self.slotRenew)
        self.connect(self.checkBox_autorenew,SIGNAL("toggled(bool)"),self.slotAutoRenew)
        self.connect(self.checkBox_credMonitoring,SIGNAL("toggled(bool)"),self.slotCredMonitoring)
        self.connect(self.pushButton_Destroy,SIGNAL("clicked()"),self.slotDestroy)
        self.connect(self.tabWidget_Credentials,SIGNAL("currentChanged(QWidget*)"),self.slotUpdateButtons)

        self.setTabOrder(self.pushButton_Renew,self.checkBox_autorenew)
        self.setTabOrder(self.checkBox_autorenew,self.pushButton_Destroy)
        self.setTabOrder(self.pushButton_Destroy,self.pushButton_Close)
        self.setTabOrder(self.pushButton_Close,self.tabWidget_Credentials)


    def languageChange(self):
        self.setCaption(self.__tr("User Credentials Manager"))
        self.textLabel_Attention.setText(self.__tr("Attention: "))
        self.tabWidget_Credentials.changeTab(self.tab,self.__tr("Example Credential"))
        self.checkBox_autorenew.setText(self.__tr("Auto renew"))
        self.checkBox_credMonitoring.setText(self.__tr("Monitoring"))
        self.pushButton_Destroy.setText(self.__tr("Destroy"))
        self.pushButton_Renew.setText(self.__tr("Renew"))
        self.pushButton_Close.setText(self.__tr("Close"))


    def slotRenew(self):
        print "Credentials_Dialog_BASE.slotRenew(): Not implemented yet"

    def slotAutoRenew(self,a0):
        print "Credentials_Dialog_BASE.slotAutoRenew(bool): Not implemented yet"

    def slotCredMonitoring(self,a0):
        print "Credentials_Dialog_BASE.slotCredMonitoring(bool): Not implemented yet"

    def slotDestroy(self):
        print "Credentials_Dialog_BASE.slotDestroy(): Not implemented yet"

    def slotUpdateButtons(self,a0):
        print "Credentials_Dialog_BASE.slotUpdateButtons(QWidget*): Not implemented yet"

    def slotClose(self):
        print "Credentials_Dialog_BASE.slotClose(): Not implemented yet"

    def __tr(self,s,c = None):
        return qApp.translate("Credentials_Dialog_BASE",s,c)
