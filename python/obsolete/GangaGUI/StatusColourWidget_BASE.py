# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UIs/StatusColourWidget_BASE.ui'
#
# Created: Wed Mar 1 16:55:28 2006
#      by: The PyQt User Interface Compiler (pyuic) 3.14.1
#
# WARNING! All changes made in this file will be lost!


from qt import *


class StatusColourWidget_BASE(QWidget):
    def __init__(self,parent = None,name = None,fl = 0):
        QWidget.__init__(self,parent,name,fl)

        if not name:
            self.setName("StatusColourWidget_BASE")


        StatusColourWidget_BASELayout = QVBoxLayout(self,0,6,"StatusColourWidget_BASELayout")

        self.groupBox_StatusColour = QGroupBox(self,"groupBox_StatusColour")
        self.groupBox_StatusColour.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,QSizePolicy.Fixed,0,0,self.groupBox_StatusColour.sizePolicy().hasHeightForWidth()))
        self.groupBox_StatusColour.setColumnLayout(0,Qt.Vertical)
        self.groupBox_StatusColour.layout().setSpacing(6)
        self.groupBox_StatusColour.layout().setMargin(11)
        groupBox_StatusColourLayout = QHBoxLayout(self.groupBox_StatusColour.layout())
        groupBox_StatusColourLayout.setAlignment(Qt.AlignTop)

        self.frame_StatusColourLV = QFrame(self.groupBox_StatusColour,"frame_StatusColourLV")
        self.frame_StatusColourLV.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,QSizePolicy.Preferred,0,0,self.frame_StatusColourLV.sizePolicy().hasHeightForWidth()))
        frame_StatusColourLVLayout = QVBoxLayout(self.frame_StatusColourLV,11,6,"frame_StatusColourLVLayout")
        groupBox_StatusColourLayout.addWidget(self.frame_StatusColourLV)

        self.frame_SubjobSliders = QFrame(self.groupBox_StatusColour,"frame_SubjobSliders")
        self.frame_SubjobSliders.setSizePolicy(QSizePolicy(QSizePolicy.Preferred,QSizePolicy.Preferred,1,0,self.frame_SubjobSliders.sizePolicy().hasHeightForWidth()))
        frame_SubjobSlidersLayout = QVBoxLayout(self.frame_SubjobSliders,0,0,"frame_SubjobSlidersLayout")

        self.textLabel2 = QLabel(self.frame_SubjobSliders,"textLabel2")
        frame_SubjobSlidersLayout.addWidget(self.textLabel2)

        self.slider_Foreground = QSlider(self.frame_SubjobSliders,"slider_Foreground")
        self.slider_Foreground.setMinValue(0)
        self.slider_Foreground.setMaxValue(255)
        self.slider_Foreground.setOrientation(QSlider.Horizontal)
        self.slider_Foreground.setTickmarks(QSlider.Below)
        self.slider_Foreground.setTickInterval(20)
        frame_SubjobSlidersLayout.addWidget(self.slider_Foreground)

        layout2 = QHBoxLayout(None,0,6,"layout2")

        self.textLabel4 = QLabel(self.frame_SubjobSliders,"textLabel4")
        layout2.addWidget(self.textLabel4)
        spacer3 = QSpacerItem(70,20,QSizePolicy.Expanding,QSizePolicy.Minimum)
        layout2.addItem(spacer3)

        self.textLabel5 = QLabel(self.frame_SubjobSliders,"textLabel5")
        layout2.addWidget(self.textLabel5)
        frame_SubjobSlidersLayout.addLayout(layout2)
        spacer7 = QSpacerItem(20,21,QSizePolicy.Minimum,QSizePolicy.Fixed)
        frame_SubjobSlidersLayout.addItem(spacer7)

        self.textLabel2_2 = QLabel(self.frame_SubjobSliders,"textLabel2_2")
        frame_SubjobSlidersLayout.addWidget(self.textLabel2_2)

        self.slider_Background = QSlider(self.frame_SubjobSliders,"slider_Background")
        self.slider_Background.setMinValue(0)
        self.slider_Background.setMaxValue(255)
        self.slider_Background.setOrientation(QSlider.Horizontal)
        self.slider_Background.setTickmarks(QSlider.Below)
        self.slider_Background.setTickInterval(20)
        frame_SubjobSlidersLayout.addWidget(self.slider_Background)

        layout3 = QHBoxLayout(None,0,6,"layout3")

        self.textLabel4_2 = QLabel(self.frame_SubjobSliders,"textLabel4_2")
        layout3.addWidget(self.textLabel4_2)
        spacer3_4 = QSpacerItem(70,20,QSizePolicy.Expanding,QSizePolicy.Minimum)
        layout3.addItem(spacer3_4)

        self.textLabel5_2 = QLabel(self.frame_SubjobSliders,"textLabel5_2")
        layout3.addWidget(self.textLabel5_2)
        frame_SubjobSlidersLayout.addLayout(layout3)
        spacer8 = QSpacerItem(20,16,QSizePolicy.Minimum,QSizePolicy.Expanding)
        frame_SubjobSlidersLayout.addItem(spacer8)
        groupBox_StatusColourLayout.addWidget(self.frame_SubjobSliders)
        StatusColourWidget_BASELayout.addWidget(self.groupBox_StatusColour)

        self.languageChange()

        self.resize(QSize(257,192).expandedTo(self.minimumSizeHint()))
        self.clearWState(Qt.WState_Polished)

        self.connect(self.slider_Background,SIGNAL("valueChanged(int)"),self.slotBGSliderMoved)
        self.connect(self.slider_Foreground,SIGNAL("valueChanged(int)"),self.slotFGSliderMoved)


    def languageChange(self):
        self.setCaption(self.__tr("Monitoring Status Customisation"))
        self.groupBox_StatusColour.setTitle(self.__tr("Status_Colour_Schema"))
        self.textLabel2.setText(self.__tr("SubJob status foreground mask"))
        self.textLabel4.setText(self.__tr("darker"))
        self.textLabel5.setText(self.__tr("lighter"))
        self.textLabel2_2.setText(self.__tr("SubJob status background mask"))
        self.textLabel4_2.setText(self.__tr("darker"))
        self.textLabel5_2.setText(self.__tr("lighter"))


    def slotBGSliderMoved(self,a0):
        print "StatusColourWidget_BASE.slotBGSliderMoved(int): Not implemented yet"

    def slotFGSliderMoved(self,a0):
        print "StatusColourWidget_BASE.slotFGSliderMoved(int): Not implemented yet"

    def __tr(self,s,c = None):
        return qApp.translate("StatusColourWidget_BASE",s,c)
