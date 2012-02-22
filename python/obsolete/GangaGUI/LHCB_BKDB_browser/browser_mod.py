import sys
import time
import os

import LHCB_BKDB

import images
from qt import *
from qttable import QTable

font_size=10

class wpage_graphics(QDialog):
	"""Wpage class: a container for all the graphical components of the interface 
	to the data base
	""" 
	def __init__(self,parent = None,name = None,modal = 0,fl=0):               
		QDialog.__init__(self,parent,name,modal,fl)
		
		self.file_image = QPixmap (images.file_image_data)
		self.dir_image = QPixmap (images.dir_image_data)
		self.cern_logo_image = QPixmap(images.cern_logo_image_data)
		self.lhcb_logo_image = QPixmap(images.lhcb_logo_image_data)
		
		if not name:
			self.setName("wpage")
		
		self.setFont(QFont("Sans", font_size))
		self.textLabelTitolo = QLabel(self,"textLabelTitolo")
		self.textLabelTitolo.setGeometry(QRect(10,0,570,39))
		
		self.Txt_event_type = QLabel(self,"Txt_event_type")        
		self.Txt_event_type.setGeometry(QRect(10,90,104,16))
		
		self.Txt_Dataset_replicated = QLabel(self,"Txt_Dataset_replicated")
		self.Txt_Dataset_replicated.setGeometry(QRect(10,120,170,16))
###########################################################
		self.Txt_req_output = QLabel(self,"textLabelreqoutput")
		self.Txt_req_output.setGeometry(QRect(10,350,170,16))

		self.combobox_req_output = QComboBox(0,self,"combobox_req_output")
		self.combobox_req_output.setGeometry(QRect(200,350,140,20))

		self.Txt_cards = QLabel(self,"textLabelcards")
		self.Txt_cards.setGeometry(QRect(10,370,170,16))

		self.combobox_cards = QComboBox(0,self,"combobox_cards")
		self.combobox_cards.setGeometry(QRect(200,370,140,20))
		
		self.pixmapLabelcern = QLabel(self,"pixmapLabel1")
		self.pixmapLabelcern.setGeometry(QRect(600,370,100,100))
		self.pixmapLabelcern.setPixmap(self.cern_logo_image)
		self.pixmapLabelcern.setScaledContents(1)

		self.Txt_n_dataset_ppg = QLabel(self,"textLabel_n_dataset_pgg")
		self.Txt_n_dataset_ppg.setGeometry(QRect(10,390,170,16))

		self.combobox_n_dataset_ppg = QComboBox(0,self,"combobox_n_dataset_ppg")
		self.combobox_n_dataset_ppg.setGeometry(QRect(200,390,70,20))

		self.Txt_n_dataset_pjc = QLabel(self,"textLabel_n_dataset_pjc")
		self.Txt_n_dataset_pjc.setGeometry(QRect(10,410,170,16))

		self.linedit_n_dataset_pjc = QLineEdit(self)
		self.linedit_n_dataset_pjc.setGeometry(QRect(200,410,140,20))
		
#####################################################

		self.submit_button = QPushButton(self,"view")
		self.submit_button.setGeometry(QRect(10,450,70,20))
		
		self.query_close_button = QPushButton(self,"Query and Close")
		self.query_close_button.setGeometry(QRect(130,450,90,20))
		
		self.close_button = QPushButton(self,"close_button")
		self.close_button.setGeometry(QRect(500,450,70,20))

#####################################################

		self.Txt_event_type = QLabel(self,"Txt_event_type")        
		self.Txt_event_type.setGeometry(QRect(10,90,104,16))

		self.combobox_evt_type = QComboBox(0,self,"combobox_evt_type")
		self.combobox_evt_type.setGeometry(QRect(190,90,220,20))
		
		self.radioXML = QCheckBox(self,"XMLradio")
		self.radioXML.setGeometry(QRect(450,90,120,21))
                self.radioXML.setChecked(True)

		self.combobox_dataset_replicated =\
			QComboBox(0,self,"combobox_dataset_replicated")        
		self.combobox_dataset_replicated.setGeometry(QRect(190,120,120,20))

		self.comboboxdbvers = QComboBox(0,self,"comboboxdbvers")
		self.comboboxdbvers.setGeometry(QRect(100,210,80,21))

		self.comboboxstep1 = QComboBox(0,self,"comboboxstep1")
		self.comboboxstep1.setGeometry(QRect(210,210,110,21))

		self.comboboxstep2 = QComboBox(0,self,"comboboxstep2")
		self.comboboxstep2.setGeometry(QRect(330,210,110,21))

		self.comboboxstep3 = QComboBox(0,self,"comboboxstep3")
		self.comboboxstep3.setGeometry(QRect(450,210,110,21))

		self.lineEditNevts = QLineEdit(self,"lineEditNevts")
		self.lineEditNevts.setGeometry(QRect(580,210,140,23))
		self.lineEditNevts.setReadOnly(1)

		self.Txt_configuratio = QLabel(self,"Txt_configuratio")
		self.Txt_configuratio.setGeometry(QRect(10,60,135,20))

		self.line1 = QFrame(self,"line1")
		self.line1.setGeometry(QRect(10,40,780,20))
		self.line1.setFrameShape(QFrame.HLine)
		self.line1.setFrameShadow(QFrame.Sunken)
		self.line1.setFrameShape(QFrame.HLine)

		self.combobox_configuratio = QComboBox(0,self,"combobox_configuratio")
		self.combobox_configuratio.setGeometry(QRect(190,60,180,20))
		
		self.pixmapLabel1 = QLabel(self,"pixmapLabel1")
		self.pixmapLabel1.setGeometry(QRect(600,60,100,100))
		self.pixmapLabel1.setPixmap(self.lhcb_logo_image)
		self.pixmapLabel1.setScaledContents(1)
		self.comboboxdatatype =QComboBox(0,self,"comboboxdatatype")

		self.comboboxdatatype.setGeometry(QRect(10,210,80,21))
		self.comboboxint_type1 = QComboBox(0,self,"comboboxint_type1")
		self.comboboxint_type1.setGeometry(QRect(210,270,110,21))

		self.comboboxint_type2 = QComboBox(0,self,"comboboxint_type2")
		self.comboboxint_type2.setGeometry(QRect(330,270,110,21))

		self.comboboxint_type3 = QComboBox(0,self,"comboboxint_type3")
		self.comboboxint_type3.setGeometry(QRect(450,270,110,21))

		self.Txt_configuratio_data_type =\
			QLabel(self,"Txt_configuratio_data_type")        
		self.Txt_configuratio_data_type.setGeometry(QRect(10,180,80,20))

		self.Txt_configuratio_db_version =\
			QLabel(self,"Txt_configuratio_db_version")        
		self.Txt_configuratio_db_version.setGeometry(QRect(100,180,80,20))

		self.Txt_configuratio_step_1 = QLabel(self,"Txt_configuratio_step_1")
		self.Txt_configuratio_step_1.setGeometry(QRect(230,180,80,20))

		self.Txt_configuratio_step_2 = QLabel(self,"Txt_configuratio_step_2")
		self.Txt_configuratio_step_2.setGeometry(QRect(350,180,80,20))

		self.Txt_configuratio_intermediate_1 =\
			QLabel(self,"Txt_configuratio_intermediate_1")        
		self.Txt_configuratio_intermediate_1.setGeometry(QRect(227,240,90,20))

		self.intermediate3 = QLabel(self,"intermediate3")
		self.intermediate3.setGeometry(QRect(467,240,90,20))

		self.Txt_configuratio_intermediate_2 =\
			QLabel(self,"Txt_configuratio_data_type_6")                
		self.Txt_configuratio_intermediate_2.setGeometry(QRect(340,240,90,20))   

		self.Txt_configuratio_step_3 = QLabel(self,"Txt_configuratio_step_3")    
		self.Txt_configuratio_step_3.setGeometry(QRect(470,180,80,20))

		self.Txt_configuratio_n_events =QLabel(self,"Txt_configuratio_n_events") 
		self.Txt_configuratio_n_events.setGeometry(QRect(599,180,120,20))	

		self.resize(QSize(760,500).expandedTo(self.minimumSizeHint()))
		self.clearWState(Qt.WState_Polished)
		
##################################################################
# Dialog box with table for reporting the results:

		self.results_dialog = QDialog()
		self.results_dialog.setCaption("Query Results")
		self.results_dialog.setFont(QFont("Sans", font_size))
		self.results_dialog.setSizeGripEnabled(1)
		
		self.results_table = QTable(self.results_dialog,"results_table")

		self.next_page_button = QPushButton(self.results_dialog,"next_page_button")
		self.next_page_button.setHidden(1)
		self.prev_page_button = QPushButton(self.results_dialog,"prev_page_button")
		self.next_page_button.setHidden(1)
		self.Txt_datasets_from_to_step =\
			QLabel(self.results_dialog,"Txt_datasets_from_to_step")
		 
		self.hide_results_dialog_button =\
			QPushButton(self.results_dialog,"close_button")
		
		self.lof_button=QPushButton(self.results_dialog,"lof_button")
		
		self.save_results_button =\
			QPushButton(self.results_dialog,"save_button")
		self.Txt_save_results= QLabel(self.results_dialog,"text save")
		
		self.view_report_button = QPushButton(self.results_dialog,"view_report")
		self.Txt_view_report = QLabel(self.results_dialog,"text report")
		
				
		self.radioAdvanced = QRadioButton(self.results_dialog,"radioAdvanced")
		self.Txt_advanced = QLabel(self.results_dialog,"text advanced")
		
#################################################################
# Dialog box to read the report file
		
		self.report_file_dialog =QDialog()
		self.report_file_dialog.setCaption("Report File")
		self.report_file_dialog.resize(QSize(640,550))
		self.report_file_dialog.setFont(QFont("Sans", font_size))
		self.close_report_button =\
			QPushButton(self.report_file_dialog,"close_report")
		self.close_report_button.setGeometry(10,520,80,25)
		self.close_report_button.setText("Close")
		
		self.report_file_dialog.report_textedit=\
			 QTextEdit(self.report_file_dialog)
		self.report_file_dialog.report_textedit.setGeometry(0,0,640,510)

#################################################################
# Save File Dialog Box

		self.save_file_dialog =QDialog()
		self.save_file_dialog.setCaption("Save File As...")
		self.save_file_dialog.resize(QSize(500,400))
		self.save_file_dialog.setFont(QFont("Sans", font_size))
		self.save_file_dialog.dir_icon_view=\
			QIconView(self.save_file_dialog,"iconView1")			
		self.save_file_dialog.dir_icon_view.\
			setGeometry(0,0,500,300)
		#self.save_file_dialog.dir_icon_view.setAutoArrange(1)
		self.save_file_dialog.dir_icon_view.setMaxItemWidth ( 50 )
		
		self.save_file_dialog.savefile_label=\
			QLabel(self.save_file_dialog)
		self.save_file_dialog.savefile_label.setGeometry(0,320,80,20)
		
		self.save_file_dialog.report_linedit = QLineEdit(self.save_file_dialog)
		self.save_file_dialog.report_linedit.setGeometry(0,340,300,20)
		self.save_file_dialog.report_linedit.setText("Results.opts")
		
		self.save_file_dialog.close_save_button =\
			QPushButton(self.save_file_dialog,"close_save")
		self.save_file_dialog.close_save_button.setGeometry(10,380,80,20)
		self.save_file_dialog.close_save_button.setText("Close")
		
		self.save_file_dialog.save_button =\
			QPushButton(self.save_file_dialog,"save_report")
		self.save_file_dialog.save_button.setGeometry(100,380,80,20)
		self.save_file_dialog.save_button.setText("Save")

#################################################################
# Warning dialog
                self.warning_dialog=QMessageBox('',
                        '''The Standalone Browser version does not support all the features of the one integrated into Ganga.''',
                        QMessageBox.Warning,
                        QMessageBox.Ok,
                        QMessageBox.NoButton,
                        QMessageBox.NoButton)
		#

#################################################################
		
		self.languageChange()
		
		self.connect(self.combobox_configuratio,\
			SIGNAL("activated(int)"),self.get_evt_type)	  

		self.connect(self.combobox_evt_type,\
			SIGNAL("activated(int)"),self.get_locations)

		self.connect(self.combobox_dataset_replicated,\
			SIGNAL("activated(int)"),self.build_comboboxes_launcher)

		self.connect(self.comboboxdatatype,\
			SIGNAL("activated(int)"),self.build_comboboxes_launcher_datatype)

		self.connect(self.comboboxdbvers,\
			SIGNAL("activated(int)"),self.build_comboboxes_launcher_dbversion)	  

		self.connect(self.comboboxstep1,\
			SIGNAL("activated(int)"),self.build_comboboxes_launcher_step_1)

		self.connect(self.comboboxstep2,\
			SIGNAL("activated(int)"),self.build_comboboxes_launcher_step_2)	  

		self.connect(self.comboboxstep3,\
			SIGNAL("activated(int)"),self.build_comboboxes_launcher_step_3)
			
		self.connect(self.submit_button,\
			SIGNAL("clicked()"),self.submit)
		
		self.connect(self.next_page_button,\
			SIGNAL("clicked()"),self.next_page)
			
		self.connect(self.prev_page_button,\
			SIGNAL("clicked()"),self.prev_page)
		
		self.connect(self.hide_results_dialog_button,\
			SIGNAL("clicked()"),self.hide_results_dialog)
			
		self.connect(self.save_results_button,\
			SIGNAL("clicked()"),self.save_results)
		
		self.connect(self.view_report_button,\
			SIGNAL("clicked()"),self.view_results)
		
		self.connect(self.close_report_button,\
			SIGNAL("clicked()"),self.close_report)
											
		self.connect(self.save_file_dialog.dir_icon_view,\
			SIGNAL("selectionChanged()"),self.icon_clicked)
											
		self.connect(self.save_file_dialog.save_button,\
			SIGNAL("clicked()"),self.save_file)
		
		self.connect(self.save_file_dialog.close_save_button,\
			SIGNAL("clicked()"),self.close_save_file)
		
		self.connect(self.radioXML,\
			SIGNAL("stateChanged(int)"),self.radio_state_changed)

		self.connect(self.lof_button,\
			SIGNAL("clicked()"),self.return_lof)

		self.connect(self.query_close_button,\
			SIGNAL("clicked()"),self.return_lof)
			
		self.connect(self.radioAdvanced,\
			SIGNAL("stateChanged(int)"),self.show_advanced)
	  
		self.connect(self.close_button,\
			SIGNAL("clicked()"),self.hide_all)	  
	  
		#self.init()
		
##############################################  

	def languageChange(self):
		self.setCaption(self.__tr("Lhc-b Database Browser"))
		self.textLabelTitolo.setText(self.__tr("<h1>Search for Datasets</h1>"))
		self.Txt_event_type.setText(self.__tr("Event type"))
		self.Txt_Dataset_replicated.setText(self.__tr("Dataset Replicated at"))
		self.Txt_configuratio.setText(self.__tr("Configuration"))
		self.Txt_configuratio_data_type.setText(self.__tr("Data type"))
		self.Txt_configuratio_db_version.setText(self.__tr("Db version"))
		self.Txt_configuratio_step_1.setText(self.__tr("Step 1"))
		self.Txt_configuratio_step_2.setText(self.__tr("Step 2"))
		self.Txt_configuratio_intermediate_1.\
		setText(self.__tr("Intermediate1"))        
		self.intermediate3.setText(self.__tr("Intermediate 3"))        
		self.Txt_configuratio_intermediate_2.\
		setText(self.__tr("Intermediate2"))
		self.Txt_configuratio_step_3.setText(self.__tr("Step 3"))                
		self.Txt_configuratio_n_events.setText(self.__tr("Number of Events"))
		self.Txt_req_output.setText(self.__tr("Requested Output"))
		self.Txt_cards.setText(self.__tr("Cards Content"))
		self.Txt_n_dataset_ppg.setText(self.__tr("Nb of dataset per page"))
		self.Txt_n_dataset_pjc.setText\
			(self.__tr("Nb of input dataset per job card"))
		self.submit_button.setText(self.__tr("View"))
		self.close_button.setText(self.__tr("Close"))
		self.radioXML.setText(self.__tr("XML RPC System"))
		self.save_file_dialog.savefile_label.setText(self.__tr("Save as:"))
		self.view_report_button.setText(self.__tr("View file"))
		self.query_close_button.setText(self.__tr("Query and Close"))
		self.next_page_button.setText(self.__tr("Next Page"))
		self.save_results_button.setText(self.__tr("Save Results"))
		self.Txt_save_results.setText\
			(self.__tr("Open a widget to save the options file corresponding to the selection on disk."))
		self.save_file_dialog.report_linedit.setText(self.__tr(".results.opts"))
		self.Txt_view_report.setText("Open a widget to view the options file corresponding to the selection.")
		self.Txt_advanced.setText("Advanced >>")
		self.lof_button.setText("Return Lof")
		self.hide_results_dialog_button.setText(self.__tr("Close"))
		self.prev_page_button.setText("Prev Page")
		#self.warning_dialog.label.setText(self.__tr(\
		#"   The Standalone Browser version does not\n   own all the features of the one integrated in Ganga."))
		
#########################################################

	def __tr(self,s,c = None):
		return qApp.translate("wpage",s,c)
		
#########################################################

class wpage (wpage_graphics):

	def __init__(self):
		wpage_graphics.__init__(self)
		
#		print "Begin db browsing\n"
		
		self.descriptions_list=[]
		# Build the configurations menu
		#config_list = self.bkdb.getAvailableConfigurations()
		#self.insert_list_in_combobox(config_list,self.combobox_configuratio)
		self.config_list_XML()
		
		# Build the output requested, cards content and number of databases 
		# menus
		outputs= ['Full Output','No output - only cards']
		self.insert_list_in_combobox(outputs,self.combobox_req_output)
		
		file_names = ['Logical Filenames','Physical Filenames']
		self.insert_list_in_combobox(file_names,self.combobox_cards)
		
		n_dataset_ppg = ['300','100','50','20','10']
		self.insert_list_in_combobox(n_dataset_ppg,self.combobox_n_dataset_ppg)
		self.combobox_n_dataset_ppg.setEditable(1)
		
#############################################################
	def is_in_list_entries(self,good_keylist,string):
		i=0
		for el in good_keylist:
			if el.find(string) != -1:
				return i
			i+=1
		return -1

#############################################

	def sort_list_lenght(self,word_list):
		t=len(word_list)
		r=t
		max_len=0
		max_len_index=0
		while(r!=0):
			for i in range(r):
				word_len=len(word_list[i])
				if word_len > max_len:
					max_len = word_len
					max_len_index=i
			aux = word_list[r-1]
			word_list[r-1]=word_list[max_len_index]
			word_list[max_len_index]=aux
			r-=1
			max_len=0
		#return it reversed
		for i in range(t/2):
			aux = word_list[i]
			word_list[i]=word_list[t-1-i]
			word_list[t-1-i]=aux 
		return word_list

####################################################################
		
	def get_evt_type(self):    	
		self.combobox_evt_type.clear()	
		#print current_item
		current_conf =str(self.combobox_configuratio.currentText())	
		#print events_types
		events_types= self.bkdb.getEventTypes(current_conf)	
		for element  in  events_types:
			event_string = ''
			for entry in element.iterkeys():
				event_string += str(element[entry]) + " "
			#print event_string
			self.combobox_evt_type.insertItem(event_string)
			event_string=" "
		self.get_locations()
		
############################################## 

	def get_locations(self):
		self.combobox_dataset_replicated.clear()
		current_conf = str(self.combobox_configuratio.currentText())
		current_type_descr = str(self.combobox_evt_type.currentText())
		current_type = int(current_type_descr.split(" ")[0])
		#print current_type 
		location_list = self.bkdb.getLocations(current_conf,current_type)
		for location in location_list:
			self.combobox_dataset_replicated.insertItem(location)
		
		self.combobox_dataset_replicated.insertItem("ANY")
		
		self.build_comboboxes_launcher()

##############################################

	def fill_content_in_rows(self,effective_cat):
		content_in_rows = []
		for line in effective_cat:
			content_in_rows.append(line.split("#"))
		# fill the empty spaces with word "*None*"
		max_len = len(content_in_rows[0])
		for row in content_in_rows:
			row_len=len(row)
			for i in range(row_len,max_len):
				row.append("*None*")
		return content_in_rows
	
############################################## 	

	def fill_content_in_columns(self,content_in_rows):
		content_in_columns = []
		for i in range(len(content_in_rows[0])):
			column=[]
			for j in range(len(content_in_rows)):
				#print 'content_in_rows['+str(j)+']['+str(i)+']='+content_in_rows[j][i]
				column.append(content_in_rows[j][i])
			content_in_columns.append(column)
		return content_in_columns

############################################## 	

	def build_comboboxes_launcher_datatype(self):
		keyword = str(self.comboboxdatatype.currentText())
		self.build_comboboxes(keyword,0)

##############################################

	def build_comboboxes_launcher_dbversion(self):
		keyword = str(self.comboboxdbvers.currentText())
		self.build_comboboxes(keyword,1)

##############################################

	def build_comboboxes_launcher_intermediate_1(self):
		keyword = str(self.comboboxint_type1.currentText())
		self.build_comboboxes(keyword,2)

##############################################

	def build_comboboxes_launcher_step_1(self):
		keyword = str(self.comboboxstep1.currentText())
		self.build_comboboxes(keyword,3)

##############################################

	def build_comboboxes_launcher_intermediate_2(self):
		keyword = str(self.comboboxint_type2.currentText())
		self.build_comboboxes(keyword,4)

##############################################

	def build_comboboxes_launcher_step_2(self):
		keyword = str(self.comboboxstep2.currentText())
		self.build_comboboxes(keyword,5)

##############################################

	def build_comboboxes_launcher_intermediate_3(self):
		keyword = str(self.comboboxint_type3.currentText())
		self.build_comboboxes(keyword,6)

##############################################

	def build_comboboxes_launcher_step_3(self):
		keyword = str(self.comboboxstep3.currentText())
		self.build_comboboxes(keyword,7)

##############################################

	def build_comboboxes_launcher(self):
		keyword = 'any'
		if str(self.combobox_dataset_replicated.currentText())=="ANY":
			file_names = ['Logical Filenames']
			self.insert_list_in_combobox(file_names,self.combobox_cards)
		else:
			file_names = ['Logical Filenames','Physical Filenames']
			self.insert_list_in_combobox(file_names,self.combobox_cards)
		self.build_comboboxes(keyword,-1)

##############################################

	def build_comboboxes(self,keyword,comboboxlabel):
		# get current configuration
		current_conf = str(self.combobox_configuratio.currentText())

		# get current event description
		events_type_descr= str(self.combobox_evt_type.currentText())
		events_type = events_type_descr.split(" ")[0]

		#get events location
		events_location= str(self.combobox_dataset_replicated.currentText())  

		# get dataset features from catalogue
		list_features = self.bkdb.getFullEventTypesAndNumbers\
									(current_conf,int(events_type),events_location )

		contents_in_rows,content_in_columns =self.build_comboboxes_contents\
				(list_features,keyword,comboboxlabel)

		combobox_list= [\
					self.comboboxdatatype,\
					self.comboboxdbvers,\
					self.comboboxint_type1,\
					self.comboboxstep1,\
					self.comboboxint_type2,\
					self.comboboxstep2,\
					self.comboboxint_type3,\
					self.comboboxstep3]

		for combobox in combobox_list:
			combobox.clear()

		self.fill_comboboxes(combobox_list,content_in_columns)

		text_label_list=[\
					self.Txt_configuratio_data_type,\
					self.Txt_configuratio_db_version,\
					self.Txt_configuratio_intermediate_1,\
					self.Txt_configuratio_step_1,\
					self.Txt_configuratio_intermediate_2,\
					self.Txt_configuratio_step_2,\
					self.intermediate3,\
					self.Txt_configuratio_step_3]

		# hide empty comboboxes and show the others
		for i in range(len(combobox_list)):
			combobox_n_elements=combobox_list[i].count()
			if 0 == combobox_n_elements:
				combobox_list[i].hide()
				text_label_list[i].hide()
			else:
				combobox_list[i].show()
				text_label_list[i].show()

		# build the line for the number_of_events query
		key=''
		for combobox in combobox_list:
			current_selection= str(combobox.currentText())
			if current_selection == '':
				break
			key+=current_selection+"#"
		key=key[:-1]
		#print "Key for getting n_events "+key
		n_events = list_features[1][key]


		self.lineEditNevts.clear()
		self.lineEditNevts.setText(str(n_events))


##############################################

	def fill_comboboxes(self,combobox_list,content_in_columns):
		n_columns=len(content_in_columns)
		p_content=''
		for i in range(n_columns):
			local_list=[]
			entry_max_len=0
			for content in content_in_columns[i]:
				if content == '*None*' or 1==local_list.count(content):
					continue
				local_list.append(content)
				combobox_list[i].insertItem(content)
				if len(content)>entry_max_len:
					entry_max_len=len(content)
					combobox_list[i].setMinimumWidth(entry_max_len*6+5)
			#combobox_list[i].insertItem("ANY")

##############################################
   
	def build_comboboxes_contents(self,list_features,keyword,comboboxlabel):
		#print "=============================="
		#print keyword
		#print "=============================="

		# build list of feature lists
		good_keys=[]
		for feat_dict in list_features:
			for key in feat_dict.iterkeys():
				if -1 == key.find('ANY'):
					#print key
					good_keys.append(key)

		# Among the good keys select the effective events cathegories
		good_keys = self.sort_list_lenght(good_keys)

		effective_cat=[good_keys[0]]
		for key in good_keys:
			in_list = self.is_in_list_entries(effective_cat,key)
			if -1!=in_list and (len(key) > len(effective_cat[in_list])):
				effective_cat[in_list]=key
			elif -1==in_list:
				effective_cat.append(key)

		#print "\n\n"
		#for el in effective_cat:
		#	print el
		#print "-"*40

		# Now the attributes are stored in a list of lists.
		# The basic idea is to think about matrix rows.
		content_in_rows = self.fill_content_in_rows(effective_cat)

		if keyword != 'any':
			#print "different from any"
			#print keyword
			aux_content_in_rows=[]
			rows_containing_keyword=[]
			# search wich rows contain the keyword:
			counter=0
			for raw in content_in_rows:
				#print raw
				if raw.count(keyword)!=0 and raw.index(keyword)==comboboxlabel:
					#print "found keyword!"
					rows_containing_keyword.append(counter)
				counter+=1
			#print rows_containing_keyword

			for row_index in rows_containing_keyword:
				aux_content_in_rows.append(content_in_rows[row_index])
			#print "content_in_rows"
			#print content_in_rows
			#print "aux_content_in_rows"
			#print aux_content_in_rows
			content_in_rows=aux_content_in_rows


		content_in_columns = self.fill_content_in_columns(content_in_rows)
		#print content_in_columns

		return content_in_rows,content_in_columns
	
############################################## 

	def insert_list_in_combobox(self,the_list,combobox):
		combobox.clear()
		for element in the_list:
			combobox.insertItem(element)

##########################################################

	def submit(self):			
		self.query_server(1)
	
##########################################################
	def query_server(self,show_results_flag):
	
		if str(self.combobox_configuratio.currentText())=='Choose any':
			print 'Illegal Query!'
			return -1
		# Set the status of the Advanced Radio Button to Unchecked
		self.radioAdvanced.setChecked(0)

		#build program and programs values from gui entries:
		prog_datatypes_combobox_list = [\
				[self.comboboxint_type1,self.comboboxstep1],\
				[self.comboboxint_type2,self.comboboxstep2],\
				[self.comboboxint_type3,self.comboboxstep3]]
		
		Pr='Program'
		If ='InputFile'
		programs={\
				If+'0':str(self.comboboxint_type1.currentText()),\
				Pr+'0':str(self.comboboxstep1.currentText())\
				}

		for i in range(1,3):
			if str(prog_datatypes_combobox_list[i][0].currentText())!="":
				programs.setdefault(If+str(i),\
					str(prog_datatypes_combobox_list[i][0].currentText()))
				programs.setdefault(Pr+str(i),\
					str(prog_datatypes_combobox_list[i][1].currentText()))
	
		#print programs
		
		program=str(prog_datatypes_combobox_list[0][1].currentText())
		# Set the card type: physical or logical.
		card_type=""
		
		if self.combobox_cards.currentItem()==0:
			card_type= "logical"
		else:
			card_type= "physical"
				
		cardtype_en=card_type	
		
		# Set The number of input jobs per card:
		n_input_jobs= str(self.linedit_n_dataset_pjc.displayText())
		if n_input_jobs == "":
			n_input_jobs="ALL" # The default value!
			
		config_en=str(self.combobox_configuratio.currentText())	
		event_type_en=str(self.combobox_evt_type.currentText()).split(" ")[0]
		data_type_en=str(self.comboboxdatatype.currentText())
		replica_en=str(self.combobox_dataset_replicated.currentText())
		dbversion_en=str(self.comboboxdbvers.currentText())
		program_en=str(program)
		programs_en=programs
		printflag_en= 1
		ndatapage_en=int(str(self.combobox_n_dataset_ppg.currentText()))
		niniputjob_en=n_input_jobs
		pagenumber_en='1'
		prevfname_en="/"
		
		selection= {\
		 'config':config_en\
		,'eventtype':event_type_en\
		,'datatype':data_type_en\
		,'replica':replica_en\
		,'dbversion':dbversion_en\
		,'program':program_en\
		,'programs':programs_en\
		,'printflag':printflag_en\
		,'ndatapage':ndatapage_en\
		,'ninputjob':niniputjob_en\
		,'pagenumber':pagenumber_en\
		,'prevfname':prevfname_en\
		,'cardtype':cardtype_en }
		
		#print "Start selection+++++++++++"
		#for key in selection.iterkeys():
		#	print "-------------"
		#	print key
		#	print selection[key]
		
		#print '*'*200

		totalresult = []
		print "Be patient while query is in progress"
                try:
                    while True:
		        print "*",
			sys.stdout.flush()
#			print selection['pagenumber']
			result=self.bkdb.getData(selection)
			totalresult += result
			if not self.radioXML.isChecked() or result ==[]:
				break
			selection['prevfname']=result[-1]['name']
			selection['pagenumber']=str(int(selection['pagenumber'])+1)

		
                except Exception,e: 
                    print e
                    errorTxt="""
                    If you get an error which looks like:

                    <ProtocolError for lhcbbk.cern.ch:8091/RPC/BookkeepingQuery: 
                    500 Exception during HTTP POST processing:cannot marshal <type XXXXXX> objects>

                    This exception is beyond Gangas control. You should inform the current maintainer 
                    of the LHCb bookkeeping system, Carmine Cioffi <c.cioffi1@physics.ox.ac.uk>, of 
                    the problem. Let him know what you were trying to do at the time. Please also 
                    cc this mail to <U.Egede@imperial.ac.uk> for information.

                    Thank you!
                    """


                    errorMsg = QMessageBox('',
                            errorTxt,
                            QMessageBox.Critical,
                            QMessageBox.Ok,
                            QMessageBox.NoButton,
                            QMessageBox.NoButton)
                    errorMsg.exec_loop()


                    #next try to recover
                    self.config_list_XML()

		print
#		self.descriptions_list=self.bkdb.getData(selection)
		self.descriptions_list=totalresult
		
		for el in self.descriptions_list:
			el['programname'],el['programversion']=\
				str(selection['program']).split(" - ")
		
#		print "Show Results"
		#for el in self.descriptions_list:
		#	print el
		
		if show_results_flag == 1:
			self.show_results(self.descriptions_list,ndatapage_en)
		
##############################################
# Builds the results window
	
	def show_results(self,descriptions_list,ndatasetppage):
		self.build_table(descriptions_list,0,ndatasetppage)
		self.results_dialog.show()
		#self.results_dialog.exec_loop()
		
##############################################

	def build_table(self,descriptions_list,last_data,ndatasetppage):
		self.radioAdvanced.setChecked(0)
		#print "ndatasetppage "+ ndatasetppage
		col_list=[\
				'Data type',\
#				'Logfile',\
				'Job Id',\
				'No. events',\
				'Size',\
				'Event type',\
				'Prg name',\
				'Ver.',\
				'Db ver.',\
				'Name',\
				'Date',\
				'Prod. Center']
		label_dict={\
				'id':-1,\
				'name':8,\
				'datatype':0,\
				'eventtype':4,\
				'nbevents':2,\
				'size':3,\
				'dbversion':7,\
				'laboratory':10,\
				'programname':5,\
				'programversion':6,\
				'date':9,\
#				'logfile':1,\
				'job_id':1,\
				'replica':-1}
				
		#clear all the lines in the table:
		self.results_table.setNumRows(0)
		self.results_table.setNumCols(0)
		
		col_label=0
		for name in col_list:
			self.results_table.setNumCols(self.results_table.numCols() + 1)
			self.results_table.horizontalHeader().\
				setLabel(self.results_table.numCols()-1,self.__tr(name))
			self.results_table.setColumnReadOnly(col_label,1)
			col_label+=1
		
		# Buil loop range:
		begin=last_data
		end=0
		len_descriptions_list=len(descriptions_list)
		int_ndatasetppage=int(ndatasetppage)
	#	print 'len_descriptions_list '+str(len_descriptions_list)
		if len_descriptions_list < last_data+int_ndatasetppage:
			end = len_descriptions_list
		else: 
			end = begin+int_ndatasetppage 
		
		#print "begin-end= "+str(begin)+" "+str(end)
		#print len_descriptions_list
		for counter in range(begin,end):
			self.results_table.setNumRows(self.results_table.numRows() + 1)
			self.results_table.verticalHeader().\
				setLabel(self.results_table.numRows()-1,self.__tr(str(counter+1)))
			#self.results_table.setRowStretchable(counter,1)
			self.results_table.setRowHeight(counter,20)
			#print counter
			for key in label_dict:
				if not descriptions_list[counter].has_key(key): continue
				col_number=label_dict[key]
				if col_number!=-1:
					entry = str(descriptions_list[counter][key])
					#print entry
					self.results_table.setText(counter%int_ndatasetppage,col_number,entry)
		
		table_lenght=1000
		table_hight=0
		if int_ndatasetppage < 50:
			table_hight=int_ndatasetppage*21+20
		else:
			table_hight=365
		#print 'table_hight '+str(table_hight)
			
		self.results_dialog.resize(QSize(table_lenght,table_hight+50).\
			expandedTo(self.minimumSizeHint()))

		self.results_table.setGeometry(QRect(0,0,table_lenght,table_hight))
		
		# Set Columns width
		# The values are empirically chosen and stored in col_width.
		col_width=[65,65,65,75,75,75,50,50,155,150,130,]
		#print 'N cols ' +str(self.results_table.numCols())
		for col_number in range(self.results_table.numCols()):
			entry_len = len(self.results_table.text(0,col_number))
			#self.results_table.setColumnStretchable(col_number,1)
			#print col_number
			self.results_table.setColumnWidth(col_number,col_width[col_number])

		# Call the function recursively if some other data has to be shown:
		# Show next page button
		if  end < len_descriptions_list:
			self.next_page_button.setGeometry(QRect(100,table_hight+20,70,20))
			self.next_page_button.setHidden(0)
		else: 
			self.next_page_button.setHidden(1)
			
		# Show prev page button
		if begin != 0:
			self.prev_page_button.setGeometry(QRect(10,table_hight+20,70,20))
#			self.prev_page_button.setText("Prev Page")
			self.prev_page_button.setHidden(0)
			#self.results_dialog.resize(QSize(table_lenght,table_hight+50).\
			#	expandedTo(self.minimumSizeHint()))
		else:
			self.prev_page_button.setHidden(1)
			
		info_page_string = "1st "+str(begin+1)+" last "+str(end)+\
			" step "+str(ndatasetppage)+" total files "+str(len_descriptions_list)
		self.Txt_datasets_from_to_step.setGeometry(QRect(200,table_hight+20,300,20))
		self.Txt_datasets_from_to_step.setText(self.__tr(info_page_string))
		
		# Close Button
		self.hide_results_dialog_button.setGeometry(QRect(740,table_hight+20,100,20))
#		self.hide_results_dialog_button.setText(self.__tr("Close"))
		
		# Lof Button
		self.lof_button.setGeometry(QRect(470,table_hight+20,100,20))
#		self.lof_button.setText("Return Lof")
		
		# Advanced radio switch
		self.radioAdvanced.setGeometry(QRect(870,table_hight+20,100,20))
#		self.Txt_advanced.setText("Advanced >>")
		self.Txt_advanced.setGeometry(QRect(885,table_hight+20,100,20))
		
		# Set the geometry of the view report button
		self.view_report_button.setGeometry(QRect(0,table_hight+70,100,20))
		#self.Txt_view_report.setText("Open a widget to view the options file corresponding to the selection.")
		self.Txt_view_report.setGeometry(QRect(120,table_hight+70,500,20))
		
		# Save on a File Button
		self.save_results_button.setGeometry(QRect(0,table_hight+100,100,20))
		self.Txt_save_results.setGeometry(QRect(120,table_hight+100,500,20))
		
##############################################################################

	def view_results(self):
		tempfilename=os.path.expanduser("~")+"/.results.opts"
		self.save_file_dialog.report_linedit.setText(tempfilename)
		self.save_file()
		report_file_name = str(self.save_file_dialog.report_linedit.text())
		results = open (report_file_name,"r")
		results_string = results.read()
		#print results_string
		self.report_file_dialog.report_textedit.clear()
		self.report_file_dialog.report_textedit.append(results_string)
		self.report_file_dialog.show()
		#self.report_file_dialog.exec_loop()
		self.save_file_dialog.report_linedit.setText("Results.opts")
		os.remove(tempfilename)
		
##############################################################################

	def close_report(self):
		self.report_file_dialog.setHidden(1)
		
##############################################################################

	def icon_clicked(self):
		#print os.getcwd()
		name= str(self.save_file_dialog.dir_icon_view.currentItem().text())
		#print name
		if os.path.isfile(name):
			self.save_file_dialog.report_linedit.setText(name)
		#else:
		#	self.save_file_dialog.report_linedit.setText(name)
		else:
			os.chdir(name)
			self.build_dir_content(name)
		
##############################################################################

	def build_dir_content(self,directory):
		
		#self.save_file_dialog.dir_icon_view.clear()
		self.save_file_dialog.dir_icon_view.setShown(0)
		del self.save_file_dialog.dir_icon_view
		self.save_file_dialog.dir_icon_view=\
			QIconView(self.save_file_dialog,"iconView1")			
		self.save_file_dialog.dir_icon_view.\
			setGeometry(0,0,500,300)
		#self.save_file_dialog.dir_icon_view.setAutoArrange(1)
		self.save_file_dialog.dir_icon_view.setMaxItemWidth ( 50 )
		self.save_file_dialog.dir_icon_view.setShown(1)
		self.connect(self.save_file_dialog.dir_icon_view,\
			SIGNAL("selectionChanged()"),self.icon_clicked)
		
		dir_list = os.listdir(os.getcwd())
		
		for file in dir_list:
			if os.path.isfile(file):
				QIconViewItem(self.save_file_dialog.dir_icon_view,\
					QString(self.__tr(file)),self.file_image)
			else:
				QIconViewItem(self.save_file_dialog.dir_icon_view,\
					QString(self.__tr(file)),self.dir_image)
		
		QIconViewItem(self.save_file_dialog.dir_icon_view,\
			QString(self.__tr("..")),self.dir_image)
		
		self.save_file_dialog.dir_icon_view.sort()
		
##############################################################################	
	
	def save_results(self):
		
		self.build_dir_content("./")
		self.save_file_dialog.show()
		#self.save_file_dialog.exec_loop()
		
##############################################################################
	
	def save_file(self):
	
		filename  = str(self.save_file_dialog.report_linedit.text())
		results = open (filename,"w")
		
		line=""
		preamble=""
		
		cards_content=''
		if self.combobox_cards.currentItem() == 0:
			print "Logical"
			preamble = "LFN:"
			cards_content = "logical"
		else:
			preamble = "PFN:"
			cards_content = "physical"
		
		localtime =time.asctime(time.localtime())
		evtype=self.descriptions_list[0]['eventtype']
		dtatype=self.descriptions_list[0]['datatype']
		cnfig=str(self.combobox_configuratio.currentText())
		filetype1=str(self.comboboxint_type1.currentText())
		prog1=str(self.comboboxstep1.currentText())
		filetype2=str(self.comboboxint_type2.currentText())
		prog2=str(self.comboboxstep2.currentText())
		filetype3=str(self.comboboxint_type3.currentText())
		prog3=str(self.comboboxstep3.currentText())
		dbversion=str(self.comboboxdbvers.currentText())
		rplicalocation=str(self.combobox_dataset_replicated.currentText())
		n_datas_str = str(len(self.descriptions_list))
		nevts_str = str(self.lineEditNevts.text())
		nl="\n"
		informations =\
		"//"+nl*2+\
		"//-- GAUDI data cardsgeneratedon"+localtime+nl+\
		"//-- For Event Type = "+evtype+" / Data type = "+dtatype+nl+\
		"//--     Configuration = "+cnfig+nl+\
		"//--     "+filetype1+" datasets produced by "+prog1+nl+\
		"//--     From "+filetype2+" datasets produced by "+prog2+nl+\
		"//--     From "+filetype3+" datasets produced by "+prog3+nl+\
		"//--     Database version = "+ dbversion+nl+\
		"//--     Cards content = " + cards_content+nl+\
		"//--"+nl+\
		"//-- Datasets replicated at " + rplicalocation+nl+\
		"//-- "+n_datas_str+" dataset(s) - NbEvents = "+nevts_str+nl+\
		"//-- "+nl+\
		"EventSelector.Input={"+nl
		
		results.write(informations)
		for element in self.descriptions_list:
			path=element['name']
			line+="\"DATAFILE=\'"
			if path.find("/") != -1:
				line+=preamble
			line+=path
			line+="\' TYP=\'POOL_ROOTTREE\' OPT=\'READ\'\",\n"
			results.write(line)
			line=""
			
		results.write("};\n//-- End of Data cards\n//\n")
		
		results.close()
		
		n_datas_str = str(len(self.descriptions_list))
		print n_datas_str + " results successfully printed on file " + filename + "!"
		
##############################################################################

	def close_save_file(self):
		self.save_file_dialog.setHidden(1)

##############################################################################

	def hide_results_dialog(self):
		self.results_dialog.setHidden(1)

##############################################################################
	
	def next_page(self):
		pos_step_list=str(self.Txt_datasets_from_to_step.text()).split(" ")
		#print pos_step_list
		last = int(pos_step_list[3])
		len_descriptions_list=len(self.descriptions_list)
		if last > len_descriptions_list:
			last = len_descriptions_list
		step = pos_step_list[5]
		self.build_table(self.descriptions_list,last,step)

##############################################################################
	
	def prev_page(self):	
		pos_step_list=str(self.Txt_datasets_from_to_step.text()).split(" ")
		begin = int(pos_step_list[1])
		last = int(pos_step_list[3])
		step = int(pos_step_list[5])
		n_files = int(pos_step_list[8])
		rest=n_files%step
		if n_files-begin < step and rest!=0:
			previous = last - rest - step
			#print "rest"
		else:
			previous = last - 2*step
			#print "norest"
		if previous < 0:
			previous =0
		
		self.build_table(self.descriptions_list,previous,str(step))

##############################################################################	

	def radio_state_changed(self):
		# Clear all the contents of the comboboxes and show them all
		combobox_list = [\
			self.comboboxdatatype,self.combobox_dataset_replicated,\
			self.comboboxdbvers,self.combobox_evt_type,\
			self.comboboxint_type1,self.comboboxstep1,\
			self.comboboxint_type2,self.comboboxstep2,\
			self.comboboxint_type3,self.comboboxstep3\
			]
			
		for combobox in combobox_list:
			combobox.clear()
			combobox.setHidden(0)
		# Clear the number of events
		self.lineEditNevts.clear()
		
		if self.radioXML.isChecked():
			self.config_list_XML()
		else:
			self.config_list_AMGA()

##############################################################################

	def show_advanced(self):
		dialog_height=self.results_dialog.height()
		dialog_width=self.results_dialog.width()
		if self.radioAdvanced.isChecked():
			self.results_dialog.resize(QSize(dialog_width,dialog_height+110))
		else:
			self.results_dialog.resize(QSize(dialog_width,dialog_height-110))
			
##############################################################################

	def config_list_AMGA(self):
		self.bkdb = LHCB_BKDB.bkdbFactory('AMGA')
		config_list = ['Choose any']
		config_list += self.bkdb.getAvailableConfigurations()
		self.insert_list_in_combobox(config_list,self.combobox_configuratio)
		
##############################################################################

	def config_list_XML(self):
		self.bkdb = LHCB_BKDB.bkdbFactory('XMLRPC')
		config_list = ['Choose any']
		config_list += self.bkdb.getAvailableConfigurations()
		self.insert_list_in_combobox(config_list,self.combobox_configuratio)

##############################################################################

	def return_lof(self):
		if str(self.combobox_configuratio.currentText())=='Choose any':
			print 'Illegal Query!'
			return -1
		if self.descriptions_list == []:
			self.query_server(0)
#			print "submit query!"

#	FIXME  ++++++++++++++++++++++++++++++++++++++++++++++++++	
		for element in self.descriptions_list:
			files_name_list.append("LFN:"+element["name"])
			# LFN ADDED BY HAND!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			#print element["name"]
# 		print \
# 		"""
# 		                 ==================================================
# 		+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 		###############################################################################
# 		# CAUTION! "LFN:" before the name of the file has been added by hand. It needs to be fixed! #
# 		###############################################################################
# 		+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 		                 ==================================================
# 		"""
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		self.hide_all()
		
###############################################################################

	def hide_all(self):
#		print "Hide All!"
		self.descriptions_list = []
		self.results_dialog.hide()
		self.report_file_dialog.hide()
		self.save_file_dialog.hide()
		self.hide()

#################################################################################

	def __tr(self,s,c = None):
		return qApp.translate("wpage",s,c)
		
#########################################################

app = QApplication(sys.argv)
aw=wpage()
app.setMainWidget(aw)
app.connect(aw.close_button,SIGNAL("clicked()"),app,SLOT("quit()"))
app.connect(aw.lof_button,SIGNAL("clicked()"),app,SLOT("quit()"))
app.connect(aw.query_close_button,SIGNAL("clicked()"),app,SLOT("quit()"))


def browse(mode="GANGA"):
	global files_name_list
	files_name_list=[]
	aw.show()
	if mode == "standalone":
		aw.warning_dialog.show()
	app.exec_loop()
	aw.hide()
	return files_name_list
