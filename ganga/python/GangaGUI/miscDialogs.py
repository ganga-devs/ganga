import qt

def warningMessage( parent, text, caption = "Oops!" ):
   return qt.QMessageBox.warning( parent, str( caption ), str( text ), "OK", "", "", 0, 0 )

def infoMessage( parent, text, caption = "Information" ):
   return qt.QMessageBox.information( parent, str( caption ), str( text ), "OK", "", "", 0, 0 )

def warningDialog( parent, text, caption = "Warning!" ):
   return qt.QMessageBox.warning( parent, str( caption ), str( text ), "OK", "&Cancel", "", 1, 1 )

def questionDialog( parent, text, caption = "Question" ):
   return qt.QMessageBox.question( parent, str( caption ), str( text ), "Yes", "No", "Cancel", 0, 2 )
