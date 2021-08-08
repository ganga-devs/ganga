#ifndef MAIN_H
#define MAIN_H

#include "TObject.h"

class Main : public TObject {

 public:

  Main(){}//needed by Root IO
  Main(char* argv[], int argc);
  int run();
  
  ClassDef(Main,1)//Needed for Cint
  
};
#endif
