#include <iostream>
#include <fstream>
using std::ofstream;
using std::endl;

#include "Main.h"

ClassImp(Main)//needed for Cint

Main::Main(char* arvv[], int argc){
  //do some setup, command line opts etc
}

int Main::run(){
  ofstream file("output.txt");
  if(file){
    file << "12345" << endl;
    file.close();
      }
  return 0;
}
