#include "systemc.h"
#include "numgen.h"

void Numgen::generate(){
  static sc_int<8> value = 0;
  out.write(++value);
}