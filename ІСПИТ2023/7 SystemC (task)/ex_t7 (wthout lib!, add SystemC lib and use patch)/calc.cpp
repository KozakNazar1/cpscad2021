#include "systemc.h"
#include "calc.h"

void Calc::updateTemp4A(){
    sc_int<8> temp4A;
    temp4A.range(1, 0) = 0;
    temp4A.range(7, 2) = sc_int<8>(inA.read().range(5, 0));
    sigTemp4A.write(temp4A);
}


