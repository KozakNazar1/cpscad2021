#include "systemc.h"
#include "addsub.h"

void AddSub::addSubAction(){
    sc_int<8> in1Temp = in1.read();
    sc_int<8> in2Temp = in2.read();

    out1.write(in1Temp + in2Temp);
    out2.write(in1Temp - in2Temp);
}


