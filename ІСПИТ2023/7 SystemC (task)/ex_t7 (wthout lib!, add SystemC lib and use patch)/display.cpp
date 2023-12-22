#include "systemc.h"
#include "display.h"

void Display::print(){
    static int rise_edge_event_index = 0;
    cout << "(rise edge event index :" << rise_edge_event_index++ << ") :: ";
    cout << "inA = " << inA.read().to_int() << " "
         << "in1 = " << inB.read().to_int() << " "
         << "sel = " << inC.read().to_int() << " "
         << "out = " << outResult.read().to_int()
         << endl;
} 

