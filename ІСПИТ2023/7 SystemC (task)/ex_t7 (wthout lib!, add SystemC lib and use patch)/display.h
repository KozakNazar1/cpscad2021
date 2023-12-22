#ifndef DISPLAY_H
#define DISPLAY_H 

struct Display : sc_module {
    sc_in_clk      clk;
    sc_in<sc_int<8>> inA;
    sc_in<sc_int<8>> inB;
    sc_in<sc_int<8>> inC;
    sc_in<sc_int<8>> outResult;
    
    void print();
    
    SC_CTOR(Display) {
        SC_METHOD( print );
        sensitive_pos << clk;
    }
};

#endif
