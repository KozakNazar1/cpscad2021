#ifndef NUMGEN_H
#define NUMGEN_H

struct Numgen : sc_module {
    sc_out<sc_int<8>> out;
    sc_in_clk      clk;

    void generate();
    
    SC_CTOR(Numgen) {
	SC_METHOD( generate );
        sensitive_pos << clk;				       
    }
};
#endif


    
