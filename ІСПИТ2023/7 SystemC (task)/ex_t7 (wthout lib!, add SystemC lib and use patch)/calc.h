#ifndef CALC_H
#define CALC_H

#include "addsub.h"

SC_MODULE(Calc) { // 4*A + B - C
    sc_in<sc_int<8>> inA;
    sc_in<sc_int<8>> inB;
    sc_in<sc_int<8>> inC;
    sc_out<sc_int<8>> outResult;

    sc_signal<sc_int<8>> sigTemp4A;
    sc_signal<sc_int<8>> sigTemp4APlusB;

    sc_signal<sc_int<8>> sig_addsub_m1_out1;
    sc_signal<sc_int<8>> sig_addsub_m2_out2;

    AddSub * addsub_m1;
    AddSub * addsub_m2;

    void updateTemp4A();

    SC_CTOR( Calc ) {
        addsub_m1 = new AddSub("addsub_m1");
        addsub_m1->in1(sigTemp4A);
        addsub_m1->in2(inB);
        addsub_m1->out1(sigTemp4APlusB);
        addsub_m1->out2(sig_addsub_m1_out1);

        addsub_m2 = new AddSub("addsub_m2");
        addsub_m2->in1(sigTemp4APlusB);
        addsub_m2->in2(inC);
        addsub_m2->out1(sig_addsub_m2_out2);
        addsub_m2->out2(outResult);

        SC_METHOD(updateTemp4A);
        sensitive << inA << inB << inC;
    }
};

#endif
