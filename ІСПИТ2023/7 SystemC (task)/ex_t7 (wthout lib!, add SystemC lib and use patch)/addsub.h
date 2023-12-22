#ifndef ADDSUB_H
#define ADDSUB_H

SC_MODULE(AddSub) {
    sc_in<sc_int<8>> in1;
    sc_in<sc_int<8>> in2;
    sc_out<sc_int<8>> out1;
    sc_out<sc_int<8>> out2;

    void addSubAction();

    SC_CTOR( AddSub ) {
        SC_METHOD(addSubAction);
        sensitive << in1 << in2;
    }
};

#endif
