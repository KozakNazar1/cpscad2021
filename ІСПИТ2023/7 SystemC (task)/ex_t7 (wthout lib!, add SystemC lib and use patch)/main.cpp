#include "systemc.h"
#include "numgen.h"
#include "addsub.h"
#include "calc.h"
#include "display.h"

#define NOT_USE_SC_CLOCK

#define EXP_LEVEL -9
#define NS_(EXP_LEVEL) * 1e##EXP_LEVEL
#define NS NS_(EXP_LEVEL)

int sc_main(int ac, char* av[]) {
	//CLK
#ifdef NOT_USE_SC_CLOCK
	sc_signal<bool> clk;
#else
	sc_clock clk("Clock", 1, 0.5, 0.0);	
#endif

	//Signals
	sc_signal<sc_int<8>> sigA(7);
	sc_signal<sc_int<8>> sigB(8);
	sc_signal<sc_int<8>> sigC(9);
	sc_signal<sc_int<8>> sigResult;

	Numgen numgen("numgen");
	numgen(sigA, clk);

	Calc calc("calc");
	calc.inA(sigA);
	calc.inB(sigB);
	calc.inC(sigC);
	calc.outResult(sigResult);

	Display display("display"); 
	display(clk, sigA, sigB, sigC, sigResult);

	sc_trace_file* tfp = sc_create_vcd_trace_file("counter_tracefile"); // (=> out\build\Debug)
	((vcd_trace_file*)tfp)->sc_set_vcd_time_unit(EXP_LEVEL); // set_time_unit(1, NS); // set tracing resolution to ns

	sc_trace(tfp, clk, "clk");
	sc_trace(tfp, sigA, "sigA");
	sc_trace(tfp, sigB, "sigB");
	sc_trace(tfp, sigC, "sigC");
	sc_trace(tfp, sigResult, "sigResult");
	
#ifdef NOT_USE_SC_CLOCK
	sc_initialize();
	for (int index = 0; index < 19; ++index) {
		clk.write(0);
		sc_cycle(10 NS);
		clk.write(1);
		sc_cycle(10 NS);
	}
#else
	//	
	sc_start(clk, 19);
	// ...
	sc_stop();
#endif
	sc_close_vcd_trace_file(tfp);

	return 0;
}