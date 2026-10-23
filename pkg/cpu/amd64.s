//go:build amd64

#include "textflag.h"

// func procYield(cycles int)
TEXT ·procYield(SB), NOSPLIT, $0-8
	MOVQ cycles+0(FP), AX
	TESTQ AX, AX
	JLE  done          // cycles <= 0: do nothing
loop:
	PAUSE
	SUBQ $1, AX
	JNZ  loop
done:
	RET
