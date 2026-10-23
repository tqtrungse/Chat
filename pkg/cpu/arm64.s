//go:build arm64

#include "textflag.h"

// func procYield(cycles int)
TEXT ·procYield(SB), NOSPLIT, $0-8
	MOVD cycles+0(FP), R0
	CMP  $0, R0
	BLE  done          // cycles <= 0: do nothing
loop:
	YIELD
	SUB  $1, R0
	CBNZ R0, loop
done:
	RET
