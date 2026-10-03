//go:build arm64

#include "textflag.h"

// TEXT ·procyield(SB), NOSPLIT, $0
TEXT ·procYield(SB), NOSPLIT, $0
    MOVW cycles+0(FP), R0
loop:
    YIELD
    SUB  $1, R0
    CBNZ R0, loop
    RET
