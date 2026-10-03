//go:build amd64

#include "textflag.h"

TEXT ·procYield(SB),7,$0
    MOVL cycles+0(FP), AX
loop:
    PAUSE
    SUBL $1, AX
    JNZ  loop
    RET
