//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

package netpoll

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestPollAttachmentUnalignedRoundTrip(t *testing.T) {
	attachment := &PollAttachment{FD: 42}
	const size = unsafe.Sizeof(attachment)
	buf := make([]byte, 2*size)
	offset := 0
	if uintptr(unsafe.Pointer(&buf[0]))%size == 0 {
		offset = 1
	}
	ptr := unsafe.Pointer(&buf[offset])
	require.NotZero(t, uintptr(ptr)%size, "the test storage must be unaligned")
	// The kernel's event ABI provides byte storage, not an aligned Go pointer.
	// This must also work with the checkptr instrumentation enabled by -race.
	convertPollAttachment(ptr, attachment)
	require.Same(t, attachment, restorePollAttachment(ptr))
	runtime.KeepAlive(attachment)
}
