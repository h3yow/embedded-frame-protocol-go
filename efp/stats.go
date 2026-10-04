package efp

// Stats is a point-in-time snapshot of a session's runtime counters.
// All counters are monotonically increasing since session creation.
type Stats struct {
	FramesDecoded      uint64 // frames successfully decoded from the stream
	DecodeErrors       uint64 // frames/bytes rejected by the decoder
	NoiseBytesSkipped  uint64 // bytes dropped while resynchronizing
	DuplicateFragments uint64 // fragments ignored because already received
	FragmentTimeouts   uint64 // incomplete messages expired by the janitor
	InFlightRejected   uint64 // new messages rejected: too many in flight
}
