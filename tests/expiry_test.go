package tests

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// respCmd encodes args as one AOF command.
func respCmd(args ...string) string {
	s := fmt.Sprintf("*%d\r\n", len(args))
	for _, a := range args {
		s += fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)
	}
	return s
}

var aofPXAT = regexp.MustCompile(`\$4\r\npxat\r\n\$\d+\r\n(\d+)\r\n`)

// lastPXAT returns the deadline of the last pxat written to the AOF.
func lastPXAT(aof []byte) (int64, error) {
	m := aofPXAT.FindAllSubmatch(aof, -1)
	if len(m) == 0 {
		return 0, fmt.Errorf("no pxat in aof")
	}
	return strconv.ParseInt(string(m[len(m)-1][1]), 10, 64)
}

// pxatNear checks that the last pxat in the AOF is d after a moment between
// before and now (rounded up to whole milliseconds).
func pxatNear(mc *mockServer, before time.Time, d time.Duration) error {
	aof, err := mc.readAOF()
	if err != nil {
		return err
	}
	ms, err := lastPXAT(aof)
	if err != nil {
		return err
	}
	lo := before.Add(d).UnixMilli()
	hi := time.Now().Add(d).UnixMilli() + 1
	if ms < lo || ms > hi {
		return fmt.Errorf("expected pxat in [%d, %d], got %d", lo, hi, ms)
	}
	return nil
}

func aof_PXAT_test(mc *mockServer) error {
	// EX is written as an absolute PXAT, every other argument as sent
	before := time.Now()
	if _, err := mc.Do("SET", "fleet", "a", "FIELD", "speed", 10, "EX", 60, "POINT", 33, -115); err != nil {
		return err
	}
	if err := pxatNear(mc, before, time.Minute); err != nil {
		return err
	}
	aof, err := mc.readAOF()
	if err != nil {
		return err
	}
	ms, _ := lastPXAT(aof)
	want := respCmd("SET", "fleet", "a", "FIELD", "speed", "10", "pxat", strconv.FormatInt(ms, 10), "POINT", "33", "-115")
	if !bytes.HasSuffix(aof, []byte(want)) {
		return fmt.Errorf("expected the aof to end with %q", want)
	}

	// PXAT is written unchanged
	deadline := pxat(time.Hour)
	if _, err := mc.Do("SET", "fleet", "b", "PXAT", deadline, "POINT", 33, -115); err != nil {
		return err
	}
	if aof, err = mc.readAOF(); err != nil {
		return err
	}
	want = respCmd("SET", "fleet", "b", "PXAT", deadline, "POINT", "33", "-115")
	if !bytes.HasSuffix(aof, []byte(want)) {
		return fmt.Errorf("expected the aof to end with %q", want)
	}

	// an expired write over an existing object is written as DEL
	if _, err := mc.Do("SET", "fleet", "a", "PXAT", pxat(-time.Minute), "POINT", 33, -115); err != nil {
		return err
	}
	if aof, err = mc.readAOF(); err != nil {
		return err
	}
	want = respCmd("del", "fleet", "a")
	if !bytes.HasSuffix(aof, []byte(want)) {
		return fmt.Errorf("expected the aof to end with %q", want)
	}

	// an expired write with nothing to remove writes nothing
	size := len(aof)
	if _, err := mc.Do("SET", "fleet", "nothing", "PXAT", pxat(-time.Minute), "POINT", 33, -115); err != nil {
		return err
	}
	if aof, err = mc.readAOF(); err != nil {
		return err
	}
	if len(aof) != size {
		return fmt.Errorf("expected no aof write, the aof grew by %d bytes", len(aof)-size)
	}
	return mc.DoBatch(Do("FLUSHDB").OK())
}

func aof_PXAT_replay_test(mc *mockServer) error {
	// Replaying keeps absolute deadlines. A deadline that passed while the
	// server was down is not an error: the object loads expired, is removed,
	// and the commands after it are applied.
	aof := respCmd("SET", "fleet", "gone", "PXAT", pxat(-time.Minute), "POINT", "33", "-115") +
		respCmd("SET", "fleet", "kept", "POINT", "33", "-115") +
		respCmd("SET", "fleet", "later", "PXAT", pxat(time.Minute), "POINT", "33", "-115") +
		respCmd("SET", "fleet", "legacy", "EX", "60", "POINT", "33", "-115")
	mc2, err := loadAOF(aof)
	if err != nil {
		return err
	}
	defer mc2.Close()
	time.Sleep(bgExpireWait)
	return mc2.DoBatch(
		Do("GET", "fleet", "gone").Str("<nil>"),
		Do("GET", "fleet", "kept").Str(`{"type":"Point","coordinates":[-115,33]}`),
		Do("TTL", "fleet", "later").Func(ttlWithin(58, 60)),
		Do("TTL", "fleet", "legacy").Func(ttlWithin(58, 60)),
	)
}

func keys_PEXPIREAT_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("PEXPIREAT", "mykey", "myid").Err("wrong number of arguments for 'pexpireat' command"),
		Do("PEXPIREAT", "mykey", "myid", "abc").Err("invalid argument 'abc'"),
		Do("PEXPIREAT", "mykey", "myid", "0").Err("invalid argument '0'"),
		Do("PEXPIREAT", "mykey", "myid", pxat(time.Minute)).Str("1"),
		Do("TTL", "mykey", "myid").Func(ttlWithin(58, 60)),
		Do("PEXPIREAT", "mykey", "missing", pxat(time.Minute)).Str("0"),
		Do("PEXPIREAT", "nokey", "myid", pxat(time.Minute)).Str("0"),
		Do("PEXPIREAT", "mykey", "missing", pxat(time.Minute)).JSON().Err("id not found"),
		Do("PEXPIREAT", "nokey", "myid", pxat(time.Minute)).JSON().Err("key not found"),

		// a deadline that has passed removes the object
		Do("PEXPIREAT", "mykey", "myid", pxat(-time.Minute)).Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),

		// so does EXPIRE with a timeout that has already run out
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("EXPIRE", "mykey", "myid", -1).Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("FLUSHDB").OK(),
	)
}

func aof_PEXPIREAT_test(mc *mockServer) error {
	if _, err := mc.Do("SET", "fleet", "a", "POINT", 33, -115); err != nil {
		return err
	}
	// EXPIRE is written as PEXPIREAT with the absolute deadline
	before := time.Now()
	if _, err := mc.Do("EXPIRE", "fleet", "a", 60); err != nil {
		return err
	}
	aof, err := mc.readAOF()
	if err != nil {
		return err
	}
	re := regexp.MustCompile(`\*4\r\n\$9\r\npexpireat\r\n\$5\r\nfleet\r\n\$1\r\na\r\n\$\d+\r\n(\d+)\r\n$`)
	m := re.FindSubmatch(aof)
	if m == nil {
		return fmt.Errorf("expected the aof to end with pexpireat")
	}
	ms, _ := strconv.ParseInt(string(m[1]), 10, 64)
	lo, hi := before.Add(time.Minute).UnixMilli(), time.Now().Add(time.Minute).UnixMilli()+1
	if ms < lo || ms > hi {
		return fmt.Errorf("expected pexpireat in [%d, %d], got %d", lo, hi, ms)
	}
	// PEXPIREAT is written unchanged
	deadline := pxat(time.Hour)
	if _, err := mc.Do("PEXPIREAT", "fleet", "a", deadline); err != nil {
		return err
	}
	if aof, err = mc.readAOF(); err != nil {
		return err
	}
	want := respCmd("PEXPIREAT", "fleet", "a", deadline)
	if !bytes.HasSuffix(aof, []byte(want)) {
		return fmt.Errorf("expected the aof to end with %q", want)
	}
	// a deadline that has passed is written as DEL
	if _, err := mc.Do("PEXPIREAT", "fleet", "a", pxat(-time.Minute)); err != nil {
		return err
	}
	if aof, err = mc.readAOF(); err != nil {
		return err
	}
	want = respCmd("del", "fleet", "a")
	if !bytes.HasSuffix(aof, []byte(want)) {
		return fmt.Errorf("expected the aof to end with %q", want)
	}
	return mc.DoBatch(Do("FLUSHDB").OK())
}

// bgExpireWait covers a few runs of the background expiration (every 100ms).
const bgExpireWait = 300 * time.Millisecond

// pxat returns the PXAT argument for a deadline d from now.
func pxat(d time.Duration) string {
	return strconv.FormatInt(time.Now().Add(d).UnixMilli(), 10)
}

// ttlWithin checks a TTL reply against the range [lo, hi] seconds.
func ttlWithin(lo, hi int) func(s string) error {
	return func(s string) error {
		n, err := strconv.Atoi(s)
		if err != nil || n < lo || n > hi {
			return fmt.Errorf("expected ttl in [%d, %d], got '%s'", lo, hi, s)
		}
		return nil
	}
}

func keys_SET_PXAT_test(mc *mockServer) error {
	return mc.DoBatch(
		// arguments
		Do("SET", "mykey", "myid", "PXAT").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "PXAT", "abc", "POINT", 33, -115).Err("invalid argument 'abc'"),
		Do("SET", "mykey", "myid", "PXAT", "0", "POINT", 33, -115).Err("invalid argument '0'"),
		Do("SET", "mykey", "myid", "PXAT", "-5", "POINT", 33, -115).Err("invalid argument '-5'"),
		Do("SET", "mykey", "myid", "PXAT", "1.5", "POINT", 33, -115).Err("invalid argument '1.5'"),

		// a deadline in the future
		Do("SET", "mykey", "myid", "PXAT", pxat(time.Minute), "POINT", 33, -115).OK(),
		Do("TTL", "mykey", "myid").Func(ttlWithin(58, 60)),

		// the last of EX and PXAT wins, as with a repeated EX
		Do("SET", "mykey", "myid", "EX", 100, "PXAT", pxat(10*time.Second), "POINT", 33, -115).OK(),
		Do("TTL", "mykey", "myid").Func(ttlWithin(8, 10)),
		Do("SET", "mykey", "myid", "PXAT", pxat(10*time.Second), "EX", 100, "POINT", 33, -115).OK(),
		Do("TTL", "mykey", "myid").Func(ttlWithin(98, 100)),

		// a deadline that has passed: the object is removed, not stored
		Do("SET", "mykey", "myid", "PXAT", pxat(-time.Minute), "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("KEYS", "*").Str("[]"),

		// ... and nothing is created when there was no object
		Do("SET", "mykey", "myid", "PXAT", pxat(-time.Minute), "POINT", 33, -115).OK(),
		Do("KEYS", "*").Str("[]"),

		// NX and XX are decided first
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid", "NX", "PXAT", pxat(-time.Minute), "POINT", 33, -115).Str("<nil>"),
		Do("GET", "mykey", "myid").Str(`{"type":"Point","coordinates":[-115,33]}`),
		Do("SET", "mykey", "other", "XX", "PXAT", pxat(-time.Minute), "POINT", 33, -115).Str("<nil>"),
		Do("SET", "mykey", "myid", "XX", "PXAT", pxat(-time.Minute), "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid").Str("<nil>"),

		// EX that has already run out behaves the same way
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid", "EX", -1, "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid").Str("<nil>"),

		Do("FLUSHDB").OK(),
	)
}
