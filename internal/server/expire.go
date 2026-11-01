package server

import (
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/log"
	"github.com/tidwall/tile38/internal/object"
)

const bgExpireDelay = time.Second / 10

// maxPXAT is the largest PXAT accepted: its deadline in nanoseconds must fit
// an int64 (year 2262).
const maxPXAT = (1<<63 - 1) / int64(time.Millisecond)

// parsePXAT parses a PXAT argument, a unix time in milliseconds, into an
// expiration in unix nanoseconds.
func parsePXAT(s string) (int64, bool) {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil || ms <= 0 || ms > maxPXAT {
		return 0, false
	}
	return ms * int64(time.Millisecond), true
}

// formatPXAT formats an expiration in unix nanoseconds as a PXAT argument.
// It rounds up, so a reloaded object never expires before the original.
func formatPXAT(ex int64) string {
	ms := ex / int64(time.Millisecond)
	if ex%int64(time.Millisecond) > 0 {
		ms++
	}
	return strconv.FormatInt(ms, 10)
}

// withAbsoluteExpiry returns a copy of args for the AOF in which the relative
// expiration at args[i] (the option) and args[i+1] (its value) is replaced by
// the absolute one, ex, under the option name abs. Replaying the AOF must not
// restart a TTL, so relative expirations are never written as is.
func withAbsoluteExpiry(args []string, i int, abs string, ex int64) []string {
	nargs := make([]string, len(args))
	copy(nargs, args)
	nargs[i] = abs
	nargs[i+1] = formatPXAT(ex)
	return nargs
}

// alreadyExpired reports whether a write with the expiration ex should be
// treated as a delete. Only a leader serving clients does so. While loading
// the AOF, or on a follower, an expiration in the past is kept as is and the
// object is removed by the background expiration (or by the leader's DEL):
// a replayed command must never fail or diverge because time has passed.
func (s *Server) alreadyExpired(ex int64) bool {
	return ex <= time.Now().UnixNano() &&
		s.loadedAndReady.Load() && s.config.followHost() == ""
}

// expireNow removes key/id like DEL does, for a write whose expiration has
// already passed. It returns the removed object, or nil if there was none.
func (s *Server) expireNow(key, id string) *object.Object {
	col, _ := s.cols.Get(key)
	if col == nil {
		return nil
	}
	old := col.Delete(id)
	if old == nil {
		return nil
	}
	if col.Count() == 0 {
		s.cols.Delete(key)
	}
	s.groupDisconnectObject(key, id)
	s.statsExpired.Add(1)
	return old
}

// expiredDetails describes the delete that replaces a write whose expiration
// has already passed, logged to the AOF as DEL key id. With no object to
// remove, nothing is written.
func expiredDetails(key, id string, old *object.Object) commandDetails {
	var d commandDetails
	d.command = "del"
	d.key = key
	d.obj = old
	d.updated = old != nil
	d.timestamp = time.Now()
	d.aofArgs = []string{"del", key, id}
	return d
}

// backgroundExpiring deletes expired items from the database.
// It's executes every 1/10 of a second.
func (s *Server) backgroundExpiring(wg *sync.WaitGroup) {
	defer wg.Done()
	s.loopUntilServerStops(bgExpireDelay, func() {
		s.mu.LockLowPriority()
		defer s.mu.Unlock()
		now := time.Now()
		s.backgroundExpireObjects(now)
		s.backgroundExpireHooks(now)
	})
}

func (s *Server) backgroundExpireObjects(now time.Time) {
	nano := now.UnixNano()
	var msgs []*Message
	s.cols.Scan(func(key string, col *collection.Collection) bool {
		col.ScanExpires(func(o *object.Object) bool {
			if nano < o.Expires() {
				return false
			}
			s.statsExpired.Add(1)
			msgs = append(msgs, &Message{Args: []string{"del", key, o.ID()}})
			return true
		})
		return true
	})
	for _, msg := range msgs {
		_, d, err := s.cmdDEL(msg)
		if err != nil {
			log.Fatal(err)
		}
		if err := s.writeAOF(msg.Args, &d); err != nil {
			log.Fatal(err)
		}
	}
	if len(msgs) > 0 {
		log.Debugf("Expired %d objects\n", len(msgs))
	}
}

func (s *Server) backgroundExpireHooks(now time.Time) {
	var msgs []*Message
	s.hookExpires.Ascend(nil, func(v interface{}) bool {
		h := v.(*Hook)
		if h.expires.After(now) {
			return false
		}
		msg := &Message{}
		if h.channel {
			msg.Args = []string{"delchan", h.Name}
		} else {
			msg.Args = []string{"delhook", h.Name}
		}
		msgs = append(msgs, msg)
		return true
	})

	for _, msg := range msgs {
		_, d, err := s.cmdDelHook(msg)
		if err != nil {
			log.Fatal(err)
		}
		if err := s.writeAOF(msg.Args, &d); err != nil {
			log.Fatal(err)
		}
	}
	if len(msgs) > 0 {
		log.Debugf("Expired %d hooks\n", len(msgs))
	}
}
