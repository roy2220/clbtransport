package clbtransport

const DefaultHotConnsPerHost = defaultHotConnsPerHost

type TransportStats struct {
	Subs         []*SubTransportStats
	NextSubIndex int
}

type SubTransportStats struct {
	MaxAge   string
	RefCount int64
}

func (t *Transport) Stats() TransportStats {
	t.lock.Lock()
	defer t.lock.Unlock()

	var stats TransportStats
	stats.Subs = make([]*SubTransportStats, len(t.subs))
	for i, sub := range t.subs {
		if sub == nil {
			continue
		}
		var subStats SubTransportStats
		subStats.MaxAge = sub.MaxAge.String()
		subStats.RefCount = sub.refCount.Load()
		stats.Subs[i] = &subStats
	}
	stats.NextSubIndex = t.nextSubIndex
	return stats
}
