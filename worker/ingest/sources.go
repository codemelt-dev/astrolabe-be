package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"stellar-explorer/worker/config"
	"stellar-explorer/worker/sep54"
	"stellar-explorer/worker/sequencer"
)

// LazySource opens a store on first use and retries the open on later
// reads if it failed, so a store that is unreachable at startup (the AWS
// lake, the archive) never blocks ingest. Open failures are reported as
// unavailability.
type LazySource struct {
	name string
	open func(ctx context.Context) (*sep54.Source, error)

	mu  sync.Mutex
	src *sep54.Source
}

// NewLazySource returns a source named name that opens with open.
func NewLazySource(name string, open func(ctx context.Context) (*sep54.Source, error)) *LazySource {
	return &LazySource{name: name, open: open}
}

func (l *LazySource) Name() string { return l.name }

// Source opens the store if needed.
func (l *LazySource) Source(ctx context.Context) (*sep54.Source, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.src != nil {
		return l.src, nil
	}
	src, err := l.open(ctx)
	if err != nil {
		return nil, err
	}
	l.src = src
	return src, nil
}

// GetLedger implements sequencer.Source. A store that cannot be opened
// (whatever the reason) counts as unavailable, so routing retries it and
// eventually falls back, rather than treating it as a missing ledger.
func (l *LazySource) GetLedger(ctx context.Context, seq uint32) (sep54.Object, error) {
	src, err := l.Source(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || sep54.IsUnavailable(err) {
			return sep54.Object{}, err
		}
		return sep54.Object{}, &sep54.UnavailableError{Op: "open " + l.name, Err: err}
	}
	return src.GetLedger(ctx, seq)
}

var _ sequencer.Source = (*LazySource)(nil)

// Sources are the three stores (lake, archive, AWS lake), opened lazily
// with their own explicit clients (never the AWS credential chain).
type Sources struct {
	Lake    *LazySource
	Archive *LazySource // nil unless archive.enabled
	// AWS is the AWS public lake (a fake in tests).
	AWS     sequencer.Source
	LakeTip *sep54.TipClient
}

// OpenSources prepares the lake (gateway.env), the archive (archive.env,
// when enabled) and the AWS public lake. Nothing is contacted yet.
func OpenSources(cfg config.Config) (Sources, error) {
	creds, err := config.LoadGatewayEnv(cfg.Credentials.GatewayEnv)
	if err != nil {
		return Sources{}, err
	}
	s := Sources{
		Lake: NewLazySource(sep54.SourceLake, func(ctx context.Context) (*sep54.Source, error) {
			return sep54.OpenLake(ctx, cfg.Lake, creds)
		}),
		AWS:     NewLazySource(sep54.SourceAWS, sep54.OpenAWSLake),
		LakeTip: sep54.NewTipClient(cfg.Lake.ControlListen),
	}
	if cfg.Archive.Enabled {
		acreds, err := config.LoadArchiveEnv(cfg.Credentials.ArchiveEnv)
		if err != nil {
			return Sources{}, err
		}
		s.Archive = NewLazySource(sep54.SourceArchive, func(ctx context.Context) (*sep54.Source, error) {
			return sep54.OpenArchive(ctx, cfg.Archive, cfg.Lake, acreds)
		})
	}
	return s, nil
}

// OpenArchiveSources prepares the archive (archive.env) and the AWS public
// lake on a host with no lake, for `worker clickhouse-init`: no
// gateway.env, and no tip client. The archive's key prefix is the lake's
// (lake.bucket_path, the default on both hosts).
func OpenArchiveSources(cfg config.Config) (Sources, error) {
	acreds, err := config.LoadArchiveEnv(cfg.Credentials.ArchiveEnv)
	if err != nil {
		return Sources{}, err
	}
	return Sources{
		Archive: NewLazySource(sep54.SourceArchive, func(ctx context.Context) (*sep54.Source, error) {
			return sep54.OpenArchive(ctx, cfg.Archive, cfg.Lake, acreds)
		}),
		AWS: NewLazySource(sep54.SourceAWS, sep54.OpenAWSLake),
	}, nil
}

// ByName returns the source for --source (lake, archive or aws).
func (s Sources) ByName(name string) (sequencer.Source, error) {
	switch name {
	case sep54.SourceLake:
		if s.Lake == nil {
			return nil, errors.New("no lake on this host")
		}
		return s.Lake, nil
	case sep54.SourceArchive:
		if s.Archive == nil {
			return nil, errors.New("the archive is disabled (archive.enabled = false)")
		}
		return s.Archive, nil
	case sep54.SourceAWS:
		return s.AWS, nil
	}
	return nil, fmt.Errorf("unknown source %q: want lake, archive or aws", name)
}

// InOrder returns the sources in routing order (lake, archive, AWS).
func (s Sources) InOrder() []sequencer.Source {
	var out []sequencer.Source
	if s.Lake != nil {
		out = append(out, s.Lake)
	}
	if s.Archive != nil {
		out = append(out, s.Archive)
	}
	return append(out, s.AWS)
}
