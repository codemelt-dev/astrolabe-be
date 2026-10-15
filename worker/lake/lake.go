// Package lake runs `worker lake`: Galexie's S3 endpoint. It embeds
// VersityGW over a POSIX backend with xattr metadata, makes every object
// durable before acknowledging it, and publishes the commit tip on a control
// endpoint that ingest long-polls.
//
// The lake has no Postgres and no outbound network, and is rarely
// redeployed: a lake restart is the only thing that disturbs Galexie.
package lake

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/embedgw"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3err"

	"stellar-explorer/worker/config"
)

const (
	// markerFile must exist under state/ on the lake Volume; without it the
	// Volume is not mounted and objects would land on the root disk.
	markerFile = "lake-id"
	// gatewayStopTimeout bounds waiting for VersityGW to stop; it drains
	// in-flight requests for at most 10 s itself.
	gatewayStopTimeout = 15 * time.Second
	// controlStopTimeout bounds closing the control listeners.
	controlStopTimeout = 5 * time.Second
)

// Options are process-level hooks. Production uses the zero value; tests
// inject failures and observe startup through them.
type Options struct {
	Logger *slog.Logger
	// Fsync performs every fsync of the commit path. Nil means (*os.File).Sync.
	Fsync func(stage Stage, f *os.File) error
	// Syncfs flushes the whole lake filesystem at startup. Nil means
	// syncfs(2) on Linux and an fsync walk on macOS.
	Syncfs func(dir string) error
	// BootID identifies the current boot. Nil means the kernel's boot id.
	BootID func() (string, error)
	// Notify sends an sd_notify state. Nil means systemd's NOTIFY_SOCKET
	// (a no-op outside systemd).
	Notify func(state string) error
	// LongPoll bounds GET /v1/tip?after=N. Zero means 30 s.
	LongPoll time.Duration
	// Now is the clock. Nil means time.Now.
	Now func() time.Time
}

func (o *Options) setDefaults() {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Fsync == nil {
		o.Fsync = defaultFsync
	}
	if o.Syncfs == nil {
		o.Syncfs = syncFilesystem
	}
	if o.BootID == nil {
		o.BootID = bootID
	}
	if o.Notify == nil {
		o.Notify = func(state string) error { _, err := daemon.SdNotify(false, state); return err }
	}
	if o.LongPoll == 0 {
		o.LongPoll = 30 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Lake is an opened lake, ready to Serve.
type Lake struct {
	cfg      config.Lake
	creds    config.GatewayCredentials
	opts     Options
	log      *slog.Logger
	s3root   string
	stateDir string
	bucket   string
	store    store
	px       *posix.Posix
	tap      *tap
	idx      *index
	inflight *inflight
	m        *metrics
	ready    atomic.Bool
	stopping atomic.Bool
	served   atomic.Bool
}

// Open performs lake startup steps 1–5: mount and marker checks,
// the POSIX backend with xattr metadata and the bucket, the startup syncfs,
// the boot-id gated checksum scan, and the tip/oldest index.
func Open(cfg config.Lake, creds config.GatewayCredentials, opts Options) (_ *Lake, err error) {
	opts.setDefaults()
	l := &Lake{
		cfg:      cfg,
		creds:    creds,
		opts:     opts,
		log:      opts.Logger.With("component", "lake"),
		s3root:   filepath.Join(cfg.Dir, "s3"),
		stateDir: filepath.Join(cfg.Dir, "state"),
		bucket:   cfg.Bucket(),
		idx:      newIndex(),
		inflight: newInflight(opts.Now),
	}
	l.store = store{prefixDir: filepath.Join(l.s3root, l.bucket, filepath.FromSlash(cfg.Prefix())), prefix: cfg.Prefix()}
	l.m = newMetrics(l.idx, l.inflight, cfg.Dir, l.log)

	// 1. All paths are absolute (config validation), because posix.New
	// changes the process working directory. Refuse to start unless the
	// Volume is mounted and carries our marker.
	if err := l.checkVolume(); err != nil {
		return nil, err
	}

	// 2. POSIX backend with xattr metadata (never sidecar or none: "none"
	// breaks If-None-Match and drops metadata), and the bucket.
	if err := os.MkdirAll(l.s3root, 0o750); err != nil {
		return nil, err
	}
	ms := meta.XattrMeta{}
	if err := ms.Test(l.s3root); err != nil {
		return nil, fmt.Errorf("%s does not support user xattrs: %w", l.s3root, err)
	}
	l.px, err = posix.New(l.s3root, ms, posix.PosixOpts{})
	if err != nil {
		return nil, fmt.Errorf("open POSIX backend: %w", err)
	}
	defer func() {
		if err != nil {
			l.px.Shutdown()
		}
	}()
	if err := l.createBucket(); err != nil {
		return nil, err
	}
	l.tap = &tap{
		Backend:  l.px,
		px:       l.px,
		s3root:   l.s3root,
		bucket:   l.bucket,
		prefix:   cfg.Prefix(),
		idx:      l.idx,
		inflight: l.inflight,
		m:        l.m,
		log:      l.log,
		fsyncFn:  opts.Fsync,
		now:      opts.Now,
	}

	// 3. Always syncfs: it makes durable any object a crashed lake linked
	// before its fsync chain finished (rule 2). An error is fatal.
	start := opts.Now()
	if err := opts.Syncfs(cfg.Dir); err != nil {
		return nil, fmt.Errorf("startup syncfs of %s failed (the lake must not start; check the volume): %w", cfg.Dir, err)
	}
	l.m.syncfsSeconds.Set(opts.Now().Sub(start).Seconds())
	l.log.Info("startup syncfs complete", "dir", cfg.Dir, "duration", opts.Now().Sub(start))

	// 4. After a reboot only: verify the newest objects, delete corrupt ones.
	res, err := l.checkBoot(context.Background())
	if err != nil {
		return nil, fmt.Errorf("startup checksum scan: %w", err)
	}
	l.m.scanChecked.Set(float64(res.checked))
	l.m.scanDeleted.Set(float64(len(res.deleted)))
	l.m.scanHoles.Set(float64(len(res.holes)))

	// 5. Tip and oldest from the partition directories (absent when empty).
	oldest, tip, tipAt, ok, err := l.store.bounds()
	if err != nil {
		return nil, fmt.Errorf("compute tip and oldest: %w", err)
	}
	l.idx.load(tip, oldest, tipAt, ok)
	if ok {
		l.log.Info("lake opened", "oldest", oldest, "tip", tip)
	} else {
		l.log.Info("lake opened empty")
	}
	return l, nil
}

func (l *Lake) checkVolume() error {
	fi, err := os.Stat(l.cfg.Dir)
	if err != nil {
		return fmt.Errorf("lake dir: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("lake dir %s is not a directory", l.cfg.Dir)
	}
	if l.cfg.AllowUnmounted {
		l.log.Warn("lake.allow_unmounted is set: not checking that the lake dir is a mountpoint (development only)", "dir", l.cfg.Dir)
	} else {
		mounted, err := isMountpoint(l.cfg.Dir)
		if err != nil {
			return fmt.Errorf("check mountpoint %s: %w", l.cfg.Dir, err)
		}
		if !mounted {
			return fmt.Errorf("refusing to start: %s is not a mountpoint (is the lake Volume mounted?)", l.cfg.Dir)
		}
	}
	marker := filepath.Join(l.stateDir, markerFile)
	mfi, err := os.Stat(marker)
	if err != nil {
		return fmt.Errorf("refusing to start: lake marker %s: %w (is the lake Volume mounted and initialised?)", marker, err)
	}
	if !mfi.Mode().IsRegular() || mfi.Size() == 0 {
		return fmt.Errorf("refusing to start: lake marker %s must be a non-empty file", marker)
	}
	return nil
}

// createBucket creates the bucket directory through the backend (with the
// ACL and ownership xattrs a CreateBucket request would store). An existing
// bucket is fine.
func (l *Lake) createBucket() error {
	owner := l.creds.AccessKey
	acl, err := auth.UpdateACL(&auth.PutBucketAclInput{
		ACL:                 types.BucketCannedACLPrivate,
		AccessControlPolicy: &auth.AccessControlPolicy{Owner: &types.Owner{ID: &owner}},
	}, auth.ACL{Owner: owner}, nil)
	if err != nil {
		return fmt.Errorf("bucket ACL: %w", err)
	}
	err = l.px.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket:                    aws.String(l.bucket),
		ObjectOwnership:           types.ObjectOwnershipBucketOwnerEnforced,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{},
	}, acl)
	var se s3err.S3Error
	if errors.As(err, &se) {
		switch se.BaseError().Code {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return nil
		}
	}
	if err != nil {
		return fmt.Errorf("create bucket %s: %w", l.bucket, err)
	}
	l.log.Info("created bucket", "bucket", l.bucket)
	return nil
}

// Serve performs startup step 6 and runs until ctx ends, then shuts down.
// It binds the control listener first, then starts the gateway, and sends
// READY=1 once both accept connections. The private metrics listener is
// bound best-effort in the background and never delays READY.
//
// Shutdown order: release every long-poll with the current values; stop
// accepting PUTs and let in-flight PUTs finish (at most 10 s); close the
// control listeners (at most 5 s).
func (l *Lake) Serve(ctx context.Context) error {
	if !l.served.CompareAndSwap(false, true) {
		return errors.New("lake: Serve called twice")
	}
	ctrl, err := l.listenControl()
	if err != nil {
		l.px.Shutdown()
		return err
	}
	bg, stopBG := context.WithCancel(context.Background())
	defer stopBG()
	var priv *http.Server
	if l.cfg.MetricsListen != "" {
		priv = l.newHTTPServer(l.handler(false))
		go l.servePrivate(bg, priv)
	}

	gwCtx, stopGW := context.WithCancel(context.Background())
	defer stopGW()
	gwDone := make(chan error, 1)
	go func() { gwDone <- l.runGateway(gwCtx) }()

	var runErr error
	select {
	case <-ctx.Done():
		l.log.Info("shutting down")
	case err := <-gwDone:
		gwDone = nil
		runErr = fmt.Errorf("gateway stopped: %w", err)
		if err == nil {
			runErr = errors.New("gateway stopped unexpectedly")
		}
	case err := <-ctrl.done:
		runErr = fmt.Errorf("control listener: %w", err)
	}

	l.stopping.Store(true)
	if err := l.opts.Notify("STOPPING=1"); err != nil {
		l.log.Warn("sd_notify STOPPING failed", "err", err)
	}
	l.idx.release()
	stopGW()
	if gwDone != nil {
		select {
		case err := <-gwDone:
			if err != nil {
				l.log.Warn("gateway shutdown", "err", err)
			}
		case <-time.After(gatewayStopTimeout):
			l.log.Error("gateway did not stop in time", "timeout", gatewayStopTimeout)
		}
	}
	stopBG()
	sctx, cancel := context.WithTimeout(context.Background(), controlStopTimeout)
	defer cancel()
	if err := ctrl.srv.Shutdown(sctx); err != nil {
		l.log.Warn("control listener shutdown", "err", err)
	}
	if priv != nil {
		if err := priv.Shutdown(sctx); err != nil {
			l.log.Warn("private metrics listener shutdown", "err", err)
		}
	}
	l.log.Info("lake stopped")
	return runErr
}

func (l *Lake) runGateway(ctx context.Context) error {
	return embedgw.RunVersityGW(ctx, l.tap, &embedgw.Config{
		RootUserAccess:    l.creds.AccessKey,
		RootUserSecret:    l.creds.SecretKey,
		Region:            l.cfg.Region, // must equal Galexie's region
		Ports:             []string{l.cfg.S3Listen},
		MaxConnections:    64,
		MaxRequests:       64,
		MultipartMaxParts: 10000,
		KeepAlive:         true,
		Quiet:             true,
		S3Options:         []s3api.Option{s3api.WithOnListen(l.onListen)},
	})
}

// onListen runs once the S3 listener is bound; the control listener was
// bound before the gateway started, so both now accept connections.
func (l *Lake) onListen() {
	l.ready.Store(true)
	l.m.ready.Set(1)
	if err := l.opts.Notify("READY=1"); err != nil {
		l.log.Warn("sd_notify READY failed", "err", err)
	}
	l.log.Info("lake ready", "s3", l.cfg.S3Listen, "control", l.cfg.ControlListen)
}

// failed reports whether the lake is fail-closed.
func (l *Lake) failed() bool { return l.tap.failed.Load() }
