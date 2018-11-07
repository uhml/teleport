/*
Copyright 2018 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package srv

import (
	"context"
	"fmt"
	"time"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/auth"
	"github.com/gravitational/teleport/lib/services"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"
)

// KeepAliveState represents state of the heartbeat
type KeepAliveState int

func (k KeepAliveState) String() string {
	switch k {
	case HeartbeatStateAnnounce:
		return "announce"
	case HeartbeatStateKeepAlive:
		return "keep-alive"
	default:
		return fmt.Sprintf("unknown state %v", int(k))
	}
}

const (
	// HeartbeatStateAnnounce is set when full
	// state has to be announced back to the auth server
	HeartbeatStateAnnounce = iota
	// HeartbeatStateKeepAlive is set when
	// only sending keep alives is necessary
	HeartbeatStateKeepAlive = iota
)

// HeartbeatMode represents the mode of the heartbeat
// node, proxy or auth server
type HeartbeatMode int

// CheckAndSetDefaults checks values and sets defaults
func (h HeartbeatMode) CheckAndSetDefaults() error {
	switch h {
	case HeartbeatModeNode, HeartbeatModeProxy, HeartbeatModeAuth:
		return nil
	default:
		return trace.BadParameter("unrecognized mode")
	}
}

const (
	// HeartbeatModeNode sets heartbeat to node
	// updates that support keep alives
	HeartbeatModeNode = iota
	// HeartbeatModeProxy sets heartbeat to proxy
	// that does not support keep alives
	HeartbeatModeProxy = iota
	// HeartbeatModeAuth sets heartbeat to auth
	// that does not support keep alives
	HeartbeatModeAuth = iota
)

// NewHeartbeat returns a new instance of heartbeat
func NewHeartbeat(cfg HeartbeatConfig) (*Heartbeat, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	ctx, cancel := context.WithCancel(cfg.Context)
	h := &Heartbeat{
		cancelCtx:       ctx,
		cancel:          cancel,
		HeartbeatConfig: cfg,
		Entry: log.WithFields(log.Fields{
			trace.Component: teleport.Component(cfg.Component, "beat"),
		}),
		keepAliveTicker: time.NewTicker(cfg.KeepAlivePeriod),
		announceTicker:  time.NewTicker(cfg.AnnouncePeriod),
		announceC:       make(chan struct{}, 1),
		sendC:           make(chan struct{}, 1),
	}
	h.Debugf("Starting heartbeat with keep alive period %v, full update period: %v", cfg.KeepAlivePeriod, cfg.AnnouncePeriod)
	return h, nil
}

// GetServerInfoFn is function that returns server info
type GetServerInfoFn func() (services.Server, error)

// HeartbeatConfig is a heartbeat configuration
type HeartbeatConfig struct {
	// Mode sets one of the proxy, auth or node moes
	Mode HeartbeatMode
	// Context is parent context that signals
	// heartbeat cancel
	Context context.Context
	// Component is a name of component used in logs
	Component string
	// AccessPoint is an acess point client
	AccessPoint auth.AccessPoint
	// GetServerInfo returns server information
	GetServerInfo GetServerInfoFn
	// KeepAlivePeriod is a period between lights weight
	// keep alive calls, that only update TTLs and don't consume
	// bandwidh, also is used to derive time between
	// failed attempts as well for auth and proxy modes
	KeepAlivePeriod time.Duration
	// AnnouncePeriod is a period between announce calls,
	// when client sends full server specification
	// to the presence service
	AnnouncePeriod time.Duration
	// Clock is a clock used to override time in tests
	Clock clockwork.Clock
}

// CheckAndSetDefaults checks and sets default values
func (cfg *HeartbeatConfig) CheckAndSetDefaults() error {
	if err := cfg.Mode.CheckAndSetDefaults(); err != nil {
		return trace.Wrap(err)
	}
	if cfg.Context == nil {
		return trace.BadParameter("missing parameter Context")
	}
	if cfg.AccessPoint == nil {
		return trace.BadParameter("missing parameter AccessPoint")
	}
	if cfg.Component == "" {
		return trace.BadParameter("missing parameter Component")
	}
	if cfg.KeepAlivePeriod == 0 {
		return trace.BadParameter("missing parameter KeepAlivePeriod")
	}
	if cfg.AnnouncePeriod == 0 {
		return trace.BadParameter("missing parameter AnnouncePeriod")
	}
	if cfg.GetServerInfo == nil {
		return trace.BadParameter("missing parameter GetServerInfo")
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	return nil
}

// Heartbeat keeps heartbeat state, it is implemented
// according to actor model - all interactions with it are to be done
// with signals
type Heartbeat struct {
	HeartbeatConfig
	cancelCtx context.Context
	cancel    context.CancelFunc
	*log.Entry
	state     KeepAliveState
	current   services.Server
	keepAlive *services.KeepAlive
	// keepAliveTicker is a faster timer to send keep alives
	keepAliveTicker *time.Ticker
	// announceTicker is a slower timers doing full update
	announceTicker *time.Ticker
	// keepAliver sends keep alive updates
	keepAliver services.KeepAliver
	// announceC is event receives an event
	// whenever new announce has been sent, used in tests
	announceC chan struct{}
	// sendC is event channel used to trigger
	// new announces
	sendC chan struct{}
}

// Run periodically calls to announce presence,
// should be called explicitly in a separate goroutine
func (h *Heartbeat) Run() error {
	defer func() {
		h.reset()
		h.keepAliveTicker.Stop()
		h.announceTicker.Stop()
	}()
	for {
		if err := h.fetchAndAnnounce(); err != nil {
			h.Warningf("Heartbeat failed %v.", err)
		}
		select {
		case <-h.ticker():
		case <-h.sendC:
			h.Debugf("Asked to send out of cycle")
		case <-h.cancelCtx.Done():
			h.Debugf("Heartbeat exited.")
			return nil
		}
	}
}

// Close closes all timers and goroutines,
// note that this function is equivalent of cancelling
// of the context passed in configuration and can be
// used interchangeably
func (h *Heartbeat) Close() error {
	// note that close does not clean up resources,
	// because it is unaware of heartbeat actual state,
	// Run() could may as well be creating new keep aliver
	// while this function attempts to close it,
	// so instead it relies on Run() loop to clean up after itself
	h.cancel()
	return nil
}

// ticker returns either fast ticker for keep alives
// or slow ticker for full server updates
// depending on the state
func (h *Heartbeat) ticker() <-chan time.Time {
	if h.state == HeartbeatStateKeepAlive {
		return h.keepAliveTicker.C
	}
	return h.announceTicker.C
}

// setState is used to debug state transitions
// as it logs in addition to setting state
func (h *Heartbeat) setState(state KeepAliveState) {
	h.state = state
	h.WithFields(log.Fields{"state": state.String()}).Debugf("Update state.")
}

// reset resets keep alive state
// and sends the state back to the initial state
// of sending full update
func (h *Heartbeat) reset() {
	h.setState(HeartbeatStateAnnounce)
	h.keepAlive = nil
	if h.keepAliver != nil {
		if err := h.keepAliver.Close(); err != nil {
			h.Warningf("Failed to close keep aliver: %v", err)
		}
		h.keepAliver = nil
	}
}

// fetch, if succeeded updates or sets current server
// to the last received server
func (h *Heartbeat) fetch() error {
	// failed to fetch server info?
	// reset so next time full server update will be sent
	server, err := h.GetServerInfo()
	if err != nil {
		h.reset()
		return trace.Wrap(err)
	}
	// no last server recorded? reset, so can send full update
	current := h.current
	h.current = server
	if current == nil {
		h.reset()
		return nil
	}
	result := services.CompareServers(server, current)
	// servers are different, reset keep alives and re-send full state
	if result == services.Different {
		h.reset()
		return nil
	}
	// keep whatever state is current, if keep alive is current
	// keep sending keep alives, otherwise update full node
	return nil
}

func (h *Heartbeat) announce() error {
	switch h.state {
	case HeartbeatStateAnnounce:
		// proxies and auth servers don't support keep alive logic yet,
		// so keep state at announce forever for proxies
		switch h.Mode {
		case HeartbeatModeProxy:
			err := h.AccessPoint.UpsertProxy(h.current)
			if err != nil {
				return trace.Wrap(err)
			}
			h.notifySend()
			return nil
		case HeartbeatModeAuth:
			err := h.AccessPoint.UpsertAuthServer(h.current)
			if err != nil {
				return trace.Wrap(err)
			}
			h.notifySend()
			return nil
		}
		keepAlive, err := h.AccessPoint.UpsertNode(h.current)
		if err != nil {
			return trace.Wrap(err)
		}
		h.notifySend()
		keepAliver, err := h.AccessPoint.NewKeepAliver(h.cancelCtx)
		if err != nil {
			h.reset()
			return trace.Wrap(err)
		}
		h.keepAlive = keepAlive
		h.keepAliver = keepAliver
		h.setState(HeartbeatStateKeepAlive)
		return nil
	case HeartbeatStateKeepAlive:
		keepAlive := *h.keepAlive
		keepAlive.Expires = h.Clock.Now().UTC().Add(h.KeepAlivePeriod)
		timeout := time.NewTimer(h.KeepAlivePeriod)
		defer timeout.Stop()
		select {
		case <-h.cancelCtx.Done():
			return nil
		case <-timeout.C:
			h.Warningf("Blocked on keep alive send, going to reset.")
			h.reset()
			return trace.ConnectionProblem(nil, "timeout sending keep alive")
		case h.keepAliver.KeepAlives() <- keepAlive:
			h.notifySend()
			return nil
		case <-h.keepAliver.Done():
			h.Warningf("Keep alive has failed: %v", h.keepAliver.Error())
			err := h.keepAliver.Error()
			h.reset()
			return trace.ConnectionProblem(err, "keep alive channel closed")
		}
	default:
		return trace.BadParameter("unsupported state: %v", h.state)
	}
}

func (h *Heartbeat) notifySend() {
	select {
	case h.announceC <- struct{}{}:
		return
	default:
	}
}

// fetchAndAnnounce fetches data about server
// and announces it to the server
func (h *Heartbeat) fetchAndAnnounce() error {
	if err := h.fetch(); err != nil {
		return trace.Wrap(err)
	}
	if err := h.announce(); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

// ForceSend forces send cycle, used in tests, returns
// nil in case of success, error otherwise
func (h *Heartbeat) ForceSend(timeout time.Duration) error {
	timeoutC := time.After(timeout)
	select {
	case h.sendC <- struct{}{}:
	case <-timeoutC:
		return trace.ConnectionProblem(nil, "timeout waiting for send")
	}
	select {
	case <-h.announceC:
		return nil
	case <-timeoutC:
		return trace.ConnectionProblem(nil, "timeout waiting for announce to be sent")
	}
}
