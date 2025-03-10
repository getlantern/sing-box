package http

import (
	"context"
	"net"
	"net/url"
	"os"

	"github.com/getlantern/algeneva"
	"github.com/gobwas/ws"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	sHTTP "github.com/sagernet/sing/protocol/http"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.HTTPOutboundOptions](registry, C.TypeHTTP, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	logger    logger.ContextLogger
	tlsConfig tls.Config
	geneva    option.GenevaHTTPOptions
	server    string
	client    *sHTTP.Client
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.HTTPOutboundOptions) (adapter.Outbound, error) {
	outboundDialer, err := dialer.New(ctx, options.DialerOptions)
	if err != nil {
		return nil, err
	}
	outbound := &Outbound{
		Adapter: outbound.NewAdapterWithDialerOptions(C.TypeHTTP, tag, []string{N.NetworkTCP}, options.DialerOptions),
		logger:  logger,
		geneva:  options.GenevaHTTPOptions,
	}
	var genevaStrategy *algeneva.HTTPStrategy
	if options.GenevaHTTPOutboundOptions.Enabled {
		genevaStrategy, err = algeneva.NewHTTPStrategy(options.GenevaHTTPOutboundOptions.Strategy)
		if err != nil {
			return nil, err
		}
		if options.TLS != nil {
			outbound.tlsConfig, err = tls.NewClient(ctx, options.Server, common.PtrValueOrDefault(options.TLS))
			if err != nil {
				return nil, err
			}
		}
	}
	if options.TLS != nil {
		outboundDialer, err = tls.NewDialerFromOptions(ctx, router, outboundDialer, options.Server, common.PtrValueOrDefault(options.TLS))
		if err != nil {
			return nil, err
		}
	}
	server := options.ServerOptions.Build()
	outbound.server = server.String()
	outbound.client = sHTTP.NewClient(sHTTP.Options{
		Dialer:         outboundDialer,
		Server:         server,
		Username:       options.Username,
		Password:       options.Password,
		Path:           options.Path,
		Headers:        options.Headers.Build(),
		GenevaStrategy: genevaStrategy,
	})
	return outbound, nil
}

func (h *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	ctx, metadata := adapter.ExtendContext(ctx)
	metadata.Outbound = h.Tag()
	metadata.Destination = destination
	h.logger.InfoContext(ctx, "outbound connection to ", destination)
	if !h.geneva.Enabled {
		return h.client.DialContext(ctx, network, destination)
	}

	conn, err := h.client.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	if h.geneva.OverWS {
		h.logger.DebugContext(ctx, "geneva upgrading to websocket")
		u, err := url.ParseRequestURI("http://" + h.server)
		if err != nil {
			h.logger.ErrorContext(ctx, err, ": parse server address")
			return nil, err
		}
		_, _, err = ws.Dialer{}.Upgrade(conn, u)
		if err != nil {
			return nil, err
		}
	}
	if h.tlsConfig != nil {
		h.logger.DebugContext(ctx, "geneva TLS handshake")
		return tls.ClientHandshake(ctx, conn, h.tlsConfig)
	}
	return conn, nil
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}
