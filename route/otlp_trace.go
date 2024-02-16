package route

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	huskyotlp "github.com/honeycombio/husky/otlp"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func (r *Router) postOTLPTrace(w http.ResponseWriter, req *http.Request) {
	ri := huskyotlp.GetRequestInfoFromHttpHeaders(req.Header)

	if !r.Config.IsAPIKeyValid(ri.ApiKey) {
		err := fmt.Errorf("api key %s not found in list of authorized keys", ri.ApiKey)
		r.handlerReturnWithError(w, ErrAuthNeeded, err)
		return
	}

	if err := ri.ValidateTracesHeaders(); err != nil {
		if errors.Is(err, huskyotlp.ErrInvalidContentType) {
			r.handlerReturnWithError(w, ErrInvalidContentType, err)
		} else {
			r.handlerReturnWithError(w, ErrAuthNeeded, err)
		}
		return
	}

	result, err := huskyotlp.TranslateTraceRequestFromReader(req.Body, ri)
	if err != nil {
		r.handlerReturnWithError(w, ErrUpstreamFailed, err)
		return
	}

	if err := processOtlpRequest(req.Context(), r, result.Batches, ri.ApiKey); err != nil {
		r.handlerReturnWithError(w, ErrUpstreamFailed, err)
	}
}

type TraceServer struct {
	router *Router
	collectortrace.UnimplementedTraceServiceServer
}

func NewTraceServer(router *Router) *TraceServer {
	traceServer := TraceServer{router: router}
	return &traceServer
}

func (t *TraceServer) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	ri := huskyotlp.GetRequestInfoFromGrpcMetadata(ctx)
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	result, err := huskyotlp.TranslateTraceRequest(req, ri)
	if err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	if err := processOtlpRequest(ctx, t.router, result.Batches, ri.ApiKey); err != nil {
		return nil, huskyotlp.AsGRPCError(err)
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}
