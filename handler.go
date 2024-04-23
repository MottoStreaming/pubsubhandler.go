package pubsubhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"time"

	"github.com/MottoStreaming/ctxslog.go"
	_ "github.com/MottoStreaming/ctxslog.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MessageHandler is a function that handles protobuf message.
type MessageHandler func(ctx context.Context, msg proto.Message) error

// MessageWithMetadataHandler is a function that handles protobuf message along with its metadata (attributes, publish_time).
type MessageWithMetadataHandler func(ctx context.Context, msg proto.Message, md MessageMetadata) error

// ensure Handler implements http.Handler.
var _ http.Handler = (*Handler)(nil)

type Handler struct {
	logger *slog.Logger
	tracer trace.Tracer
	// topicMessageTypes is a map of topic name to the message type.
	topicMessageTypes map[string]protoreflect.MessageType
	// topicMDHandlers is a map of topic name to handler with metadata function.
	topicHandlers map[string]MessageWithMetadataHandler
}

// NewHandler creates a new handler.
func NewHandler(logger *slog.Logger) *Handler {
	return &Handler{
		logger:            logger,
		tracer:            otel.Tracer("pubsubhandler"),
		topicMessageTypes: make(map[string]protoreflect.MessageType),
		topicHandlers:     make(map[string]MessageWithMetadataHandler),
	}
}

// RegisterTopicHandler registers a handler function for a topic.
// It is not safe to call this method concurrently with ServeHTTP.
func (h *Handler) RegisterTopicHandler(topicName string, mt protoreflect.MessageType, handler MessageHandler) {
	if mt == nil {
		panic("message type must not be nil")
	}
	if handler == nil {
		panic("handler must not be nil")
	}
	h.topicMessageTypes[topicName] = mt
	h.topicHandlers[topicName] = func(ctx context.Context, msg proto.Message, _ MessageMetadata) error {
		return handler(ctx, msg)
	}
}

// RegisterTopicHandlerWithMetadata registers a handler function for a topic.
// It is not safe to call this method concurrently with ServeHTTP.
func (h *Handler) RegisterTopicHandlerWithMetadata(topicName string, mt protoreflect.MessageType, handler MessageWithMetadataHandler) {
	if mt == nil {
		panic("message type must not be nil")
	}
	if handler == nil {
		panic("handler must not be nil")
	}
	h.topicMessageTypes[topicName] = mt
	h.topicHandlers[topicName] = handler
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract topic name from the last segment of the URL path.
	// So for /pubsubhandler/CM_Orders, the topic name is "CM_Orders".
	topicName := path.Base(r.URL.Path)
	logger := h.logger.With("topic", topicName)

	var psMsg PubSubMessage
	err := json.NewDecoder(r.Body).Decode(&psMsg)
	if err != nil {
		logger.ErrorContext(r.Context(), "failed to decode JSON", "error", err)
		err = fmt.Errorf("failed to decode JSON: %w", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// ignore parsing errors
	pt, _ := time.Parse(time.RFC3339Nano, psMsg.Message.PublishTime)
	md := MessageMetadata{
		ID:          psMsg.Message.ID,
		Attributes:  psMsg.Message.Attributes,
		PublishTime: pt,
		OrderingKey: psMsg.Message.OrderingKey,
	}

	// Update logger with more context.
	logger = logger.With(
		"message_id", psMsg.Message.ID,
		"ordering_key", psMsg.Message.OrderingKey,
	)

	messageCtx := otel.GetTextMapPropagator().Extract(
		r.Context(),
		// Use the message attributes as the carrier.
		propagation.MapCarrier(psMsg.Message.Attributes),
	)

	ctx, span := h.tracer.Start(r.Context(), "process message",
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(trace.LinkFromContext(messageCtx)),
		trace.WithAttributes(
			semconv.MessagingSystemGCPPubsub,
			semconv.MessagingOperationDeliver,
			semconv.MessagingDestinationName(topicName),
			semconv.MessagingGCPPubsubMessageOrderingKey(psMsg.Message.OrderingKey),
			semconv.MessagingMessageID(psMsg.Message.ID),
		),
	)

	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	// Check if message type is registered.
	mt, ok := h.topicMessageTypes[topicName]
	if !ok {
		logger.ErrorContext(ctx, "no handler for topic")
		err = fmt.Errorf("no handler for topic %q", topicName)
		http.Error(w, "no handler for topic", http.StatusNotFound)
		return
	}

	// Use reflection to create a new instance of the message type.
	pbMsg := mt.New().Interface()

	err = proto.Unmarshal(psMsg.Message.Data, pbMsg)
	if err != nil {
		logger.ErrorContext(ctx, "failed to unmarshal message data", "error", err)
		err = fmt.Errorf("failed to unmarshal message data: %w", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	handler, ok := h.topicHandlers[topicName]
	if !ok {
		logger.ErrorContext(ctx, "no handler for topic")
		err = fmt.Errorf("no handler for topic %q", topicName)
		http.Error(w, "no handler for topic", http.StatusNotFound)
		return
	}

	ctx = ctxslog.ToContext(ctx, logger)
	err = handler(ctx, pbMsg, md)

	if err != nil {
		ctxslog.Error(ctx, "failed to process message", "error", err)
		err = fmt.Errorf("failed to process message: %w", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctxslog.Info(ctx, "message processed")
	// Return a 200 OK response.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
	span.SetStatus(codes.Ok, "OK")
}

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Message struct {
		Data        []byte            `json:"data"`
		Attributes  map[string]string `json:"attributes"`
		ID          string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
		OrderingKey string            `json:"orderingKey"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

type MessageMetadata struct {
	ID          string
	Attributes  map[string]string
	PublishTime time.Time
	OrderingKey string
}
