# pubsubhandler.go
[![PkgGoDev][doc:image]][doc:url]

This package makes it easier to develop PubSub push handlers for Protobuf encoded messages.

It also includes tracing and logging support.

```bash
go get "github.com/MottoStreaming/pubsubhandler.go"
```

main.go:
```go
func main() {
	handler := pubsubhandler.NewHandler(slog.Default())
	// Register handler for topic `CM_Orders`
	handler.RegisterHandler(
		"CM_Orders",
		(*orders.Updated)(nil).ProtoReflect().Type(),
		func(ctx context.Context, m proto.Message) error {
			order := orderFromProto(m.(*orders.Updated))
			// Handle message
			return orderService.Update(ctx, order)
		},
	)
	// Register more topics and handlers here

	// Now you can use handler as http.Handler
	mux := http.NewServeMux()
	// You can register PubSub push handler with endpoint
	// `/pubsub/CM_Orders`
	mux.Handle("/pubsub", handler)
}
```


[doc:image]:  https://pkg.go.dev/badge/github.com/MottoStreaming/pubsubhandler.go
[doc:url]:    https://pkg.go.dev/github.com/MottoStreaming/pubsubhandler.go
