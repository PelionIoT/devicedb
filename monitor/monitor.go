package monitor

import (
    "context"
    "devicedb/data"
)

type Monitor interface {
    Listen(ctx context.Context) chan data.Row
}