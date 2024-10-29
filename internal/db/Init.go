package db

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

func Connect() (*pgx.Conn, error) {
	connectionURL := "postgresql://postgres:postgres@localhost:5432/truck-analytics"
	connect, err := pgx.Connect(context.Background(), connectionURL)
	if err != nil {
		slog.Error("Can't connect to DB")
		return nil, err
	}

	err = connect.Ping(context.Background())
	if err != nil {
		slog.Error("Can't Ping DB")
		return nil, err
	}

	return connect, nil
}
