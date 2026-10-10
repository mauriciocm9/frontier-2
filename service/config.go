package main

import (
	"net/url"
	"os"
)

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func databaseURL() string {
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	u := url.URL{Scheme: "postgres", Host: os.Getenv("DB_HOST") + ":" + env("DB_PORT", "5432"), Path: "/" + env("DB_NAME", "frontier"), User: url.UserPassword(os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"))}
	query := url.Values{"sslmode": {env("DB_SSLMODE", "verify-full")}, "sslrootcert": {env("DB_SSLROOTCERT", "/rds-ca-bundle.pem")}}
	u.RawQuery = query.Encode()
	return u.String()
}
