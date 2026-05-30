package cmd

import "embed"

//go:embed static/*
var staticFS embed.FS

func init() {
	StaticFS = staticFS
}
