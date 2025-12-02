package main

import (
	"flag"

	"ethdataset"
)

var (
	path string
)

func init() {
	flag.StringVar(&path, "path", "./config.toml", "")
}

func main() {
	flag.Parse()

	var cfg ethdataset.ExportStorageProofsConfig
	ethdataset.ReadConfig(path, &cfg)
	ethdataset.ExportStorageProofs(cfg)
}
