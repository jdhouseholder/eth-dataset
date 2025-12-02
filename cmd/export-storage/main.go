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

	var cfg ethdataset.ExportStorageConfig
	ethdataset.ReadConfig(path, &cfg)
	ethdataset.ExportStorage(cfg)
}
