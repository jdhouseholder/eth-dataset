package ethdataset

import (
	"log"
	"os"

	"github.com/BurntSushi/toml"
)

func ReadConfig(path string, config interface{}) {
	s, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := toml.Decode(string(s), config); err != nil {
		log.Fatal(err)
	}
}
