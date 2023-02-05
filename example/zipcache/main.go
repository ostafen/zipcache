package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/ostafen/zipcache"
)

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	f, err := os.Open("../airlines.json")
	fatalIfErr(err)

	data, err := io.ReadAll(f)
	fatalIfErr(err)

	cache := zipcache.New()

	x := make([]map[string]any, 0)
	err = json.Unmarshal(data, &x)
	fatalIfErr(err)

	total := time.Duration(0)
	for i, item := range x {
		data, err := json.Marshal(item)
		fatalIfErr(err)

		start := time.Now()
		err = cache.Put([]byte(strconv.Itoa(i)), data)
		total += time.Since(start)
		fatalIfErr(err)
	}

	fmt.Printf("time (seconds):\t %f\n", total.Seconds())
	fmt.Printf("ratio (%%):\t %.2f\n", float64(cache.Size())/float64(len(data)))
}
