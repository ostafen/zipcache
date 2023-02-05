package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func compress(data []byte) []byte {
	var buf bytes.Buffer

	w := gzip.NewWriter(&buf)
	_, err := w.Write(data)
	fatalIfErr(err)

	err = w.Flush()
	fatalIfErr(err)

	err = w.Close()
	fatalIfErr(err)

	return buf.Bytes()
}

func main() {
	f, err := os.Open("../airlines.json")
	fatalIfErr(err)

	data, err := io.ReadAll(f)
	fatalIfErr(err)

	m := make(map[string]string)

	x := make([]map[string]any, 0)
	err = json.Unmarshal(data, &x)
	fatalIfErr(err)

	totalTime := time.Duration(0)
	for i, item := range x {
		data, err := json.Marshal(item)
		fatalIfErr(err)

		start := time.Now()
		m[strconv.Itoa(i)] = string(compress(data))
		totalTime += time.Since(start)
		fatalIfErr(err)
	}

	size := 0
	for _, v := range m {
		size += len(v)
	}
	fmt.Printf("time (seconds):\t %f\n", totalTime.Seconds())
	fmt.Printf("ratio (%%):\t %.2f\n", float64(size)/float64(len(data)))
}
