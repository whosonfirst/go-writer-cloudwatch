package main

import (
	"bufio"
	"flag"
	"github.com/whosonfirst/go-writer-cloudwatch"
	"log"
	"os"
	"strings"
)

func main() {

	dsn := flag.String("dsn", "", "A valid (go-whosonfirst-aws) CloudWatch DSN")
	flag.Parse()

	wr, err := cloudwatch.NewCloudWatchWriter(*dsn)

	if err != nil {
		log.Fatal(err)
	}

	args := flag.Args()

	if len(args) >= 1 && args[0] == "-" {

		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {

			_, err := wr.Write(scanner.Bytes())

			if err != nil {
				log.Fatal(err)
			}
		}

	} else {

		msg := strings.Join(flag.Args(), " ")
		_, err := wr.Write([]byte(msg))

		if err != nil {
			log.Fatal(err)
		}
	}
}
