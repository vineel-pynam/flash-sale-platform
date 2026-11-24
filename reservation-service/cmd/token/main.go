// Command token issues an admission token, standing in for the waiting room during local testing.
//
//	go run ./cmd/token -user alice
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"reservation-service/internal/admission"
)

func main() {
	user := flag.String("user", "", "user id")
	drop := flag.String("drop", os.Getenv("DROP_ID"), "drop id (defaults to $DROP_ID)")
	ttl := flag.Duration("ttl", 10*time.Minute, "token lifetime")
	flag.Parse()

	key := os.Getenv("ADMISSION_TOKEN_KEY")
	if *user == "" || *drop == "" || len(key) < 16 {
		fmt.Fprintln(os.Stderr, "usage: ADMISSION_TOKEN_KEY=... go run ./cmd/token -user <id> [-drop <id>]")
		os.Exit(2)
	}
	tok, err := admission.Sign([]byte(key), admission.Claims{UserID: *user, DropID: *drop, ExpiresAt: time.Now().Add(*ttl).Unix()})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(tok)
}
