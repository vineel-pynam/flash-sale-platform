// Package admission signs and verifies waiting-room admission tokens. The gateway verifies them
// locally with no network call, so queue skippers are rejected at no cost (docs/04-architecture.md, section 2).
package admission

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrMalformed = errors.New("admission: malformed token")
	ErrSignature = errors.New("admission: bad signature")
	ErrExpired   = errors.New("admission: token expired")
	ErrWrongDrop = errors.New("admission: token is for another drop")
)

// Claims are the facts the waiting room vouches for.
type Claims struct {
	UserID    string `json:"u"`
	DropID    string `json:"d"`
	ExpiresAt int64  `json:"exp"` // unix seconds
}

var enc = base64.RawURLEncoding

// Sign produces "<payload>.<signature>", both base64url without padding.
func Sign(key []byte, c Claims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	p := enc.EncodeToString(payload)
	return p + "." + enc.EncodeToString(mac(key, p)), nil
}

// Verify checks the signature, expiry and drop. It runs in constant time with respect to the
// signature.
func Verify(key []byte, token, dropID string, now time.Time) (Claims, error) {
	p, s, ok := strings.Cut(token, ".")
	if !ok || p == "" || s == "" {
		return Claims{}, ErrMalformed
	}
	sig, err := enc.DecodeString(s)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	if !hmac.Equal(sig, mac(key, p)) {
		return Claims{}, ErrSignature
	}
	raw, err := enc.DecodeString(p)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	var c Claims
	if err := json.Unmarshal(raw, &c); err != nil || c.UserID == "" {
		return Claims{}, ErrMalformed
	}
	if now.Unix() >= c.ExpiresAt {
		return Claims{}, ErrExpired
	}
	if dropID != "" && c.DropID != dropID {
		return Claims{}, ErrWrongDrop
	}
	return c, nil
}

func mac(key []byte, payload string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(payload))
	return h.Sum(nil)
}
