package admission_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"reservation-service/internal/admission"
)

var key = []byte("test-key-0123456789abcdef")

func TestVerify(t *testing.T) {
	now := time.Now()
	valid, _ := admission.Sign(key, admission.Claims{UserID: "u1", DropID: "d1", ExpiresAt: now.Add(10 * time.Minute).Unix()})
	expired, _ := admission.Sign(key, admission.Claims{UserID: "u1", DropID: "d1", ExpiresAt: now.Add(-time.Second).Unix()})
	otherKey, _ := admission.Sign([]byte("another-key-0123456789"), admission.Claims{UserID: "u1", DropID: "d1", ExpiresAt: now.Add(time.Minute).Unix()})
	p, s, _ := strings.Cut(valid, ".")
	tampered := p[:len(p)-2] + "xx." + s

	cases := []struct {
		name  string
		token string
		drop  string
		want  error
	}{
		{"valid", valid, "d1", nil},
		{"expired", expired, "d1", admission.ErrExpired},
		{"wrong drop", valid, "d2", admission.ErrWrongDrop},
		{"signed with another key", otherKey, "d1", admission.ErrSignature},
		{"tampered payload", tampered, "d1", admission.ErrSignature},
		{"empty", "", "d1", admission.ErrMalformed},
		{"garbage", "not-a-token", "d1", admission.ErrMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := admission.Verify(key, tc.token, tc.drop, now)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if tc.want == nil && c.UserID != "u1" {
				t.Fatalf("user = %q", c.UserID)
			}
		})
	}
}
