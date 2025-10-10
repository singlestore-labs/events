package events

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding"
	"encoding/json"
	"net"
	"os"
	"strings"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type SASLMethod string

const (
	SASLNone   SASLMethod = "none"
	SASLPlain  SASLMethod = "plain"
	SASLSHA256 SASLMethod = "sha256"
	SASLSHA512 SASLMethod = "sha512"
)

var (
	_ json.Unmarshaler         = &TLSConfig{}
	_ encoding.TextUnmarshaler = &SASLConfig{}
)

// TLSConfig wraps tls.Config so that special features can be added to
// its unmarshaler.
//
// The following extras are defined:
//
//	"DelayHostnameValidation":true - if included, InsecureSkipVerify
//	will be set to true and custom validation of certificates will be done
//	with the ServerName only validated after it is set and
//	PeerCertificates are set.
type TLSConfig struct {
	*tls.Config
}

func (sc *TLSConfig) UnmarshalJSON(b []byte) error {
	var wrapper struct {
		tls.Config
		DelayHostnameValidation bool
	}
	err := json.Unmarshal(b, &wrapper)
	if err != nil {
		return err
	}
	sc.Config = &wrapper.Config
	if wrapper.DelayHostnameValidation {
		//nolint:staticcheck // QF1008: could remove embedded field "Config" from selector
		sc.Config.InsecureSkipVerify = true
		err := sc.generateCustomTLSVerifier()
		if err != nil {
			return err
		}
	}
	return nil
}

// SASLConfig unmarshals into a sasl.Mechanism from a strings like:
//
//	"none"
//	"plain:username:password"
//	"sha256:username:password"
//	"sha512:username:password"
type SASLConfig struct {
	Mechanism sasl.Mechanism
}

func (sc *SASLConfig) UnmarshalText(text []byte) error {
	components := bytes.Split(text, []byte{':'})
	if bytes.Equal(text, []byte(SASLNone)) {
		sc.Mechanism = nil
		return nil
	}
	if len(components) != 3 {
		return errors.Errorf("event library invalid SASL config (%s), should have two ':'s", string(text))
	}
	method, username, password := SASLMethod(components[0]), string(components[1]), string(components[2])
	var m sasl.Mechanism
	var err error
	switch method {
	case SASLNone:
		sc.Mechanism = nil
		return nil
	case SASLPlain:
		sc.Mechanism = plain.Mechanism{
			Username: username,
			Password: password,
		}
		return nil
	case SASLSHA256:
		m, err = scram.Mechanism(scram.SHA256, username, password)
	case SASLSHA512:
		m, err = scram.Mechanism(scram.SHA512, username, password)
	default:
		return errors.Errorf("event library invalid SASL config (%s), method (%s) not supported", string(text), string(method))
	}
	if err != nil {
		return errors.Errorf("event library could not create SASL mechanism: %w", err)
	}
	sc.Mechanism = m
	return nil
}

// SASLConfigFromString returns nil for errors and is meant for testing situations
func SASLConfigFromString(s string) sasl.Mechanism {
	var sm SASLConfig
	_ = sm.UnmarshalText([]byte(s))
	return sm.Mechanism
}

func (lib *LibraryNoDB) dialer() *kafka.Dialer {
	nBrokers := len(lib.brokers)
	if nBrokers == 0 {
		nBrokers = 1
	}
	return &kafka.Dialer{
		ClientID:      lib.clientID,
		Timeout:       dialTimeout * time.Duration(nBrokers), // timeout is divided by number of brokers
		DualStack:     true,
		KeepAlive:     time.Second * 5,
		SASLMechanism: lib.mechanism,
		TLS:           lib.tlsConfig,
	}
}

func (lib *LibraryNoDB) transport() *kafka.Transport {
	return &kafka.Transport{
		Dial: (&net.Dialer{
			Timeout:   dialTimeout,
			DualStack: true,
		}).DialContext,
		ClientID:    lib.clientID,
		DialTimeout: time.Second * 5,
		IdleTimeout: time.Second * 30,
		SASL:        lib.mechanism,
		TLS:         lib.tlsConfig,
	}
}

// generateCustomTLSVerifier installs TLS validation functions.
// The VerifyPeerCertificate function skips hostname validation.
// The VerifyConnection function only validates the ServerName when it and PeerCertificates are provided.
func (tc *TLSConfig) generateCustomTLSVerifier() error {
	certPool := x509.NewCertPool()
	var certFound bool
	for _, certfile := range []string{
		"/etc/ssl/certs/ca-certificates.crt",                // Debian/Ubuntu/Gentoo etc.
		"/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora/RHEL 6
		"/etc/ssl/ca-bundle.pem",                            // OpenSUSE
		"/etc/pki/tls/cacert.pem",                           // OpenELEC
		"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
		"/etc/ssl/cert.pem",                                 // Alpine Linux
	} {
		r, err := os.ReadFile(certfile)
		if err != nil {
			continue
		}
		if certPool.AppendCertsFromPEM(r) {
			certFound = true
			break
		}
	}
	if !certFound {
		return errors.Alertf("could not load system certificate authority certificates")
	}
	//nolint:staticcheck // QF1008: could remove embedded field "Config" from selector
	tc.Config.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		opts := x509.VerifyOptions{
			CurrentTime:   time.Now(),
			DNSName:       "", // skip checking hostname in Verify()
			Roots:         certPool,
			Intermediates: x509.NewCertPool(),
		}
		certs := make([]*x509.Certificate, len(rawCerts))
		for i, asn1Data := range rawCerts {
			cert, err := x509.ParseCertificate(asn1Data)
			if err != nil {
				return errors.Errorf("parse %s certificate to validate peers",
					map[bool]string{
						true:  "lead",
						false: "intermediate",
					}[i == 0])
			}
			certs[i] = cert
			if i > 0 {
				opts.Intermediates.AddCert(cert)
			}
		}
		_, err := certs[0].Verify(opts)
		return errors.Wrap(err, "validate TLS peer certificate")
	}
	//nolint:staticcheck // QF1008: could remove embedded field "Config" from selector
	tc.Config.VerifyConnection = func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return nil
		}
		if cs.ServerName == "" {
			return nil
		}
		lcSN := strings.ToLower(cs.ServerName)
		parts := strings.SplitN(lcSN, ".", 2)
		for _, dnsName := range cs.PeerCertificates[0].DNSNames {
			lcDN := strings.ToLower(dnsName)
			if strings.HasPrefix(lcDN, "*.") && len(parts) == 2 {
				if parts[1] == lcDN[2:] {
					return nil
				}
			} else if lcDN == lcSN {
				return nil
			}
		}
		return errors.Errorf("could not match server name (%s) to any certificate domain name", cs.ServerName)
	}
	return nil
}
