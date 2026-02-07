package randomx

import (
	"math/rand"
	"time"
)

type config struct {
	rand *rand.Rand
}

// Option configures a sampler.
type Option func(*config)

// WithRand sets a custom random source.
func WithRand(r *rand.Rand) Option {
	return func(c *config) {
		if r != nil {
			c.rand = r
		}
	}
}

func defaultConfig() config {
	return config{rand: rand.New(rand.NewSource(time.Now().UnixNano()))}
}
