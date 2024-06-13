package math

import (
	"math"
)

// Round 按照指定精度四舍五入
func Round(val float64, precision int) float64 {
	switch {
	case precision == 0:
		return math.Round(val)
	case precision < 0:
		v := math.Pow10(precision)
		return math.Floor(val*v+0.5) * math.Pow10(-precision)
	default:
		v := math.Pow10(precision)
		return math.Floor(val*v+0.5) / v
	}
}
