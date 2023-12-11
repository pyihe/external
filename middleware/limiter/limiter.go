package limiter

import (
	"net/http"

	"github.com/gin-gonic/gin"
	stdLimit "golang.org/x/time/rate"
)

func WithLimiter(rate int, burst int) gin.HandlerFunc {
	var limiter = stdLimit.NewLimiter(stdLimit.Limit(rate), burst)
	return func(ctx *gin.Context) {
		if !limiter.Allow() {
			ctx.AbortWithStatus(http.StatusRequestTimeout)
			return
		}
		ctx.Next()
	}
}
