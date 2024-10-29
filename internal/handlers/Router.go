package handlers

import (
	"net/http"

	september "truck-analytics-platform/internal/handlers/2023/september"

	"github.com/gin-gonic/gin"
)

func InitRouter() {
	server := gin.Default()

	// Tractors
	server.Handle("GET", "/9m2023tractors4x2", september.NineMonth2023Tractors4x2)
	server.Handle("GET", "/9m2023tractors6x4", september.NineMonth2023Tractors6x4)

	// Dumpers
	server.Handle("GET", "/9m2023dumpers6x4", september.NineMonth2023Dumpers6x4)
	server.Handle("GET", "/9m2023dumpers8x4", september.NineMonth2023Dumpers8x4)

	http.ListenAndServe(":8080", server)

}
