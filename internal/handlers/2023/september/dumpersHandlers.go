package september

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"truck-analytics-platform/internal/db"

	"github.com/gin-gonic/gin"
)

func NineMonth2023Dumpers6x4(ctx *gin.Context) {
	type TruckAnalytics struct {
		RegionName string `json:"region_name"`
		FAW        *int   `json:"faw"`
		HOWO       *int   `json:"howo"`
		JAC        *int   `json:"jac"`
		SANY       *int   `json:"sany"`
		SITRAK     *int   `json:"sitrak"`
		TOTAL      int    `json:"total"`
	}

	// TruckAnalyticsResponse wraps the response data
	type TruckAnalyticsResponse struct {
		Data  []TruckAnalytics `json:"data"`
		Error string           `json:"error,omitempty"`
	}

	query := `
		WITH base_data AS (
			SELECT 
				truck_analytics_2023_01_12."Federal_district",
				truck_analytics_2023_01_12."Region",
				truck_analytics_2023_01_12."Brand",
				SUM(truck_analytics_2023_01_12."Quantity") as total_sales
			FROM truck_analytics_2023_01_12
			WHERE 
				truck_analytics_2023_01_12."Wheel_formula" = '6x4'
				AND truck_analytics_2023_01_12."Brand" IN ('FAW', 'HOWO', 'JAC', 'SANY', 'SITRAK')
				AND truck_analytics_2023_01_12."Month_of_registration" <= 9
				AND truck_analytics_2023_01_12."Body_type" = 'Самосвал'
				AND truck_analytics_2023_01_12."Mass_in_segment_1" = '32001-40000'
			GROUP BY 
				truck_analytics_2023_01_12."Federal_district", 
				truck_analytics_2023_01_12."Region", 
				truck_analytics_2023_01_12."Brand"
		),
		federal_totals AS (
			SELECT 
				"Federal_district",
				"Federal_district" as "Region",
				"Brand",
				SUM(total_sales) as total_sales
			FROM base_data
			GROUP BY "Federal_district", "Brand"
		),
		combined_data AS (
			SELECT * FROM base_data
			UNION ALL
			SELECT * FROM federal_totals
		)
		SELECT 
			COALESCE("Region", "Federal_district") as Region_name,
			MAX(CASE WHEN "Brand" = 'FAW' THEN total_sales END) as FAW,
			MAX(CASE WHEN "Brand" = 'HOWO' THEN total_sales END) as HOWO,
			MAX(CASE WHEN "Brand" = 'JAC' THEN total_sales END) as JAC,
			MAX(CASE WHEN "Brand" = 'SANY' THEN total_sales END) as SANY,
			MAX(CASE WHEN "Brand" = 'SITRAK' THEN total_sales END) as SITRAK,
			COALESCE(MAX(CASE WHEN "Brand" = 'FAW' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'HOWO' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'JAC' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'SANY' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'SITRAK' THEN total_sales END), 0) as TOTAL
		FROM combined_data
		GROUP BY 
			"Federal_district",
			"Region"
		ORDER BY 
			"Federal_district",
			CASE 
				WHEN "Region" = "Federal_district" THEN 1 
				ELSE 0 
			END,
			"Region"
		`

	db, err := db.Connect()
	if err != nil {
		slog.Warn("Can't get data from db")
		return
	}

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		response := TruckAnalyticsResponse{
			Error: "Failed to execute query: " + err.Error(),
		}
		json.NewEncoder(ctx.Writer).Encode(response)
		return
	}
	defer rows.Close()

	var results []TruckAnalytics
	for rows.Next() {
		var ta TruckAnalytics
		err := rows.Scan(
			&ta.RegionName,
			&ta.FAW,
			&ta.HOWO,
			&ta.JAC,
			&ta.SANY,
			&ta.SITRAK,
			&ta.TOTAL,
		)
		if err != nil {
			response := TruckAnalyticsResponse{
				Error: "Failed to scan row: " + err.Error(),
			}
			json.NewEncoder(ctx.Writer).Encode(response)
			return
		}
		results = append(results, ta)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		response := TruckAnalyticsResponse{
			Error: "Error iterating over rows: " + err.Error(),
		}
		json.NewEncoder(ctx.Writer).Encode(response)
		return
	}

	// Send the response
	response := TruckAnalyticsResponse{
		Data: results,
	}
	ctx.JSON(http.StatusOK, response)
}

func NineMonth2023Dumpers8x4(ctx *gin.Context) {
	type TruckAnalytics struct {
		RegionName string `json:"region_name"`
		FAW        *int   `json:"faw"`
		HOWO       *int   `json:"howo"`
		SHACMAN    *int   `json:"shacman"`
		SITRAK     *int   `json:"sitrak"`
		TOTAL      int    `json:"total"`
	}

	// TruckAnalyticsResponse wraps the response data
	type TruckAnalyticsResponse struct {
		Data  []TruckAnalytics `json:"data"`
		Error string           `json:"error,omitempty"`
	}

	query := `
		WITH base_data AS (
			SELECT 
				truck_analytics_2023_01_12."Federal_district",
				truck_analytics_2023_01_12."Region",
				truck_analytics_2023_01_12."Brand",
				SUM(truck_analytics_2023_01_12."Quantity") as total_sales
			FROM truck_analytics_2023_01_12
			WHERE 
				truck_analytics_2023_01_12."Wheel_formula" = '8x4'
				AND truck_analytics_2023_01_12."Brand" IN ( 'FAW', 'HOWO', 'SHACMAN', 'SITRAK')
				AND truck_analytics_2023_01_12."Month_of_registration" <= 9
				AND truck_analytics_2023_01_12."Body_type" = 'Самосвал'
				AND truck_analytics_2023_01_12."Weight_in_segment_4" = '35001-45000'
			GROUP BY 
				truck_analytics_2023_01_12."Federal_district", 
				truck_analytics_2023_01_12."Region", 
				truck_analytics_2023_01_12."Brand"
		),
		federal_totals AS (
			SELECT 
				"Federal_district",
				"Federal_district" as "Region",
				"Brand",
				SUM(total_sales) as total_sales
			FROM base_data
			GROUP BY "Federal_district", "Brand"
		),
		combined_data AS (
			SELECT * FROM base_data
			UNION ALL
			SELECT * FROM federal_totals
		)
		SELECT 
			COALESCE("Region", "Federal_district") as Region_name,
			MAX(CASE WHEN "Brand" = 'FAW' THEN total_sales END) as FAW,
			MAX(CASE WHEN "Brand" = 'HOWO' THEN total_sales END) as HOWO,
			MAX(CASE WHEN "Brand" = 'SHACMAN' THEN total_sales END) as SHACMAN,
			MAX(CASE WHEN "Brand" = 'SITRAK' THEN total_sales END) as SITRAK,
			COALESCE(MAX(CASE WHEN "Brand" = 'FAW' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'HOWO' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'SHACMAN' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'SITRAK' THEN total_sales END), 0) as TOTAL
		FROM combined_data
		GROUP BY 
			"Federal_district",
			"Region"
		ORDER BY 
			"Federal_district",
			CASE 
				WHEN "Region" = "Federal_district" THEN 1 
				ELSE 0 
			END,
			"Region"
		`

	db, err := db.Connect()
	if err != nil {
		slog.Warn("Can't get data from db")
		return
	}

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		response := TruckAnalyticsResponse{
			Error: "Failed to execute query: " + err.Error(),
		}
		json.NewEncoder(ctx.Writer).Encode(response)
		return
	}
	defer rows.Close()

	var results []TruckAnalytics
	for rows.Next() {
		var ta TruckAnalytics
		err := rows.Scan(
			&ta.RegionName,
			&ta.FAW,
			&ta.HOWO,
			&ta.SHACMAN,
			&ta.SITRAK,
			&ta.TOTAL,
		)
		if err != nil {
			response := TruckAnalyticsResponse{
				Error: "Failed to scan row: " + err.Error(),
			}
			json.NewEncoder(ctx.Writer).Encode(response)
			return
		}
		results = append(results, ta)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		response := TruckAnalyticsResponse{
			Error: "Error iterating over rows: " + err.Error(),
		}
		json.NewEncoder(ctx.Writer).Encode(response)
		return
	}

	// Send the response
	response := TruckAnalyticsResponse{
		Data: results,
	}
	ctx.JSON(http.StatusOK, response)
}
