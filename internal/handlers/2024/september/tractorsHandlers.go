package september

import (
	"context"
	"log/slog"
	"net/http"
	"truck-analytics-platform/internal/db"

	"github.com/gin-gonic/gin"
)

func Home(ctx *gin.Context) {
	ctx.JSON(200, "Cool")
}

func NineMonth2023Tractors4x2(ctx *gin.Context) {
	type TruckAnalytics struct {
		RegionName string `json:"region_name"`
		DONGFENG   *int   `json:"dongfeng"`
		FAW        *int   `json:"faw"`
		FOTON      *int   `json:"foton"`
		JAC        *int   `json:"jac"`
		SHACMAN    *int   `json:"shacman"`
		SITRAK     *int   `json:"sitrak"`
		TOTAL      int    `json:"total"`
	}

	// Создаем структуру для ответа
	type TruckAnalyticsResponse struct {
		Data  map[string][]TruckAnalytics `json:"data"`
		Error string                      `json:"error,omitempty"`
	}

	query := `
		WITH base_data AS (
			SELECT 
				truck_analytics_2024_01_09."Federal_district",
				truck_analytics_2024_01_09."Region",
				truck_analytics_2024_01_09."Brand",
				SUM(truck_analytics_2024_01_09."Quantity") as total_sales
			FROM truck_analytics_2024_01_09
			WHERE 
				truck_analytics_2024_01_09."Wheel_formula" = '4x2'
				AND truck_analytics_2024_01_09."Brand" IN ('DONGFENG', 'FAW', 'FOTON', 'JAC', 'SHACMAN', 'SITRAK')
				AND truck_analytics_2024_01_09."Body_type" = 'Седельный тягач'
				AND truck_analytics_2024_01_09."Exact_mass" = 18000
			GROUP BY 
				truck_analytics_2024_01_09."Federal_district", 
				truck_analytics_2024_01_09."Region", 
				truck_analytics_2024_01_09."Brand"
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
			"Federal_district",
			COALESCE("Region", "Federal_district") as Region_name,
			MAX(CASE WHEN "Brand" = 'DONGFENG' THEN total_sales END) as DONGFENG,
			MAX(CASE WHEN "Brand" = 'FAW' THEN total_sales END) as FAW,
			MAX(CASE WHEN "Brand" = 'FOTON' THEN total_sales END) as FOTON,
			MAX(CASE WHEN "Brand" = 'JAC' THEN total_sales END) as JAC,
			MAX(CASE WHEN "Brand" = 'SHACMAN' THEN total_sales END) as SHACMAN,
			MAX(CASE WHEN "Brand" = 'SITRAK' THEN total_sales END) as SITRAK,
			COALESCE(MAX(CASE WHEN "Brand" = 'DONGFENG' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'FAW' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'FOTON' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'JAC' THEN total_sales END), 0) +
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
		slog.Warn("Can't connect to database")
		return
	}

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		response := TruckAnalyticsResponse{
			Error: "Failed to execute query: " + err.Error(),
		}
		ctx.JSON(http.StatusInternalServerError, response)
		return
	}
	defer rows.Close()

	// Map для группировки данных по федеральным округам
	dataByDistrict := make(map[string][]TruckAnalytics)

	// Читаем строки результатов и группируем по федеральным округам
	for rows.Next() {
		var ta TruckAnalytics
		var federalDistrict string

		err := rows.Scan(
			&federalDistrict,
			&ta.RegionName,
			&ta.DONGFENG,
			&ta.FAW,
			&ta.FOTON,
			&ta.JAC,
			&ta.SHACMAN,
			&ta.SITRAK,
			&ta.TOTAL,
		)
		if err != nil {
			response := TruckAnalyticsResponse{
				Error: "Failed to scan row: " + err.Error(),
			}
			ctx.JSON(http.StatusInternalServerError, response)
			return
		}

		// Добавляем данные региона в соответствующий федеральный округ
		dataByDistrict[federalDistrict] = append(dataByDistrict[federalDistrict], ta)
	}

	// Проверка на ошибки при итерации по строкам
	if err := rows.Err(); err != nil {
		response := TruckAnalyticsResponse{
			Error: "Error iterating over rows: " + err.Error(),
		}
		ctx.JSON(http.StatusInternalServerError, response)
		return
	}

	// Отправляем ответ
	response := TruckAnalyticsResponse{
		Data: dataByDistrict,
	}
	ctx.JSON(http.StatusOK, response)
}

func NineMonth2023Tractors6x4(ctx *gin.Context) {
	type TruckAnalytics struct {
		RegionName string `json:"region_name"`
		DONGFENG   *int   `json:"dongfeng"`
		FAW        *int   `json:"faw"`
		FOTON      *int   `json:"foton"`
		HOWO       *int   `json:"howo"`
		SHACMAN    *int   `json:"shacman"`
		SITRAK     *int   `json:"sitrak"`
		TOTAL      int    `json:"total"`
	}

	// TruckAnalyticsResponse structure for wrapping the response data
	type TruckAnalyticsResponse struct {
		Data  map[string][]TruckAnalytics `json:"data"`
		Error string                      `json:"error,omitempty"`
	}

	query := `
		WITH base_data AS (
			SELECT 
				truck_analytics_2024_01_09."Federal_district",
				truck_analytics_2024_01_09."Region",
				truck_analytics_2024_01_09."Brand",
				SUM(truck_analytics_2024_01_09."Quantity") as total_sales
			FROM truck_analytics_2024_01_09
			WHERE 
				truck_analytics_2024_01_09."Wheel_formula" = '6x4'
				AND truck_analytics_2024_01_09."Brand" IN ('DONGFENG', 'FAW', 'FOTON', 'HOWO', 'SHACMAN', 'SITRAK')
				AND truck_analytics_2024_01_09."Body_type" = 'Седельный тягач'
				AND truck_analytics_2024_01_09."Exact_mass" = 25000
			GROUP BY 
				truck_analytics_2024_01_09."Federal_district", 
				truck_analytics_2024_01_09."Region", 
				truck_analytics_2024_01_09."Brand"
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
			"Federal_district",
			COALESCE("Region", "Federal_district") as Region_name,
			MAX(CASE WHEN "Brand" = 'DONGFENG' THEN total_sales END) as DONGFENG,
			MAX(CASE WHEN "Brand" = 'FAW' THEN total_sales END) as FAW,
			MAX(CASE WHEN "Brand" = 'FOTON' THEN total_sales END) as FOTON,
			MAX(CASE WHEN "Brand" = 'HOWO' THEN total_sales END) as HOWO,
			MAX(CASE WHEN "Brand" = 'SHACMAN' THEN total_sales END) as SHACMAN,
			MAX(CASE WHEN "Brand" = 'SITRAK' THEN total_sales END) as SITRAK,
			COALESCE(MAX(CASE WHEN "Brand" = 'DONGFENG' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'FAW' THEN total_sales END), 0) +
			COALESCE(MAX(CASE WHEN "Brand" = 'FOTON' THEN total_sales END), 0) +
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
		slog.Warn("Can't connect to database")
		return
	}

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		response := TruckAnalyticsResponse{
			Error: "Failed to execute query: " + err.Error(),
		}
		ctx.JSON(http.StatusInternalServerError, response)
		return
	}
	defer rows.Close()

	// Map for grouping data by federal district
	dataByDistrict := make(map[string][]TruckAnalytics)

	// Process query results and group by federal district
	for rows.Next() {
		var ta TruckAnalytics
		var federalDistrict string

		err := rows.Scan(
			&federalDistrict,
			&ta.RegionName,
			&ta.DONGFENG,
			&ta.FAW,
			&ta.FOTON,
			&ta.HOWO,
			&ta.SHACMAN,
			&ta.SITRAK,
			&ta.TOTAL,
		)
		if err != nil {
			response := TruckAnalyticsResponse{
				Error: "Failed to scan row: " + err.Error(),
			}
			ctx.JSON(http.StatusInternalServerError, response)
			return
		}

		// Append the region data to the corresponding federal district
		dataByDistrict[federalDistrict] = append(dataByDistrict[federalDistrict], ta)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		response := TruckAnalyticsResponse{
			Error: "Error iterating over rows: " + err.Error(),
		}
		ctx.JSON(http.StatusInternalServerError, response)
		return
	}

	// Send the response
	response := TruckAnalyticsResponse{
		Data: dataByDistrict,
	}
	ctx.JSON(http.StatusOK, response)
}
