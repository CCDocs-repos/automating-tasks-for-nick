const express = require("express");
const mysql = require("mysql2/promise");
const path = require("path");
const moment = require("moment");
require("dotenv").config({ path: "../.env" });

const app = express();
const PORT = process.env.PORT || 3000;

// App title
const APP_TITLE = "Sales KPI Tracking";

// Database configuration
const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
};

// Set view engine and static files
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, "public")));
app.use(
  "/avatar.jpg",
  express.static(path.join(__dirname, "public/avatar.jpg")),
);
app.use(express.json());

// Helper function to get database connection
async function getDbConnection() {
  try {
    const connection = await mysql.createConnection(dbConfig);
    return connection;
  } catch (error) {
    console.error("Database connection error:", error);
    throw error;
  }
}

// Helper function to format metric values
function formatMetricValue(value, type) {
  switch (type) {
    case "currency":
      return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
      }).format(value);
    case "percentage":
      return `${parseFloat(value).toFixed(1)}%`;
    case "count":
    default:
      return Math.round(value).toString();
  }
}

// Routes
app.get("/", async (req, res) => {
  try {
    const connection = await getDbConnection();

    // Get specific date for filtering (default to today)
    const selectedDate = req.query.date || moment().format("YYYY-MM-DD");

    // Get all metrics for the selected date, including "master" metrics
    const [metrics] = await connection.execute(
      `
            SELECT
                metric_date,
                representative,
                metric_name,
                metric_value,
                metric_type,
                source,
                created_at
            FROM daily_metrics
            WHERE metric_date = ?
            AND metric_name LIKE 'master_%'
            ORDER BY representative, metric_name
        `,
      [selectedDate],
    );

    // Get total metrics for the selected date
    const [totalMetrics] = await connection.execute(
      `
            SELECT
                REPLACE(REPLACE(metric_name, 'calculated_', ''), 'Calculated ', '') as clean_metric_name,
                CASE
                    WHEN metric_name LIKE '%average_deal_size%' THEN AVG(metric_value)
                    ELSE SUM(metric_value)
                END as total_value,
                metric_type
            FROM daily_metrics
            WHERE metric_date = ?
            AND metric_name LIKE 'calculated_%'
            AND (
                metric_name LIKE '%appointments_booked%' OR
                metric_name LIKE '%appointments_canceled%' OR
                metric_name LIKE '%appointments_conducted%' OR
                metric_name LIKE '%average_deal_size%'
            )
            GROUP BY clean_metric_name, metric_type
            ORDER BY
                CASE
                    WHEN clean_metric_name LIKE '%appointments_booked%' THEN 1
                    WHEN clean_metric_name LIKE '%appointments_canceled%' THEN 2
                    WHEN clean_metric_name LIKE '%appointments_conducted%' THEN 3
                    WHEN clean_metric_name LIKE '%average_deal_size%' THEN 4
                    ELSE 5
                END
        `,
      [selectedDate],
    );

    // Get metrics by representative for the selected date
    const [repMetrics] = await connection.execute(
      `
            SELECT
                representative,
                CASE
                    WHEN metric_name IN (
                        'master_new_clients_closed',
                        'master_organic_clients_closed',
                        'master_total_new_clients_closed',
                        'master_new_client_revenue',
                        'master_rebuy_revenue',
                        'master_total_revenue'
                    ) THEN REPLACE(metric_name, 'master_', '')
                    WHEN metric_name LIKE 'calculated_%' THEN REPLACE(REPLACE(metric_name, 'calculated_', ''), 'Calculated ', '')
                    ELSE metric_name
                END as clean_metric_name,
                metric_value,
                metric_type,
                metric_date,
                source
            FROM daily_metrics
            WHERE metric_date = ?
            AND (
                metric_name IN (
                    'master_new_clients_closed',
                    'master_organic_clients_closed',
                    'master_total_new_clients_closed',
                    'master_new_client_revenue',
                    'master_rebuy_revenue',
                    'master_total_revenue'
                ) OR
                (metric_name LIKE 'calculated_%' AND metric_name NOT IN (
                    'calculated_new_clients_closed',
                    'calculated_organic_clients_closed',
                    'calculated_total_new_clients_closed',
                    'calculated_new_client_revenue',
                    'calculated_rebuy_revenue',
                    'calculated_total_revenue'
                ))
            )
            ORDER BY representative, clean_metric_name
        `,
      [selectedDate],
    );

    // Get chart data for the selected date only (single day view)
    const [chartData] = await connection.execute(
      `
            SELECT
                metric_date,
                representative,
                CASE
                    WHEN metric_name IN (
                        'master_new_clients_closed',
                        'master_organic_clients_closed',
                        'master_total_new_clients_closed',
                        'master_new_client_revenue',
                        'master_rebuy_revenue',
                        'master_total_revenue'
                    ) THEN REPLACE(metric_name, 'master_', '')
                    WHEN metric_name LIKE 'calculated_%' THEN REPLACE(REPLACE(metric_name, 'calculated_', ''), 'Calculated ', '')
                    ELSE metric_name
                END as clean_metric_name,
                metric_value,
                metric_type
            FROM daily_metrics
            WHERE metric_date = ?
            AND (
                metric_name IN (
                    'calculated_appointments_conducted',
                    'calculated_average_deal_size'
                )
            )
            ORDER BY representative, clean_metric_name
        `,
      [selectedDate],
    );

    await connection.end();

    // Process data
    const representativeData = groupMetricsByRepresentative(repMetrics);
    const processedChartData = processMetricsForCharts(chartData);

    res.render("dashboard", {
      appTitle: APP_TITLE,
      selectedDate,
      totalMetrics,
      representativeData,
      processedChartData,
      moment,
      formatMetricValue,
    });
  } catch (error) {
    console.error("Error fetching dashboard data:", error);
    res.status(500).render("error", { error: "Failed to load dashboard data" });
  }
});

// API endpoint for metrics data (for AJAX requests)
app.get("/api/metrics", async (req, res) => {
  try {
    const connection = await getDbConnection();
    const selectedDate = req.query.date || moment().format("YYYY-MM-DD");
    const representative = req.query.representative;
    const metricName = req.query.metric;

    let query = `
            SELECT
                metric_date,
                representative,
                CASE
                    WHEN metric_name IN (
                        'master_new_clients_closed',
                        'master_organic_clients_closed',
                        'master_total_new_clients_closed',
                        'master_new_client_revenue',
                        'master_rebuy_revenue',
                        'master_total_revenue'
                    ) THEN REPLACE(metric_name, 'master_', '')
                    WHEN metric_name LIKE 'calculated_%' THEN REPLACE(REPLACE(metric_name, 'calculated_', ''), 'Calculated ', '')
                    ELSE metric_name
                END as clean_metric_name,
                metric_value,
                metric_type
            FROM daily_metrics
            WHERE metric_date = ?
            AND (
                metric_name IN (
                    'master_new_clients_closed',
                    'master_organic_clients_closed',
                    'master_total_new_clients_closed',
                    'master_new_client_revenue',
                    'master_rebuy_revenue',
                    'master_total_revenue'
                ) OR
                (metric_name LIKE 'calculated_%' AND metric_name NOT IN (
                    'calculated_new_clients_closed',
                    'calculated_organic_clients_closed',
                    'calculated_total_new_clients_closed',
                    'calculated_new_client_revenue',
                    'calculated_rebuy_revenue',
                    'calculated_total_revenue'
                ))
            )
        `;
    let params = [selectedDate];

    if (representative) {
      query += " AND representative = ?";
      params.push(representative);
    }

    if (metricName) {
      query += " AND clean_metric_name = ?";
      params.push(metricName);
    }

    query += " ORDER BY representative, clean_metric_name";

    const [metrics] = await connection.execute(query, params);
    await connection.end();

    res.json({
      success: true,
      data: metrics,
      count: metrics.length,
    });
  } catch (error) {
    console.error("API error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch metrics",
    });
  }
});

// Helper function to process metrics for charts (single day data)
function processMetricsForCharts(metrics) {
  const chartData = {};

  metrics.forEach((metric) => {
    const metricName = metric.clean_metric_name;
    if (!chartData[metricName]) {
      chartData[metricName] = {
        metricName: metricName,
        metricType: metric.metric_type,
        representatives: {},
        totalValue: 0,
        chartType: getChartType(metric.metric_type, metricName),
      };
    }

    // Store single day value for each representative
    chartData[metricName].representatives[metric.representative] = {
      value: parseFloat(metric.metric_value),
      date: metric.metric_date,
    };

    // Calculate total for percentage calculations
    chartData[metricName].totalValue += parseFloat(metric.metric_value);
  });

  return chartData;
}

// Helper function to determine chart type based on metric
function getChartType(metricType, metricName) {
  // Appointment metrics
  if (
    metricName.includes("appointments_booked") ||
    metricName.includes("appointments_conducted")
  ) {
    return "doughnut"; // Doughnut chart for appointment distribution
  } else if (metricName.includes("average_deal_size")) {
    return "bar"; // Bar chart for deal size comparison
  }
  // Sales metrics
  else if (
    metricName.includes("new_clients_closed") &&
    !metricName.includes("total")
  ) {
    return "doughnut"; // Doughnut chart for showing distribution
  } else if (
    metricName.includes("total_revenue") ||
    metricName.includes("total_new_clients_closed")
  ) {
    return "bar"; // Bar chart for comparing totals
  } else {
    return "bar"; // Default to bar chart
  }
}

// Helper function to group metrics by representative
function groupMetricsByRepresentative(metrics) {
  const grouped = {};

  metrics.forEach((metric) => {
    if (!grouped[metric.representative]) {
      grouped[metric.representative] = {};
    }
    grouped[metric.representative][metric.clean_metric_name] = metric;
  });

  return grouped;
}

// Error handler
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).render("error", {
    error: "Something went wrong!",
    details: process.env.NODE_ENV === "development" ? err.message : null,
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).render("error", {
    error: "Page not found",
    details: `The page ${req.url} does not exist.`,
  });
});

app.listen(PORT, () => {
  console.log(`🚀 ${APP_TITLE} running on http://localhost:${PORT}`);
  console.log(
    `📊 Connected to database: ${dbConfig.host}:${dbConfig.port}/${dbConfig.database}`,
  );
});

module.exports = app;
