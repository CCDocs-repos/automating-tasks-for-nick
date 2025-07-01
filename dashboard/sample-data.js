/**
 * Sample Data Generator for Sales Dashboard
 * This script inserts sample data into the daily_metrics table
 * to ensure charts display properly for testing purposes.
 */

const mysql = require("mysql2/promise");
const dotenv = require("dotenv");
const path = require("path");
const moment = require("moment");

// Load environment variables from parent directory
dotenv.config({ path: path.join(__dirname, "..", ".env") });

// Database configuration
const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
};

// Metric names to generate data for (without "master_" prefix as we'll add it)
const metricNames = [
  { name: "new_clients_closed", type: "count" },
  { name: "organic_clients_closed", type: "count" },
  { name: "total_new_clients_closed", type: "count" },
  { name: "new_client_revenue", type: "currency" },
  { name: "rebuy_revenue", type: "currency" },
  { name: "total_revenue", type: "currency" },
];

// Representatives
const representatives = ["sierra", "mikaela", "mike"];

// Generate 30 days of historical data
async function generateSampleData() {
  console.log("Connecting to database...");
  let connection;

  try {
    connection = await mysql.createConnection(dbConfig);
    console.log("Connected to database successfully!");

    // Verify table exists
    await ensureTableExists(connection);

    const today = moment();
    const startDate = moment().subtract(30, "days");

    console.log(
      `Generating sample data from ${startDate.format("YYYY-MM-DD")} to ${today.format("YYYY-MM-DD")}`,
    );

    // Generate data for each day
    for (
      let date = moment(startDate);
      date.isSameOrBefore(today);
      date.add(1, "day")
    ) {
      const formattedDate = date.format("YYYY-MM-DD");
      console.log(`Generating data for ${formattedDate}...`);

      // Generate data for each representative
      for (const rep of representatives) {
        await generateDataForRepresentative(connection, formattedDate, rep);
      }
    }

    console.log("Sample data generation completed successfully!");
  } catch (error) {
    console.error("Error generating sample data:", error);
  } finally {
    if (connection) {
      await connection.end();
      console.log("Database connection closed");
    }
  }
}

// Ensure the daily_metrics table exists
async function ensureTableExists(connection) {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS daily_metrics (
      id INT AUTO_INCREMENT PRIMARY KEY,
      metric_date DATE NOT NULL,
      representative ENUM('sierra','mikaela','mike') NOT NULL,
      metric_name VARCHAR(100) NOT NULL,
      metric_value DECIMAL(15,2) NOT NULL DEFAULT 0.00,
      metric_type ENUM('count','percentage','currency') NOT NULL DEFAULT 'count',
      source VARCHAR(50) DEFAULT 'sample-data',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY unique_metric (metric_date, representative, metric_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `;

  await connection.execute(createTableQuery);
  console.log("Table daily_metrics verified/created");
}

// Generate data for a specific representative on a specific date
async function generateDataForRepresentative(connection, date, representative) {
  for (const metric of metricNames) {
    // Generate realistic sample values based on metric type
    let value;

    // Add some randomness but keep trends consistent for each rep
    const repFactor =
      representative === "sierra"
        ? 1.2
        : representative === "mikaela"
          ? 1.0
          : 0.8;

    // Add a date factor so values trend upward over time
    const daysSinceStart = moment(date).diff(
      moment().subtract(30, "days"),
      "days",
    );
    const dateFactor = 1 + daysSinceStart * 0.01;

    // Generate appropriate values for each metric type
    switch (metric.type) {
      case "count":
        if (metric.name === "new_clients_closed") {
          value = Math.round(3 * repFactor * dateFactor + Math.random() * 4);
        } else if (metric.name === "organic_clients_closed") {
          value = Math.round(2 * repFactor * dateFactor + Math.random() * 3);
        } else if (metric.name === "total_new_clients_closed") {
          // Total should be new + organic, with some randomness
          const newClients = Math.round(
            3 * repFactor * dateFactor + Math.random() * 4,
          );
          const organicClients = Math.round(
            2 * repFactor * dateFactor + Math.random() * 3,
          );
          value = newClients + organicClients;
        } else {
          value = Math.round(8 * repFactor * dateFactor + Math.random() * 5);
        }
        break;

      case "currency":
        if (metric.name === "new_client_revenue") {
          // New client revenue between $5000 and $15000
          value = 5000 + 8000 * repFactor * dateFactor + Math.random() * 2000;
        } else if (metric.name === "rebuy_revenue") {
          // Rebuy revenue between $3000 and $10000
          value = 3000 + 5000 * repFactor * dateFactor + Math.random() * 2000;
        } else if (metric.name === "total_revenue") {
          // Total revenue is new + rebuy
          const newRevenue =
            5000 + 8000 * repFactor * dateFactor + Math.random() * 2000;
          const rebuyRevenue =
            3000 + 5000 * repFactor * dateFactor + Math.random() * 2000;
          value = newRevenue + rebuyRevenue;
        } else {
          // Default currency value
          value = 2000 + 3000 * repFactor * dateFactor + Math.random() * 1000;
        }
        // Round to nearest 100
        value = Math.round(value / 100) * 100;
        break;

      default:
        value = Math.round(10 * repFactor * dateFactor);
    }

    // Format the metric name with the "master_" prefix
    const metricName = `master_${metric.name}`;

    // Insert or update the metric
    try {
      const query = `
        INSERT INTO daily_metrics
          (metric_date, representative, metric_name, metric_value, metric_type, source)
        VALUES
          (?, ?, ?, ?, ?, 'sample-data')
        ON DUPLICATE KEY UPDATE
          metric_value = VALUES(metric_value),
          updated_at = CURRENT_TIMESTAMP
      `;

      await connection.execute(query, [
        date,
        representative,
        metricName,
        value.toFixed(2),
        metric.type,
      ]);
    } catch (error) {
      console.error(
        `Error inserting metric ${metricName} for ${representative} on ${date}:`,
        error,
      );
    }
  }
}

// Execute the function if run directly
if (require.main === module) {
  generateSampleData()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error("Fatal error:", err);
      process.exit(1);
    });
}

module.exports = { generateSampleData };
