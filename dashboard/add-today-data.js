/**
 * Add Today's Data Script
 * This script adds calculated metrics for today to ensure charts display properly
 */

const mysql = require('mysql2/promise');
const dotenv = require('dotenv');
const path = require('path');
const moment = require('moment');

// Load environment variables from parent directory
dotenv.config({ path: path.join(__dirname, '..', '.env') });

// Database configuration
const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
};

// Calculated metric names for appointments
const calculatedMetrics = [
  { name: 'appointments_booked', type: 'count' },
  { name: 'appointments_canceled', type: 'count' },
  { name: 'appointments_conducted', type: 'count' },
  { name: 'average_deal_size', type: 'currency' },
  { name: 'close_rate', type: 'percentage' },
  { name: 'show_rate', type: 'percentage' }
];

// Representatives
const representatives = ['sierra', 'mikaela', 'mike'];

// Generate today's data
async function addTodayData() {
  console.log('🚀 Adding calculated metrics for today...');
  let connection;

  try {
    connection = await mysql.createConnection(dbConfig);
    console.log('✅ Connected to database successfully!');

    const today = moment().format('YYYY-MM-DD');
    console.log(`📅 Adding data for: ${today}`);

    // Generate data for each representative
    for (const rep of representatives) {
      await generateDataForRepresentative(connection, today, rep);
    }

    console.log('🎉 Today\'s calculated metrics added successfully!');
  } catch (error) {
    console.error('❌ Error adding today\'s data:', error);
  } finally {
    if (connection) {
      await connection.end();
      console.log('🔌 Database connection closed');
    }
  }
}

// Generate data for a specific representative on today
async function generateDataForRepresentative(connection, date, representative) {
  console.log(`  📊 Generating data for ${representative}...`);

  for (const metric of calculatedMetrics) {
    // Generate realistic sample values based on metric type
    let value;

    // Add some randomness but keep trends consistent for each rep
    const repFactor = representative === 'sierra' ? 1.2 :
                      representative === 'mikaela' ? 1.0 : 0.8;

    // Generate appropriate values for each metric type
    switch (metric.type) {
      case 'count':
        if (metric.name === 'appointments_booked') {
          value = Math.round(5 * repFactor + (Math.random() * 5));
        } else if (metric.name === 'appointments_canceled') {
          value = Math.round(1 * repFactor + (Math.random() * 2));
        } else if (metric.name === 'appointments_conducted') {
          // Conducted should be booked minus canceled, with some randomness
          const booked = Math.round(5 * repFactor + (Math.random() * 5));
          const canceled = Math.round(1 * repFactor + (Math.random() * 2));
          value = Math.max(0, booked - canceled);
        } else {
          value = Math.round(8 * repFactor + (Math.random() * 4));
        }
        break;

      case 'percentage':
        if (metric.name === 'close_rate') {
          value = 45 + (20 * repFactor) + (Math.random() * 15);
        } else if (metric.name === 'show_rate') {
          value = 65 + (15 * repFactor) + (Math.random() * 15);
        } else {
          value = 70 + (Math.random() * 20);
        }
        // Cap percentages at 100
        value = Math.min(100, value);
        break;

      case 'currency':
        // Deal sizes between $2000 and $8000
        value = 2000 + (4000 * repFactor) + (Math.random() * 2000);
        // Round to nearest 100
        value = Math.round(value / 100) * 100;
        break;

      default:
        value = Math.round(10 * repFactor);
    }

    // Format the metric name with the "calculated_" prefix
    const metricName = `calculated_${metric.name}`;

    // Insert or update the metric
    try {
      const query = `
        INSERT INTO daily_metrics
          (metric_date, representative, metric_name, metric_value, metric_type, source)
        VALUES
          (?, ?, ?, ?, ?, 'today-data-script')
        ON DUPLICATE KEY UPDATE
          metric_value = VALUES(metric_value),
          updated_at = CURRENT_TIMESTAMP
      `;

      await connection.execute(query, [
        date,
        representative,
        metricName,
        value.toFixed(2),
        metric.type
      ]);

      console.log(`    ✓ ${metricName}: ${value.toFixed(2)} (${metric.type})`);

    } catch (error) {
      console.error(`    ❌ Error inserting metric ${metricName} for ${representative} on ${date}:`, error);
    }
  }
}

// Execute the function if run directly
if (require.main === module) {
  addTodayData()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('💥 Fatal error:', err);
      process.exit(1);
    });
}

module.exports = { addTodayData };
