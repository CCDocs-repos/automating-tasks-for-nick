/**
 * Metrics Verification and Cleanup Script
 * This script verifies master metrics exist and optionally cleans up old calculated metrics
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

// Helper function to get database connection
async function getDbConnection() {
  try {
    const connection = await mysql.createConnection(dbConfig);
    return connection;
  } catch (error) {
    console.error('Database connection error:', error);
    throw error;
  }
}

// Main verification function
async function verifyAndCleanupMetrics() {
  console.log('🔍 Starting metrics verification and cleanup...');
  let connection;

  try {
    connection = await getDbConnection();
    console.log('✅ Connected to database successfully!');

    // Check current metrics
    await checkCurrentMetrics(connection);

    // Check for master metrics
    await checkMasterMetrics(connection);

    // Check for calculated metrics
    await checkCalculatedMetrics(connection);

    // Optionally clean up calculated metrics
    const shouldCleanup = process.argv.includes('--cleanup');
    if (shouldCleanup) {
      await cleanupCalculatedMetrics(connection);
    } else {
      console.log('\n💡 Run with --cleanup flag to remove calculated metrics');
    }

    console.log('\n🎉 Verification completed successfully!');
  } catch (error) {
    console.error('❌ Error during verification:', error);
  } finally {
    if (connection) {
      await connection.end();
      console.log('🔌 Database connection closed');
    }
  }
}

// Check all current metrics
async function checkCurrentMetrics(connection) {
  console.log('\n📊 Checking current metrics...');

  const [metrics] = await connection.execute(`
    SELECT
      metric_name,
      COUNT(*) as count,
      MIN(metric_date) as earliest_date,
      MAX(metric_date) as latest_date
    FROM daily_metrics
    GROUP BY metric_name
    ORDER BY metric_name
  `);

  console.log(`Found ${metrics.length} different metric types:`);
  metrics.forEach(metric => {
    console.log(`  • ${metric.metric_name}: ${metric.count} records (${metric.earliest_date} to ${metric.latest_date})`);
  });
}

// Check master metrics specifically
async function checkMasterMetrics(connection) {
  console.log('\n🎯 Checking master metrics...');

  const [masterMetrics] = await connection.execute(`
    SELECT
      metric_name,
      COUNT(*) as count,
      COUNT(DISTINCT representative) as rep_count,
      MIN(metric_date) as earliest_date,
      MAX(metric_date) as latest_date
    FROM daily_metrics
    WHERE metric_name LIKE 'master_%'
    GROUP BY metric_name
    ORDER BY metric_name
  `);

  if (masterMetrics.length === 0) {
    console.log('⚠️  No master metrics found!');
    console.log('   Run: node sample-data.js to generate sample master metrics');
  } else {
    console.log(`✅ Found ${masterMetrics.length} master metric types:`);
    masterMetrics.forEach(metric => {
      console.log(`  • ${metric.metric_name}: ${metric.count} records across ${metric.rep_count} reps`);
    });
  }

  // Check for today's master metrics
  const today = moment().format('YYYY-MM-DD');
  const [todayMetrics] = await connection.execute(`
    SELECT
      metric_name,
      representative,
      metric_value,
      metric_type
    FROM daily_metrics
    WHERE metric_name LIKE 'master_%'
    AND metric_date = ?
    ORDER BY representative, metric_name
  `, [today]);

  if (todayMetrics.length === 0) {
    console.log(`⚠️  No master metrics found for today (${today})`);
  } else {
    console.log(`✅ Found ${todayMetrics.length} master metrics for today:`);
    const groupedByRep = {};
    todayMetrics.forEach(metric => {
      if (!groupedByRep[metric.representative]) {
        groupedByRep[metric.representative] = [];
      }
      groupedByRep[metric.representative].push(metric);
    });

    Object.keys(groupedByRep).forEach(rep => {
      console.log(`  ${rep.toUpperCase()}:`);
      groupedByRep[rep].forEach(metric => {
        const formattedValue = formatMetricValue(metric.metric_value, metric.metric_type);
        console.log(`    • ${metric.metric_name}: ${formattedValue}`);
      });
    });
  }
}

// Check calculated metrics
async function checkCalculatedMetrics(connection) {
  console.log('\n🔄 Checking calculated metrics...');

  const [calculatedMetrics] = await connection.execute(`
    SELECT
      metric_name,
      COUNT(*) as count,
      MIN(metric_date) as earliest_date,
      MAX(metric_date) as latest_date
    FROM daily_metrics
    WHERE metric_name LIKE 'calculated_%'
    GROUP BY metric_name
    ORDER BY metric_name
  `);

  if (calculatedMetrics.length === 0) {
    console.log('✅ No calculated metrics found (already clean)');
  } else {
    console.log(`⚠️  Found ${calculatedMetrics.length} calculated metric types:`);
    calculatedMetrics.forEach(metric => {
      console.log(`  • ${metric.metric_name}: ${metric.count} records`);
    });
  }
}

// Clean up calculated metrics
async function cleanupCalculatedMetrics(connection) {
  console.log('\n🧹 Cleaning up calculated metrics...');

  // First, check how many will be deleted
  const [countResult] = await connection.execute(`
    SELECT COUNT(*) as count
    FROM daily_metrics
    WHERE metric_name LIKE 'calculated_%'
  `);

  const countToDelete = countResult[0].count;

  if (countToDelete === 0) {
    console.log('✅ No calculated metrics to clean up');
    return;
  }

  console.log(`🗑️  Will delete ${countToDelete} calculated metric records`);

  // Confirm deletion (in a real scenario, you might want user confirmation)
  const [deleteResult] = await connection.execute(`
    DELETE FROM daily_metrics
    WHERE metric_name LIKE 'calculated_%'
  `);

  console.log(`✅ Deleted ${deleteResult.affectedRows} calculated metric records`);
}

// Helper function to format metric values
function formatMetricValue(value, type) {
  switch (type) {
    case 'currency':
      return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD'
      }).format(value);
    case 'percentage':
      return `${parseFloat(value).toFixed(1)}%`;
    case 'count':
    default:
      return Math.round(value).toString();
  }
}

// Show usage information
function showUsage() {
  console.log(`
📖 Usage:
  node verify-metrics.js           # Check metrics status
  node verify-metrics.js --cleanup # Check and cleanup old calculated metrics

🎯 This script will:
  • Show all current metrics in the database
  • Verify master metrics are present
  • Check for old calculated metrics
  • Optionally clean up calculated metrics (with --cleanup flag)

💡 Example workflow:
  1. Run: node verify-metrics.js
  2. If no master metrics found, run: node sample-data.js
  3. Run: node verify-metrics.js --cleanup (to remove old calculated metrics)
  4. Start dashboard: npm start
`);
}

// Execute the function if run directly
if (require.main === module) {
  if (process.argv.includes('--help') || process.argv.includes('-h')) {
    showUsage();
    process.exit(0);
  }

  verifyAndCleanupMetrics()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('💥 Fatal error:', err);
      process.exit(1);
    });
}

module.exports = { verifyAndCleanupMetrics };
