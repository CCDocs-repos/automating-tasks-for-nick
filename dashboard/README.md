# Sales Metrics Dashboard

A modern, sleek dashboard for visualizing sales metrics with dark mode support and daily date navigation.

![Dashboard Preview](https://via.placeholder.com/800x450?text=Sales+Dashboard+Preview)

## Features

- **Clean, Modern UI**: Matte design with unified color scheme
- **Dark Mode**: Toggle between light and dark themes
- **Date Navigation**: Day-by-day navigation with arrow controls
- **Total Metrics**: Aggregated metrics across all representatives
- **Interactive Charts**: Visual data representation for key metrics
- **Representative Cards**: Organized display of individual metrics
- **Responsive Design**: Works on desktop, tablet, and mobile devices

## Quick Start

1. Navigate to the dashboard directory:
   ```bash
   cd sales-automate/dashboard
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the dashboard:
   ```bash
   npm start
   ```

4. Access the dashboard at:
   ```
   http://localhost:3000
   ```

Alternatively, use the startup script for a guided setup:
```bash
./start.sh
```

## Usage Guide

### Date Navigation
- Use the **left arrow** to view the previous day's metrics
- Use the **right arrow** to view the next day's metrics
- The current selected date is displayed in the center

### Dark Mode
- Click the **moon/sun icon** in the top right to toggle between light and dark modes
- Your preference is saved between sessions

### Total Metrics Section
The top section displays aggregated metrics for all representatives:
- Total Appointments Booked
- Total Appointments Canceled
- Total Appointments Conducted
- Total Average Deal Size

### Charts
Three interactive charts display key metrics:
- Appointments Booked
- Appointments Conducted
- Average Deal Size

Hover over data points to see detailed information.

### Representative Metrics
Each sales representative has their own card with:
- Color-coded header for quick identification
- All relevant metrics organized in a clean list
- Visual indicators for metric types (count, percentage, currency)

## Technical Details

### Environment Configuration
Make sure your `.env` file in the parent directory contains the database configuration:

```env
DB_HOST=your_mysql_host
DB_PORT=3306
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=your_database_name
```

### Database Schema
The dashboard reads from the `daily_metrics` table created by the Python script:

```sql
CREATE TABLE daily_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_date DATE NOT NULL,
    representative ENUM('sierra','mikaela','mike') NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    metric_type ENUM('count','percentage','currency') NOT NULL DEFAULT 'count',
    source VARCHAR(50) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_metric (metric_date, representative, metric_name)
);
```

### API Endpoints

#### GET `/`
Main dashboard view with date query parameter.

**Query Parameters:**
- `date` (optional): The date to display metrics for (YYYY-MM-DD format). Defaults to today.

#### GET `/api/metrics`
JSON API endpoint for fetching metrics data.

**Query Parameters:**
- `date` (optional): The date to fetch metrics for (YYYY-MM-DD format)
- `representative` (optional): Filter by specific representative
- `metric` (optional): Filter by specific metric name

## Development

### Running in Development Mode
```bash
npm run dev
```

### Running Tests
```bash
./start.sh --test
```

## Troubleshooting

### No Data Displayed
- Ensure the Python script has run and populated the database
- Check that data exists for the selected date
- Verify database connection settings

### Display Issues
- Try refreshing the browser cache
- Ensure you're using a modern browser (Chrome, Firefox, Safari, Edge)
- Check console for any JavaScript errors

## License

MIT License