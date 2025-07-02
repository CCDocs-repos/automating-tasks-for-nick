/**
 * Sales Dashboard Chart Manager v3.0
 * Handles single-day data visualization with bar and doughnut charts
 * Fixes dark mode initialization and removes cluttered visualizations
 */

class DashboardChartManager {
  constructor() {
    this.charts = new Map();
    this.isDarkMode = false;
    this.isInitialized = false;

    // Color schemes for representatives
    this.representativeColors = {
      sierra: {
        light: "#3b82f6",
        dark: "#60a5fa",
        rgb: "59, 130, 246",
      },
      mikaela: {
        light: "#ec4899",
        dark: "#f472b6",
        rgb: "236, 72, 153",
      },
      mike: {
        light: "#06b6d4",
        dark: "#22d3ee",
        rgb: "6, 182, 212",
      },
    };

    // Chart type configurations
    this.chartConfigs = {
      appointments_booked: {
        type: "doughnut",
        title: "Appointments Booked Distribution",
      },
      appointments_conducted: {
        type: "doughnut",
        title: "Appointments Conducted Distribution",
      },
      average_deal_size: {
        type: "bar",
        title: "Average Deal Size Comparison",
      },
    };

    this.init();
  }

  init() {
    // Ensure proper initialization order
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", () => this.setup());
    } else {
      this.setup();
    }
  }

  setup() {
    console.log("DashboardChartManager: Setting up...");

    // Check theme immediately and apply to containers
    this.updateTheme();
    this.applyInitialTheme();

    // Setup theme listener
    this.setupThemeListener();

    // Wait for Chart.js and data to be available
    this.waitForDependencies()
      .then(() => {
        this.configureChartDefaults();
        this.initializeCharts();
        this.isInitialized = true;
        console.log("DashboardChartManager: Fully initialized");
      })
      .catch((error) => {
        console.error("DashboardChartManager: Initialization failed", error);
        this.showGlobalError();
      });
  }

  async waitForDependencies() {
    // Wait for Chart.js
    let attempts = 0;
    while (typeof Chart === "undefined" && attempts < 50) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      attempts++;
    }

    if (typeof Chart === "undefined") {
      throw new Error("Chart.js failed to load");
    }

    // Wait for chart data
    attempts = 0;
    while (!window.chartData && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      attempts++;
    }

    if (!window.chartData) {
      throw new Error("Chart data not available");
    }

    console.log("Dependencies loaded successfully");
  }

  updateTheme() {
    this.isDarkMode =
      document.documentElement.getAttribute("data-theme") === "dark";
    console.log("Theme updated:", this.isDarkMode ? "dark" : "light");
  }

  applyInitialTheme() {
    // Apply theme-appropriate backgrounds to chart containers immediately
    document.querySelectorAll(".chart-container").forEach((container) => {
      container.style.backgroundColor = this.isDarkMode ? "#334155" : "#ffffff";
      // Force repaint to prevent color glitches
      container.style.display = "none";
      container.offsetHeight; // trigger reflow
      container.style.display = "flex";
    });

    document.querySelectorAll(".chart-wrapper").forEach((wrapper) => {
      if (this.isDarkMode) {
        wrapper.style.background =
          "linear-gradient(135deg, rgba(51, 65, 85, 0.95) 0%, rgba(30, 41, 59, 0.9) 100%)";
      } else {
        wrapper.style.background =
          "linear-gradient(135deg, rgba(255, 255, 255, 0.9) 0%, rgba(248, 250, 252, 0.8) 100%)";
      }
      // Force repaint to prevent color glitches
      wrapper.style.display = "none";
      wrapper.offsetHeight; // trigger reflow
      wrapper.style.display = "flex";
    });
  }

  setupThemeListener() {
    const themeToggle = document.getElementById("themeToggle");
    if (themeToggle) {
      themeToggle.addEventListener("click", () => {
        // Wait for theme to be applied
        setTimeout(() => {
          this.updateTheme();
          this.applyInitialTheme();
          if (this.isInitialized) {
            this.updateAllChartsTheme();
          }
        }, 100);
      });
    }
  }

  configureChartDefaults() {
    Chart.defaults.font.family =
      "'SF Pro Display', 'Inter', system-ui, sans-serif";
    Chart.defaults.font.size = 13;
    Chart.defaults.font.weight = "500";
    Chart.defaults.color = this.isDarkMode ? "#ffffff" : "#475569";
    if (Chart.defaults.plugins && Chart.defaults.plugins.legend) {
      Chart.defaults.plugins.legend.labels.color = this.isDarkMode
        ? "#ffffff"
        : "#475569";
    }
    Chart.defaults.borderColor = this.isDarkMode
      ? "rgba(255, 255, 255, 0.1)"
      : "rgba(203, 213, 225, 0.3)";
    Chart.defaults.backgroundColor = this.isDarkMode
      ? "rgba(255, 255, 255, 0.05)"
      : "rgba(248, 250, 252, 0.8)";
    Chart.defaults.elements.arc.borderWidth = 0;
    Chart.defaults.elements.bar.borderRadius = 8;
    Chart.defaults.elements.bar.borderSkipped = false;

    // Force all text to be white in dark mode
    if (this.isDarkMode) {
      Chart.defaults.scales = Chart.defaults.scales || {};
      Chart.defaults.scales.category = Chart.defaults.scales.category || {};
      Chart.defaults.scales.category.ticks =
        Chart.defaults.scales.category.ticks || {};
      Chart.defaults.scales.category.ticks.color = "#ffffff";
      Chart.defaults.scales.linear = Chart.defaults.scales.linear || {};
      Chart.defaults.scales.linear.ticks =
        Chart.defaults.scales.linear.ticks || {};
      Chart.defaults.scales.linear.ticks.color = "#ffffff";
    }
  }

  initializeCharts() {
    console.log("Initializing charts with data:", window.chartData);

    if (!window.chartData || Object.keys(window.chartData).length === 0) {
      console.log("No chart data available, showing fallback message");
      this.showNoDataForAllCharts();
      return;
    }

    console.log("Available chart data keys:", Object.keys(window.chartData));

    // Initialize each target metric chart
    Object.keys(this.chartConfigs).forEach((metricName) => {
      const canvasId = `chart-${metricName.replace(/[^a-zA-Z0-9]/g, "")}`;
      const canvas = document.getElementById(canvasId);

      if (canvas) {
        this.createChart(canvas, metricName);
      } else {
        console.warn(`Canvas not found: ${canvasId}`);
      }
    });
  }

  createChart(canvas, metricName) {
    try {
      const chartData = window.chartData[metricName];
      const config = this.chartConfigs[metricName];

      if (
        !chartData ||
        !chartData.representatives ||
        Object.keys(chartData.representatives).length === 0
      ) {
        this.showNoDataMessage(canvas, metricName);
        return;
      }

      // Prepare canvas
      this.prepareCanvas(canvas);

      // Destroy existing chart
      if (this.charts.has(canvas.id)) {
        this.charts.get(canvas.id).destroy();
      }

      // Create chart based on type
      let chart;
      if (config.type === "doughnut") {
        chart = this.createDoughnutChart(canvas, chartData, metricName);
      } else if (config.type === "bar") {
        chart = this.createBarChart(canvas, chartData, metricName);
      }

      if (chart) {
        this.charts.set(canvas.id, chart);
        this.markChartLoaded(canvas);
        console.log(`Chart created: ${metricName} (${config.type})`);
      }
    } catch (error) {
      console.error(`Error creating chart ${metricName}:`, error);
      this.showErrorMessage(canvas, error.message);
    }
  }

  createDoughnutChart(canvas, chartData, metricName) {
    const representatives = Object.keys(chartData.representatives);
    const data = representatives.map(
      (rep) => chartData.representatives[rep].value,
    );
    const labels = representatives.map(
      (rep) => rep.charAt(0).toUpperCase() + rep.slice(1),
    );
    const colors = representatives.map((rep) =>
      this.getRepresentativeColor(rep),
    );
    const hoverColors = representatives.map((rep) =>
      this.getRepresentativeHoverColor(rep),
    );

    const config = {
      type: "doughnut",
      data: {
        labels: labels,
        datasets: [
          {
            data: data,
            backgroundColor: colors,
            hoverBackgroundColor: hoverColors,
            borderWidth: 0,
            hoverBorderWidth: 3,
            hoverBorderColor: this.isDarkMode ? "#ffffff" : "#1e293b",
            cutout: "60%",
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: {
          animateRotate: true,
          animateScale: false,
          duration: 800,
          easing: "easeOutQuad",
        },
        layout: {
          padding: 20,
        },
        plugins: {
          legend: {
            position: "bottom",
            align: "center",
            labels: {
              usePointStyle: true,
              pointStyle: "circle",
              padding: 20,
              font: {
                size: 14,
                weight: "600",
              },
              color: this.isDarkMode ? "#ffffff" : "#374151",
              generateLabels: (chart) => {
                const data = chart.data;
                return data.labels.map((label, i) => ({
                  text: `${label}: ${this.formatValue(data.datasets[0].data[i], chartData.metricType)}`,
                  fillStyle: data.datasets[0].backgroundColor[i],
                  strokeStyle: data.datasets[0].backgroundColor[i],
                  pointStyle: "circle",
                  hidden: false,
                  index: i,
                  fontColor: this.isDarkMode ? "#ffffff" : "#374151",
                  color: this.isDarkMode ? "#ffffff" : "#374151",
                }));
              },
            },
          },
          tooltip: {
            enabled: true,
            backgroundColor: this.isDarkMode
              ? "rgba(30, 41, 59, 0.95)"
              : "rgba(255, 255, 255, 0.95)",
            titleColor: this.isDarkMode ? "#ffffff" : "#1e293b",
            bodyColor: this.isDarkMode ? "#ffffff" : "#475569",
            borderColor: this.isDarkMode ? "#475569" : "#e2e8f0",
            borderWidth: 1,
            cornerRadius: 12,
            padding: 16,
            displayColors: true,
            callbacks: {
              label: (context) => {
                const value = context.parsed;
                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                const percentage = ((value / total) * 100).toFixed(1);
                return `${context.label}: ${this.formatValue(value, chartData.metricType)} (${percentage}%)`;
              },
            },
          },
        },
        onHover: (event, elements) => {
          canvas.style.cursor = elements.length > 0 ? "pointer" : "default";
        },
      },
    };

    return new Chart(canvas.getContext("2d"), config);
  }

  createBarChart(canvas, chartData, metricName) {
    const representatives = Object.keys(chartData.representatives);
    const data = representatives.map(
      (rep) => chartData.representatives[rep].value,
    );
    const labels = representatives.map(
      (rep) => rep.charAt(0).toUpperCase() + rep.slice(1),
    );
    const colors = representatives.map((rep) =>
      this.getRepresentativeColor(rep),
    );
    const hoverColors = representatives.map((rep) =>
      this.getRepresentativeHoverColor(rep),
    );

    const config = {
      type: "bar",
      data: {
        labels: labels,
        datasets: [
          {
            label: this.chartConfigs[metricName].title,
            data: data,
            backgroundColor: colors,
            hoverBackgroundColor: hoverColors,
            borderWidth: 0,
            borderRadius: {
              topLeft: 8,
              topRight: 8,
              bottomLeft: 4,
              bottomRight: 4,
            },
            borderSkipped: false,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: {
          duration: 600,
          easing: "easeOutQuad",
          delay: (context) => context.dataIndex * 50,
        },
        layout: {
          padding: {
            top: 20,
            bottom: 10,
            left: 10,
            right: 10,
          },
        },
        scales: {
          x: {
            display: true,
            grid: {
              display: false,
            },
            ticks: {
              color: this.isDarkMode ? "#ffffff" : "#64748b",
              font: {
                size: 13,
                weight: "600",
              },
              padding: 10,
            },
            title: {
              color: this.isDarkMode ? "#ffffff" : "#64748b",
              display: false,
            },
          },
          y: {
            display: true,
            beginAtZero: true,
            grid: {
              color: this.isDarkMode
                ? "rgba(255, 255, 255, 0.1)"
                : "rgba(203, 213, 225, 0.3)",
              drawBorder: false,
            },
            ticks: {
              color: this.isDarkMode ? "#ffffff" : "#64748b",
              font: {
                size: 12,
                weight: "500",
              },
              padding: 10,
              callback: (value) =>
                this.formatValue(value, chartData.metricType),
            },
            title: {
              color: this.isDarkMode ? "#ffffff" : "#64748b",
              display: false,
            },
          },
        },
        plugins: {
          legend: {
            display: false,
          },
          tooltip: {
            enabled: true,
            backgroundColor: this.isDarkMode
              ? "rgba(30, 41, 59, 0.95)"
              : "rgba(255, 255, 255, 0.95)",
            titleColor: this.isDarkMode ? "#ffffff" : "#1e293b",
            bodyColor: this.isDarkMode ? "#ffffff" : "#475569",
            borderColor: this.isDarkMode ? "#475569" : "#e2e8f0",
            borderWidth: 1,
            cornerRadius: 12,
            padding: 16,
            displayColors: false,
            callbacks: {
              title: (items) => items[0].label,
              label: (context) =>
                `${this.formatValue(context.parsed.y, chartData.metricType)}`,
            },
          },
        },
        onHover: (event, elements) => {
          canvas.style.cursor = elements.length > 0 ? "pointer" : "default";
        },
      },
    };

    return new Chart(canvas.getContext("2d"), config);
  }

  getRepresentativeColor(rep) {
    const colorScheme = this.representativeColors[rep] || {
      light: "#6b7280",
      dark: "#9ca3af",
    };
    return this.isDarkMode ? colorScheme.dark : colorScheme.light;
  }

  getRepresentativeHoverColor(rep) {
    const colorScheme = this.representativeColors[rep];
    if (!colorScheme) return this.isDarkMode ? "#a5b4fc" : "#4f46e5";

    // Create hover color by adjusting opacity
    const baseColor = this.isDarkMode ? colorScheme.dark : colorScheme.light;
    const rgb = colorScheme.rgb;
    return `rgba(${rgb}, 0.8)`;
  }

  formatValue(value, metricType) {
    switch (metricType) {
      case "currency":
        return new Intl.NumberFormat("en-US", {
          style: "currency",
          currency: "USD",
          minimumFractionDigits: 0,
          maximumFractionDigits: 0,
        }).format(value);
      case "percentage":
        return `${parseFloat(value).toFixed(1)}%`;
      default:
        return Math.round(value).toString();
    }
  }

  prepareCanvas(canvas) {
    canvas.width = 800;
    canvas.height = 500;
    canvas.style.width = "100%";
    canvas.style.height = "280px";
    canvas.style.backgroundColor = "transparent";
    canvas.style.display = "block";
  }

  markChartLoaded(canvas) {
    const container = canvas.closest(".chart-container");
    if (container) {
      container.classList.add("chart-loaded");
      container.classList.remove("chart-loading", "no-data");

      // Apply theme-appropriate background
      container.style.backgroundColor = this.isDarkMode ? "#334155" : "#ffffff";

      // Simple loaded state without animation
      container.style.opacity = "1";
    }
  }

  showNoDataMessage(canvas, metricName) {
    this.prepareCanvas(canvas);
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // Background
    const bgColor = this.isDarkMode
      ? "rgba(51, 65, 85, 0.6)"
      : "rgba(248, 250, 252, 0.8)";
    ctx.fillStyle = bgColor;
    this.roundRect(ctx, 40, 40, canvas.width - 80, canvas.height - 80, 12);
    ctx.fill();

    // Border
    ctx.strokeStyle = this.isDarkMode
      ? "rgba(99, 102, 241, 0.3)"
      : "rgba(59, 130, 246, 0.3)";
    ctx.lineWidth = 2;
    this.roundRect(ctx, 40, 40, canvas.width - 80, canvas.height - 80, 12);
    ctx.stroke();

    // Icon
    ctx.font = "48px system-ui";
    ctx.textAlign = "center";
    ctx.fillStyle = this.isDarkMode ? "#64748b" : "#94a3b8";
    ctx.fillText("📊", canvas.width / 2, canvas.height / 2 - 30);

    // Main text
    ctx.font = "bold 20px system-ui";
    ctx.fillStyle = this.isDarkMode ? "#cbd5e1" : "#475569";
    ctx.fillText("No data available", canvas.width / 2, canvas.height / 2 + 20);

    // Sub text
    ctx.font = "16px system-ui";
    ctx.fillStyle = this.isDarkMode ? "#94a3b8" : "#64748b";
    ctx.fillText(
      "for this metric on selected date",
      canvas.width / 2,
      canvas.height / 2 + 50,
    );

    // Mark container
    const container = canvas.closest(".chart-container");
    if (container) {
      container.classList.add("no-data");
      container.classList.remove("chart-loaded", "chart-loading");
      container.style.backgroundColor = this.isDarkMode ? "#334155" : "#f8fafc";
    }
  }

  showErrorMessage(canvas, errorMsg) {
    this.prepareCanvas(canvas);
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // Error background
    ctx.fillStyle = "rgba(239, 68, 68, 0.1)";
    this.roundRect(ctx, 40, 40, canvas.width - 80, canvas.height - 80, 12);
    ctx.fill();

    // Error border
    ctx.strokeStyle = "rgba(239, 68, 68, 0.4)";
    ctx.lineWidth = 2;
    this.roundRect(ctx, 40, 40, canvas.width - 80, canvas.height - 80, 12);
    ctx.stroke();

    // Error icon
    ctx.font = "36px system-ui";
    ctx.textAlign = "center";
    ctx.fillStyle = "#ef4444";
    ctx.fillText("⚠️", canvas.width / 2, canvas.height / 2 - 15);

    // Error text
    ctx.font = "bold 18px system-ui";
    ctx.fillStyle = "#ef4444";
    ctx.fillText("Chart Error", canvas.width / 2, canvas.height / 2 + 20);

    if (errorMsg) {
      ctx.font = "14px system-ui";
      ctx.fillStyle = "#64748b";
      ctx.fillText(
        errorMsg.substring(0, 40),
        canvas.width / 2,
        canvas.height / 2 + 45,
      );
    }
  }

  showNoDataForAllCharts() {
    Object.keys(this.chartConfigs).forEach((metricName) => {
      const canvasId = `chart-${metricName.replace(/[^a-zA-Z0-9]/g, "")}`;
      const canvas = document.getElementById(canvasId);
      if (canvas) {
        this.showNoDataMessage(canvas, metricName);
      }
    });
  }

  showGlobalError() {
    Object.keys(this.chartConfigs).forEach((metricName) => {
      const canvasId = `chart-${metricName.replace(/[^a-zA-Z0-9]/g, "")}`;
      const canvas = document.getElementById(canvasId);
      if (canvas) {
        this.showErrorMessage(canvas, "Failed to initialize charts");
      }
    });
  }

  updateAllChartsTheme() {
    console.log("Updating all charts for theme change");
    this.configureChartDefaults();

    // Force repaint all chart containers to prevent glitches
    document.querySelectorAll(".chart-container").forEach((container) => {
      container.style.transform = "translateZ(0)";
      container.style.backfaceVisibility = "hidden";
    });

    // Recreate all charts with new theme
    this.charts.forEach((chart, canvasId) => {
      const canvas = document.getElementById(canvasId);
      if (canvas) {
        // Extract metric name from canvas ID
        const metricName = Object.keys(this.chartConfigs).find(
          (name) => canvasId === `chart-${name.replace(/[^a-zA-Z0-9]/g, "")}`,
        );

        if (metricName) {
          // Destroy existing chart first
          if (chart) {
            chart.destroy();
          }

          // Force canvas reset to prevent color glitches
          const ctx = canvas.getContext("2d");
          ctx.clearRect(0, 0, canvas.width, canvas.height);

          // Force repaint
          canvas.style.display = "none";
          canvas.offsetHeight;
          canvas.style.display = "block";

          setTimeout(() => {
            this.createChart(canvas, metricName);
          }, 200);
        }
      }
    });
  }

  roundRect(ctx, x, y, width, height, radius) {
    ctx.beginPath();
    ctx.moveTo(x + radius, y);
    ctx.lineTo(x + width - radius, y);
    ctx.quadraticCurveTo(x + width, y, x + width, y + radius);
    ctx.lineTo(x + width, y + height - radius);
    ctx.quadraticCurveTo(x + width, y + height, x + width - radius, y + height);
    ctx.lineTo(x + radius, y + height);
    ctx.quadraticCurveTo(x, y + height, x, y + height - radius);
    ctx.lineTo(x, y + radius);
    ctx.quadraticCurveTo(x, y, x + radius, y);
    ctx.closePath();
  }
}

// Initialize the chart manager
let chartManager;

// Ensure proper initialization
if (typeof Chart !== "undefined") {
  chartManager = new DashboardChartManager();
} else {
  // Wait for Chart.js to load
  const checkInterval = setInterval(() => {
    if (typeof Chart !== "undefined") {
      clearInterval(checkInterval);
      chartManager = new DashboardChartManager();
    }
  }, 100);

  // Timeout after 10 seconds
  setTimeout(() => {
    clearInterval(checkInterval);
    if (!chartManager) {
      console.error("Chart.js failed to load within timeout");
    }
  }, 10000);
}

// Expose for backward compatibility
window.chartFix = {
  initializeCharts: () => chartManager?.initializeCharts(),
  reinitializeCharts: () => chartManager?.updateAllChartsTheme(),
  applyThemeStyles: () => chartManager?.updateAllChartsTheme(),
};

// Also expose the manager directly
window.chartManager = chartManager;
