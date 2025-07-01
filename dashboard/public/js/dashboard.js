// Dashboard JavaScript - Chart Initialization and Interactivity

// Chart color palette matching the uniform design
const CHART_COLORS = {
  sierra: "#4a6cff",
  mikaela: "#7789ff",
  mike: "#5271f2",
  currency: "#16a085",
  percentage: "#9b59b6",
  count: "#3498db",
  accent: ["#4a6cff", "#7789ff", "#5271f2", "#16a085", "#9b59b6", "#3498db"],
};

// Dark mode chart colors
const DARK_CHART_COLORS = {
  sierra: "#5a78ff",
  mikaela: "#8696ff",
  mike: "#627ef9",
  currency: "#1abc9c",
  percentage: "#a569bd",
  count: "#3498db",
  accent: ["#5a78ff", "#8696ff", "#627ef9", "#1abc9c", "#a569bd", "#3498db"],
};

// Chart.js default configuration
Chart.defaults.font.family =
  "Inter, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif";
Chart.defaults.font.size = 12;
Chart.defaults.color = "#636e72";
Chart.defaults.plugins.legend.display = true;
Chart.defaults.plugins.tooltip.enabled = true;

// Update chart colors based on theme
function getChartColorsByTheme() {
  const isDarkMode =
    document.documentElement.getAttribute("data-theme") === "dark";
  return isDarkMode ? DARK_CHART_COLORS : CHART_COLORS;
}

// Update chart colors when theme changes
document.addEventListener("DOMContentLoaded", function () {
  const themeToggle = document.getElementById("themeToggle");
  if (themeToggle) {
    themeToggle.addEventListener("click", function () {
      // Delay to allow the CSS transitions to complete
      setTimeout(() => {
        updateChartsForTheme();
      }, 300);
    });
  }
});

// Update all charts when theme changes
function updateChartsForTheme() {
  const charts = Object.values(Chart.instances || {});
  if (charts.length) {
    charts.forEach((chart) => {
      updateChartColors(chart);

      // Update with animation
      chart.update({
        duration: 800,
        easing: "easeOutQuad",
      });

      // Add transition effect to chart canvas
      const canvas = chart.canvas;
      if (canvas) {
        canvas.style.transition = "all 0.5s ease";
        canvas.style.opacity = 0.7;
        setTimeout(() => {
          canvas.style.opacity = 1;
        }, 300);
      }
    });
  }
}

// Update colors for a specific chart
function updateChartColors(chart) {
  const colors = getChartColorsByTheme();
  const datasets = chart.data.datasets;

  datasets.forEach((dataset, index) => {
    const rep = dataset.label.toLowerCase();
    if (colors[rep]) {
      dataset.borderColor = colors[rep];
      dataset.backgroundColor = colors[rep] + "20";
      dataset.pointBackgroundColor = colors[rep];
      dataset.pointHoverBackgroundColor = colors[rep];
    } else {
      const colorIndex = index % colors.accent.length;
      dataset.borderColor = colors.accent[colorIndex];
      dataset.backgroundColor = colors.accent[colorIndex] + "20";
      dataset.pointBackgroundColor = colors.accent[colorIndex];
      dataset.pointHoverBackgroundColor = colors.accent[colorIndex];
    }
  });
}

// Initialize all charts
function initializeCharts(chartData, animate = false) {
  // Create charts for each metric
  const chartMetricNames = [
    "appointments_booked",
    "appointments_conducted",
    "average_deal_size",
  ];

  // Create charts for each specified metric
  chartMetricNames.forEach((metricName, index) => {
    const metricData = chartData[metricName];
    const canvasId = `chart-${metricName.replace(/[^a-zA-Z0-9]/g, "")}`;
    const canvas = document.getElementById(canvasId);

    if (canvas) {
      // Set fixed dimensions for the canvas
      canvas.width = 400;
      canvas.height = 250;
      canvas.style.width = "100%";
      canvas.style.height = "250px";

      console.log(`Initializing chart for ${metricName}`, {
        canvas: canvas,
        canvasId: canvasId,
        hasData:
          metricData &&
          Object.keys(metricData.representatives || {}).length > 0,
        metricData: metricData,
      });

      // Add delay for sequential animation
      setTimeout(() => {
        try {
          if (
            metricData &&
            Object.keys(metricData.representatives || {}).length > 0
          ) {
            createChart(canvas, metricName, metricData, animate);
          } else {
            // Display fallback message when no data
            const ctx = canvas.getContext("2d");
            if (ctx) {
              // Clear canvas first
              ctx.clearRect(0, 0, canvas.width, canvas.height);
              ctx.font = "16px Inter, sans-serif";
              ctx.textAlign = "center";
              ctx.fillStyle = "#636e72";
              ctx.fillText(
                "No chart data available",
                canvas.width / 2,
                canvas.height / 2,
              );

              // Draw a border
              ctx.strokeStyle = "#ddd";
              ctx.lineWidth = 2;
              ctx.strokeRect(10, 10, canvas.width - 20, canvas.height - 20);
            }
          }
        } catch (error) {
          console.error(`Error creating chart for ${metricName}:`, error);
        }

        // Add visible class to parent container for fade-in effect
        const container = canvas.closest(".chart-container");
        if (container) {
          container.classList.add("chart-visible");
        }
      }, index * 300);
    }
  });
}

// Create individual chart
function createChart(canvas, metricName, metricData, animate = false) {
  try {
    // Safety check for canvas
    if (!canvas) {
      console.error("Canvas element is null", { metricName });
      return;
    }

    const ctx = canvas.getContext("2d");
    if (!ctx) {
      console.error("Could not get 2D context from canvas", {
        metricName,
        canvas,
      });
      return;
    }

    // Clear existing content
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    const colors = getChartColorsByTheme();
    console.log(`Creating chart for ${metricName} with canvas:`, canvas);

    // Prepare datasets
    const datasets = [];

    // Create a dataset for each representative
    if (
      metricData.representatives &&
      Object.keys(metricData.representatives).length > 0
    ) {
      Object.keys(metricData.representatives).forEach(
        (representative, index) => {
          const repData = metricData.representatives[representative] || [];
          if (repData.length === 0) return;

          const color =
            colors[representative] ||
            colors.accent[index % colors.accent.length];

          // Log the data points
          console.log(`Data for ${representative}:`, repData);

          datasets.push({
            label:
              representative.charAt(0).toUpperCase() + representative.slice(1),
            data: repData.map((point) => ({
              x: point.date,
              y: point.value,
            })),
            borderColor: color,
            backgroundColor: color + "20", // Add transparency
            borderWidth: 3,
            fill: false,
            tension: 0.4,
            pointBackgroundColor: color,
            pointBorderColor: "#ffffff",
            pointBorderWidth: 2,
            pointRadius: 4,
            pointHoverRadius: 7,
            pointHoverBackgroundColor: color,
            pointHoverBorderColor: "#ffffff",
            pointHoverBorderWidth: 3,
            // Add shadow for line
            borderCapStyle: "round",
            borderJoinStyle: "round",
            // Add gradient effect
            segment: {
              borderColor: (ctx) => {
                if (!ctx || !ctx.chart || !ctx.chart.ctx) return color;
                try {
                  const gradient = ctx.chart.ctx.createLinearGradient(
                    0,
                    0,
                    0,
                    ctx.chart.height,
                  );
                  gradient.addColorStop(0, color);
                  gradient.addColorStop(1, color + "80");
                  return gradient;
                } catch (e) {
                  console.error("Gradient error:", e);
                  return color;
                }
              },
            },
          });
        },
      );
    }

    // Only proceed if we have datasets
    if (datasets.length === 0) {
      console.warn(`No datasets created for ${metricName}`);
      ctx.font = "16px Inter, sans-serif";
      ctx.textAlign = "center";
      ctx.fillStyle = "#636e72";
      ctx.fillText(
        "No data available for chart",
        canvas.width / 2,
        canvas.height / 2,
      );
      return;
    }

    // Determine chart type and configuration based on metric type
    const metricType = metricData.metricType || "count";
    const chartConfig = getChartConfig(
      metricName,
      metricType,
      datasets,
      animate,
    );

    // Destroy any existing chart
    if (canvas.chart) {
      canvas.chart.destroy();
    }

    // Create new chart and store reference
    canvas.chart = new Chart(ctx, chartConfig);
    console.log(`Chart created for ${metricName}`, canvas.chart);
  } catch (error) {
    console.error("Error creating chart:", error, { metricName, canvas });
    // Fallback to basic canvas rendering
    try {
      const ctx = canvas.getContext("2d");
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.font = "16px Inter, sans-serif";
      ctx.textAlign = "center";
      ctx.fillStyle = "#636e72";
      ctx.fillText(
        "Error creating chart",
        canvas.width / 2,
        canvas.height / 2 - 20,
      );
      ctx.fillText(
        error.message || "Unknown error",
        canvas.width / 2,
        canvas.height / 2 + 20,
      );
    } catch (e) {
      console.error("Fallback rendering failed:", e);
    }
  }
}

// Get chart configuration based on metric type
function getChartConfig(metricName, metricType, datasets, animate) {
  const baseConfig = {
    type: "line",
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false,
      },
      plugins: {
        title: {
          display: false,
        },
        legend: {
          position: "bottom",
          labels: {
            usePointStyle: true,
            pointStyle: "circle",
            padding: 20,
            font: {
              size: 11,
              weight: "500",
            },
          },
        },
        tooltip: {
          backgroundColor:
            document.documentElement.getAttribute("data-theme") === "dark"
              ? "rgba(42, 47, 54, 0.95)"
              : "rgba(255, 255, 255, 0.95)",
          titleColor:
            document.documentElement.getAttribute("data-theme") === "dark"
              ? "#ecf0f1"
              : "#2d3436",
          bodyColor:
            document.documentElement.getAttribute("data-theme") === "dark"
              ? "#bdc3c7"
              : "#636e72",
          borderColor:
            document.documentElement.getAttribute("data-theme") === "dark"
              ? "#323741"
              : "#ddd",
          borderWidth: 1,
          cornerRadius: 8,
          displayColors: true,
          titleFont: {
            size: 13,
            weight: "600",
          },
          bodyFont: {
            size: 12,
          },
          callbacks: {
            label: function (context) {
              const value = formatTooltipValue(context.parsed.y, metricType);
              return `${context.dataset.label}: ${value}`;
            },
            title: function (tooltipItems) {
              return moment(tooltipItems[0].parsed.x).format("MMMM DD, YYYY");
            },
          },
        },
      },
      scales: {
        x: {
          type: "time",
          time: {
            parser: "YYYY-MM-DD",
            displayFormats: {
              day: "MMM DD",
              week: "MMM DD",
              month: "MMM YYYY",
            },
          },
          title: {
            display: true,
            text: "Date",
            font: {
              size: 11,
              weight: "500",
            },
            color: "#636e72",
          },
          grid: {
            color:
              document.documentElement.getAttribute("data-theme") === "dark"
                ? "rgba(70, 80, 90, 0.2)"
                : "rgba(178, 190, 195, 0.2)",
            borderColor:
              document.documentElement.getAttribute("data-theme") === "dark"
                ? "rgba(70, 80, 90, 0.3)"
                : "rgba(178, 190, 195, 0.3)",
          },
          ticks: {
            color:
              document.documentElement.getAttribute("data-theme") === "dark"
                ? "#bdc3c7"
                : "#636e72",
            font: {
              size: 10,
            },
          },
        },
        y: {
          beginAtZero: metricType !== "percentage",
          title: {
            display: true,
            text: getYAxisLabel(metricType),
            font: {
              size: 11,
              weight: "500",
            },
            color: "#636e72",
          },
          grid: {
            color:
              document.documentElement.getAttribute("data-theme") === "dark"
                ? "rgba(70, 80, 90, 0.2)"
                : "rgba(178, 190, 195, 0.2)",
            borderColor:
              document.documentElement.getAttribute("data-theme") === "dark"
                ? "rgba(70, 80, 90, 0.3)"
                : "rgba(178, 190, 195, 0.3)",
          },
          ticks: {
            color:
              document.documentElement.getAttribute("data-theme") === "dark"
                ? "#bdc3c7"
                : "#636e72",
            font: {
              size: 10,
            },
            callback: function (value) {
              return formatTickValue(value, metricType);
            },
          },
        },
      },
      elements: {
        point: {
          hoverBorderWidth: 3,
        },
        line: {
          tension: 0.4,
        },
      },
      animation: animate
        ? {
            tension: {
              duration: 1500,
              easing: "easeOutQuart",
              from: 0,
              to: 0.4,
              loop: false,
            },
            y: {
              duration: 1500,
              easing: "easeOutCubic",
              delay: (ctx) => ctx.dataIndex * 100 + ctx.datasetIndex * 300,
              from: (ctx) => {
                // Start from the bottom of the chart
                if (ctx.type === "data" && ctx.mode === "default") {
                  return ctx.chart.scales.y.getPixelForValue(0);
                }
                return undefined;
              },
            },
            x: {
              duration: 1000,
              easing: "easeOutSine",
            },
            onProgress: function (animation) {
              try {
                const chartInstance = animation.chart;
                const ctx = chartInstance.ctx;
                if (ctx) {
                  ctx.save();
                  ctx.shadowColor = "rgba(0, 0, 0, 0.15)";
                  ctx.shadowBlur = 10;
                  ctx.shadowOffsetX = 0;
                  ctx.shadowOffsetY = 4;
                  ctx.restore();
                }
              } catch (e) {
                console.error("Animation error:", e);
              }
            },
          }
        : {
            duration: 0, // When animate is false, set duration to 0 instead of disabling animations completely
          },
    },
  };

  return baseConfig;
}

// Format tooltip values based on metric type
function formatTooltipValue(value, type) {
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

// Format Y-axis tick values
function formatTickValue(value, type) {
  switch (type) {
    case "currency":
      return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        notation: "compact",
      }).format(value);
    case "percentage":
      return `${value}%`;
    case "count":
    default:
      return new Intl.NumberFormat("en-US", {
        notation: "compact",
      }).format(value);
  }
}

// Get Y-axis label based on metric type
function getYAxisLabel(type) {
  switch (type) {
    case "currency":
      return "Amount (USD)";
    case "percentage":
      return "Percentage (%)";
    case "count":
    default:
      return "Count";
  }
}

// Utility functions for dashboard interactivity
document.addEventListener("DOMContentLoaded", function () {
  // Add smooth scrolling to all anchor links
  document.querySelectorAll('a[href^="#"]').forEach((anchor) => {
    anchor.addEventListener("click", function (e) {
      e.preventDefault();
      const target = document.querySelector(this.getAttribute("href"));
      if (target) {
        target.scrollIntoView({
          behavior: "smooth",
          block: "start",
        });
      }
    });
  });

  // Add hover effects to representative cards
  document.querySelectorAll(".rep-card").forEach((card) => {
    card.addEventListener("mouseenter", function () {
      this.style.transform = "translateY(-8px)";
      this.style.boxShadow = "var(--shadow-heavy)";
    });

    card.addEventListener("mouseleave", function () {
      this.style.transform = "translateY(-5px)";
      this.style.boxShadow = "var(--shadow-medium)";
    });
  });

  // Add hover effects to total metric cards
  document.querySelectorAll(".total-metric").forEach((card) => {
    card.addEventListener("mouseenter", function () {
      this.style.transform = "translateY(-8px)";
    });

    card.addEventListener("mouseleave", function () {
      this.style.transform = "translateY(-5px)";
    });
  });

  // Initialize tooltips for metric cards
  initializeTooltips();

  // Add keyboard navigation support
  addKeyboardNavigation();
});

// Initialize tooltips for better UX
function initializeTooltips() {
  const tooltipElements = document.querySelectorAll("[data-tooltip]");

  tooltipElements.forEach((element) => {
    element.addEventListener("mouseenter", showTooltip);
    element.addEventListener("mouseleave", hideTooltip);
  });
}

// Show custom tooltip
function showTooltip(event) {
  const tooltip = document.createElement("div");
  tooltip.className = "custom-tooltip";
  tooltip.textContent = event.target.getAttribute("data-tooltip");

  tooltip.style.cssText = `
        position: absolute;
        background: rgba(45, 52, 54, 0.9);
        color: white;
        padding: 8px 12px;
        border-radius: 6px;
        font-size: 12px;
        pointer-events: none;
        z-index: 1000;
        opacity: 0;
        transition: opacity 0.2s ease;
        backdrop-filter: blur(10px);
    `;

  document.body.appendChild(tooltip);

  const rect = event.target.getBoundingClientRect();
  tooltip.style.left =
    rect.left + rect.width / 2 - tooltip.offsetWidth / 2 + "px";
  tooltip.style.top = rect.top - tooltip.offsetHeight - 8 + "px";

  setTimeout(() => (tooltip.style.opacity = "1"), 10);

  event.target._tooltip = tooltip;
}

// Hide custom tooltip
function hideTooltip(event) {
  if (event.target._tooltip) {
    const tooltip = event.target._tooltip;
    tooltip.style.opacity = "0";
    setTimeout(() => {
      if (document.body.contains(tooltip)) {
        document.body.removeChild(tooltip);
      }
    }, 200);
    delete event.target._tooltip;
  }
}

// Add keyboard navigation support
function addKeyboardNavigation() {
  document.addEventListener("keydown", function (event) {
    // ESC key to close any open modals or dropdowns
    if (event.key === "Escape") {
      const openDropdowns = document.querySelectorAll(".dropdown.open");
      openDropdowns.forEach((dropdown) => {
        dropdown.classList.remove("open");
      });
    }

    // Arrow key navigation for metric cards
    if (event.key.startsWith("Arrow")) {
      const focusedElement = document.activeElement;
      const metricCards = Array.from(document.querySelectorAll(".metric-card"));

      if (metricCards.includes(focusedElement)) {
        event.preventDefault();
        const currentIndex = metricCards.indexOf(focusedElement);
        let nextIndex;

        switch (event.key) {
          case "ArrowRight":
          case "ArrowDown":
            nextIndex = (currentIndex + 1) % metricCards.length;
            break;
          case "ArrowLeft":
          case "ArrowUp":
            nextIndex =
              (currentIndex - 1 + metricCards.length) % metricCards.length;
            break;
        }

        if (nextIndex !== undefined) {
          metricCards[nextIndex].focus();
        }
      }
    }
  });

  // Make metric cards focusable
  document.querySelectorAll(".metric-card").forEach((card) => {
    card.setAttribute("tabindex", "0");
    card.addEventListener("focus", function () {
      this.style.outline = "2px solid #74b9ff";
      this.style.outlineOffset = "2px";
    });
    card.addEventListener("blur", function () {
      this.style.outline = "none";
    });
  });
}

// Performance monitoring
function trackPerformance() {
  if ("performance" in window) {
    window.addEventListener("load", function () {
      setTimeout(function () {
        const perfData = performance.getEntriesByType("navigation")[0];
        console.log("Dashboard Load Performance:", {
          pageLoadTime: Math.round(perfData.loadEventEnd - perfData.fetchStart),
          domContentLoaded: Math.round(
            perfData.domContentLoadedEventEnd - perfData.fetchStart,
          ),
          timeToInteractive: Math.round(
            perfData.loadEventEnd - perfData.fetchStart,
          ),
        });
      }, 0);
    });
  }
}

// Initialize performance tracking
if (typeof window !== "undefined") {
  trackPerformance();
}

// Ensure Chart.js is loaded
function checkChartJsLoaded() {
  if (typeof Chart === "undefined") {
    console.error(
      "Chart.js is not loaded! Attempting to load it dynamically...",
    );
    const script = document.createElement("script");
    script.src = "https://cdn.jsdelivr.net/npm/chart.js";
    script.onload = function () {
      console.log("Chart.js loaded successfully!");
      initializeDashboard();
    };
    script.onerror = function () {
      console.error("Failed to load Chart.js dynamically");
      displayChartLoadingError();
    };
    document.head.appendChild(script);
    return false;
  }
  return true;
}

// Display error message when Chart.js fails to load
function displayChartLoadingError() {
  const chartContainers = document.querySelectorAll(".chart-wrapper");
  chartContainers.forEach((container) => {
    container.innerHTML = `
      <div class="no-data-message">
        <p>Failed to load chart library</p>
        <p>Please refresh the page or check your internet connection</p>
      </div>
    `;
  });
}

// Main dashboard initialization
function initializeDashboard() {
  // Export functions for external use
  window.DashboardJS = {
    initializeCharts,
    createChart,
    formatTooltipValue,
    getChartColorsByTheme,
    updateChartsForTheme,
  };

  // Apply smooth page transition and animations
  document.body.classList.add("loaded");

  // Add animation classes to chart containers
  setTimeout(() => {
    const chartContainers = document.querySelectorAll(".chart-container");
    chartContainers.forEach((container, index) => {
      setTimeout(() => {
        container.classList.add("chart-visible");
      }, index * 200);
    });

    // Add pulse effect to representative cards
    const repCards = document.querySelectorAll(".rep-card");
    repCards.forEach((card, index) => {
      setTimeout(
        () => {
          card.classList.add("rep-card-visible");

          // Animate metric items sequentially
          const metricItems = card.querySelectorAll(".rep-metric-item");
          metricItems.forEach((item, itemIndex) => {
            setTimeout(() => {
              item.classList.add("metric-item-visible");
            }, itemIndex * 100);
          });
        },
        index * 300 + 500,
      );
    });

    // Add shine effect to total metrics
    const totalMetrics = document.querySelectorAll(".total-metric");
    totalMetrics.forEach((metric, index) => {
      setTimeout(
        () => {
          metric.classList.add("total-metric-visible");
        },
        index * 150 + 300,
      );
    });
  }, 300);
}

// Initialize on DOM ready
if (typeof window !== "undefined") {
  document.addEventListener("DOMContentLoaded", function () {
    if (checkChartJsLoaded()) {
      initializeDashboard();
    }
  });
}
