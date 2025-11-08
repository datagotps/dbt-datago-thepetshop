// Power BI Dashboard Mockup - Charts Configuration

// Color Palette
const colors = {
    primary: '#0078D4',
    success: '#107C10',
    warning: '#FF8C00',
    danger: '#D13438',
    gray: '#605E5C',
    lightBlue: '#DEECF9',
    lightGreen: '#DFF6DD',
    lightOrange: '#FFF4CE',
    lightRed: '#FDE7E9'
};

// Chart Storage
const chartInstances = {};

// Destroy existing chart if it exists
function destroyChart(canvasId) {
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
        delete chartInstances[canvasId];
    }
}

// Create all charts
function createAllCharts() {
    // Page 1: Executive Summary
    createSalesTrendChart();
    createChannelMixChart();
    createTopProductsChart();
    createTopLocationsChart();
    createTopCategoriesChart();
    createGauges();

    // Page 2: Sales Performance
    createWaterfallChart();
    createMTDComparisonChart();
    createYTDComparisonChart();
    createDailySalesChart();

    // Page 3: Customer Analytics
    createCustomerSegmentChart();
    createCustomerAcquisitionChart();
    createCustomerBehaviorChart();

    // Page 4: Product Performance
    createProductScatterChart();
    createCategoryChart();
    createDivisionChart();
    createParetoChart();

    // Page 5: Location & Channel
    createLocationChart();
    createCityChart();
    createChannelTrendChart();
    createOnlineChannelChart();
    createOfflineStoreChart();

    // Page 6: Payment & Transactions
    createPaymentGatewayChart();
    createPaymentMethodTrendChart();
    createCODHeatmapChart();
    createTransactionTypeChart();
    createRefundTrendChart();

    // Page 7: Discount Analysis
    createDiscountTrendChart();
    createDiscountStatusChart();
    createDiscountEffectivenessChart();
    createOnlineOffersChart();
    createOfflineOffersChart();

    // Page 8: Time Trends
    createLongTermTrendChart();
    createSeasonalityChart();
    createDayOfWeekChart();
    createMovingAverageChart();
}

// ======== PAGE 1: EXECUTIVE SUMMARY ========

function createSalesTrendChart() {
    const ctx = document.getElementById('salesTrendChart');
    if (!ctx) return;

    destroyChart('salesTrendChart');

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const salesData = [1.8, 1.9, 2.2, 2.1, 2.3, 2.4, 2.2, 2.5, 2.3, 2.4, 2.1, 2.3];
    const movingAvg = [1.8, 1.85, 1.97, 2.05, 2.12, 2.17, 2.24, 2.26, 2.29, 2.32, 2.28, 2.27];

    chartInstances['salesTrendChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: months,
            datasets: [{
                label: 'Net Sales (AED M)',
                data: salesData,
                borderColor: colors.primary,
                backgroundColor: 'rgba(0, 120, 212, 0.1)',
                borderWidth: 3,
                fill: true,
                tension: 0.4
            }, {
                label: '30-Day Moving Avg',
                data: movingAvg,
                borderColor: colors.warning,
                borderWidth: 2,
                borderDash: [5, 5],
                fill: false,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { display: true, position: 'top' },
                tooltip: { mode: 'index', intersect: false }
            },
            scales: {
                y: { beginAtZero: true, title: { display: true, text: 'Sales (AED M)' } }
            }
        }
    });
}

function createChannelMixChart() {
    const ctx = document.getElementById('channelMixChart');
    if (!ctx) return;

    destroyChart('channelMixChart');

    chartInstances['channelMixChart'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Online', 'Shop', 'B2B', 'Affiliate', 'Service'],
            datasets: [{
                data: [55, 30, 8, 5, 2],
                backgroundColor: [colors.primary, colors.success, colors.warning, '#8B5CF6', colors.gray],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'right' },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.label + ': ' + context.parsed + '%';
                        }
                    }
                }
            }
        }
    });
}

function createTopProductsChart() {
    const ctx = document.getElementById('topProductsChart');
    if (!ctx) return;

    destroyChart('topProductsChart');

    chartInstances['topProductsChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Royal Canin Dog Food', 'Whiskas Cat Food', 'Pet Toy Bundle', 'Dog Leash Premium', 'Cat Litter Box'],
            datasets: [{
                label: 'Sales (AED K)',
                data: [450, 380, 320, 290, 250],
                backgroundColor: colors.primary,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } }
        }
    });
}

function createTopLocationsChart() {
    const ctx = document.getElementById('topLocationsChart');
    if (!ctx) return;

    destroyChart('topLocationsChart');

    chartInstances['topLocationsChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['DIP', 'FZN', 'UMSQ', 'REM', 'WSL'],
            datasets: [{
                label: 'Sales (AED M)',
                data: [5.2, 4.8, 3.9, 3.5, 2.8],
                backgroundColor: colors.success,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } }
        }
    });
}

function createTopCategoriesChart() {
    const ctx = document.getElementById('topCategoriesChart');
    if (!ctx) return;

    destroyChart('topCategoriesChart');

    chartInstances['topCategoriesChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Dog Food', 'Cat Food', 'Toys', 'Accessories', 'Grooming'],
            datasets: [{
                label: 'Sales (AED M)',
                data: [8.5, 7.2, 4.3, 3.8, 2.1],
                backgroundColor: colors.warning,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } }
        }
    });
}

function createGauges() {
    createGauge('salesGauge', 95, 'Sales Target');
    createGauge('profitGauge', 102, 'Profit Target');
    createGauge('loyaltyGauge', 68, 'Loyalty Rate');
}

function createGauge(canvasId, value, label) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    destroyChart(canvasId);

    const color = value >= 100 ? colors.success : value >= 75 ? colors.warning : colors.danger;

    chartInstances[canvasId] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            datasets: [{
                data: [value, 100 - value],
                backgroundColor: [color, '#E1E1E1'],
                borderWidth: 0,
                circumference: 180,
                rotation: 270
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { display: false },
                tooltip: { enabled: false }
            }
        }
    });
}

// ======== PAGE 2: SALES PERFORMANCE ========

function createWaterfallChart() {
    const ctx = document.getElementById('waterfallChart');
    if (!ctx) return;

    destroyChart('waterfallChart');

    chartInstances['waterfallChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Gross Sales', 'Discounts', 'Net Sales', 'Cost', 'Gross Profit'],
            datasets: [{
                label: 'Amount (AED M)',
                data: [28.2, -2.7, 25.5, -17.3, 8.2],
                backgroundColor: [colors.primary, colors.danger, colors.primary, colors.danger, colors.success],
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: false } }
        }
    });
}

function createMTDComparisonChart() {
    const ctx = document.getElementById('mtdComparisonChart');
    if (!ctx) return;

    destroyChart('mtdComparisonChart');

    chartInstances['mtdComparisonChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Sales', 'Orders', 'Customers'],
            datasets: [{
                label: 'MTD',
                data: [2.1, 3500, 980],
                backgroundColor: colors.primary
            }, {
                label: 'LMTD',
                data: [1.9, 3200, 920],
                backgroundColor: colors.gray
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } }
        }
    });
}

function createYTDComparisonChart() {
    const ctx = document.getElementById('ytdComparisonChart');
    if (!ctx) return;

    destroyChart('ytdComparisonChart');

    chartInstances['ytdComparisonChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Sales', 'Orders', 'Customers'],
            datasets: [{
                label: 'YTD',
                data: [25.5, 45234, 12456],
                backgroundColor: colors.primary
            }, {
                label: 'LYTD',
                data: [22.1, 38500, 11500],
                backgroundColor: colors.gray
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } }
        }
    });
}

function createDailySalesChart() {
    const ctx = document.getElementById('dailySalesChart');
    if (!ctx) return;

    destroyChart('dailySalesChart');

    const days = Array.from({length: 90}, (_, i) => `Day ${i+1}`);
    const salesData = days.map(() => 70 + Math.random() * 60);

    chartInstances['dailySalesChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: days,
            datasets: [{
                label: 'Daily Sales (AED K)',
                data: salesData,
                borderColor: colors.primary,
                backgroundColor: 'rgba(0, 120, 212, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.3
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { display: true }
            },
            scales: {
                x: { display: false },
                y: { beginAtZero: true }
            }
        }
    });
}

// ======== PAGE 3: CUSTOMER ANALYTICS ========

function createCustomerSegmentChart() {
    const ctx = document.getElementById('customerSegmentChart');
    if (!ctx) return;

    destroyChart('customerSegmentChart');

    chartInstances['customerSegmentChart'] = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: ['Loyalty Members', 'Verified Non-Loyalty', 'Unverified'],
            datasets: [{
                data: [68, 22, 10],
                backgroundColor: [colors.success, colors.primary, colors.gray]
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'bottom' } }
        }
    });
}

function createCustomerAcquisitionChart() {
    const ctx = document.getElementById('customerAcquisitionChart');
    if (!ctx) return;

    destroyChart('customerAcquisitionChart');

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    chartInstances['customerAcquisitionChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: months,
            datasets: [{
                label: 'New Customers',
                data: [280, 310, 295, 320, 305, 330, 315, 340, 325, 350, 335, 360],
                borderColor: colors.success,
                backgroundColor: 'rgba(16, 124, 16, 0.1)',
                fill: true,
                tension: 0.4
            }, {
                label: 'Repeat Customers',
                data: [720, 750, 780, 810, 830, 850, 870, 900, 920, 940, 960, 980],
                borderColor: colors.primary,
                backgroundColor: 'rgba(0, 120, 212, 0.1)',
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: { y: { stacked: false } }
        }
    });
}

function createCustomerBehaviorChart() {
    const ctx = document.getElementById('customerBehaviorChart');
    if (!ctx) return;

    destroyChart('customerBehaviorChart');

    chartInstances['customerBehaviorChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Loyalty Members', 'Verified', 'Online Shoppers', 'In-Store Shoppers', 'B2B Customers'],
            datasets: [{
                label: 'Avg Orders per Customer',
                data: [4.5, 3.2, 3.8, 2.9, 6.2],
                backgroundColor: colors.primary,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

// ======== PAGE 4: PRODUCT PERFORMANCE ========

function createProductScatterChart() {
    const ctx = document.getElementById('productScatterChart');
    if (!ctx) return;

    destroyChart('productScatterChart');

    const scatterData = Array.from({length: 50}, () => ({
        x: Math.random() * 10000,
        y: 20 + Math.random() * 30
    }));

    chartInstances['productScatterChart'] = new Chart(ctx, {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Products',
                data: scatterData,
                backgroundColor: colors.primary,
                pointRadius: 6,
                pointHoverRadius: 8
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `Qty: ${context.parsed.x.toFixed(0)}, Margin: ${context.parsed.y.toFixed(1)}%`;
                        }
                    }
                }
            },
            scales: {
                x: { title: { display: true, text: 'Quantity Sold' } },
                y: { title: { display: true, text: 'Profit Margin %' } }
            }
        }
    });
}

function createCategoryChart() {
    const ctx = document.getElementById('categoryChart');
    if (!ctx) return;

    destroyChart('categoryChart');

    chartInstances['categoryChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Dog Food', 'Cat Food', 'Toys', 'Accessories', 'Grooming', 'Health', 'Treats', 'Beds', 'Bowls', 'Collars'],
            datasets: [{
                label: 'Sales (AED M)',
                data: [8.5, 7.2, 4.3, 3.8, 2.1, 1.8, 1.5, 1.2, 0.9, 0.7],
                backgroundColor: colors.primary,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } }
        }
    });
}

function createDivisionChart() {
    const ctx = document.getElementById('divisionChart');
    if (!ctx) return;

    destroyChart('divisionChart');

    chartInstances['divisionChart'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Dog Products', 'Cat Products', 'Bird Products', 'Fish Products', 'Small Pets'],
            datasets: [{
                data: [45, 35, 8, 7, 5],
                backgroundColor: [colors.primary, colors.success, colors.warning, '#8B5CF6', colors.gray]
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'right' } }
        }
    });
}

function createParetoChart() {
    const ctx = document.getElementById('paretoChart');
    if (!ctx) return;

    destroyChart('paretoChart');

    const products = Array.from({length: 20}, (_, i) => `P${i+1}`);
    const sales = [5000, 4200, 3800, 3200, 2800, 2400, 2000, 1700, 1400, 1200, 1000, 850, 700, 600, 500, 400, 350, 300, 250, 200];
    const cumulative = sales.map((val, i) => {
        const sum = sales.slice(0, i + 1).reduce((a, b) => a + b, 0);
        const total = sales.reduce((a, b) => a + b, 0);
        return (sum / total) * 100;
    });

    chartInstances['paretoChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: products,
            datasets: [{
                type: 'bar',
                label: 'Sales',
                data: sales,
                backgroundColor: colors.primary,
                yAxisID: 'y',
                order: 2
            }, {
                type: 'line',
                label: 'Cumulative %',
                data: cumulative,
                borderColor: colors.danger,
                backgroundColor: 'transparent',
                yAxisID: 'y1',
                order: 1,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: {
                y: { type: 'linear', position: 'left', beginAtZero: true },
                y1: { type: 'linear', position: 'right', min: 0, max: 100, grid: { drawOnChartArea: false } }
            }
        }
    });
}

// ======== PAGE 5: LOCATION & CHANNEL ========

function createLocationChart() {
    const ctx = document.getElementById('locationChart');
    if (!ctx) return;

    destroyChart('locationChart');

    chartInstances['locationChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['DIP', 'FZN', 'UMSQ', 'REM', 'WSL', 'DXB', 'SHJ', 'AUH', 'RAK', 'FUJ'],
            datasets: [{
                label: 'Sales (AED M)',
                data: [5.2, 4.8, 3.9, 3.5, 2.8, 2.1, 1.8, 1.5, 1.2, 0.9],
                backgroundColor: colors.primary,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } }
        }
    });
}

function createCityChart() {
    const ctx = document.getElementById('cityChart');
    if (!ctx) return;

    destroyChart('cityChart');

    chartInstances['cityChart'] = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: ['Dubai', 'Abu Dhabi', 'Sharjah', 'Ras Al Khaimah', 'Others'],
            datasets: [{
                data: [55, 25, 12, 5, 3],
                backgroundColor: [colors.primary, colors.success, colors.warning, '#8B5CF6', colors.gray]
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'right' } }
        }
    });
}

function createChannelTrendChart() {
    const ctx = document.getElementById('channelTrendChart');
    if (!ctx) return;

    destroyChart('channelTrendChart');

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    chartInstances['channelTrendChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: [{
                label: 'Online',
                data: [1.0, 1.1, 1.2, 1.15, 1.25, 1.3, 1.2, 1.35, 1.25, 1.3, 1.15, 1.25],
                backgroundColor: colors.primary
            }, {
                label: 'Shop',
                data: [0.6, 0.65, 0.7, 0.68, 0.72, 0.75, 0.7, 0.78, 0.73, 0.75, 0.68, 0.72],
                backgroundColor: colors.success
            }, {
                label: 'B2B',
                data: [0.15, 0.12, 0.18, 0.16, 0.19, 0.2, 0.18, 0.21, 0.19, 0.2, 0.16, 0.18],
                backgroundColor: colors.warning
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: { x: {}, y: { stacked: true } }
        }
    });
}

function createOnlineChannelChart() {
    const ctx = document.getElementById('onlineChannelChart');
    if (!ctx) return;

    destroyChart('onlineChannelChart');

    chartInstances['onlineChannelChart'] = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: ['Website', 'Android', 'iOS', 'CRM', 'Unmapped'],
            datasets: [{
                data: [40, 28, 22, 7, 3],
                backgroundColor: [colors.primary, colors.success, colors.warning, '#8B5CF6', colors.gray]
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'bottom' } }
        }
    });
}

function createOfflineStoreChart() {
    const ctx = document.getElementById('offlineStoreChart');
    if (!ctx) return;

    destroyChart('offlineStoreChart');

    chartInstances['offlineStoreChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['DIP Store', 'FZN Store', 'UMSQ Store', 'REM Store', 'WSL Store'],
            datasets: [{
                label: 'Sales (AED M)',
                data: [2.5, 2.1, 1.8, 1.5, 1.2],
                backgroundColor: colors.success,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } }
        }
    });
}

// ======== PAGE 6: PAYMENT & TRANSACTIONS ========

function createPaymentGatewayChart() {
    const ctx = document.getElementById('paymentGatewayChart');
    if (!ctx) return;

    destroyChart('paymentGatewayChart');

    chartInstances['paymentGatewayChart'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Credit Card', 'Cash', 'COD', 'Tabby', 'PayPal', 'Others'],
            datasets: [{
                data: [35, 25, 20, 12, 5, 3],
                backgroundColor: [colors.primary, colors.success, colors.warning, '#8B5CF6', '#EC4899', colors.gray]
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'right' } }
        }
    });
}

function createPaymentMethodTrendChart() {
    const ctx = document.getElementById('paymentMethodTrendChart');
    if (!ctx) return;

    destroyChart('paymentMethodTrendChart');

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    chartInstances['paymentMethodTrendChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: months,
            datasets: [{
                label: 'Prepaid',
                data: [65, 67, 68, 70, 71, 72, 73, 74, 75, 76, 77, 78],
                borderColor: colors.primary,
                tension: 0.4
            }, {
                label: 'COD',
                data: [35, 33, 32, 30, 29, 28, 27, 26, 25, 24, 23, 22],
                borderColor: colors.warning,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: { y: { min: 0, max: 100 } }
        }
    });
}

function createCODHeatmapChart() {
    const ctx = document.getElementById('codHeatmapChart');
    if (!ctx) return;

    destroyChart('codHeatmapChart');

    chartInstances['codHeatmapChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Online-Dubai', 'Online-AUH', 'Shop-Dubai', 'Shop-AUH', 'B2B-Dubai'],
            datasets: [{
                label: 'COD Rate %',
                data: [25, 30, 15, 20, 5],
                backgroundColor: [
                    'rgba(255, 140, 0, 0.4)',
                    'rgba(255, 140, 0, 0.6)',
                    'rgba(255, 140, 0, 0.3)',
                    'rgba(255, 140, 0, 0.5)',
                    'rgba(255, 140, 0, 0.2)'
                ],
                borderColor: colors.warning,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true, max: 100 } }
        }
    });
}

function createTransactionTypeChart() {
    const ctx = document.getElementById('transactionTypeChart');
    if (!ctx) return;

    destroyChart('transactionTypeChart');

    chartInstances['transactionTypeChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Sale', 'Refund', 'Exchange', 'Other'],
            datasets: [{
                label: 'Count',
                data: [42000, 2100, 980, 154],
                backgroundColor: [colors.success, colors.danger, colors.warning, colors.gray],
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } }
        }
    });
}

function createRefundTrendChart() {
    const ctx = document.getElementById('refundTrendChart');
    if (!ctx) return;

    destroyChart('refundTrendChart');

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    chartInstances['refundTrendChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: months,
            datasets: [{
                label: 'Refund Rate %',
                data: [5.2, 4.9, 4.7, 4.5, 4.3, 4.4, 4.6, 4.8, 4.7, 4.9, 4.8, 4.8],
                borderColor: colors.danger,
                backgroundColor: 'rgba(209, 52, 56, 0.1)',
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { min: 0, max: 10 } }
        }
    });
}

// ======== PAGE 7: DISCOUNT ANALYSIS ========

function createDiscountTrendChart() {
    const ctx = document.getElementById('discountTrendChart');
    if (!ctx) return;

    destroyChart('discountTrendChart');

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    chartInstances['discountTrendChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: months,
            datasets: [{
                label: 'Online Discount %',
                data: [10.5, 9.8, 9.2, 8.8, 8.5, 8.9, 9.3, 9.7, 9.5, 10.1, 10.5, 11.2],
                borderColor: colors.primary,
                tension: 0.4
            }, {
                label: 'Offline Discount %',
                data: [8.2, 7.9, 7.5, 7.2, 6.9, 7.1, 7.4, 7.8, 7.6, 8.0, 8.3, 8.9],
                borderColor: colors.success,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: { y: { min: 0, max: 15 } }
        }
    });
}

function createDiscountStatusChart() {
    const ctx = document.getElementById('discountStatusChart');
    if (!ctx) return;

    destroyChart('discountStatusChart');

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    chartInstances['discountStatusChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: [{
                label: 'Discounted',
                data: [0.8, 0.85, 0.9, 0.92, 0.95, 1.0, 0.97, 1.05, 1.02, 1.08, 1.1, 1.15],
                backgroundColor: colors.warning
            }, {
                label: 'No Discount',
                data: [1.0, 1.05, 1.1, 1.08, 1.12, 1.15, 1.13, 1.2, 1.18, 1.22, 1.0, 1.05],
                backgroundColor: colors.primary
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: { x: {}, y: { stacked: true } }
        }
    });
}

function createDiscountEffectivenessChart() {
    const ctx = document.getElementById('discountEffectivenessChart');
    if (!ctx) return;

    destroyChart('discountEffectivenessChart');

    const scatterData = Array.from({length: 30}, () => ({
        x: 5 + Math.random() * 20,
        y: 50000 + Math.random() * 150000
    }));

    chartInstances['discountEffectivenessChart'] = new Chart(ctx, {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Offers',
                data: scatterData,
                backgroundColor: colors.warning,
                pointRadius: 6
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
                x: { title: { display: true, text: 'Discount %' } },
                y: { title: { display: true, text: 'Sales (AED)' } }
            }
        }
    });
}

function createOnlineOffersChart() {
    const ctx = document.getElementById('onlineOffersChart');
    if (!ctx) return;

    destroyChart('onlineOffersChart');

    chartInstances['onlineOffersChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['SAVE20', 'FIRST10', 'LOYALTY15', 'FLASH25', 'BUNDLE30', 'VIP40', 'WEEKEND12', 'SUMMER18', 'BACK5', 'NEW50'],
            datasets: [{
                label: 'Redemptions',
                data: [1250, 980, 850, 720, 650, 580, 520, 480, 420, 380],
                backgroundColor: colors.primary,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } }
        }
    });
}

function createOfflineOffersChart() {
    const ctx = document.getElementById('offlineOffersChart');
    if (!ctx) return;

    destroyChart('offlineOffersChart');

    chartInstances['offlineOffersChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['STORE10', 'INSTORE15', 'WEEKEND20', 'FLASH30', 'VIP25', 'MEMBER12', 'PROMO18', 'SPECIAL22', 'DEAL8', 'BONUS5'],
            datasets: [{
                label: 'Redemptions',
                data: [890, 720, 650, 580, 520, 480, 420, 380, 340, 280],
                backgroundColor: colors.success,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } }
        }
    });
}

// ======== PAGE 8: TIME TRENDS ========

function createLongTermTrendChart() {
    const ctx = document.getElementById('longTermTrendChart');
    if (!ctx) return;

    destroyChart('longTermTrendChart');

    const months = [];
    for (let year = 2023; year <= 2024; year++) {
        for (let month = 1; month <= 12; month++) {
            months.push(`${year}-${String(month).padStart(2, '0')}`);
        }
    }

    const currentYear = months.map((_, i) => 1.5 + (i * 0.03) + (Math.random() * 0.2 - 0.1));
    const lastYear = months.map((_, i) => 1.3 + (i * 0.025) + (Math.random() * 0.15 - 0.075));

    chartInstances['longTermTrendChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: months,
            datasets: [{
                label: '2024',
                data: currentYear.slice(12),
                borderColor: colors.primary,
                backgroundColor: 'rgba(0, 120, 212, 0.1)',
                fill: true,
                tension: 0.4
            }, {
                label: '2023',
                data: lastYear.slice(0, 12),
                borderColor: colors.gray,
                backgroundColor: 'rgba(96, 94, 92, 0.1)',
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

function createSeasonalityChart() {
    const ctx = document.getElementById('seasonalityChart');
    if (!ctx) return;

    destroyChart('seasonalityChart');

    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    chartInstances['seasonalityChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: [{
                label: 'Avg Sales by Month',
                data: [1.8, 1.7, 2.0, 1.9, 2.1, 2.3, 2.0, 2.4, 2.2, 2.3, 2.1, 2.5],
                backgroundColor: colors.primary,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

function createDayOfWeekChart() {
    const ctx = document.getElementById('dayOfWeekChart');
    if (!ctx) return;

    destroyChart('dayOfWeekChart');

    chartInstances['dayOfWeekChart'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
            datasets: [{
                label: 'Avg Sales by Day',
                data: [68, 72, 75, 78, 95, 120, 110],
                backgroundColor: colors.success,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

function createMovingAverageChart() {
    const ctx = document.getElementById('movingAverageChart');
    if (!ctx) return;

    destroyChart('movingAverageChart');

    const days = Array.from({length: 90}, (_, i) => i + 1);
    const dailySales = days.map(() => 70 + Math.random() * 60);
    const ma7 = dailySales.map((_, i) => {
        if (i < 6) return null;
        return dailySales.slice(i - 6, i + 1).reduce((a, b) => a + b) / 7;
    });
    const ma30 = dailySales.map((_, i) => {
        if (i < 29) return null;
        return dailySales.slice(i - 29, i + 1).reduce((a, b) => a + b) / 30;
    });

    chartInstances['movingAverageChart'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: days,
            datasets: [{
                label: 'Daily Sales',
                data: dailySales,
                borderColor: colors.gray,
                backgroundColor: 'transparent',
                borderWidth: 1,
                pointRadius: 0
            }, {
                label: '7-Day MA',
                data: ma7,
                borderColor: colors.primary,
                backgroundColor: 'transparent',
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.4
            }, {
                label: '30-Day MA',
                data: ma30,
                borderColor: colors.danger,
                backgroundColor: 'transparent',
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: {
                x: { display: false },
                y: { beginAtZero: true }
            }
        }
    });
}

console.log('📊 Charts module loaded successfully');
