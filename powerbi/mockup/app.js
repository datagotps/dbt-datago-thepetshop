// Power BI Dashboard Mockup - Application Logic

// Initialize dashboard on page load
document.addEventListener('DOMContentLoaded', function() {
    updateLastUpdatedTime();
    showPage('executive'); // Show executive summary by default
    initializeCharts(); // Initialize all charts
});

// Page Navigation
function showPage(pageName) {
    // Hide all pages
    const pages = document.querySelectorAll('.page-content');
    pages.forEach(page => page.classList.remove('active'));

    // Remove active class from all tabs
    const tabs = document.querySelectorAll('.tab-btn');
    tabs.forEach(tab => tab.classList.remove('active'));

    // Show selected page
    const selectedPage = document.getElementById(pageName + 'Page');
    if (selectedPage) {
        selectedPage.classList.add('active');
    }

    // Activate selected tab
    const activeTabs = Array.from(tabs).filter(tab =>
        tab.textContent.toLowerCase().includes(getPageKeyword(pageName))
    );
    if (activeTabs.length > 0) {
        activeTabs[0].classList.add('active');
    }

    // Scroll to top
    window.scrollTo(0, 0);
}

function getPageKeyword(pageName) {
    const keywords = {
        'executive': 'executive',
        'sales': 'sales',
        'customers': 'customer',
        'products': 'product',
        'location': 'location',
        'payment': 'payment',
        'discount': 'discount',
        'trends': 'trends'
    };
    return keywords[pageName] || '';
}

// Filters Panel Toggle
function toggleFilters() {
    const panel = document.getElementById('filtersPanel');
    panel.classList.toggle('open');
}

// Apply Filters
function applyFilters() {
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    const company = document.getElementById('companyFilter').value;
    const channel = document.getElementById('channelFilter').value;
    const city = document.getElementById('cityFilter').value;
    const transaction = document.getElementById('transactionFilter').value;

    // Show loading
    showLoading();

    // Simulate API call delay
    setTimeout(() => {
        console.log('Filters applied:', {
            startDate, endDate, company, channel, city, transaction
        });

        // Update KPIs based on filters (simulation)
        updateKPIs(company, channel);

        // Re-initialize charts with filtered data
        initializeCharts();

        hideLoading();

        // Close filters panel
        toggleFilters();

        // Show notification
        showNotification('Filters applied successfully!');
    }, 1000);
}

// Clear Filters
function clearFilters() {
    document.getElementById('startDate').value = '2024-01-01';
    document.getElementById('endDate').value = '2024-12-31';
    document.getElementById('companyFilter').value = 'all';
    document.getElementById('channelFilter').value = 'all';
    document.getElementById('cityFilter').value = 'all';
    document.getElementById('transactionFilter').value = 'all';

    applyFilters();
}

// Update KPIs based on filters
function updateKPIs(company, channel) {
    // Simulate dynamic KPI updates
    let salesMultiplier = 1;

    if (company === 'petshop') salesMultiplier = 0.75;
    if (company === 'pethaus') salesMultiplier = 0.25;

    if (channel === 'online') salesMultiplier *= 0.55;
    if (channel === 'shop') salesMultiplier *= 0.35;

    const baseSales = 25.5;
    const baseProfit = 8.2;
    const baseOrders = 45234;
    const baseCustomers = 12456;

    document.getElementById('totalSales').textContent =
        `AED ${(baseSales * salesMultiplier).toFixed(1)}M`;
    document.getElementById('grossProfit').textContent =
        `AED ${(baseProfit * salesMultiplier).toFixed(1)}M`;
    document.getElementById('totalOrders').textContent =
        Math.round(baseOrders * salesMultiplier).toLocaleString();
    document.getElementById('totalCustomers').textContent =
        Math.round(baseCustomers * salesMultiplier).toLocaleString();
}

// Refresh Data
function refreshData() {
    showLoading();

    setTimeout(() => {
        initializeCharts();
        updateLastUpdatedTime();
        hideLoading();
        showNotification('Data refreshed successfully!');
    }, 1500);
}

// Update Last Updated Time
function updateLastUpdatedTime() {
    const now = new Date();
    const formatted = now.toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
    const element = document.getElementById('lastUpdate');
    if (element) {
        element.textContent = formatted;
    }
}

// Loading Overlay
function showLoading() {
    const overlay = document.getElementById('loadingOverlay');
    if (overlay) {
        overlay.classList.add('show');
    }
}

function hideLoading() {
    const overlay = document.getElementById('loadingOverlay');
    if (overlay) {
        overlay.classList.remove('show');
    }
}

// Show Notification
function showNotification(message) {
    // Create notification element
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 80px;
        right: 20px;
        background: #107C10;
        color: white;
        padding: 15px 25px;
        border-radius: 4px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        z-index: 10000;
        animation: slideIn 0.3s ease-out;
    `;
    notification.textContent = message;

    document.body.appendChild(notification);

    // Remove after 3 seconds
    setTimeout(() => {
        notification.style.animation = 'fadeOut 0.3s ease-out';
        setTimeout(() => {
            document.body.removeChild(notification);
        }, 300);
    }, 3000);
}

// Initialize all charts (will be populated in charts.js)
function initializeCharts() {
    if (typeof createAllCharts === 'function') {
        createAllCharts();
    }
}

// Export functionality (simulation)
function exportData(format) {
    showLoading();
    setTimeout(() => {
        hideLoading();
        showNotification(`Exporting data to ${format}...`);
    }, 1000);
}

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
    // F5 to refresh
    if (e.key === 'F5') {
        e.preventDefault();
        refreshData();
    }

    // Esc to close filters
    if (e.key === 'Escape') {
        const panel = document.getElementById('filtersPanel');
        if (panel && panel.classList.contains('open')) {
            toggleFilters();
        }
    }

    // Ctrl/Cmd + F to open filters
    if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
        e.preventDefault();
        toggleFilters();
    }

    // Number keys 1-8 for page navigation
    if (e.key >= '1' && e.key <= '8' && !e.ctrlKey && !e.metaKey) {
        const pages = ['executive', 'sales', 'customers', 'products', 'location', 'payment', 'discount', 'trends'];
        const pageIndex = parseInt(e.key) - 1;
        if (pages[pageIndex]) {
            showPage(pages[pageIndex]);
        }
    }
});

// Click outside filters panel to close
document.addEventListener('click', function(e) {
    const panel = document.getElementById('filtersPanel');
    const filterBtn = e.target.closest('.nav-btn');

    if (panel && panel.classList.contains('open') &&
        !panel.contains(e.target) &&
        (!filterBtn || !filterBtn.textContent.includes('Filters'))) {
        toggleFilters();
    }
});

// Simulate cross-filtering (click on chart to filter)
function crossFilter(filterType, filterValue) {
    showNotification(`Filtering by ${filterType}: ${filterValue}`);

    // Update relevant filters
    if (filterType === 'channel') {
        document.getElementById('channelFilter').value = filterValue;
    } else if (filterType === 'city') {
        document.getElementById('cityFilter').value = filterValue;
    }

    applyFilters();
}

// Print current page
function printPage() {
    window.print();
}

// Add CSS for fadeOut animation
const style = document.createElement('style');
style.textContent = `
    @keyframes fadeOut {
        from {
            opacity: 1;
            transform: translateY(0);
        }
        to {
            opacity: 0;
            transform: translateY(-10px);
        }
    }
`;
document.head.appendChild(style);

console.log('🎨 Power BI Dashboard Mockup Loaded');
console.log('⌨️  Keyboard Shortcuts:');
console.log('   1-8: Navigate between pages');
console.log('   F5: Refresh data');
console.log('   Esc: Close filters panel');
console.log('   Ctrl+F: Open filters panel');
