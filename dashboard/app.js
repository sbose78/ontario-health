// Ontario Health Dashboard - Frontend Logic

const API_BASE = '/api';

// Fetch and render current week respiratory data
async function loadRespiratoryData() {
    try {
        const response = await fetch(`${API_BASE}/current-week`);
        const data = await response.json();
        
        const grid = document.getElementById('respiratory-grid');
        grid.innerHTML = '';
        
        data.forEach(virus => {
            const severity = getViralLoadSeverity(virus.avg_viral_load);
            
            const metric = document.createElement('div');
            metric.className = 'metric';
            metric.style.borderLeftColor = severity.color;
            metric.innerHTML = `
                <div class="metric-label">${virus.virus_name}</div>
                <div class="metric-value">${virus.avg_viral_load.toFixed(2)}</div>
                <div class="metric-status status-${severity.class}">
                    ${severity.label} | ${virus.sites_reporting} sites
                </div>
            `;
            grid.appendChild(metric);
        });
        
    } catch (error) {
        console.error('Error loading respiratory data:', error);
        document.getElementById('respiratory-grid').innerHTML = 
            '<div class="error">Unable to load respiratory data</div>';
    }
}

// Fetch and render ED wait times
async function loadEDData() {
    try {
        const response = await fetch(`${API_BASE}/ed-status`);
        const data = await response.json();
        
        const grid = document.getElementById('ed-grid');
        grid.innerHTML = '';
        
        data.forEach(hospital => {
            const card = document.createElement('div');
            card.className = 'hospital-card';
            card.innerHTML = `
                <div class="hospital-name">${hospital.hospital_name}</div>
                <div class="wait-time">${hospital.wait_hours}h ${hospital.wait_minutes}m</div>
                <div class="metric-status status-${hospital.wait_severity.toLowerCase()}">
                    ${hospital.wait_severity}
                </div>
                <div class="source-note">Last: ${new Date(hospital.scraped_at).toLocaleString()}</div>
            `;
            grid.appendChild(card);
        });
        
    } catch (error) {
        console.error('Error loading ED data:', error);
        document.getElementById('ed-grid').innerHTML = 
            '<div class="error">Unable to load ED wait times</div>';
    }
}

// Fetch and render viral trends
async function loadViralTrends() {
    try {
        const response = await fetch(`${API_BASE}/viral-trends`);
        const data = await response.json();
        
        renderTrendsChart(data);
        
    } catch (error) {
        console.error('Error loading trends:', error);
        document.getElementById('trends-chart').innerHTML = 
            '<div class="error">Unable to load trends</div>';
    }
}

// Render viral trends chart using Chart.js
function renderTrendsChart(data) {
    const canvas = document.getElementById('viral-trends-chart');
    
    // Group by virus
    const byVirus = {};
    data.forEach(row => {
        if (!byVirus[row.virus_name]) {
            byVirus[row.virus_name] = [];
        }
        byVirus[row.virus_name].push(row);
    });
    
    // Get unique weeks, sorted
    const weeks = [...new Set(data.map(r => r.epi_week))].sort((a, b) => a - b);
    
    // Create datasets for each virus
    const datasets = Object.keys(byVirus).map(virus => {
        const color = getVirusColor(virus);
        const virusData = byVirus[virus].sort((a, b) => a.epi_week - b.epi_week);
        
        return {
            label: virus,
            data: weeks.map(week => {
                const weekData = virusData.find(d => d.epi_week === week);
                return weekData ? weekData.avg_viral_load : null;
            }),
            borderColor: color,
            backgroundColor: color + '20',
            borderWidth: 3,
            tension: 0.3,
            fill: false
        };
    });
    
    new Chart(canvas, {
        type: 'line',
        data: {
            labels: weeks.map(w => `Week ${w}`),
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top'
                },
                title: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Viral Load (copies/mL)'
                    }
                }
            }
        }
    });
}

// Get color for each virus
function getVirusColor(virus) {
    const colors = {
        'Influenza A': '#d63031',
        'COVID-19': '#0984e3',
        'RSV': '#fdcb6e',
        'Influenza B': '#6c5ce7'
    };
    return colors[virus] || '#636e72';
}

// Load and render ED wait times history
async function loadEDHistory() {
    try {
        const response = await fetch(`${API_BASE}/ed-history`);
        const data = await response.json();
        
        renderEDHistoryChart(data);
        
    } catch (error) {
        console.error('Error loading ED history:', error);
    }
}

// Render ED history chart
function renderEDHistoryChart(data) {
    const canvas = document.getElementById('ed-history-chart');
    
    // Group by hospital
    const byHospital = {};
    data.forEach(row => {
        if (!byHospital[row.hospital_name]) {
            byHospital[row.hospital_name] = [];
        }
        byHospital[row.hospital_name].push(row);
    });
    
    // Create dataset for each hospital
    const datasets = Object.keys(byHospital).map((hospital, idx) => {
        const colors = ['#0984e3', '#d63031', '#00b894'];
        const hospitalData = byHospital[hospital].sort((a, b) => 
            new Date(a.scraped_at).getTime() - new Date(b.scraped_at).getTime()
        );
        
        return {
            label: hospital.replace(' Hospital', '').replace('Trafalgar Memorial', 'TM'),
            data: hospitalData.map(d => ({
                x: new Date(d.scraped_at),
                y: d.wait_total_minutes
            })),
            borderColor: colors[idx % colors.length],
            backgroundColor: colors[idx % colors.length] + '40',
            borderWidth: 2,
            tension: 0.3,
            fill: false
        };
    });
    
    new Chart(canvas, {
        type: 'line',
        data: { datasets: datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top'
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'hour',
                        displayFormats: {
                            hour: 'MMM d, ha'
                        }
                    },
                    title: {
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Wait Time (minutes)'
                    },
                    ticks: {
                        callback: function(value) {
                            const hours = Math.floor(value / 60);
                            const mins = value % 60;
                            return hours > 0 ? `${hours}h ${mins}m` : `${mins}m`;
                        }
                    }
                }
            }
        }
    });
}

// Determine viral load severity
function getViralLoadSeverity(load) {
    if (load >= 50) return { label: 'Very High', class: 'very-high', color: '#d63031' };
    if (load >= 30) return { label: 'High', class: 'high', color: '#ff7675' };
    if (load >= 15) return { label: 'Moderate', class: 'moderate', color: '#fdcb6e' };
    return { label: 'Low', class: 'low', color: '#00b894' };
}

// Load data freshness
async function loadFreshness() {
    try {
        const response = await fetch(`${API_BASE}/data-freshness`);
        const data = await response.json();
        
        const container = document.getElementById('freshness-table');
        container.innerHTML = `
            <table>
                <thead>
                    <tr>
                        <th>Dataset</th>
                        <th>Latest Data</th>
                        <th>Records</th>
                    </tr>
                </thead>
                <tbody>
                    ${data.map(row => `
                        <tr>
                            <td>${row.dataset}</td>
                            <td>${row.latest_data_date || 'N/A'}</td>
                            <td>${row.total_records.toLocaleString()}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
        
        // Update last-update timestamp
        document.getElementById('last-update').textContent = 
            new Date().toLocaleString('en-US', { 
                dateStyle: 'medium', 
                timeStyle: 'short' 
            });
        
    } catch (error) {
        console.error('Error loading freshness:', error);
    }
}

// Initialize dashboard
async function init() {
    await Promise.all([
        loadRespiratoryData(),
        loadEDData(),
        loadViralTrends(),
        loadEDHistory(),
        loadFreshness()
    ]);
    
    // Auto-refresh every 5 minutes
    setInterval(() => {
        loadRespiratoryData();
        loadEDData();
        loadFreshness();
        loadEDHistory();
    }, 5 * 60 * 1000);
}

// Start when page loads
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}


