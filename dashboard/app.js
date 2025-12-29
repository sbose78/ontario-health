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

// Render simple trends chart (ASCII-style)
function renderTrendsChart(data) {
    const container = document.getElementById('trends-chart');
    
    // Group by virus
    const byVirus = {};
    data.forEach(row => {
        if (!byVirus[row.virus_name]) {
            byVirus[row.virus_name] = [];
        }
        byVirus[row.virus_name].push(row);
    });
    
    container.innerHTML = '<table><thead><tr><th>Week</th>' +
        Object.keys(byVirus).map(v => `<th>${v}</th>`).join('') +
        '</tr></thead><tbody>';
    
    // Get all unique weeks
    const weeks = [...new Set(data.map(r => r.epi_week))].sort((a, b) => b - a).slice(0, 4);
    
    weeks.forEach(week => {
        let row = `<tr><td><strong>Week ${week}</strong></td>`;
        Object.keys(byVirus).forEach(virus => {
            const weekData = byVirus[virus].find(d => d.epi_week === week);
            if (weekData) {
                const change = weekData.week_over_week_pct || 0;
                const arrow = change > 0 ? '↑' : change < 0 ? '↓' : '→';
                const color = change > 0 ? 'var(--danger)' : change < 0 ? 'var(--success)' : 'var(--text-light)';
                row += `<td>${weekData.avg_viral_load.toFixed(1)} <span style="color: ${color}">${arrow} ${Math.abs(change)}%</span></td>`;
            } else {
                row += '<td>--</td>';
            }
        });
        row += '</tr>';
        container.querySelector('tbody').innerHTML += row;
    });
    
    container.innerHTML += '</tbody></table>';
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
        loadFreshness()
    ]);
    
    // Auto-refresh every 5 minutes
    setInterval(() => {
        loadRespiratoryData();
        loadEDData();
        loadFreshness();
    }, 5 * 60 * 1000);
}

// Start when page loads
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}

