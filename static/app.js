// ══════════════════════════════════════════
//  Calendar Spread Trading — Frontend
// ══════════════════════════════════════════

const API = '';
let state = { connected: false, account: null, accountValue: 1023443 };
let refreshTimer = null;
let scannerPollTimer = null;
let _signalsList = [];  // cached for add-to-portfolio modal
let _pendingSignal = null;  // signal being added
let _lastPortfolioData = null;  // cached for close modal
let _pendingClose = null;  // position being closed

// ── WebSocket Monitor State ──
let _monitorWs = null;
let _monitorWsReconnectTimer = null;
let _monitorWsBackoff = 1000;  // exponential backoff start
let _monitorWsConnected = false;
let _monitorLastSnapshot = null;  // last snapshot for DOM diffing
let _monitorActiveTab = false;  // true when monitor tab is active

// ── Formatters ──
const fmt = {
    usd: (v) => v == null ? '-' : '$' + Number(v).toLocaleString('en-US', {minimumFractionDigits: 0, maximumFractionDigits: 0}),
    usd2: (v) => v == null ? '-' : '$' + Number(v).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}),
    pct: (v) => v == null ? '-' : Number(v).toFixed(1) + '%',
    pct2: (v) => v == null ? '-' : Number(v).toFixed(2) + '%',
    num: (v) => v == null ? '-' : Number(v).toLocaleString('en-US'),
    pnl: (v) => {
        if (v == null) return '-';
        const s = v >= 0 ? '+' : '';
        return s + '$' + Number(v).toLocaleString('en-US', {minimumFractionDigits: 2});
    },
};

// ── API Calls ──
async function api(path, opts = {}) {
    try {
        const res = await fetch(API + path, {
            headers: { 'Content-Type': 'application/json' },
            ...opts,
        });
        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: res.statusText }));
            throw new Error(err.detail || res.statusText);
        }
        return await res.json();
    } catch (e) {
        console.error(`API ${path}:`, e);
        throw e;
    }
}

// ── Tab Navigation ──
function switchTab(tabId) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
    document.getElementById('tab-' + tabId).classList.add('active');
    document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');

    // WebSocket lifecycle for monitor tab
    if (tabId === 'monitor') {
        _monitorActiveTab = true;
        connectMonitorWs();
    } else {
        _monitorActiveTab = false;
        disconnectMonitorWs();
    }

    loadTab(tabId);
}

let _homeRefreshTimer = null;

function loadTab(tabId) {
    // Clear home auto-refresh when leaving home tab
    if (tabId !== 'home' && _homeRefreshTimer) {
        clearInterval(_homeRefreshTimer);
        _homeRefreshTimer = null;
    }

    const loaders = {
        home: loadHome,
        signals: loadSignals,
        portfolio: loadPortfolio,
        orders: loadOrders,
        'track-record': loadTrackRecord,
        straddle: loadStraddle,
        monitor: loadMonitor,
    };
    if (loaders[tabId]) loaders[tabId]();
}

// ── Status ──
async function refreshStatus() {
    try {
        const data = await api('/api/status');
        state.connected = data.connected;
        state.account = data.account;
        if (data.account_value > 0) state.accountValue = data.account_value;

        // Update header
        const btn = document.getElementById('conn-btn');
        if (data.connected) {
            btn.classList.add('connected');
            btn.innerHTML = `<span class="status-dot"></span> IBKR: ${data.account}`;
        } else {
            btn.classList.remove('connected');
            btn.innerHTML = `<span class="status-dot"></span> IBKR OFFLINE`;
        }

        // Update sidebar stats
        document.getElementById('stat-account').textContent = fmt.usd(state.accountValue);
        document.getElementById('stat-winrate').textContent = data.win_rate != null ? (data.win_rate * 100).toFixed(0) + '%' : '-';
        document.getElementById('stat-positions').textContent = `${data.n_active}/${data.max_positions}`;
        document.getElementById('stat-deployed').textContent = fmt.usd(data.total_deployed);

        // Update signal badge
        const sigBadge = document.getElementById('badge-signals');
        if (sigBadge) sigBadge.textContent = '';  // will update when signals load

        // Update unrealized P&L in sidebar (async, non-blocking)
        api('/api/portfolio').then(pdata => {
            const unrealEl = document.getElementById('stat-unrealized');
            if (unrealEl && pdata.account_summary) {
                const u = pdata.account_summary.total_unrealized_pnl || 0;
                const hasPrices = pdata.account_summary.n_priced > 0;
                unrealEl.textContent = hasPrices ? fmt.pnl(u) : '-';
                unrealEl.className = 'stat-value ' + (u >= 0 ? 'green' : 'red');
            }
        }).catch(() => {});
    } catch (e) {
        console.error('Status refresh failed:', e);
    }
}

// ── Home Tab ──
async function loadHome() {
    const container = document.getElementById('home-content');
    try {
        const data = await api('/api/account');
        if (data.connected) {
            renderHomeConnected(container, data);
            // Auto-refresh while on home tab
            if (!_homeRefreshTimer) {
                _homeRefreshTimer = setInterval(loadHome, 30000);
            }
        } else {
            renderHomeDisconnected(container, data);
            if (_homeRefreshTimer) {
                clearInterval(_homeRefreshTimer);
                _homeRefreshTimer = null;
            }
        }
    } catch (e) {
        container.innerHTML = `<div class="loading">Error loading home: ${e.message}</div>`;
    }
}

function renderHomeDisconnected(container, data) {
    const lp = data.local_portfolio || {};
    const ss = data.system_status || {};

    const thetaOn = ss.theta_terminal;
    const scanTime = ss.last_scan_time ? new Date(ss.last_scan_time).toLocaleString() : null;

    container.innerHTML = `
    <div class="home-hero">
        <div class="home-hero-logo">CS TERMINAL</div>
        <div class="home-hero-sub">Double Calendar Spread System</div>
    </div>

    <div class="home-grid">
        <!-- Left: IBKR Login -->
        <div class="home-card">
            <div class="home-card-title">
                <span class="card-icon" style="background:var(--accent);color:#000;">IB</span>
                Interactive Brokers Login
            </div>
            <div class="connect-form-row">
                <div class="connect-field connect-field-full">
                    <label>Username</label>
                    <input id="home-username" type="text" placeholder="Your IBKR username" autocomplete="username">
                </div>
            </div>
            <div class="connect-form-row">
                <div class="connect-field connect-field-full">
                    <label>Password</label>
                    <input id="home-password" type="password" placeholder="Your IBKR password" autocomplete="current-password"
                           onkeydown="if(event.key==='Enter') homeDoLogin()">
                </div>
            </div>
            <div class="connect-form-row">
                <div class="connect-field">
                    <label>Trading Mode</label>
                    <select id="home-mode">
                        <option value="paper">Paper Trading</option>
                        <option value="live">Live Trading</option>
                    </select>
                </div>
                <div class="connect-field" style="display:flex;align-items:flex-end">
                    <button class="home-connect-btn" id="home-login-btn" onclick="homeDoLogin()" style="margin:0;">
                        Login
                    </button>
                </div>
            </div>
            <div id="home-login-progress" style="display:none;margin-top:10px;">
                <div style="display:flex;align-items:center;gap:8px;">
                    <span class="btn-spinner"></span>
                    <span id="home-login-step" style="font-size:11px;color:var(--accent);">Launching IB Gateway...</span>
                </div>
            </div>
            <div class="connect-error" id="home-login-error"></div>

            <!-- Collapsible: manual connect -->
            <div style="margin-top:14px;border-top:1px solid var(--border);padding-top:10px;">
                <div onclick="document.getElementById('home-advanced').classList.toggle('show')"
                     style="cursor:pointer;font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;user-select:none;">
                    &#9654; Advanced — Manual Connection
                </div>
                <div id="home-advanced" class="home-advanced-section">
                    <div class="connect-form-row" style="margin-top:8px;">
                        <div class="connect-field">
                            <label>Host</label>
                            <input id="home-host" type="text" value="127.0.0.1" placeholder="127.0.0.1">
                        </div>
                        <div class="connect-field">
                            <label>Port</label>
                            <input id="home-port" type="number" value="4002" placeholder="4002"
                                   onkeydown="if(event.key==='Enter') homeDoConnect()">
                        </div>
                    </div>
                    <button class="home-connect-btn" id="home-connect-btn" onclick="homeDoConnect()"
                            style="font-size:11px;padding:6px;">
                        Manual Connect
                    </button>
                    <div class="connect-error" id="home-connect-error"></div>
                </div>
            </div>
        </div>

        <!-- Right: System status -->
        <div class="home-card">
            <div class="home-card-title"><span class="card-icon">S</span> System Status</div>
            <div class="sys-status-row">
                <span class="sys-status-label">Theta Terminal</span>
                <span class="sys-status-value">
                    <span class="sys-dot ${thetaOn ? 'on' : 'off'}"></span>
                    <span style="color:${thetaOn ? 'var(--green)' : 'var(--red)'}">${thetaOn ? 'ONLINE' : 'OFFLINE'}</span>
                </span>
            </div>
            <div class="sys-status-row">
                <span class="sys-status-label">IBKR Connection</span>
                <span class="sys-status-value">
                    <span class="sys-dot off"></span>
                    <span style="color:var(--red)">DISCONNECTED</span>
                </span>
            </div>
            <div class="sys-status-row">
                <span class="sys-status-label">Last Scan</span>
                <span class="sys-status-value">${scanTime || 'Never'}</span>
            </div>

            <div style="margin-top:14px;border-top:1px solid var(--border);padding-top:10px;">
                <div class="sys-status-row">
                    <span class="sys-status-label">Local Positions</span>
                    <span class="sys-status-value" style="color:var(--accent)">${lp.n_active || 0} active</span>
                </div>
                <div class="sys-status-row">
                    <span class="sys-status-label">Closed Trades</span>
                    <span class="sys-status-value">${lp.n_closed || 0}</span>
                </div>
                <div class="sys-status-row">
                    <span class="sys-status-label">Total Deployed</span>
                    <span class="sys-status-value" style="color:var(--green)">${fmt.usd(lp.total_deployed)}</span>
                </div>
            </div>

            <div style="margin-top:14px;border-top:1px solid var(--border);padding-top:10px;">
                <div class="home-card-title" style="font-size:10px;margin-bottom:6px;padding-bottom:4px;">
                    <span class="card-icon" style="width:18px;height:18px;font-size:9px;">?</span> How it works
                </div>
                <ol class="setup-steps">
                    <li class="setup-step">
                        <span class="setup-step-num">1</span>
                        <div>Enter your <strong>IBKR</strong> username & password above</div>
                    </li>
                    <li class="setup-step">
                        <span class="setup-step-num">2</span>
                        <div>IB Gateway launches automatically in the background</div>
                    </li>
                    <li class="setup-step">
                        <span class="setup-step-num">3</span>
                        <div>Accept the <strong>2FA prompt</strong> on your IBKR mobile app</div>
                    </li>
                    <li class="setup-step">
                        <span class="setup-step-num">4</span>
                        <div>CS Terminal connects and syncs your portfolio</div>
                    </li>
                </ol>
            </div>
        </div>
    </div>

    <div class="home-card" style="margin-top:16px;grid-column:1/-1;">
        <div class="home-card-title">
            <span class="card-icon" style="background:var(--green);color:#000;">A</span>
            Autopilot
            <span id="daemon-status-dot" class="sys-dot"></span>
        </div>
        <div class="sys-status-row">
            <span class="sys-status-label">Status</span>
            <span class="sys-status-value" id="daemon-running">Loading...</span>
        </div>
        <div class="sys-status-row">
            <span class="sys-status-label">Schedule (ET)</span>
            <span class="sys-status-value">09:00 Scan | 10:15 Trade | 16:30 Monitor</span>
        </div>
        <div id="daemon-jobs-info"></div>
        <div style="font-size:10px;color:var(--text-muted);margin-top:6px;">
            IBKR not connected — scan & monitor will run, trades will be skipped
        </div>
        <div style="margin-top:10px;display:flex;gap:8px;">
            <button class="btn" id="daemon-toggle-btn" onclick="toggleDaemon()">Start</button>
            <button class="btn" onclick="loadDaemonStatus()">Refresh</button>
        </div>
        <div id="daemon-logs" style="margin-top:10px;max-height:200px;overflow-y:auto;
             font-size:10px;font-family:monospace;color:var(--text-muted);"></div>
    </div>`;
    loadDaemonStatus();
}

function renderHomeConnected(container, data) {
    const s = data.summary || {};
    const lp = data.local_portfolio || {};
    const ss = data.system_status || {};
    const gp = data.grouped_positions || [];
    const oo = data.open_orders || [];

    const nlv = s.NetLiquidation || 0;
    const bp = s.BuyingPower || 0;
    const af = s.AvailableFunds || 0;
    const cash = s.TotalCashValue || 0;
    const gpv = s.GrossPositionValue || 0;
    const initM = s.InitMarginReq || 0;
    const maintM = s.MaintMarginReq || 0;
    const excess = s.ExcessLiquidity || 0;
    const marginPct = nlv > 0 ? (maintM / nlv * 100) : 0;

    const totalUnrPnl = gp.reduce((sum, g) => sum + g.totalUnrealizedPnl, 0);
    const positionsPct = nlv > 0 ? (gpv / nlv * 100) : 0;
    const cashPct = Math.max(0, 100 - positionsPct);

    // Connection bar
    let html = `
    <div class="home-conn-bar">
        <div class="conn-item"><span class="conn-label">Account</span> <span class="conn-value green">${data.account}</span></div>
        <div class="conn-item"><span class="conn-label">Host</span> <span class="conn-value">${data.host}:${data.port}</span></div>
        <div class="conn-item"><span class="conn-label">Theta</span> <span class="conn-value ${ss.theta_terminal ? 'green' : 'red'}">${ss.theta_terminal ? 'ONLINE' : 'OFF'}</span></div>
        <div class="conn-item"><span class="conn-label">Scan</span> <span class="conn-value">${ss.last_scan_file || 'None'}</span></div>
        ${data.connect_time ? `<span class="conn-uptime">Connected ${new Date(data.connect_time).toLocaleTimeString()}</span>` : ''}
    </div>`;

    // Metric cards row 1 — account
    html += `<div class="metrics-row">
        <div class="metric-card"><div class="metric-label">Net Liquidation</div><div class="metric-value accent">${fmt.usd(nlv)}</div></div>
        <div class="metric-card"><div class="metric-label">Buying Power</div><div class="metric-value">${fmt.usd(bp)}</div></div>
        <div class="metric-card"><div class="metric-label">Available Funds</div><div class="metric-value">${fmt.usd(af)}</div></div>
        <div class="metric-card"><div class="metric-label">Cash</div><div class="metric-value">${fmt.usd(cash)}</div></div>
        <div class="metric-card"><div class="metric-label">Unrealized P&L</div><div class="metric-value ${totalUnrPnl >= 0 ? 'green' : 'red'}">${fmt.pnl(totalUnrPnl)}</div></div>
        <div class="metric-card"><div class="metric-label">Gross Position</div><div class="metric-value">${fmt.usd(gpv)}</div></div>
    </div>`;

    // Metric cards row 2 — margin
    html += `<div class="metrics-row">
        <div class="metric-card"><div class="metric-label">Init Margin</div><div class="metric-value">${fmt.usd(initM)}</div></div>
        <div class="metric-card"><div class="metric-label">Maint Margin</div><div class="metric-value">${fmt.usd(maintM)}</div></div>
        <div class="metric-card"><div class="metric-label">Margin Used</div><div class="metric-value ${marginPct > 80 ? 'red' : marginPct > 50 ? 'accent' : 'green'}">${marginPct.toFixed(1)}%</div></div>
        <div class="metric-card"><div class="metric-label">Excess Liquidity</div><div class="metric-value">${fmt.usd(excess)}</div></div>
    </div>`;

    // Allocation bar
    if (nlv > 0) {
        html += `<div class="alloc-bar">
            <div class="alloc-segment alloc-spreads" style="width:${Math.min(positionsPct, 100).toFixed(1)}%">${positionsPct > 5 ? positionsPct.toFixed(0) + '% Positions' : ''}</div>
            <div class="alloc-segment alloc-cash" style="width:${Math.min(cashPct, 100).toFixed(1)}%">${cashPct > 5 ? cashPct.toFixed(0) + '% Cash' : ''}</div>
        </div>`;
    }

    // Quick actions
    html += `<div class="home-quick-actions">
        <button class="btn" onclick="loadHome()">Refresh</button>
        <button class="btn" id="home-sync-btn" onclick="homeSyncPortfolio()">Sync Portfolio</button>
        <button class="btn btn-danger" onclick="homeDisconnect()">Disconnect</button>
    </div>`;

    // Autopilot panel
    html += `<div class="home-card" style="margin-bottom:14px;">
        <div class="home-card-title">
            <span class="card-icon" style="background:var(--green);color:#000;">A</span>
            Autopilot
            <span id="daemon-status-dot" class="sys-dot"></span>
        </div>
        <div class="sys-status-row">
            <span class="sys-status-label">Status</span>
            <span class="sys-status-value" id="daemon-running">Loading...</span>
        </div>
        <div class="sys-status-row">
            <span class="sys-status-label">Schedule (ET)</span>
            <span class="sys-status-value">09:00 Scan | 10:15 Trade | 16:30 Monitor</span>
        </div>
        <div id="daemon-jobs-info"></div>
        <div style="margin-top:10px;display:flex;gap:8px;">
            <button class="btn" id="daemon-toggle-btn" onclick="toggleDaemon()">Start</button>
            <button class="btn" onclick="loadDaemonStatus()">Refresh</button>
        </div>
        <div id="daemon-logs" style="margin-top:10px;max-height:200px;overflow-y:auto;
             font-size:10px;font-family:monospace;color:var(--text-muted);"></div>
    </div>`;

    // IBKR Positions table (grouped)
    html += `<div class="table-wrapper">
        <div class="table-header">
            <span class="table-title">IBKR Positions (${gp.length} symbols)</span>
        </div>
        <table><thead><tr>
            <th>Symbol</th><th>Legs</th><th>Market Value</th><th>Unrealized P&L</th>
        </tr></thead><tbody>`;

    if (gp.length === 0) {
        html += '<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:12px;">No open positions</td></tr>';
    } else {
        for (const g of gp) {
            const legsDesc = g.legs.map(l => {
                if (l.secType === 'OPT') {
                    const dir = l.position > 0 ? '+' : '';
                    const exp = l.expiry ? l.expiry.substring(2) : '';
                    return `${dir}${l.position} ${l.right}${l.strike} ${exp}`;
                }
                return `${l.position} ${l.secType}`;
            }).join(' | ');
            const pnlCls = g.totalUnrealizedPnl >= 0 ? 'cell-pos' : 'cell-neg';
            html += `<tr>
                <td class="cell-ticker">${g.symbol}</td>
                <td style="font-size:10px;max-width:450px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${legsDesc}</td>
                <td>${fmt.usd2(g.totalMarketValue)}</td>
                <td class="${pnlCls}">${fmt.pnl(g.totalUnrealizedPnl)}</td>
            </tr>`;
        }
    }
    html += '</tbody></table></div>';

    // Portfolio sync comparison
    const ibkrSymbols = gp.map(g => g.symbol).sort();
    const localSymbols = (lp.active_tickers || []).sort();
    const onlyIbkr = ibkrSymbols.filter(s => !localSymbols.includes(s));
    const onlyLocal = localSymbols.filter(s => !ibkrSymbols.includes(s));
    const synced = ibkrSymbols.filter(s => localSymbols.includes(s));

    html += `<div class="home-sync-panel">
        <div class="sync-col">
            <div class="sync-col-title">IBKR (${ibkrSymbols.length})</div>
            ${synced.map(s => `<div class="sync-item ok">${s}</div>`).join('')}
            ${onlyIbkr.map(s => `<div class="sync-item warn">${s} — IBKR only</div>`).join('')}
            ${ibkrSymbols.length === 0 ? '<div class="sync-item" style="color:var(--text-muted)">No positions</div>' : ''}
        </div>
        <div class="sync-col">
            <div class="sync-col-title">Local Portfolio (${localSymbols.length})</div>
            ${synced.map(s => `<div class="sync-item ok">${s}</div>`).join('')}
            ${onlyLocal.map(s => `<div class="sync-item miss">${s} — local only</div>`).join('')}
            ${localSymbols.length === 0 ? '<div class="sync-item" style="color:var(--text-muted)">No positions</div>' : ''}
        </div>
    </div>`;

    // Open orders
    if (oo.length > 0) {
        html += `<div class="table-wrapper" style="margin-top:14px;">
            <div class="table-header">
                <span class="table-title">Open Orders (${oo.length})</span>
            </div>
            <table><thead><tr>
                <th>ID</th><th>Symbol</th><th>Action</th><th>Qty</th><th>Status</th>
            </tr></thead><tbody>`;
        for (const o of oo) {
            html += `<tr>
                <td>${o.orderId}</td>
                <td class="cell-ticker">${o.symbol}</td>
                <td>${o.action}</td>
                <td>${o.qty}</td>
                <td><span class="status-badge status-${(o.status||'').toLowerCase()}">${o.status}</span></td>
            </tr>`;
        }
        html += '</tbody></table></div>';
    }

    container.innerHTML = html;
    loadDaemonStatus();
}

// ── Daemon / Autopilot ──
async function loadDaemonStatus() {
    try {
        const data = await api('/api/daemon/status');
        const dot = document.getElementById('daemon-status-dot');
        const running = document.getElementById('daemon-running');
        const btn = document.getElementById('daemon-toggle-btn');
        const jobsInfo = document.getElementById('daemon-jobs-info');
        const logsDiv = document.getElementById('daemon-logs');

        if (!running) return;  // panel not in DOM

        if (data.running) {
            if (dot) { dot.className = 'sys-dot on'; }
            running.innerHTML = '<span style="color:var(--green)">RUNNING</span>';
            if (btn) { btn.textContent = 'Stop'; btn.className = 'btn btn-danger'; }
        } else {
            if (dot) { dot.className = 'sys-dot off'; }
            running.innerHTML = '<span style="color:var(--red)">STOPPED</span>';
            if (btn) { btn.textContent = 'Start'; btn.className = 'btn btn-primary'; }
        }

        // Last job times
        if (jobsInfo) {
            let info = '';
            if (data.last_scan) info += `<div class="sys-status-row"><span class="sys-status-label">Last Scan</span><span class="sys-status-value">${new Date(data.last_scan).toLocaleString()}</span></div>`;
            if (data.last_trade) info += `<div class="sys-status-row"><span class="sys-status-label">Last Trade</span><span class="sys-status-value">${new Date(data.last_trade).toLocaleString()}</span></div>`;
            if (data.last_monitor) info += `<div class="sys-status-row"><span class="sys-status-label">Last Monitor</span><span class="sys-status-value">${new Date(data.last_monitor).toLocaleString()}</span></div>`;
            jobsInfo.innerHTML = info;
        }

        // Logs
        if (logsDiv && data.logs && data.logs.length > 0) {
            logsDiv.innerHTML = data.logs.map(l => {
                const t = new Date(l.time).toLocaleTimeString();
                const cls = l.level === 'error' ? 'error' : l.level === 'warning' ? 'warning' : '';
                return `<div class="daemon-log-entry ${cls}"><span class="daemon-log-time">${t}</span><span class="daemon-log-job ${l.job}">${l.job}</span>${l.msg}</div>`;
            }).join('');
            logsDiv.scrollTop = logsDiv.scrollHeight;
        } else if (logsDiv) {
            logsDiv.innerHTML = '<span style="color:var(--text-muted)">No log entries yet</span>';
        }
    } catch (e) {
        console.error('Daemon status error:', e);
    }
}

async function toggleDaemon() {
    try {
        const data = await api('/api/daemon/status');
        if (data.running) {
            await api('/api/daemon/stop', { method: 'POST' });
        } else {
            await api('/api/daemon/start', { method: 'POST' });
        }
        loadDaemonStatus();
    } catch (e) {
        console.error('Daemon toggle error:', e);
    }
}

// ── Signals Tab ──
async function loadSignals() {
    const container = document.getElementById('signals-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div> Loading signals...</div>';

    try {
        const data = await api(`/api/sizing?account_value=${state.accountValue}`);
        const badge = document.getElementById('badge-signals');
        if (badge) badge.textContent = data.n_positions;

        let html = `
        <div class="section-header">
            <h2 class="section-title">Scanner Signals & Kelly Sizing</h2>
            <div class="btn-group">
                <button class="btn btn-primary" onclick="autoManage()" id="btn-manage">
                    Auto-Manage
                </button>
                <button class="btn" onclick="runScanner()" id="btn-scan">
                    Run Scanner
                </button>
                <button class="btn" onclick="showOrderConfirm()" id="btn-place"
                    ${!state.connected ? 'disabled title="Connect IBKR first"' : ''}>
                    Place Orders
                </button>
                <button class="btn" onclick="scanAndEnter()" id="btn-scan-enter"
                    ${!state.connected ? 'disabled title="Connect IBKR first"' : ''}>
                    Scan + Execute
                </button>
            </div>
        </div>

        <div class="metrics-row">
            <div class="metric-card">
                <div class="metric-label">Kelly Target</div>
                <div class="metric-value accent">${fmt.usd(data.kelly_target)}</div>
                <div class="metric-sub">f = ${(data.kelly_f * 100).toFixed(2)}%</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Deployed</div>
                <div class="metric-value">${fmt.usd(data.total_deployed)}</div>
                <div class="metric-sub">Gap: ${fmt.usd(data.gap)} (${data.gap_pct > 0 ? '+' : ''}${data.gap_pct}%)</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Positions</div>
                <div class="metric-value">${data.n_positions}</div>
                <div class="metric-sub">Alloc/pos: ${fmt.usd(data.alloc_per_pos)}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Cash Reserve</div>
                <div class="metric-value green">${fmt.usd(data.account_value - data.total_deployed)}</div>
                <div class="metric-sub">+${fmt.usd((data.account_value - data.total_deployed) * 0.045)}/yr @ 4.5%</div>
            </div>
        </div>

        <div class="alloc-bar">
            <div class="alloc-segment alloc-spreads" style="width: ${(data.total_deployed / data.account_value * 100).toFixed(1)}%">
                Spreads ${(data.total_deployed / data.account_value * 100).toFixed(1)}%
            </div>
            <div class="alloc-segment alloc-cash" style="width: ${(100 - data.total_deployed / data.account_value * 100).toFixed(1)}%">
                Cash ${(100 - data.total_deployed / data.account_value * 100).toFixed(1)}%
            </div>
        </div>

        <div class="table-wrapper">
            <div class="table-header">
                <span class="table-title">Double Calendar Signals (${data.n_positions} positions)</span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th></th>
                        <th>Ticker</th><th>Combo</th><th>CallK</th><th>PutK</th><th>Stock</th>
                        <th>FF%</th><th>BA%</th><th>Ctr</th><th>Deployed</th>
                        <th>Front</th><th>Back</th>
                        <th>IV F/B</th>
                        <th>Call</th><th>Put</th><th>Total</th>
                    </tr>
                </thead>
                <tbody>`;

        const activeTickers = new Set(data.active_tickers || []);
        _signalsList = data.sizing;

        for (let i = 0; i < data.sizing.length; i++) {
            const s = data.sizing[i];
            const ffClass = s.ff > 100 ? 'green' : s.ff > 50 ? 'accent' : '';
            const ivStr = (s.front_iv && s.back_iv) ? `${s.front_iv.toFixed(0)}/${s.back_iv.toFixed(0)}` : '-';
            const totalCost = s.dbl_cost || s.call_cost;
            const baPct = s.ba_pct != null ? (s.ba_pct * 100) : null;
            const baClass = baPct == null ? '' : baPct <= 5 ? 'cell-pos' : baPct <= 10 ? 'cell-ff' : 'cell-neg';
            const baStr = baPct != null ? baPct.toFixed(1) + '%' : '-';
            const inPortfolio = activeTickers.has(s.ticker);
            const addBtn = inPortfolio
                ? `<span style="color:var(--green);font-size:11px;font-weight:700;">IN</span>`
                : `<button class="btn btn-sm" style="padding:2px 8px;font-size:11px;" onclick="showAddModal(${i})" id="add-${s.ticker}">+Add</button>`;
            const putK = s.put_strike ? fmt.usd(s.put_strike) : '-';
            html += `<tr>
                <td>${addBtn}</td>
                <td class="cell-ticker">${s.ticker}</td>
                <td>${s.combo || '-'}</td>
                <td>${fmt.usd(s.strike)}</td>
                <td>${putK}</td>
                <td>${fmt.usd2(s.stock_px)}</td>
                <td class="cell-ff ${ffClass}">${fmt.pct(s.ff)}</td>
                <td class="${baClass}">${baStr}</td>
                <td>${s.contracts}</td>
                <td>${fmt.usd(s.deployed)}</td>
                <td>${s.front_exp ? s.front_exp.substring(5) : '-'}</td>
                <td>${s.back_exp ? s.back_exp.substring(5) : '-'}</td>
                <td>${ivStr}</td>
                <td>${s.call_cost ? fmt.usd2(s.call_cost) : '-'}</td>
                <td>${s.put_cost ? fmt.usd2(s.put_cost) : '-'}</td>
                <td class="accent" style="font-weight:700">${totalCost ? fmt.usd2(totalCost) : '-'}</td>
            </tr>`;
        }

        html += '</tbody></table></div>';
        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = `<div class="loading">Error loading signals: ${e.message}</div>`;
    }
}

// ── Portfolio Tab (Paper Trading Dashboard) ──
async function loadPortfolio() {
    const container = document.getElementById('portfolio-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div> Loading portfolio...</div>';

    try {
        const data = await api('/api/portfolio');
        _lastPortfolioData = data;  // cache for close modal
        const badge = document.getElementById('badge-portfolio');
        if (badge) badge.textContent = data.n_active || '';

        const acctSummary = data.account_summary || {};
        const unrealizedPnl = acctSummary.total_unrealized_pnl || 0;
        const realizedPnl = acctSummary.realized_pnl || 0;
        const cash = state.accountValue - data.total_deployed;
        const accountVal = state.accountValue + unrealizedPnl + realizedPnl;

        // Update sidebar stat
        const unrealEl = document.getElementById('stat-unrealized');
        if (unrealEl) {
            unrealEl.textContent = acctSummary.n_priced > 0 ? fmt.pnl(unrealizedPnl) : '-';
            unrealEl.className = 'stat-value ' + (unrealizedPnl >= 0 ? 'green' : 'red');
        }

        let html = `
        <div class="section-header">
            <h2 class="section-title">Portfolio Dashboard</h2>
            <div class="btn-group">
                <button class="btn" onclick="refreshPortfolioPrices()" id="btn-portfolio-refresh"
                    ${data.refresh_running ? 'disabled' : ''}>
                    ${data.refresh_running ? '<span class="btn-spinner"></span> Pricing...' : 'Refresh Prices'}
                </button>
                <button class="btn btn-primary" onclick="autoManage()" id="btn-manage">
                    Auto-Manage
                </button>
            </div>
        </div>

        <div class="metrics-row">
            <div class="metric-card">
                <div class="metric-label">Account Value</div>
                <div class="metric-value">${fmt.usd(accountVal)}</div>
                <div class="metric-sub">Base: ${fmt.usd(state.accountValue)}</div>
            </div>
            <div class="metric-card ${unrealizedPnl >= 0 ? 'risk-kpi-green' : 'risk-kpi-red'}">
                <div class="metric-label">Unrealized P&L</div>
                <div class="metric-value ${unrealizedPnl >= 0 ? 'green' : 'red'}">${acctSummary.n_priced > 0 ? fmt.pnl(unrealizedPnl) : '-'}</div>
                <div class="metric-sub">${data.cached_date ? 'ThetaData ' + data.cached_date : 'Click Refresh Prices'}</div>
            </div>
            <div class="metric-card ${realizedPnl >= 0 ? 'risk-kpi-green' : 'risk-kpi-red'}">
                <div class="metric-label">Realized P&L</div>
                <div class="metric-value ${realizedPnl >= 0 ? 'green' : 'red'}">${data.closed.length > 0 ? fmt.pnl(realizedPnl) : '-'}</div>
                <div class="metric-sub">${acctSummary.n_wins || 0}W / ${acctSummary.n_losses || 0}L</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Deployed</div>
                <div class="metric-value">${fmt.usd(data.total_deployed)}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Cash</div>
                <div class="metric-value green">${fmt.usd(cash)}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Positions</div>
                <div class="metric-value">${data.n_active}/20</div>
            </div>
        </div>`;

        // ── Active Positions Table ──
        if (data.active.length === 0) {
            html += `<div class="table-wrapper"><div style="padding: 40px; text-align: center; color: var(--text-secondary);">
                No active positions. Add signals from the Signals tab.
            </div></div>`;
        } else {
            let totalPnl = 0;
            let totalDeployed = 0;

            html += `<div class="table-wrapper">
            <div class="table-header">
                <span class="table-title">Active Positions (${data.n_active})</span>
                ${data.cached_date ? '<span style="font-size:12px;color:var(--text-muted)">Prices: ' + data.cached_date + '</span>' : ''}
            </div>
            <table><thead><tr>
                <th>Ticker</th><th>Combo</th><th>CallK/PutK</th><th>Cts</th>
                <th>Entry</th><th>Current</th><th>P&L</th><th>Ret%</th>
                <th>DTE</th><th>FF%</th><th>Stock</th><th>Entry Date</th><th></th>
            </tr></thead><tbody>`;

            const sorted = [...data.active].sort((a, b) => {
                const aPnl = a.unrealized_pnl != null ? a.unrealized_pnl : -Infinity;
                const bPnl = b.unrealized_pnl != null ? b.unrealized_pnl : -Infinity;
                return bPnl - aPnl;
            });

            for (const p of sorted) {
                const dte = p.days_to_exp != null ? p.days_to_exp + 'd' : '-';
                const dteClass = p.days_to_exp != null && p.days_to_exp <= 5 ? 'cell-neg' : p.days_to_exp != null && p.days_to_exp <= 14 ? 'cell-ff' : '';
                const ffStr = p.ff != null ? (p.ff >= 1 ? p.ff.toFixed(0) : (p.ff * 100).toFixed(1)) + '%' : '-';
                const putK = p.put_strike && p.put_strike !== p.strike ? '/' + Number(p.put_strike).toFixed(0) : '';
                const strikeStr = Number(p.strike).toFixed(0) + putK;
                totalDeployed += p.total_deployed || 0;

                const hasPx = p.current_cost != null;
                const pnlVal = p.unrealized_pnl || 0;
                const retPct = p.return_pct != null ? (p.return_pct * 100).toFixed(1) + '%' : '-';
                const pnlClass = pnlVal >= 0 ? 'cell-pos' : 'cell-neg';

                if (hasPx) totalPnl += pnlVal;

                html += `<tr>
                    <td class="cell-ticker">${p.ticker}</td>
                    <td>${p.combo || '-'}</td>
                    <td>${strikeStr}</td>
                    <td>${p.contracts}</td>
                    <td>${fmt.usd2(p.cost_per_share)}</td>
                    <td>${hasPx ? fmt.usd2(p.current_cost) : '<span style="color:var(--text-muted)">--</span>'}</td>
                    <td class="${hasPx ? pnlClass : ''}">${hasPx ? fmt.pnl(pnlVal) : '<span style="color:var(--text-muted)">--</span>'}</td>
                    <td class="${hasPx ? pnlClass : ''}">${hasPx ? retPct : '<span style="color:var(--text-muted)">--</span>'}</td>
                    <td class="${dteClass}">${dte}</td>
                    <td class="cell-ff">${ffStr}</td>
                    <td>${p.stock_px != null ? fmt.usd2(p.stock_px) : '<span style="color:var(--text-muted)">--</span>'}</td>
                    <td>${p.entry_date || '-'}</td>
                    <td><button class="btn-close-pos" onclick="closePosition('${p.id}', '${p.ticker}')" title="Close position">X</button></td>
                </tr>`;
            }

            // Total row
            if (acctSummary.n_priced > 0) {
                const pnlClass = totalPnl >= 0 ? 'cell-pos' : 'cell-neg';
                html += `<tr style="font-weight:700;background:var(--bg-secondary)">
                    <td>TOTAL</td><td></td><td></td><td></td><td></td><td></td>
                    <td class="${pnlClass}">${fmt.pnl(totalPnl)}</td>
                    <td></td><td></td><td></td><td></td><td></td><td></td>
                </tr>`;
            }

            html += '</tbody></table></div>';
        }

        // ── Closed Positions Table ──
        if (data.closed.length > 0) {
            html += `<div class="table-wrapper" style="margin-top: 24px;">
            <div class="table-header"><span class="table-title">Closed Positions (${data.closed.length})</span></div>
            <table><thead><tr>
                <th>Ticker</th><th>Combo</th><th>Entry$</th><th>Exit$</th><th>P&L</th><th>Return</th>
                <th>Entry Date</th><th>Exit Date</th>
            </tr></thead><tbody>`;
            for (const p of data.closed) {
                const pnlClass = (p.pnl || 0) >= 0 ? 'cell-pos' : 'cell-neg';
                html += `<tr>
                    <td class="cell-ticker">${p.ticker}</td>
                    <td>${p.combo}</td>
                    <td>${fmt.usd2(p.cost_per_share)}</td>
                    <td>${p.exit_price != null ? fmt.usd2(p.exit_price) : '-'}</td>
                    <td class="${pnlClass}">${fmt.pnl(p.pnl)}</td>
                    <td class="${pnlClass}">${p.return_pct != null ? (p.return_pct * 100).toFixed(1) + '%' : '-'}</td>
                    <td>${p.entry_date}</td>
                    <td>${p.exit_date || '-'}</td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        // ── P&L History ──
        const pnlHistory = data.pnl_history || [];
        if (pnlHistory.length > 0) {
            html += `<div class="table-wrapper" style="margin-top: 24px;">
            <div class="table-header"><span class="table-title">P&L History (${pnlHistory.length} snapshots)</span></div>
            <table><thead><tr>
                <th>Date</th><th>Positions</th><th>Unrealized P&L</th>
            </tr></thead><tbody>`;
            for (const h of pnlHistory.slice().reverse().slice(0, 30)) {
                const pnlClass = (h.total_pnl || 0) >= 0 ? 'cell-pos' : 'cell-neg';
                html += `<tr>
                    <td>${h.date}</td>
                    <td>${h.n_positions}</td>
                    <td class="${pnlClass}">${fmt.pnl(h.total_pnl)}</td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        container.innerHTML = html;

        // If refresh was running, start polling
        if (data.refresh_running) {
            pollRefreshStatus(function() { loadPortfolio(); });
        }
    } catch (e) {
        container.innerHTML = `<div class="loading">Error: ${e.message}</div>`;
    }
}

function closePosition(positionId, ticker) {
    // Find position from cached portfolio data
    if (!_lastPortfolioData) { alert('Portfolio not loaded yet'); return; }
    const pos = _lastPortfolioData.active.find(p => p.id === positionId);
    if (!pos) { alert('Position not found'); return; }

    _pendingClose = pos;
    const isConnected = state.connected;
    const isZeroContracts = pos.contracts === 0;

    const putK = pos.put_strike && pos.put_strike !== pos.strike
        ? '/' + Number(pos.put_strike).toFixed(0) : '';
    const strikeStr = Number(pos.strike).toFixed(0) + putK;
    const hasPx = pos.current_cost != null;

    // Build modal body
    let body = `<table style="width:100%;font-size:11px;border-collapse:collapse;">
        <tr><td style="color:var(--text-muted);padding:3px 0">Ticker</td>
            <td style="font-weight:700;color:var(--accent)">${pos.ticker}</td></tr>
        <tr><td style="color:var(--text-muted);padding:3px 0">Combo</td>
            <td>${pos.combo || '-'}</td></tr>
        <tr><td style="color:var(--text-muted);padding:3px 0">Strikes (Call/Put)</td>
            <td>${strikeStr}</td></tr>
        <tr><td style="color:var(--text-muted);padding:3px 0">Contracts</td>
            <td>${pos.contracts}${isZeroContracts ? ' <span style="color:var(--red)">(failed entry)</span>' : ''}</td></tr>
        <tr><td style="color:var(--text-muted);padding:3px 0">Entry Price</td>
            <td>${fmt.usd2(pos.cost_per_share)}</td></tr>`;

    if (hasPx) {
        const pnlClass = (pos.unrealized_pnl || 0) >= 0 ? 'var(--green)' : 'var(--red)';
        body += `<tr><td style="color:var(--text-muted);padding:3px 0">Current Price</td>
            <td>${fmt.usd2(pos.current_cost)}</td></tr>
        <tr><td style="color:var(--text-muted);padding:3px 0">Unrealized P&L</td>
            <td style="font-weight:700;color:${pnlClass}">${fmt.pnl(pos.unrealized_pnl)}</td></tr>`;
    }
    body += `</table>`;

    // Legs being closed
    if (!isZeroContracts) {
        body += `<div style="margin-top:10px;padding-top:8px;border-top:1px solid var(--border);font-size:10px;color:var(--text-muted)">
            <div style="margin-bottom:4px;font-weight:700;text-transform:uppercase">Close Legs:</div>
            <div>SELL ${pos.contracts}x ${pos.combo?.split('-')[1] || 'back'}d Call/Put K=${strikeStr} (close long back)</div>
            <div>BUY ${pos.contracts}x ${pos.combo?.split('-')[0] || 'front'}d Call/Put K=${strikeStr} (close short front)</div>
        </div>`;
    }

    // Connection status
    if (isConnected && !isZeroContracts) {
        body += `<div style="margin-top:10px;padding:6px 8px;border:1px solid var(--green);font-size:10px;color:var(--green)">
            <span style="display:inline-block;width:6px;height:6px;background:var(--green);border-radius:50%!important;margin-right:4px"></span>
            IBKR Connected — orders will execute on broker
        </div>`;
    } else if (isZeroContracts) {
        body += `<div style="margin-top:10px;padding:6px 8px;border:1px solid var(--yellow);font-size:10px;color:var(--yellow)">
            0 contracts — paper close only (no IBKR orders)
        </div>`;
    } else {
        body += `<div style="margin-top:10px;padding:6px 8px;border:1px solid var(--yellow);font-size:10px;color:var(--yellow)">
            IBKR Offline — paper close with estimated prices
        </div>`;
    }

    document.getElementById('close-position-body').innerHTML = body;

    // Show/hide buttons based on connection state
    const ibkrBtn = document.getElementById('close-pos-confirm');
    const paperBtn = document.getElementById('close-pos-paper');

    if (isZeroContracts) {
        // Failed entry: paper close only
        ibkrBtn.style.display = 'none';
        paperBtn.style.display = '';
        paperBtn.textContent = 'Remove Position';
    } else if (isConnected) {
        ibkrBtn.style.display = '';
        ibkrBtn.textContent = 'Close on IBKR';
        ibkrBtn.disabled = false;
        paperBtn.style.display = '';
        paperBtn.textContent = 'Paper Close';
    } else {
        ibkrBtn.style.display = 'none';
        paperBtn.style.display = '';
        paperBtn.textContent = 'Paper Close';
    }

    document.getElementById('close-position-modal').classList.add('show');
}

async function confirmClosePosition(useIbkr) {
    if (!_pendingClose) return;
    const positionId = _pendingClose.id;
    const ticker = _pendingClose.ticker;

    // Disable buttons & show spinner
    const ibkrBtn = document.getElementById('close-pos-confirm');
    const paperBtn = document.getElementById('close-pos-paper');
    const activeBtn = useIbkr ? ibkrBtn : paperBtn;
    const origText = activeBtn.textContent;
    activeBtn.disabled = true;
    activeBtn.innerHTML = '<span class="btn-spinner"></span> Closing...';
    if (useIbkr) paperBtn.style.display = 'none';
    else ibkrBtn.style.display = 'none';

    try {
        const result = await api('/api/portfolio/close', {
            method: 'POST',
            body: JSON.stringify({ position_id: positionId, use_ibkr: useIbkr }),
        });
        closeModal('close-position-modal');
        const method = result.close_method === 'ibkr' ? 'IBKR' : 'Paper';
        showToast(`${method} closed ${result.ticker}: P&L ${fmt.pnl(result.pnl)} (${(result.return_pct * 100).toFixed(1)}%)`);
        _pendingClose = null;
        loadPortfolio();
        refreshStatus();
    } catch (e) {
        activeBtn.disabled = false;
        activeBtn.textContent = origText;
        // Show error in modal instead of alert
        const body = document.getElementById('close-position-body');
        body.innerHTML += `<div style="margin-top:8px;padding:6px;border:1px solid var(--red);color:var(--red);font-size:11px">
            Error: ${e.message}</div>`;
    }
}

async function refreshPortfolioPrices() {
    const btn = document.getElementById('btn-portfolio-refresh');
    if (!btn) return;

    btn.disabled = true;
    btn.innerHTML = '<span class="btn-spinner"></span> Pricing...';

    try {
        await api('/api/monitor/refresh', { method: 'POST' });
        pollRefreshStatus(function() { loadPortfolio(); });
    } catch (e) {
        btn.disabled = false;
        btn.textContent = 'Refresh Prices';
        alert('Refresh error: ' + e.message);
    }
}

// ── Orders Tab ──
async function loadOrders() {
    const container = document.getElementById('orders-content');

    try {
        const data = await api('/api/orders');

        const openOrders = data.open_orders || [];
        const filledOrders = data.filled_orders || [];
        const positions = data.positions || [];

        // Split open orders into pending vs cancelled/rejected
        const pending = openOrders.filter(o => !['Cancelled', 'ApiCancelled', 'Inactive'].includes(o.status));
        const rejected = openOrders.filter(o => ['Cancelled', 'ApiCancelled', 'Inactive'].includes(o.status));

        let html = `
        <div class="section-header">
            <h2 class="section-title">Orders & Executions</h2>
            <div class="btn-group">
                <button class="btn" onclick="loadOrders()">Refresh</button>
                <button class="btn btn-danger" onclick="cancelAllOrders()" ${!state.connected ? 'disabled' : ''}>
                    Cancel All
                </button>
            </div>
        </div>

        <div class="metrics-row">
            <div class="metric-card">
                <div class="metric-label">Pending Orders</div>
                <div class="metric-value accent">${pending.length}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Filled</div>
                <div class="metric-value green">${filledOrders.length}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Cancelled</div>
                <div class="metric-value red">${rejected.length}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">IBKR Positions</div>
                <div class="metric-value">${positions.length}</div>
            </div>
        </div>`;

        // ── Pending orders table ──
        if (pending.length > 0) {
            html += `<div class="table-wrapper">
            <div class="table-header"><span class="table-title">Pending Orders (${pending.length})</span></div>
            <table><thead><tr>
                <th>ID</th><th>Symbol</th><th>Type</th><th>Action</th><th>Qty</th>
                <th>Filled</th><th>Remaining</th><th>Order Type</th><th>Limit</th>
                <th>Status</th><th>TIF</th><th></th>
            </tr></thead><tbody>`;

            for (const o of pending) {
                const statusCls = o.status === 'Submitted' ? 'status-submitted' :
                                  o.status === 'PreSubmitted' ? 'status-presubmitted' : '';
                html += `<tr>
                    <td>${o.orderId}</td>
                    <td class="cell-ticker">${o.symbol}</td>
                    <td>${o.secType}</td>
                    <td class="${o.action === 'BUY' ? 'cell-pos' : 'cell-neg'}">${o.action}</td>
                    <td>${o.qty}</td>
                    <td>${o.filled}</td>
                    <td>${o.remaining}</td>
                    <td>${o.orderType}</td>
                    <td>${o.limitPrice != null ? fmt.usd2(o.limitPrice) : '-'}</td>
                    <td><span class="status-badge ${statusCls}">${o.status}</span></td>
                    <td>${o.tif}</td>
                    <td><button class="btn btn-sm" onclick="cancelOrder(${o.orderId})">Cancel</button></td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        } else if (state.connected) {
            html += `<div class="table-wrapper"><div style="padding: 24px; text-align: center; color: var(--text-secondary);">
                No pending orders.
            </div></div>`;
        }

        // ── Filled / Executions table ──
        if (filledOrders.length > 0) {
            html += `<div class="table-wrapper" style="margin-top: 16px;">
            <div class="table-header"><span class="table-title">Executions (${filledOrders.length})</span></div>
            <table><thead><tr>
                <th>ID</th><th>Symbol</th><th>Type</th><th>Side</th><th>Qty</th>
                <th>Avg Price</th><th>Exchange</th><th>Time</th>
            </tr></thead><tbody>`;

            for (const o of filledOrders) {
                html += `<tr>
                    <td>${o.orderId}</td>
                    <td class="cell-ticker">${o.symbol}</td>
                    <td>${o.secType}</td>
                    <td class="${o.action === 'BOT' || o.action === 'BUY' ? 'cell-pos' : 'cell-neg'}">${o.action}</td>
                    <td>${o.qty}</td>
                    <td>${fmt.usd2(o.avgFillPrice)}</td>
                    <td>${o.exchange || '-'}</td>
                    <td>${o.execTime ? o.execTime.split('T')[1]?.substring(0,8) || o.execTime : '-'}</td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        // ── Cancelled / Rejected ──
        if (rejected.length > 0) {
            html += `<div class="table-wrapper" style="margin-top: 16px;">
            <div class="table-header"><span class="table-title">Cancelled / Rejected (${rejected.length})</span></div>
            <table><thead><tr>
                <th>ID</th><th>Symbol</th><th>Action</th><th>Qty</th>
                <th>Type</th><th>Limit</th><th>Status</th>
            </tr></thead><tbody>`;

            for (const o of rejected) {
                html += `<tr>
                    <td>${o.orderId}</td>
                    <td class="cell-ticker">${o.symbol}</td>
                    <td>${o.action}</td>
                    <td>${o.qty}</td>
                    <td>${o.orderType}</td>
                    <td>${o.limitPrice != null ? fmt.usd2(o.limitPrice) : '-'}</td>
                    <td><span class="status-badge status-cancelled">${o.status}</span></td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        // ── IBKR Live Positions ──
        if (positions.length > 0) {
            const totalUnrlz = positions.reduce((s, p) => s + (p.unrealizedPnl || 0), 0);
            const totalMktVal = positions.reduce((s, p) => s + (p.marketValue || 0), 0);

            html += `<div class="table-wrapper" style="margin-top: 16px;">
            <div class="table-header">
                <span class="table-title">IBKR Positions (${positions.length})</span>
                <span style="font-size:13px;">
                    MktVal: ${fmt.usd(totalMktVal)} &nbsp;|&nbsp;
                    Unrealized P&L: <span class="${totalUnrlz >= 0 ? 'cell-pos' : 'cell-neg'}">${fmt.pnl(totalUnrlz)}</span>
                </span>
            </div>
            <table><thead><tr>
                <th>Symbol</th><th>Type</th><th>Position</th>
                <th>Mkt Price</th><th>Mkt Value</th><th>Avg Cost</th>
                <th>Unrealized P&L</th><th>Realized P&L</th>
            </tr></thead><tbody>`;

            for (const p of positions) {
                const pnlClass = p.unrealizedPnl >= 0 ? 'cell-pos' : 'cell-neg';
                html += `<tr>
                    <td class="cell-ticker">${p.symbol}</td>
                    <td>${p.secType}</td>
                    <td>${p.position}</td>
                    <td>${fmt.usd2(p.marketPrice)}</td>
                    <td>${fmt.usd(p.marketValue)}</td>
                    <td>${fmt.usd2(p.avgCost)}</td>
                    <td class="${pnlClass}">${fmt.pnl(p.unrealizedPnl)}</td>
                    <td>${fmt.pnl(p.realizedPnl)}</td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        // ── Not connected ──
        if (!state.connected) {
            html += `<div class="table-wrapper"><div style="padding: 30px; text-align: center; color: var(--text-secondary);">
                Connect to IBKR to see live orders and positions.
            </div></div>`;
        }

        // ── Slippage Tracking ──
        const fillLogs = (data.log || []).filter(l => l.type === 'fill');
        if (fillLogs.length > 0) {
            html += `<div class="table-wrapper" style="margin-top: 24px;">
            <div class="table-header"><span class="table-title">Slippage Tracking (${fillLogs.length} fills)</span></div>
            <div class="order-log">`;

            for (const f of fillLogs.slice().reverse()) {
                const time = f.time ? f.time.split('T')[1]?.substring(0, 8) : '';
                html += `<div class="log-entry">
                    <span class="log-time">${time}</span>
                    <span class="log-type fill">fill</span>
                    <span class="log-status-icon ok">&#10003;</span>
                    <span class="log-msg">${f.message}</span>
                </div>`;
            }
            html += '</div></div>';
        }

        // ── App Order Log ──
        if (data.log && data.log.length > 0) {
            html += `<div class="table-wrapper" style="margin-top: 24px;">
            <div class="table-header"><span class="table-title">Application Log (${data.log_count})</span></div>
            <div class="order-log">`;

            for (const o of data.log.slice().reverse()) {
                const time = o.time ? o.time.split('T')[1]?.substring(0, 8) : '';
                const statusIcon = o.status === 'ok' ? '&#10003;' : o.status === 'error' ? '&#10007;' : '&#8226;';
                html += `<div class="log-entry">
                    <span class="log-time">${time}</span>
                    <span class="log-type ${o.type}">${o.type}</span>
                    <span class="log-status-icon ${o.status}">${statusIcon}</span>
                    <span class="log-msg">${o.message}</span>
                </div>`;
            }

            html += '</div></div>';
        }

        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = `<div class="loading">Error: ${e.message}</div>`;
    }
}

// ── Kelly Tab ──
// ── Track Record Tab ──
async function loadTrackRecord() {
    const container = document.getElementById('track-record-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div> Loading track record...</div>';

    try {
        const data = await api('/api/track-record');

        function metricsTable(title, m) {
            if (!m) return `<div class="table-wrapper">
                <div class="table-header"><span class="table-title">${title}</span></div>
                <div style="padding:24px;color:var(--text-secondary);text-align:center;">No trades yet</div>
            </div>`;
            return `<div class="table-wrapper">
                <div class="table-header"><span class="table-title">${title}</span></div>
                <table class="kelly-table"><tbody>
                    <tr><td>CAGR</td><td class="cell-pos">${(m.cagr * 100).toFixed(1)}%</td></tr>
                    <tr><td>Sharpe</td><td>${m.sharpe.toFixed(2)}</td></tr>
                    <tr><td>Sortino</td><td>${m.sortino.toFixed(2)}</td></tr>
                    <tr><td>Max Drawdown</td><td class="cell-neg">${(m.max_dd * 100).toFixed(1)}%</td></tr>
                    <tr><td>Win Rate</td><td>${(m.win_rate * 100).toFixed(1)}%</td></tr>
                    <tr><td>Profit Factor</td><td>${m.profit_factor === Infinity ? '∞' : m.profit_factor.toFixed(2)}</td></tr>
                    <tr><td>Avg Win</td><td class="cell-pos">${(m.avg_win * 100).toFixed(2)}%</td></tr>
                    <tr><td>Avg Loss</td><td class="cell-neg">${(m.avg_loss * 100).toFixed(2)}%</td></tr>
                    <tr><td>Total Trades</td><td style="color:var(--accent)">${m.total_trades}</td></tr>
                    <tr><td>Total P&L</td><td class="${m.total_pnl >= 0 ? 'cell-pos' : 'cell-neg'}">${fmt.usd(m.total_pnl)}</td></tr>
                    <tr><td>Final Equity</td><td style="color:var(--accent);font-size:16px">${fmt.usd(m.final_equity)}</td></tr>
                </tbody></table>
            </div>`;
        }

        function livePortfolioTable(live) {
            const pf = live.portfolio || {};
            const m = live.metrics;
            const nOpen = pf.n_open || live.n_open || 0;
            const nPriced = pf.n_priced || 0;
            const nWin = pf.n_win || 0;
            const deployed = pf.total_deployed || 0;
            const unrealized = pf.total_unrealized_pnl || 0;
            const retOnDeployed = pf.return_on_deployed || 0;
            const daysActive = pf.days_active || 0;
            const nClosed = live.n_trades || 0;
            const finalEq = live.final_equity || 0;
            const snapDate = pf.snap_date || '-';
            const winRate = nPriced > 0 ? (nWin / nPriced * 100) : 0;
            const unrealClass = unrealized >= 0 ? 'cell-pos' : 'cell-neg';

            return `<div class="table-wrapper">
                <div class="table-header"><span class="table-title">Live (Out-of-Sample)</span></div>
                <table class="kelly-table"><tbody>
                    <tr><td>Positions</td><td>${nOpen} open${nClosed > 0 ? ', ' + nClosed + ' closed' : ''}</td></tr>
                    <tr><td>Deployed</td><td>${fmt.usd(deployed)}</td></tr>
                    <tr><td>Unrealized P&L</td><td class="${unrealClass}">${nPriced > 0 ? fmt.pnl(unrealized) : '-'}</td></tr>
                    <tr><td>Return</td><td class="${unrealClass}">${nPriced > 0 ? (retOnDeployed * 100).toFixed(2) + '%' : '-'}</td></tr>
                    <tr><td>Win Rate</td><td>${nPriced > 0 ? winRate.toFixed(0) + '% (' + nWin + '/' + nPriced + ')' : '-'}</td></tr>
                    ${m && m.max_dd < 0 ? `<tr><td>Max Drawdown</td><td class="cell-neg">${(m.max_dd * 100).toFixed(1)}%</td></tr>` : ''}
                    ${nClosed > 0 && m ? `<tr><td>Realized P&L</td><td class="${m.total_pnl >= 0 ? 'cell-pos' : 'cell-neg'}">${fmt.pnl(m.total_pnl)}</td></tr>` : ''}
                    <tr><td>Final Equity</td><td style="color:var(--accent);font-size:16px">${fmt.usd(finalEq)}</td></tr>
                    <tr><td style="color:var(--text-muted)">Since</td><td style="color:var(--text-muted)">${pf.first_entry || '-'} (${daysActive}d)</td></tr>
                </tbody></table>
            </div>`;
        }

        let html = `
        <div class="section-header">
            <h2 class="section-title">Track Record</h2>
            <div class="btn-group">
                <button class="btn" onclick="loadTrackRecord()">Refresh</button>
            </div>
        </div>`;

        // Equity curve chart
        if (data.chart) {
            html += `<div class="table-wrapper" style="padding: 12px; margin-bottom: 24px;">
                <img src="${data.chart}?t=${Date.now()}" style="width: 100%;" alt="Equity Curve">
            </div>`;
        }

        // Metrics tables side by side
        const btMetrics = data.backtest ? data.backtest.metrics : null;

        // For live: use portfolio table if open positions exist, standard metrics otherwise
        let liveHtml;
        if (data.live && (data.live.n_open > 0 || (data.live.portfolio && data.live.portfolio.n_open > 0))) {
            liveHtml = livePortfolioTable(data.live);
        } else {
            const liveMetrics = data.live ? data.live.metrics : null;
            liveHtml = metricsTable('Live (Out-of-Sample)', liveMetrics);
        }

        html += `<div class="kelly-grid">
            ${metricsTable('Backtest (In-Sample)', btMetrics)}
            ${liveHtml}
        </div>`;

        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = `<div class="loading">Error: ${e.message}</div>`;
    }
}

// ── Actions ──
async function connectIBKR() {
    if (state.connected) {
        if (confirm('Disconnect from IBKR?')) {
            await api('/api/disconnect', { method: 'POST' });
            state.connected = false;
            state.account = null;
            await refreshStatus();
            loadHome();
        }
        return;
    }
    // Go to home tab for connection form
    switchTab('home');
}

async function homeDoConnect() {
    const host = document.getElementById('home-host').value;
    const port = parseInt(document.getElementById('home-port').value);
    const btn = document.getElementById('home-connect-btn');
    const errEl = document.getElementById('home-connect-error');

    btn.disabled = true;
    btn.innerHTML = '<span class="btn-spinner"></span> Connecting...';
    errEl.textContent = '';

    try {
        const data = await api('/api/connect', {
            method: 'POST',
            body: JSON.stringify({ host, port }),
        });
        state.connected = true;
        state.account = data.account;
        if (data.account_value) state.accountValue = data.account_value;

        // Show sync toast
        if (data.sync) {
            const s = data.sync;
            showToast(`Synced: ${s.kept_count} kept, ${s.removed.length} removed, ${s.updated.length} updated`);
        }

        await refreshStatus();
        loadHome();
    } catch (e) {
        errEl.textContent = 'Connection failed: ' + e.message;
    } finally {
        btn.disabled = false;
        btn.textContent = 'Connect';
    }
}

async function homeDisconnect() {
    if (!confirm('Disconnect from IBKR?')) return;
    try {
        await api('/api/disconnect', { method: 'POST' });
        state.connected = false;
        state.account = null;
        await refreshStatus();
        loadHome();
    } catch (e) {
        showToast('Disconnect failed: ' + e.message, true);
    }
}

async function homeSyncPortfolio() {
    const btn = document.getElementById('home-sync-btn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="btn-spinner"></span> Syncing...';
    }
    try {
        const data = await api('/api/sync', { method: 'POST' });
        const s = data.sync;
        showToast(`Sync complete: ${s.kept_count} kept, ${s.removed.length} removed, ${s.updated.length} updated`);
        loadHome();
    } catch (e) {
        showToast('Sync failed: ' + e.message, true);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = 'Sync Portfolio';
        }
    }
}

let _loginPollTimer = null;
let _loginPort = null;

async function homeDoLogin() {
    const username = document.getElementById('home-username').value.trim();
    const password = document.getElementById('home-password').value;
    const mode = document.getElementById('home-mode').value;
    const btn = document.getElementById('home-login-btn');
    const errEl = document.getElementById('home-login-error');
    const progress = document.getElementById('home-login-progress');
    const stepEl = document.getElementById('home-login-step');

    if (!username || !password) {
        errEl.textContent = 'Please enter your IBKR username and password.';
        return;
    }

    btn.disabled = true;
    btn.textContent = 'Launching...';
    errEl.textContent = '';
    progress.style.display = 'block';
    stepEl.textContent = 'Launching IB Gateway...';

    try {
        // Step 1: Launch Gateway (returns immediately)
        const gw = await api('/api/login', {
            method: 'POST',
            body: JSON.stringify({ username, password, mode }),
        });

        _loginPort = gw.port;

        if (gw.status === 'already_running') {
            // Gateway already up, go straight to connect
            stepEl.textContent = 'Gateway already running — connecting...';
            await homeFinishConnect(gw.port, btn, errEl, progress, stepEl);
            return;
        }

        // Step 2: Gateway launched — show 2FA input + polling
        const progressDiv = document.getElementById('home-login-progress');
        progressDiv.innerHTML = `
            <div style="margin-bottom:8px;font-size:11px;color:var(--accent);">
                <span class="btn-spinner"></span>
                IB Gateway launched — enter your 2FA code below:
            </div>
            <div style="display:flex;gap:6px;align-items:center;">
                <input id="home-2fa-code" type="text" placeholder="2FA code"
                       maxlength="10" autocomplete="one-time-code"
                       style="width:140px;padding:6px 10px;border:1px solid var(--accent);background:var(--bg-primary);color:var(--text-primary);font-size:14px;font-family:Consolas,monospace;letter-spacing:3px;text-align:center;"
                       onkeydown="if(event.key==='Enter') homeSend2FA()">
                <button class="btn btn-primary" onclick="homeSend2FA()" id="home-2fa-btn">Submit</button>
            </div>
            <div style="font-size:10px;color:var(--text-muted);margin-top:6px;">
                Waiting for authentication to complete...
            </div>
        `;
        // Auto-focus the 2FA input
        setTimeout(() => {
            const inp = document.getElementById('home-2fa-code');
            if (inp) inp.focus();
        }, 200);

        _loginPollTimer = setInterval(async () => {
            try {
                const check = await api(`/api/gateway_ready?port=${_loginPort}`);
                if (check.error) {
                    clearInterval(_loginPollTimer);
                    _loginPollTimer = null;
                    errEl.textContent = check.error;
                    progress.style.display = 'none';
                    btn.disabled = false;
                    btn.textContent = 'Login';
                    return;
                }
                if (check.ready) {
                    clearInterval(_loginPollTimer);
                    _loginPollTimer = null;
                    stepEl.textContent = '2FA accepted — connecting to account...';
                    await homeFinishConnect(_loginPort, btn, errEl, progress, stepEl);
                }
            } catch (e) {
                // Polling error, keep trying
            }
        }, 3000);

    } catch (e) {
        errEl.textContent = e.message;
        progress.style.display = 'none';
        btn.disabled = false;
        btn.textContent = 'Login';
    }
}

async function homeSend2FA() {
    const input = document.getElementById('home-2fa-code');
    const btn2fa = document.getElementById('home-2fa-btn');
    if (!input || !input.value.trim()) return;

    const code = input.value.trim();
    btn2fa.disabled = true;
    btn2fa.textContent = 'Sending...';

    try {
        await api(`/api/send_2fa?code=${encodeURIComponent(code)}`, { method: 'POST' });
        input.value = '';
        input.placeholder = 'Sent!';
    } catch (e) {
        const errEl = document.getElementById('home-login-error');
        if (errEl) errEl.textContent = e.message;
    } finally {
        btn2fa.disabled = false;
        btn2fa.textContent = 'Submit';
    }
}

async function homeFinishConnect(port, btn, errEl, progress, stepEl) {
    try {
        const data = await api(`/api/gateway_connect?port=${port}`, {
            method: 'POST',
        });

        state.connected = true;
        state.account = data.account;
        if (data.account_value) state.accountValue = data.account_value;

        if (data.sync) {
            const s = data.sync;
            showToast(`Connected! Synced: ${s.kept_count} kept, ${s.removed.length} removed, ${s.updated.length} updated`);
        } else {
            showToast('Connected to ' + data.account);
        }

        await refreshStatus();
        loadHome();
    } catch (e) {
        errEl.textContent = e.message;
    } finally {
        progress.style.display = 'none';
        btn.disabled = false;
        btn.textContent = 'Login';
    }
}

function showToast(msg, isError = false) {
    const existing = document.querySelector('.home-toast');
    if (existing) existing.remove();
    const el = document.createElement('div');
    el.className = 'home-toast' + (isError ? ' error' : '');
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 4000);
}

function closeModal(id) {
    document.getElementById(id).classList.remove('show');
}

async function enableAutopilot() {
    closeModal('autopilot-modal');
    sessionStorage.setItem('autopilot_prompted', '1');
    try {
        await api('/api/daemon/start', { method: 'POST' });
        showToast('Autopilot enabled');
        loadDaemonStatus();
    } catch (e) {
        showToast('Failed to start autopilot: ' + e.message, true);
    }
}

function skipAutopilot() {
    closeModal('autopilot-modal');
    sessionStorage.setItem('autopilot_prompted', '1');
    loadDaemonStatus();
}

async function placeOrders() {
    showOrderConfirm();
}

async function runScanner() {
    const btn = document.getElementById('btn-scan');
    if (!btn) return;

    btn.disabled = true;
    btn.innerHTML = '<span class="btn-spinner"></span> Scanning...';

    try {
        await api('/api/scan', { method: 'POST' });

        // Poll for completion
        scannerPollTimer = setInterval(async () => {
            try {
                const status = await api('/api/scanner_status');
                if (!status.running) {
                    clearInterval(scannerPollTimer);
                    scannerPollTimer = null;
                    btn.disabled = false;
                    btn.textContent = 'Run Scanner';
                    loadSignals();
                }
            } catch (e) {
                clearInterval(scannerPollTimer);
                scannerPollTimer = null;
                btn.disabled = false;
                btn.textContent = 'Run Scanner';
            }
        }, 5000);
    } catch (e) {
        btn.disabled = false;
        btn.textContent = 'Run Scanner';
        alert('Scanner error: ' + e.message);
    }
}

async function showOrderConfirm() {
    if (!state.connected) return alert('Connect to IBKR first');

    const sizing = await api(`/api/sizing?account_value=${state.accountValue}`);

    // Check optimal window (ET time)
    let timeWarning = '';
    try {
        const now = new Date();
        const etStr = now.toLocaleString('en-US', {timeZone: 'America/New_York', hour: 'numeric', minute: 'numeric', hour12: false});
        const [h, m] = etStr.split(':').map(Number);
        const etMins = h * 60 + m;
        if (etMins < 660 || etMins > 870) { // before 11:00 or after 14:30
            timeWarning = `<div style="background:rgba(248,81,73,0.1);border:1px solid #f85149;padding:12px;margin-bottom:12px;">
                <strong style="color:#f85149;">Outside optimal window (11:00-14:30 ET)</strong><br>
                <span style="font-size:12px;">Spreads may be wider. Consider waiting for better fills.</span>
            </div>`;
        } else {
            timeWarning = `<div style="background:rgba(63,185,80,0.1);border:1px solid #3fb950;padding:12px;margin-bottom:12px;">
                <strong style="color:#3fb950;">Within optimal window</strong>
                <span style="font-size:12px;"> — best bid-ask spreads (11:00-14:30 ET)</span>
            </div>`;
        }
    } catch (e) {}

    const body = document.getElementById('order-confirm-body');
    body.innerHTML = `
        ${timeWarning}
        <p><strong>${sizing.n_positions}</strong> positions to place</p>
        <p>Total deployment: <strong>${fmt.usd(sizing.total_deployed)}</strong></p>
        <p>Kelly target: ${fmt.usd(sizing.kelly_target)} (f = ${(sizing.kelly_f * 100).toFixed(2)}%)</p>
        <div style="margin-top:12px; padding:10px; background:var(--bg-secondary);  font-size:12px; color:var(--text-muted);">
            <strong>Execution strategy:</strong><br>
            1. Combo order LMT at mid (5 min timeout, walk $0.05/30s)<br>
            2. If combo fails: individual legs LMT + walk (2 min/leg)<br>
            3. BUY back-month first (no naked short risk)
        </div>
    `;
    document.getElementById('order-modal').classList.add('show');
}

async function confirmPlaceOrders() {
    closeModal('order-modal');

    const btn = document.getElementById('btn-place');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="btn-spinner"></span> Executing...';
    }

    switchTab('orders');

    try {
        await api('/api/enter', { method: 'POST', body: JSON.stringify({}) });
        await loadOrders();
        await refreshStatus();
    } catch (e) {
        alert('Order error: ' + e.message);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = 'Place Orders';
        }
    }
}

async function scanAndEnter() {
    if (!state.connected) return alert('Connect to IBKR first');
    if (!confirm('Run scanner and then automatically place all orders with optimal execution?')) return;

    const btn = document.getElementById('btn-scan-enter');
    const scanBtn = document.getElementById('btn-scan');
    const placeBtn = document.getElementById('btn-place');
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="btn-spinner"></span> Scan+Exec...'; }
    if (scanBtn) scanBtn.disabled = true;
    if (placeBtn) placeBtn.disabled = true;

    try {
        await api('/api/scan_and_enter', { method: 'POST', body: JSON.stringify({}) });

        // Poll for completion
        const pollTimer = setInterval(async () => {
            try {
                const orders = await api('/api/orders');
                const lastLog = orders.log?.[orders.log.length - 1];
                if (lastLog && lastLog.type === 'scan_and_enter' &&
                    (lastLog.status === 'ok' || lastLog.status === 'error')) {
                    clearInterval(pollTimer);
                    if (btn) { btn.disabled = false; btn.textContent = 'Scan + Execute'; }
                    if (scanBtn) scanBtn.disabled = false;
                    if (placeBtn) placeBtn.disabled = false;
                    switchTab('orders');
                    await loadOrders();
                    await refreshStatus();
                }
            } catch (e) {}
        }, 5000);
    } catch (e) {
        if (btn) { btn.disabled = false; btn.textContent = 'Scan + Execute'; }
        if (scanBtn) scanBtn.disabled = false;
        if (placeBtn) placeBtn.disabled = false;
        alert('Error: ' + e.message);
    }
}

function showAddModal(idx) {
    const s = _signalsList[idx];
    if (!s) return;
    _pendingSignal = s;

    const costPerContract = s.dbl_cost * 100;  // per share -> per contract (x100)
    const commPerContract = 4 * 0.65;  // 4 legs x $0.65
    const totalPerContract = costPerContract + commPerContract;
    const isConnected = state.connected;

    document.getElementById('add-position-body').innerHTML = `
        <div style="margin-bottom:12px;">
            <strong style="font-size:16px;color:var(--text-primary)">${s.ticker}</strong>
            <span style="margin-left:8px;color:var(--accent)">${s.combo}</span>
        </div>
        <table style="width:100%;font-size:13px;margin-bottom:12px;">
            <tr><td>Call Strike</td><td style="text-align:right">${fmt.usd(s.strike)}</td></tr>
            <tr><td>Put Strike</td><td style="text-align:right">${s.put_strike ? fmt.usd(s.put_strike) : '-'}</td></tr>
            <tr><td>Stock</td><td style="text-align:right">${fmt.usd2(s.stock_px)}</td></tr>
            <tr><td>Front Exp</td><td style="text-align:right">${s.front_exp}</td></tr>
            <tr><td>Back Exp</td><td style="text-align:right">${s.back_exp}</td></tr>
            <tr><td>FF</td><td style="text-align:right;color:var(--accent)">${fmt.pct(s.ff)}</td></tr>
            <tr><td>BA%</td><td style="text-align:right">${s.ba_pct != null ? (s.ba_pct * 100).toFixed(1) + '%' : '-'}</td></tr>
            <tr style="border-top:1px solid var(--border)">
                <td>Cost / share</td><td style="text-align:right">${fmt.usd2(s.dbl_cost)}</td></tr>
            <tr><td>Cost / contract</td><td style="text-align:right;font-weight:700">${fmt.usd(totalPerContract)}</td></tr>
        </table>
        <div style="background:var(--bg-secondary);padding:10px;font-size:12px;">
            <strong>Kelly recommends: ${s.contracts} contract${s.contracts > 1 ? 's' : ''}</strong>
            (${fmt.usd(s.deployed)} deployed)
        </div>
    `;

    document.getElementById('add-pos-contracts').value = s.contracts || 1;

    // Show/hide IBKR vs Track buttons based on connection
    const ibkrBtn = document.getElementById('add-pos-confirm');
    const trackBtn = document.getElementById('add-pos-track');
    const statusDiv = document.getElementById('add-pos-ibkr-status');

    if (isConnected) {
        ibkrBtn.style.display = '';
        ibkrBtn.textContent = 'Send to IBKR';
        ibkrBtn.disabled = false;
        trackBtn.style.display = '';
        statusDiv.innerHTML = `<span style="color:var(--green)">
            <span style="display:inline-block;width:6px;height:6px;background:var(--green);border-radius:50%!important;margin-right:4px"></span>
            IBKR Connected — combo LMT at mid, legs fallback</span>`;
    } else {
        ibkrBtn.style.display = 'none';
        trackBtn.style.display = '';
        trackBtn.textContent = 'Track Only';
        statusDiv.innerHTML = `<span style="color:var(--yellow)">
            IBKR Offline — track position only (no orders)</span>`;
    }

    document.getElementById('add-position-modal').classList.add('show');
}

async function confirmAddPosition(sendToIbkr) {
    if (!_pendingSignal) return;
    const s = _pendingSignal;
    const contracts = parseInt(document.getElementById('add-pos-contracts').value) || 1;

    const ibkrBtn = document.getElementById('add-pos-confirm');
    const trackBtn = document.getElementById('add-pos-track');
    const activeBtn = sendToIbkr ? ibkrBtn : trackBtn;
    const origText = activeBtn.textContent;

    activeBtn.disabled = true;
    if (sendToIbkr) {
        activeBtn.innerHTML = '<span class="btn-spinner"></span> Executing...';
        trackBtn.style.display = 'none';
    } else {
        activeBtn.innerHTML = '<span class="btn-spinner"></span> Adding...';
        ibkrBtn.style.display = 'none';
    }

    const body = {
        ticker: s.ticker,
        combo: s.combo,
        strike: s.strike,
        put_strike: s.put_strike || null,
        front_exp: s.front_exp,
        back_exp: s.back_exp,
        contracts: contracts,
        cost_per_share: s.dbl_cost,
        ff: s.ff,
        spread_type: 'double',
        n_legs: 4,
        send_to_ibkr: sendToIbkr,
    };

    try {
        const result = await api('/api/portfolio/add', {
            method: 'POST',
            body: JSON.stringify(body),
        });
        closeModal('add-position-modal');
        // Replace button with "IN" label
        const btn = document.getElementById('add-' + s.ticker);
        if (btn) {
            btn.outerHTML = `<span style="color:var(--green);font-size:11px;font-weight:700;">IN</span>`;
        }
        _pendingSignal = null;

        if (result.execution === 'ibkr') {
            showToast(`${s.ticker}: IBKR fill @ ${fmt.usd2(result.fill_cost)} via ${result.method} (slip: ${result.slippage >= 0 ? '+' : ''}${result.slippage.toFixed(3)})`);
        } else {
            showToast(`${s.ticker}: Added to portfolio (track only)`);
        }
        await refreshStatus();
    } catch (e) {
        activeBtn.disabled = false;
        activeBtn.textContent = origText;
        // Show error in status div
        const statusDiv = document.getElementById('add-pos-ibkr-status');
        if (statusDiv) {
            statusDiv.innerHTML = `<span style="color:var(--red)">Error: ${e.message}</span>`;
        }
    }
}

async function autoManage() {
    const btn = document.getElementById('btn-manage');
    if (!btn) return;

    btn.disabled = true;
    btn.innerHTML = '<span class="btn-spinner"></span> Managing...';

    // Disable other buttons during auto-manage
    const scanBtn = document.getElementById('btn-scan');
    const placeBtn = document.getElementById('btn-place');
    const scanEnterBtn = document.getElementById('btn-scan-enter');
    if (scanBtn) scanBtn.disabled = true;
    if (placeBtn) placeBtn.disabled = true;
    if (scanEnterBtn) scanEnterBtn.disabled = true;

    try {
        await api('/api/auto_manage', {
            method: 'POST',
            body: JSON.stringify({ account_value: state.accountValue }),
        });

        // Poll for completion via scanner_status + auto_manage_result
        const pollTimer = setInterval(async () => {
            try {
                const status = await api('/api/scanner_status');
                if (!status.running) {
                    // Check auto_manage result
                    const mgr = await api('/api/auto_manage_result');
                    if (!mgr.running && mgr.result) {
                        clearInterval(pollTimer);
                        btn.disabled = false;
                        btn.textContent = 'Auto-Manage';
                        if (scanBtn) scanBtn.disabled = false;
                        if (placeBtn) placeBtn.disabled = false;
                        if (scanEnterBtn) scanEnterBtn.disabled = false;

                        // Show summary
                        const r = mgr.result;
                        const parts = [];
                        if (r.removed && r.removed.length > 0) parts.push(`Removed ${r.removed.length} expired: ${r.removed.join(', ')}`);
                        if (r.added && r.added.length > 0) parts.push(`Added ${r.added.length}: ${r.added.map(a => a.ticker).join(', ')}`);
                        if (r.filtered_ba && r.filtered_ba.length > 0) parts.push(`BA% filtered: ${r.filtered_ba.length}`);
                        if (r.errors && r.errors.length > 0) parts.push(`Errors: ${r.errors.join('; ')}`);
                        if (parts.length === 0) parts.push(`Scan complete: ${r.n_signals} signals, no changes needed`);

                        showToast(parts.join('\n'));

                        // Refresh tabs
                        loadSignals();
                        await refreshStatus();
                    }
                }
            } catch (e) {
                clearInterval(pollTimer);
                btn.disabled = false;
                btn.textContent = 'Auto-Manage';
                if (scanBtn) scanBtn.disabled = false;
                if (placeBtn) placeBtn.disabled = false;
                if (scanEnterBtn) scanEnterBtn.disabled = false;
            }
        }, 5000);
    } catch (e) {
        btn.disabled = false;
        btn.textContent = 'Auto-Manage';
        if (scanBtn) scanBtn.disabled = false;
        if (placeBtn) placeBtn.disabled = false;
        if (scanEnterBtn) scanEnterBtn.disabled = false;
        alert('Auto-Manage error: ' + e.message);
    }
}

function showToast(message) {
    // Remove existing toast if any
    const existing = document.getElementById('manage-toast');
    if (existing) existing.remove();

    const toast = document.createElement('div');
    toast.id = 'manage-toast';
    toast.style.cssText = `
        position: fixed; top: 12px; right: 12px; z-index: 10000;
        background: #0a0a0a; border: 1px solid #ff8c00;
        padding: 10px 14px; max-width: 420px;
        font-size: 11px; font-family: Consolas, 'Courier New', monospace;
        color: #d4d4d4; white-space: pre-line;
    `;
    toast.innerHTML = `<div style="font-weight:700;margin-bottom:4px;color:#ff8c00;">AUTO-MANAGE COMPLETE</div>${message}`;
    document.body.appendChild(toast);

    setTimeout(() => toast.remove(), 10000);
}

async function closeExpiring() {
    if (!state.connected) return alert('Connect to IBKR first');
    if (!confirm('Close all expiring positions?')) return;

    try {
        await api('/api/close', { method: 'POST' });
        await loadPortfolio();
        await refreshStatus();
    } catch (e) {
        alert('Close error: ' + e.message);
    }
}

async function cancelOrder(orderId) {
    if (!confirm(`Cancel order #${orderId}?`)) return;
    try {
        await api(`/api/cancel_order?order_id=${orderId}`, { method: 'POST' });
        await loadOrders();
    } catch (e) {
        alert('Cancel error: ' + e.message);
    }
}

async function cancelAllOrders() {
    if (!state.connected) return alert('Connect to IBKR first');
    if (!confirm('Cancel ALL pending orders?')) return;
    try {
        await api('/api/cancel_all', { method: 'POST' });
        await loadOrders();
    } catch (e) {
        alert('Cancel error: ' + e.message);
    }
}

// ── Monitor Tab ──
let simPollTimer = null;
let refreshPollTimer = null;

// ── WebSocket Monitor Functions ──
function connectMonitorWs() {
    console.log('[WS] connectMonitorWs called, current state:', _monitorWs ? _monitorWs.readyState : 'null');
    if (_monitorWs && (_monitorWs.readyState === WebSocket.OPEN || _monitorWs.readyState === WebSocket.CONNECTING)) {
        console.log('[WS] already connected/connecting, skip');
        return;
    }

    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/api/ws/monitor`;
    console.log('[WS] connecting to', url);

    try {
        _monitorWs = new WebSocket(url);
    } catch (e) {
        console.error('[WS] create error:', e);
        _scheduleWsReconnect();
        return;
    }

    _monitorWs.onopen = function() {
        console.log('[WS] CONNECTED');
        _monitorWsConnected = true;
        _monitorWsBackoff = 1000;
        _updateWsIndicator(true);
        setTimeout(() => {
            if (_monitorWsConnected && !_monitorLastSnapshot) {
                console.log('[WS] no snapshot after 5s, HTTP fallback');
                _monitorWsConnected = false;
                loadMonitor().then(() => { _monitorWsConnected = true; });
            }
        }, 5000);
    };

    _monitorWs.onmessage = function(event) {
        console.log('[WS] message received, length:', event.data.length);
        try {
            const msg = JSON.parse(event.data);
            console.log('[WS] type:', msg.type, 'positions:', (msg.positions||[]).length);
            if (msg.type === 'snapshot') {
                _handleWsSnapshot(msg);
            } else if (msg.type === 'error') {
                console.warn('[WS] pricing error:', msg.message);
            }
        } catch (e) {
            console.error('[WS] parse error:', e);
        }
    };

    _monitorWs.onclose = function(event) {
        console.log('[WS] CLOSED code:', event.code, 'reason:', event.reason);
        _monitorWsConnected = false;
        _monitorWs = null;
        _updateWsIndicator(false);
        if (_monitorActiveTab) {
            _scheduleWsReconnect();
        }
    };

    _monitorWs.onerror = function(e) {
        console.error('[WS] ERROR:', e);
    };
}

function disconnectMonitorWs() {
    if (_monitorWsReconnectTimer) {
        clearTimeout(_monitorWsReconnectTimer);
        _monitorWsReconnectTimer = null;
    }
    if (_monitorWs) {
        _monitorWs.onclose = null;  // prevent reconnect
        _monitorWs.close();
        _monitorWs = null;
    }
    _monitorWsConnected = false;
    _monitorLastSnapshot = null;
}

function _scheduleWsReconnect() {
    if (_monitorWsReconnectTimer) return;
    _monitorWsReconnectTimer = setTimeout(() => {
        _monitorWsReconnectTimer = null;
        if (_monitorActiveTab) {
            connectMonitorWs();
        }
    }, _monitorWsBackoff);
    _monitorWsBackoff = Math.min(_monitorWsBackoff * 2, 30000);  // cap at 30s
}

function _updateWsIndicator(connected) {
    const el = document.getElementById('ws-live-indicator');
    if (!el) return;
    if (connected) {
        el.className = 'ws-live-indicator';
        el.innerHTML = '<span class="ws-live-dot"></span> LIVE';
    } else {
        el.className = 'ws-live-indicator disconnected';
        el.innerHTML = '<span class="ws-live-dot"></span> OFFLINE';
    }
}

function _handleWsSnapshot(snapshot) {
    const container = document.getElementById('monitor-content');
    console.log('[WS] _handleWsSnapshot, container:', container ? 'found' : 'NULL');
    if (!container) return;

    const prevTickers = _monitorLastSnapshot ? _monitorLastSnapshot.positions.map(p => p.ticker).join(',') : '';
    const newTickers = snapshot.positions.map(p => p.ticker).join(',');
    const needsRebuild = !_monitorLastSnapshot
        || prevTickers !== newTickers
        || snapshot.n_active !== _monitorLastSnapshot.n_active
        || !document.getElementById('ws-monitor-table');

    console.log('[WS] needsRebuild:', needsRebuild, 'pnl:', snapshot.total_unrealized_pnl, 'time:', snapshot.time);

    if (needsRebuild) {
        _buildMonitorDom(container, snapshot);
        console.log('[WS] DOM rebuilt');
    } else {
        _updateMonitorDom(snapshot);
        console.log('[WS] DOM patched');
    }
    _monitorLastSnapshot = snapshot;

    // Update sidebar unrealized P&L
    const unrealEl = document.getElementById('stat-unrealized');
    if (unrealEl && snapshot.n_priced > 0) {
        const u = snapshot.total_unrealized_pnl || 0;
        unrealEl.textContent = fmt.pnl(u);
        unrealEl.className = 'stat-value ' + (u >= 0 ? 'green' : 'red');
    }

    // Update monitor badge
    const badge = document.getElementById('badge-monitor');
    if (badge) badge.textContent = snapshot.n_active > 0 ? snapshot.n_active : '';
}

function _buildMonitorDom(container, snapshot) {
    const positions = snapshot.positions || [];
    const errors = snapshot.errors || [];
    const nActive = snapshot.n_active || 0;
    const nPriced = snapshot.n_priced || 0;
    const totalPnl = snapshot.total_unrealized_pnl || 0;
    const nWin = positions.filter(p => (p.unrealized_pnl || 0) > 0).length;

    // Load portfolio for entry-only fields (DTE, ff, entry_date, etc.)
    // We'll use what the snapshot provides + augment with portfolio data
    let html = `
    <div class="section-header">
        <h2 class="section-title">P&L Monitor</h2>
        <div class="btn-group">
            <span id="ws-live-indicator" class="ws-live-indicator${_monitorWsConnected ? '' : ' disconnected'}">
                <span class="ws-live-dot"></span> ${_monitorWsConnected ? 'LIVE' : 'OFFLINE'}
            </span>
            <button class="btn" onclick="wsForceRefresh()" title="Force immediate re-price">
                Refresh Now
            </button>
            <button class="btn" onclick="refreshMonitorPrices()" id="btn-monitor-refresh">
                Refresh Prices (ThetaData)
            </button>
            <button class="btn btn-primary" onclick="runSimulation()" id="btn-simulate">
                Run Simulation
            </button>
        </div>
    </div>

    <div class="metrics-row">
        <div class="metric-card ${totalPnl >= 0 ? 'risk-kpi-green' : 'risk-kpi-red'}">
            <div class="metric-label">Live P&L</div>
            <div class="metric-value ${totalPnl >= 0 ? 'green' : 'red'}" id="ws-total-pnl">${nPriced > 0 ? fmt.pnl(totalPnl) : '-'}</div>
            <div class="metric-sub" id="ws-price-time">${snapshot.time ? 'Updated ' + snapshot.time : ''}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Active Positions</div>
            <div class="metric-value" id="ws-n-active">${nActive}/20</div>
            <div class="metric-sub" id="ws-priced-info">${nPriced > 0 ? nPriced + ' priced / ' + nWin + ' win' : '-'}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Win Rate</div>
            <div class="metric-value accent" id="ws-win-rate">${nPriced > 0 ? (nWin / nPriced * 100).toFixed(0) + '%' : '-'}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Errors</div>
            <div class="metric-value ${errors.length > 0 ? 'red' : ''}" id="ws-errors">${errors.length}</div>
            <div class="metric-sub" id="ws-error-list">${errors.length > 0 ? errors.join(', ') : 'All OK'}</div>
        </div>
    </div>`;

    // Positions table
    if (positions.length > 0 || nActive > 0) {
        const sorted = [...positions].sort((a, b) => (b.unrealized_pnl || 0) - (a.unrealized_pnl || 0));

        html += `<div class="table-wrapper">
        <div class="table-header">
            <span class="table-title">Live Portfolio (${nActive} positions)</span>
            <span style="font-size:12px;color:var(--text-muted)" id="ws-table-time">Updated: ${snapshot.time || '-'}</span>
        </div>
        <table id="ws-monitor-table"><thead><tr>
            <th>Ticker</th><th>Combo</th><th>Strike</th><th>Cts</th><th>Entry</th>
            <th>Current</th><th>P&L</th><th>Ret%</th><th>DTE</th>
            <th>Stock</th>
        </tr></thead><tbody>`;

        for (const p of sorted) {
            const pnlClass = (p.unrealized_pnl || 0) >= 0 ? 'cell-pos' : 'cell-neg';
            const retPct = p.return_pct != null ? (p.return_pct * 100).toFixed(1) + '%' : '-';
            const dteStr = p.front_dte != null ? p.front_dte + 'd' : '-';
            const dteClass = p.front_dte != null && p.front_dte <= 5 ? 'cell-neg' : p.front_dte != null && p.front_dte <= 14 ? 'cell-ff' : '';
            const putK = p.put_strike && p.put_strike !== p.strike ? '/' + Number(p.put_strike).toFixed(0) : '';

            html += `<tr data-ticker="${p.ticker}">
                <td class="cell-ticker">${p.ticker}</td>
                <td>${p.combo || '-'}</td>
                <td>${Number(p.strike).toFixed(0)}${putK}</td>
                <td>${p.contracts}</td>
                <td>${fmt.usd2(p.entry_cost)}</td>
                <td class="ws-current">${fmt.usd2(p.current_cost)}</td>
                <td class="ws-pnl ${pnlClass}">${fmt.pnl(p.unrealized_pnl)}</td>
                <td class="ws-ret ${pnlClass}">${retPct}</td>
                <td class="${dteClass}">${dteStr}</td>
                <td class="ws-stock">${fmt.usd2(p.stock_px)}</td>
            </tr>`;
        }

        // Total row
        if (nPriced > 0) {
            const pnlClass = totalPnl >= 0 ? 'cell-pos' : 'cell-neg';
            html += `<tr style="font-weight:700;background:var(--bg-secondary)" id="ws-total-row">
                <td>TOTAL</td><td></td><td></td><td></td><td></td><td></td>
                <td class="${pnlClass}" id="ws-total-pnl-cell">${fmt.pnl(totalPnl)}</td>
                <td></td><td></td><td></td>
            </tr>`;
        }

        html += '</tbody></table></div>';
    } else {
        html += `<div class="table-wrapper"><div style="padding: 40px; text-align: center; color: var(--text-secondary);">
            No active positions in portfolio. Place orders from the Signals tab.
        </div></div>`;
    }

    container.innerHTML = html;
}

function _updateMonitorDom(snapshot) {
    const positions = snapshot.positions || [];
    const errors = snapshot.errors || [];
    const totalPnl = snapshot.total_unrealized_pnl || 0;
    const nPriced = snapshot.n_priced || 0;
    const nWin = positions.filter(p => (p.unrealized_pnl || 0) > 0).length;
    const prev = _monitorLastSnapshot || {};
    const prevPositions = prev.positions || [];

    // Build lookup of previous values by ticker
    const prevByTicker = {};
    for (const p of prevPositions) {
        prevByTicker[p.ticker] = p;
    }

    // Update metric cards
    const totalPnlEl = document.getElementById('ws-total-pnl');
    if (totalPnlEl) {
        totalPnlEl.textContent = nPriced > 0 ? fmt.pnl(totalPnl) : '-';
        totalPnlEl.className = 'metric-value ' + (totalPnl >= 0 ? 'green' : 'red');
        if (prev.total_unrealized_pnl !== undefined && Math.abs(totalPnl - prev.total_unrealized_pnl) > 0.01) {
            _flashElement(totalPnlEl);
        }
    }

    const timeEl = document.getElementById('ws-price-time');
    if (timeEl) timeEl.textContent = snapshot.time ? 'Updated ' + snapshot.time : '';

    const tableTimeEl = document.getElementById('ws-table-time');
    if (tableTimeEl) tableTimeEl.textContent = 'Updated: ' + (snapshot.time || '-');

    const pricedEl = document.getElementById('ws-priced-info');
    if (pricedEl) pricedEl.textContent = nPriced > 0 ? nPriced + ' priced / ' + nWin + ' win' : '-';

    const winRateEl = document.getElementById('ws-win-rate');
    if (winRateEl) winRateEl.textContent = nPriced > 0 ? (nWin / nPriced * 100).toFixed(0) + '%' : '-';

    const errEl = document.getElementById('ws-errors');
    if (errEl) errEl.textContent = errors.length;

    const errListEl = document.getElementById('ws-error-list');
    if (errListEl) errListEl.textContent = errors.length > 0 ? errors.join(', ') : 'All OK';

    // Update table rows
    for (const p of positions) {
        const row = document.querySelector(`tr[data-ticker="${p.ticker}"]`);
        if (!row) continue;

        const prevP = prevByTicker[p.ticker] || {};
        const pnlClass = (p.unrealized_pnl || 0) >= 0 ? 'cell-pos' : 'cell-neg';
        const retPct = p.return_pct != null ? (p.return_pct * 100).toFixed(1) + '%' : '-';

        _updateCell(row, 'ws-current', fmt.usd2(p.current_cost), prevP.current_cost, p.current_cost);
        _updateCell(row, 'ws-pnl', fmt.pnl(p.unrealized_pnl), prevP.unrealized_pnl, p.unrealized_pnl, pnlClass);
        _updateCell(row, 'ws-ret', retPct, prevP.return_pct, p.return_pct, pnlClass);
        _updateCell(row, 'ws-stock', fmt.usd2(p.stock_px), prevP.stock_px, p.stock_px);
    }

    // Update total row
    const totalCell = document.getElementById('ws-total-pnl-cell');
    if (totalCell) {
        totalCell.textContent = fmt.pnl(totalPnl);
        totalCell.className = (totalPnl >= 0 ? 'cell-pos' : 'cell-neg');
    }
}

function _updateCell(row, cls, newText, oldVal, newVal, extraClass) {
    const cell = row.querySelector('.' + cls);
    if (!cell) return;
    cell.textContent = newText;
    if (extraClass) cell.className = cls + ' ' + extraClass;
    // Flash if value changed
    if (oldVal !== undefined && newVal !== undefined && oldVal !== newVal) {
        _flashElement(cell);
    }
}

function _flashElement(el) {
    el.classList.remove('ws-flash');
    // Force reflow to restart animation
    void el.offsetWidth;
    el.classList.add('ws-flash');
}

function wsForceRefresh() {
    if (_monitorWs && _monitorWs.readyState === WebSocket.OPEN) {
        _monitorWs.send(JSON.stringify({ action: 'refresh' }));
    }
}

async function loadMonitor() {
    // If WS is connected and we already have data, skip HTTP fetch
    if (_monitorWsConnected && _monitorLastSnapshot) {
        return;
    }

    // If WS is connecting, show a connecting state and let the WS handle it
    if (_monitorWs && _monitorWs.readyState === WebSocket.CONNECTING) {
        const container = document.getElementById('monitor-content');
        container.innerHTML = '<div class="loading"><div class="spinner"></div> Connecting live feed...</div>';
        return;
    }

    const container = document.getElementById('monitor-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div> Loading monitor...</div>';

    // Load portfolio positions + cached prices
    let monitorData = null;
    try {
        monitorData = await api('/api/monitor');
    } catch (e) {
        container.innerHTML = `<div class="loading">Error loading monitor: ${e.message}</div>`;
        return;
    }

    // Check if there's a previous simulation result
    let simData = null;
    try {
        const simStatus = await api('/api/monitor/simulate/status');
        if (simStatus.status === 'done' && simStatus.result) {
            simData = simStatus.result;
        }
    } catch (e) {}

    const active = monitorData.active || [];
    const cached = monitorData.cached_prices || {};
    const cachedDate = monitorData.cached_date;

    // Build the page
    let html = `
    <div class="section-header">
        <h2 class="section-title">P&L Monitor</h2>
        <div class="btn-group">
            <button class="btn" onclick="refreshMonitorPrices()" id="btn-monitor-refresh"
                ${monitorData.refresh_running ? 'disabled' : ''}>
                ${monitorData.refresh_running ? '<span class="btn-spinner"></span> Pricing...' : 'Refresh Prices (ThetaData)'}
            </button>
            <button class="btn btn-primary" onclick="runSimulation()" id="btn-simulate">
                Run Simulation
            </button>
        </div>
    </div>`;

    // ── Live Portfolio Section ──
    // Merge portfolio positions with cached prices
    let livePnl = 0;
    let nPriced = 0;
    let nWin = 0;
    let totalDeployed = 0;

    const merged = active.map(p => {
        const c = cached[p.ticker];
        const entry = p.cost_per_share || 0;
        const contracts = p.contracts || 0;
        totalDeployed += p.total_deployed || 0;

        if (c) {
            nPriced++;
            livePnl += c.unrealized_pnl || 0;
            if ((c.unrealized_pnl || 0) > 0) nWin++;
            return { ...p, ...c, has_price: true };
        }
        return { ...p, has_price: false };
    });

    const liveWinRate = nPriced > 0 ? (nWin / nPriced * 100) : 0;

    html += `
    <div class="metrics-row">
        <div class="metric-card ${livePnl >= 0 ? 'risk-kpi-green' : 'risk-kpi-red'}">
            <div class="metric-label">Live P&L</div>
            <div class="metric-value ${livePnl >= 0 ? 'green' : 'red'}">${nPriced > 0 ? fmt.pnl(livePnl) : '-'}</div>
            <div class="metric-sub">${cachedDate ? 'ThetaData ' + cachedDate : 'Click Refresh Prices'}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Active Positions</div>
            <div class="metric-value">${active.length}/20</div>
            <div class="metric-sub">${nPriced > 0 ? nPriced + ' priced / ' + nWin + ' win' : '-'}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Win Rate</div>
            <div class="metric-value accent">${nPriced > 0 ? liveWinRate.toFixed(0) + '%' : '-'}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Capital Deployed</div>
            <div class="metric-value">${totalDeployed > 0 ? fmt.usd(totalDeployed) : '-'}</div>
            <div class="metric-sub">${totalDeployed > 0 && livePnl !== 0 ? 'Return: ' + (livePnl / totalDeployed * 100).toFixed(2) + '%' : ''}</div>
        </div>
    </div>`;

    // Live positions table
    if (active.length > 0) {
        html += `<div class="table-wrapper">
        <div class="table-header">
            <span class="table-title">Live Portfolio (${active.length} positions)</span>
            ${cachedDate ? '<span style="font-size:12px;color:var(--text-muted)">Prices: ' + cachedDate + '</span>' : ''}
        </div>
        <table><thead><tr>
            <th>Ticker</th><th>Combo</th><th>Strike</th><th>Cts</th><th>Entry</th>
            <th>Current</th><th>P&L</th><th>Ret%</th><th>DTE</th><th>FF%</th>
            <th>Stock</th><th>Entry Date</th>
        </tr></thead><tbody>`;

        const sorted = [...merged].sort((a, b) => {
            if (a.has_price && b.has_price) return (b.unrealized_pnl || 0) - (a.unrealized_pnl || 0);
            if (a.has_price) return -1;
            if (b.has_price) return 1;
            return 0;
        });

        for (const p of sorted) {
            const dte = p.days_to_exp != null ? p.days_to_exp + 'd' : '-';
            const dteClass = p.days_to_exp != null && p.days_to_exp <= 5 ? 'cell-neg' : p.days_to_exp != null && p.days_to_exp <= 14 ? 'cell-ff' : '';
            const ffStr = p.ff != null ? (p.ff >= 1 ? p.ff.toFixed(0) : (p.ff * 100).toFixed(1)) + '%' : '-';
            const putK = p.put_strike && p.put_strike !== p.strike ? '/' + Number(p.put_strike).toFixed(0) : '';

            if (p.has_price) {
                const pnlClass = (p.unrealized_pnl || 0) >= 0 ? 'cell-pos' : 'cell-neg';
                const retPct = p.return_pct != null ? (p.return_pct * 100).toFixed(1) + '%' : '-';
                html += `<tr>
                    <td class="cell-ticker">${p.ticker}</td>
                    <td>${p.combo || '-'}</td>
                    <td>${Number(p.strike).toFixed(0)}${putK}</td>
                    <td>${p.contracts}</td>
                    <td>${fmt.usd2(p.entry_cost || p.cost_per_share)}</td>
                    <td>${fmt.usd2(p.current_cost)}</td>
                    <td class="${pnlClass}">${fmt.pnl(p.unrealized_pnl)}</td>
                    <td class="${pnlClass}">${retPct}</td>
                    <td class="${dteClass}">${dte}</td>
                    <td class="cell-ff">${ffStr}</td>
                    <td>${fmt.usd2(p.stock_px)}</td>
                    <td>${p.entry_date || '-'}</td>
                </tr>`;
            } else {
                html += `<tr>
                    <td class="cell-ticker">${p.ticker}</td>
                    <td>${p.combo || '-'}</td>
                    <td>${Number(p.strike).toFixed(0)}${putK}</td>
                    <td>${p.contracts}</td>
                    <td>${fmt.usd2(p.cost_per_share)}</td>
                    <td style="color:var(--text-muted)">--</td>
                    <td style="color:var(--text-muted)">--</td>
                    <td style="color:var(--text-muted)">--</td>
                    <td class="${dteClass}">${dte}</td>
                    <td class="cell-ff">${ffStr}</td>
                    <td style="color:var(--text-muted)">--</td>
                    <td>${p.entry_date || '-'}</td>
                </tr>`;
            }
        }

        // Total row
        if (nPriced > 0) {
            const pnlClass = livePnl >= 0 ? 'cell-pos' : 'cell-neg';
            html += `<tr style="font-weight:700;background:var(--bg-secondary)">
                <td>TOTAL</td><td></td><td></td><td></td><td></td><td></td>
                <td class="${pnlClass}">${fmt.pnl(livePnl)}</td>
                <td></td><td></td><td></td><td></td><td></td>
            </tr>`;
        }

        html += '</tbody></table></div>';
    } else {
        html += `<div class="table-wrapper"><div style="padding: 40px; text-align: center; color: var(--text-secondary);">
            No active positions in portfolio. Place orders from the Signals tab.
        </div></div>`;
    }

    // ── Simulation Section ──
    if (simData && simData.positions && simData.positions.length > 0) {
        html += `<div class="table-wrapper" style="margin-top: 24px;">
        <div class="table-header">
            <span class="table-title">Simulation (${simData.n_priced} positions from signals)</span>
            <span style="font-size:12px;color:var(--text-muted)">
                P&L: <span class="${simData.total_unrealized_pnl >= 0 ? 'cell-pos' : 'cell-neg'}">${fmt.pnl(simData.total_unrealized_pnl)}</span>
                &nbsp;|&nbsp; WR: ${simData.win_rate}%
                &nbsp;|&nbsp; Invested: ${fmt.usd(simData.total_invested)}
            </span>
        </div>
        <table><thead><tr>
            <th>Ticker</th><th>Combo</th><th>Cts</th><th>Entry</th><th>Current</th>
            <th>P&L</th><th>Ret%</th><th>DTE</th><th>Stock</th>
            <th>Entry Date</th><th>FF%</th><th>Stk Entry</th>
        </tr></thead><tbody>`;

        const simSorted = [...simData.positions].sort((a, b) => (b.unrealized_pnl || 0) - (a.unrealized_pnl || 0));
        for (const p of simSorted) {
            const pnlClass = (p.unrealized_pnl || 0) >= 0 ? 'cell-pos' : 'cell-neg';
            const dteStr = p.front_dte >= 0 ? p.front_dte + 'd' : 'exp';
            const retPct = p.return_pct != null ? (p.return_pct * 100).toFixed(1) + '%' : '-';
            const ffStr = p.ff != null ? (p.ff < 1 ? (p.ff * 100).toFixed(1) : p.ff.toFixed(0)) + '%' : '-';

            html += `<tr>
                <td class="cell-ticker">${p.ticker}</td>
                <td>${p.combo || '-'}</td>
                <td>${p.contracts}</td>
                <td>${fmt.usd2(p.entry_cost)}</td>
                <td>${fmt.usd2(p.current_cost)}</td>
                <td class="${pnlClass}">${fmt.pnl(p.unrealized_pnl)}</td>
                <td class="${pnlClass}">${retPct}</td>
                <td>${dteStr}</td>
                <td>${fmt.usd2(p.stock_px || p.stock_px_now)}</td>
                <td>${p.entry_date || '-'}</td>
                <td class="cell-ff">${ffStr}</td>
                <td>${p.stock_px_entry ? fmt.usd2(p.stock_px_entry) : '-'}</td>
            </tr>`;
        }

        const simTotal = simData.positions.reduce((s, p) => s + (p.unrealized_pnl || 0), 0);
        const simTotalClass = simTotal >= 0 ? 'cell-pos' : 'cell-neg';
        html += `<tr style="font-weight:700;background:var(--bg-secondary)">
            <td>TOTAL</td><td></td><td></td><td></td><td></td>
            <td class="${simTotalClass}">${fmt.pnl(simTotal)}</td>
            <td></td><td></td><td></td><td></td><td></td><td></td>
        </tr>`;

        html += '</tbody></table></div>';

        if (simData.errors && simData.errors.length > 0) {
            html += `<div style="padding: 4px 16px; color: var(--text-muted); font-size: 12px;">
                Failed to price: ${simData.errors.join(', ')}
            </div>`;
        }
    }

    // ── History Section ──
    try {
        const history = await api('/api/monitor/history');
        if (history.count > 0) {
            html += `<div class="table-wrapper" style="margin-top: 24px;">
            <div class="table-header"><span class="table-title">Snapshot History (${history.count})</span></div>
            <table><thead><tr>
                <th>Date</th><th>Type</th><th>Positions</th><th>Total P&L</th><th>File</th>
            </tr></thead><tbody>`;

            for (const s of history.snapshots.slice(0, 20)) {
                const pnlClass = (s.total_unrealized_pnl || 0) >= 0 ? 'cell-pos' : 'cell-neg';
                const nPos = (s.positions || []).length;
                const typeLabel = s.is_sim ? 'SIM' : 'LIVE';
                const typeClass = s.is_sim ? 'accent' : 'green';
                html += `<tr>
                    <td>${s.date || '-'}</td>
                    <td style="color:var(--${typeClass});font-weight:700">${typeLabel}</td>
                    <td>${nPos}</td>
                    <td class="${pnlClass}">${fmt.pnl(s.total_unrealized_pnl)}</td>
                    <td style="color:var(--text-muted);font-size:11px">${s.file || '-'}</td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }
    } catch (e) {}

    container.innerHTML = html;

    // Badge = number of active positions
    const badge = document.getElementById('badge-monitor');
    if (badge) badge.textContent = active.length > 0 ? active.length : '';

    // If refresh was running, start polling
    if (monitorData.refresh_running) {
        pollRefreshStatus(function() { loadMonitor(); });
    }
}


function pollRefreshStatus(onComplete) {
    const monBtn = document.getElementById('btn-monitor-refresh');
    const portBtn = document.getElementById('btn-portfolio-refresh');
    if (monBtn) { monBtn.disabled = true; monBtn.innerHTML = '<span class="btn-spinner"></span> Pricing...'; }
    if (portBtn) { portBtn.disabled = true; portBtn.innerHTML = '<span class="btn-spinner"></span> Pricing...'; }

    refreshPollTimer = setInterval(async () => {
        try {
            const status = await api('/api/monitor/refresh/status');
            if (status.status === 'done') {
                clearInterval(refreshPollTimer);
                refreshPollTimer = null;
                if (onComplete) onComplete();
                else loadMonitor();
            } else if (status.status === 'error') {
                clearInterval(refreshPollTimer);
                refreshPollTimer = null;
                alert('Refresh error: ' + (status.error || 'Unknown'));
                if (onComplete) onComplete();
                else loadMonitor();
            }
        } catch (e) {
            clearInterval(refreshPollTimer);
            refreshPollTimer = null;
            if (monBtn) { monBtn.disabled = false; monBtn.textContent = 'Refresh Prices (ThetaData)'; }
            if (portBtn) { portBtn.disabled = false; portBtn.textContent = 'Refresh Prices'; }
        }
    }, 3000);
}


async function refreshMonitorPrices() {
    const btn = document.getElementById('btn-monitor-refresh');
    if (!btn) return;

    btn.disabled = true;
    btn.innerHTML = '<span class="btn-spinner"></span> Pricing...';

    try {
        await api('/api/monitor/refresh', { method: 'POST' });
        pollRefreshStatus(function() { loadMonitor(); });
    } catch (e) {
        btn.disabled = false;
        btn.textContent = 'Refresh Prices (ThetaData)';
        alert('Monitor error: ' + e.message);
    }
}


async function runSimulation() {
    const btn = document.getElementById('btn-simulate');
    if (!btn) return;

    btn.disabled = true;
    btn.innerHTML = '<span class="btn-spinner"></span> Simulating...';

    try {
        await api('/api/monitor/simulate', { method: 'POST' });

        // Poll for completion
        simPollTimer = setInterval(async () => {
            try {
                const status = await api('/api/monitor/simulate/status');
                if (status.status === 'done') {
                    clearInterval(simPollTimer);
                    simPollTimer = null;
                    btn.disabled = false;
                    btn.textContent = 'Run Simulation';
                    loadMonitor();
                } else if (status.status === 'error') {
                    clearInterval(simPollTimer);
                    simPollTimer = null;
                    btn.disabled = false;
                    btn.textContent = 'Run Simulation';
                    alert('Simulation error: ' + (status.error || 'Unknown'));
                    loadMonitor();
                }
            } catch (e) {
                clearInterval(simPollTimer);
                simPollTimer = null;
                btn.disabled = false;
                btn.textContent = 'Run Simulation';
            }
        }, 3000);
    } catch (e) {
        btn.disabled = false;
        btn.textContent = 'Run Simulation';
        alert('Simulation error: ' + e.message);
    }
}


// ── Init ──
// ── Vol Ramp (Earnings Straddle) ──
async function loadStraddle() {
    const container = document.getElementById('straddle-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div> Computing straddle analytics (first load may take a few minutes)...</div>';

    try {
        const data = await api('/api/straddle');

        if (data.error) {
            container.innerHTML = `<div class="loading">${data.error}</div>`;
            return;
        }

        const s = data.stats || {};
        const m = data.model || {};
        const h = data.history_stats || {};
        const scanner = data.scanner || [];
        const charts = data.charts || [];
        const trades = data.recent_trades || [];

        // Badge
        const badge = document.getElementById('badge-straddle');
        if (badge && scanner.length > 0) badge.textContent = scanner.length;

        let html = '';

        // ── KPI Cards ──
        html += '<div class="metrics-row">';
        html += `<div class="metric-card risk-kpi-green">
            <div class="metric-label">Events</div>
            <div class="metric-value">${fmt.num(h.n_total_events)}</div>
            <div class="metric-sub">${h.tickers || '-'} tickers</div></div>`;
        html += `<div class="metric-card">
            <div class="metric-label">Tradeable</div>
            <div class="metric-value">${fmt.num(h.n_tradeable)}</div>
            <div class="metric-sub">pred > 0</div></div>`;
        html += `<div class="metric-card ${s.win_rate >= 40 ? 'risk-kpi-green' : 'risk-kpi-red'}">
            <div class="metric-label">Win Rate</div>
            <div class="metric-value">${fmt.pct(s.win_rate)}</div>
            <div class="metric-sub">${s.n_trades || '-'} trades</div></div>`;
        html += `<div class="metric-card ${s.mean_return >= 0 ? 'risk-kpi-green' : 'risk-kpi-red'}">
            <div class="metric-label">Mean Return</div>
            <div class="metric-value">${fmt.pct2(s.mean_return)}</div>
            <div class="metric-sub">median ${fmt.pct2(s.median_return)}</div></div>`;
        html += `<div class="metric-card">
            <div class="metric-label">Sharpe</div>
            <div class="metric-value">${s.sharpe != null ? s.sharpe.toFixed(2) : '-'}</div>
            <div class="metric-sub">CAGR ${fmt.pct2(s.cagr)}</div></div>`;
        html += `<div class="metric-card risk-kpi-red">
            <div class="metric-label">Max DD</div>
            <div class="metric-value">${fmt.pct(s.max_drawdown)}</div>
            <div class="metric-sub">skew ${s.skewness != null ? s.skewness.toFixed(2) : '-'}</div></div>`;
        html += '</div>';

        // ── Scanner: Upcoming Opportunities ──
        if (scanner.length > 0) {
            html += '<div class="table-wrapper"><div class="table-header">Upcoming Opportunities (' + scanner.length + ')</div>';
            html += '<table><thead><tr><th>Ticker</th><th>Earnings</th><th>Days</th><th>Timing</th><th>Expiration</th>';
            html += '<th>Hist Events</th><th>Avg Impl%</th><th>Avg Real%</th><th>Pred Return</th></tr></thead><tbody>';
            for (const o of scanner) {
                const predClass = o.predicted_return > 0 ? 'cell-pos' : 'cell-neg';
                html += `<tr>
                    <td><strong>${o.root}</strong></td>
                    <td>${o.report_date_str}</td>
                    <td>${o.days_to_earnings}d</td>
                    <td>${o.before_after || '-'}</td>
                    <td>${o.expiration_str}</td>
                    <td>${o.n_historical_events}</td>
                    <td>${o.avg_implied_move}%</td>
                    <td>${o.avg_realized_move}%</td>
                    <td class="${predClass}"><strong>${o.predicted_return > 0 ? '+' : ''}${o.predicted_return}%</strong></td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        } else {
            html += '<div class="table-wrapper"><div class="table-header">Upcoming Opportunities</div>';
            html += '<div style="padding:20px;color:var(--text-secondary)">No earnings in the 10-18 day window right now</div></div>';
        }

        // ── Charts (2x2 grid) ──
        if (charts.length > 0) {
            html += '<div class="risk-charts-grid">';
            for (const c of charts) {
                html += `<div class="chart-card"><img src="/output/${c}?t=${Date.now()}" alt="${c}" style="width:100%;"></div>`;
            }
            html += '</div>';
        }

        // ── Two-column tables ──
        html += '<div class="kelly-grid">';

        // Model Coefficients
        html += '<div class="table-wrapper"><div class="table-header">Regression Model</div>';
        html += '<table><thead><tr><th>Signal</th><th>Coefficient</th></tr></thead><tbody>';
        const sigLabels = {
            'sig_impl_vs_last_impl': 'Implied / Last Implied',
            'sig_impl_minus_last_real': 'Implied - Last Realized',
            'sig_impl_minus_avg_real': 'Implied - Avg Realized',
            'sig_impl_vs_avg_impl': 'Implied / Avg Implied',
        };
        if (m.coefficients) {
            for (const [k, v] of Object.entries(m.coefficients)) {
                const label = sigLabels[k] || k;
                const cls = v < 0 ? 'cell-neg' : 'cell-pos';
                html += `<tr><td>${label}</td><td class="${cls}">${v > 0 ? '+' : ''}${v.toFixed(4)}</td></tr>`;
            }
        }
        html += `<tr><td>Intercept</td><td>${m.intercept != null ? m.intercept.toFixed(4) : '-'}</td></tr>`;
        html += `<tr><td>R&sup2; (in-sample)</td><td>${m.r2 != null ? m.r2.toFixed(4) : '-'}</td></tr>`;
        html += `<tr><td>OOS Correlation</td><td class="accent">${m.oos_correlation != null ? m.oos_correlation.toFixed(4) : '-'}</td></tr>`;
        html += `<tr><td>OOS p-value</td><td>${m.oos_pvalue != null ? m.oos_pvalue.toFixed(4) : '-'}</td></tr>`;
        html += `<tr><td>Training Events</td><td>${fmt.num(m.n_train)}</td></tr>`;
        html += '</tbody></table></div>';

        // Backtest Stats
        html += '<div class="table-wrapper"><div class="table-header">Backtest Performance</div>';
        html += '<table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>';
        const statsRows = [
            ['Trades', fmt.num(s.n_trades)],
            ['Win Rate', fmt.pct(s.win_rate)],
            ['Mean Return', fmt.pct2(s.mean_return)],
            ['Median Return', fmt.pct2(s.median_return)],
            ['Std Return', fmt.pct2(s.std_return)],
            ['Skewness', s.skewness != null ? s.skewness.toFixed(2) : '-'],
            ['CAGR', fmt.pct2(s.cagr)],
            ['Sharpe', s.sharpe != null ? s.sharpe.toFixed(2) : '-'],
            ['Max Drawdown', fmt.pct(s.max_drawdown)],
            ['Total P&L', fmt.pnl(s.total_pnl)],
            ['Initial Capital', fmt.usd(s.initial_capital)],
            ['Final Equity', fmt.usd(s.final_equity)],
        ];
        for (const [label, val] of statsRows) {
            html += `<tr><td>${label}</td><td>${val}</td></tr>`;
        }
        html += '</tbody></table></div>';

        html += '</div>'; // kelly-grid

        // ── History Stats ──
        html += '<div class="table-wrapper"><div class="table-header">Data Summary</div>';
        html += '<table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>';
        html += `<tr><td>Total Earnings Events</td><td>${fmt.num(h.n_total_events)}</td></tr>`;
        html += `<tr><td>With Valid Signals</td><td>${fmt.num(h.n_with_signals)}</td></tr>`;
        html += `<tr><td>Tradeable (pred > 0)</td><td>${fmt.num(h.n_tradeable)}</td></tr>`;
        html += `<tr><td>Date Range</td><td>${h.date_range || '-'}</td></tr>`;
        html += `<tr><td>Tickers</td><td>${h.tickers || '-'}</td></tr>`;
        html += `<tr><td>Avg Implied Move</td><td>${h.avg_implied_move}%</td></tr>`;
        html += `<tr><td>Avg Realized Move</td><td>${h.avg_realized_move}%</td></tr>`;
        html += '</tbody></table></div>';

        // ── Recent Trades ──
        if (trades.length > 0) {
            html += '<div class="table-wrapper"><div class="table-header">Recent Trades (last 20)</div>';
            html += '<table><thead><tr><th>Ticker</th><th>Entry</th><th>Exit</th><th>Ctr</th>';
            html += '<th>Entry $</th><th>Exit $</th><th>P&L</th><th>Return</th><th>Impl Move</th></tr></thead><tbody>';
            for (const t of trades) {
                const retClass = t.return_pct >= 0 ? 'cell-pos' : 'cell-neg';
                html += `<tr>
                    <td><strong>${t.root}</strong></td>
                    <td>${String(t.entry_date).slice(4,6)}-${String(t.entry_date).slice(6)}</td>
                    <td>${String(t.exit_date).slice(4,6)}-${String(t.exit_date).slice(6)}</td>
                    <td>${t.contracts}</td>
                    <td>${fmt.usd2(t.entry_price)}</td>
                    <td>${fmt.usd2(t.exit_price)}</td>
                    <td class="${retClass}">${fmt.pnl(t.pnl)}</td>
                    <td class="${retClass}">${t.return_pct > 0 ? '+' : ''}${t.return_pct}%</td>
                    <td>${(t.implied_move * 100).toFixed(1)}%</td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        container.innerHTML = html;

    } catch (e) {
        container.innerHTML = `<div class="loading">Error: ${e.message}</div>`;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // Nav clicks
    document.querySelectorAll('.nav-item').forEach(el => {
        el.addEventListener('click', () => switchTab(el.dataset.tab));
    });

    // Connection button
    document.getElementById('conn-btn').addEventListener('click', connectIBKR);

    // Initial load
    refreshStatus();
    switchTab('home');

    // Auto-refresh every 30s
    refreshTimer = setInterval(refreshStatus, 30000);

    // Show autopilot confirmation modal on first load of session
    if (!sessionStorage.getItem('autopilot_prompted')) {
        setTimeout(() => {
            document.getElementById('autopilot-modal').classList.add('show');
        }, 800);
    }
});
