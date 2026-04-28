// ══════════════════════════════════════════
//  Calendar Spread Trading — Frontend
// ══════════════════════════════════════════

const API = '';
let state = { connected: false, account: null, accountValue: 1023443 };
let refreshTimer = null;
let scannerPollTimer = null;
let _signalsList = [];  // cached for add-to-portfolio modal
let _pendingSignal = null;  // signal being added

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
    loadTab(tabId);
}

function loadTab(tabId) {
    const loaders = {
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

async function closePosition(positionId, ticker) {
    if (!confirm(`Close position ${ticker}? This will record the P&L based on latest cached price.`)) return;

    try {
        const result = await api('/api/portfolio/close', {
            method: 'POST',
            body: JSON.stringify({ position_id: positionId }),
        });
        showToast(`Closed ${result.ticker}: P&L ${fmt.pnl(result.pnl)} (${(result.return_pct * 100).toFixed(1)}%)`);
        loadPortfolio();
        refreshStatus();
    } catch (e) {
        alert('Close error: ' + e.message);
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
            await refreshStatus();
            loadTab('signals');
        }
        return;
    }

    const modal = document.getElementById('connect-modal');
    modal.classList.add('show');
}

async function doConnect() {
    const host = document.getElementById('ibkr-host').value;
    const port = parseInt(document.getElementById('ibkr-port').value);
    const modal = document.getElementById('connect-modal');
    const btn = document.getElementById('connect-go');

    btn.disabled = true;
    btn.textContent = 'Connecting...';

    try {
        const data = await api('/api/connect', {
            method: 'POST',
            body: JSON.stringify({ host, port }),
        });
        modal.classList.remove('show');
        state.connected = true;
        state.account = data.account;
        if (data.account_value) state.accountValue = data.account_value;
        await refreshStatus();
        loadTab('signals');
    } catch (e) {
        alert('Connection failed: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Connect';
    }
}

function closeModal(id) {
    document.getElementById(id).classList.remove('show');
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
    document.getElementById('add-position-modal').classList.add('show');
}

async function confirmAddPosition() {
    if (!_pendingSignal) return;
    const s = _pendingSignal;
    const contracts = parseInt(document.getElementById('add-pos-contracts').value) || 1;

    const confirmBtn = document.getElementById('add-pos-confirm');
    confirmBtn.disabled = true;
    confirmBtn.textContent = 'Adding...';

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
    };

    try {
        await api('/api/portfolio/add', {
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
        await refreshStatus();
    } catch (e) {
        alert('Error: ' + e.message);
    } finally {
        confirmBtn.disabled = false;
        confirmBtn.textContent = 'Add';
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

async function loadMonitor() {
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
    switchTab('signals');

    // Auto-refresh every 30s
    refreshTimer = setInterval(refreshStatus, 30000);
});
