// ══════════════════════════════════════════
//  Calendar Spread Trading — Frontend
// ══════════════════════════════════════════

const API = '';
let state = { connected: false, account: null, accountValue: 1023443 };
let refreshTimer = null;
let scannerPollTimer = null;

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
        kelly: loadKelly,
        risk: loadRisk,
        straddle: loadStraddle,
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
            btn.innerHTML = `<span class="status-dot"></span> ${data.account}`;
        } else {
            btn.classList.remove('connected');
            btn.innerHTML = `<span class="status-dot"></span> Connect IBKR`;
        }

        // Update sidebar stats
        document.getElementById('stat-account').textContent = fmt.usd(state.accountValue);
        document.getElementById('stat-kelly').textContent = (data.kelly_f * 100).toFixed(1) + '%';
        document.getElementById('stat-positions').textContent = `${data.n_active}/${data.max_positions}`;
        document.getElementById('stat-deployed').textContent = fmt.usd(data.total_deployed);

        // Update signal badge
        const sigBadge = document.getElementById('badge-signals');
        if (sigBadge) sigBadge.textContent = '';  // will update when signals load
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
                <button class="btn" onclick="runScanner()" id="btn-scan">
                    Run Scanner
                </button>
                <button class="btn btn-primary" onclick="showOrderConfirm()" id="btn-place"
                    ${!state.connected ? 'disabled title="Connect IBKR first"' : ''}>
                    Place Orders
                </button>
                <button class="btn btn-primary" onclick="scanAndEnter()" id="btn-scan-enter"
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
                <span class="table-title">Kelly-Optimized Sizing (${data.n_positions} positions)</span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Ticker</th><th>Combo</th><th>Strike</th><th>Stock</th>
                        <th>FF%</th><th>Type</th><th>Ctr</th><th>Deployed</th>
                        <th>Front</th><th>Back</th>
                        <th>IV F/B</th>
                        <th>Call</th><th>Put</th><th>Total</th><th>Vol</th>
                    </tr>
                </thead>
                <tbody>`;

        for (const s of data.sizing) {
            const ffClass = s.ff > 100 ? 'green' : s.ff > 50 ? 'accent' : '';
            const ivStr = (s.front_iv && s.back_iv) ? `${s.front_iv.toFixed(0)}/${s.back_iv.toFixed(0)}` : '-';
            const isDbl = s.dbl_cost && s.dbl_cost > 0;
            const typeLabel = isDbl ? 'DBL' : 'SGL';
            const typeClass = isDbl ? 'cell-pos' : 'cell-neg';
            const totalCost = isDbl ? s.dbl_cost : s.call_cost;
            html += `<tr>
                <td class="cell-ticker">${s.ticker}</td>
                <td>${s.combo || '-'}</td>
                <td>${fmt.usd(s.strike)}</td>
                <td>${fmt.usd2(s.stock_px)}</td>
                <td class="cell-ff ${ffClass}">${fmt.pct(s.ff)}</td>
                <td class="${typeClass}" style="font-weight:700">${typeLabel}</td>
                <td>${s.contracts}</td>
                <td>${fmt.usd(s.deployed)}</td>
                <td>${s.front_exp ? s.front_exp.substring(5) : '-'}</td>
                <td>${s.back_exp ? s.back_exp.substring(5) : '-'}</td>
                <td>${ivStr}</td>
                <td>${s.call_cost ? fmt.usd2(s.call_cost) : '-'}</td>
                <td>${s.put_cost ? fmt.usd2(s.put_cost) : '<span class="cell-neg">--</span>'}</td>
                <td class="accent" style="font-weight:700">${totalCost ? fmt.usd2(totalCost) : '-'}</td>
                <td>${s.volume != null ? s.volume : '-'}</td>
            </tr>`;
        }

        html += '</tbody></table></div>';
        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = `<div class="loading">Error loading signals: ${e.message}</div>`;
    }
}

// ── Portfolio Tab ──
async function loadPortfolio() {
    const container = document.getElementById('portfolio-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div> Loading portfolio...</div>';

    try {
        const data = await api('/api/portfolio');
        const badge = document.getElementById('badge-portfolio');
        if (badge) badge.textContent = data.n_active || '';

        let html = `
        <div class="section-header">
            <h2 class="section-title">Active Portfolio</h2>
            <div class="btn-group">
                <button class="btn btn-danger" onclick="closeExpiring()" ${!state.connected ? 'disabled' : ''}>
                    Close Expiring
                </button>
            </div>
        </div>

        <div class="metrics-row">
            <div class="metric-card">
                <div class="metric-label">Active Positions</div>
                <div class="metric-value">${data.n_active}/20</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Deployed</div>
                <div class="metric-value">${fmt.usd(data.total_deployed)}</div>
            </div>
        </div>`;

        if (data.active.length === 0) {
            html += `<div class="table-wrapper"><div style="padding: 40px; text-align: center; color: var(--text-secondary);">
                No active positions. Place orders from the Signals tab.
            </div></div>`;
        } else {
            html += `<div class="table-wrapper">
            <div class="table-header"><span class="table-title">Active Positions</span></div>
            <table><thead><tr>
                <th>Ticker</th><th>Combo</th><th>Strike</th><th>Type</th>
                <th>Ctr</th><th>Cost</th><th>Deployed</th><th>FF%</th>
                <th>Front Exp</th><th>Days</th><th>Entry</th>
            </tr></thead><tbody>`;

            for (const p of data.active) {
                const daysClass = p.days_to_exp <= 5 ? 'cell-neg' : p.days_to_exp <= 14 ? 'cell-ff' : '';
                html += `<tr>
                    <td class="cell-ticker">${p.ticker}</td>
                    <td>${p.combo}</td>
                    <td>${fmt.usd(p.strike)}</td>
                    <td>${p.spread_type}</td>
                    <td>${p.contracts}</td>
                    <td>${fmt.usd2(p.cost_per_share)}</td>
                    <td>${fmt.usd(p.total_deployed)}</td>
                    <td class="cell-ff">${fmt.pct(p.ff)}</td>
                    <td>${p.front_exp}</td>
                    <td class="${daysClass}">${p.days_to_exp}d</td>
                    <td>${p.entry_date}</td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        if (data.closed.length > 0) {
            html += `<div class="table-wrapper" style="margin-top: 24px;">
            <div class="table-header"><span class="table-title">Closed Positions (${data.closed.length})</span></div>
            <table><thead><tr>
                <th>Ticker</th><th>Combo</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Return</th>
            </tr></thead><tbody>`;
            for (const p of data.closed) {
                const pnlClass = (p.pnl || 0) >= 0 ? 'cell-pos' : 'cell-neg';
                html += `<tr>
                    <td class="cell-ticker">${p.ticker}</td>
                    <td>${p.combo}</td>
                    <td>${p.entry_date}</td>
                    <td>${p.exit_date || '-'}</td>
                    <td class="${pnlClass}">${fmt.pnl(p.pnl)}</td>
                    <td class="${pnlClass}">${p.exit_price ? fmt.pct2((p.exit_price - p.cost_per_share) / p.cost_per_share * 100) : '-'}</td>
                </tr>`;
            }
            html += '</tbody></table></div>';
        }

        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = `<div class="loading">Error: ${e.message}</div>`;
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
async function loadKelly() {
    const container = document.getElementById('kelly-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div> Loading Kelly data...</div>';

    try {
        const data = await api(`/api/kelly?account_value=${state.accountValue}`);

        const spreadPct = (data.kelly_f * 100).toFixed(1);
        const cashPct = (100 - data.kelly_f * 100).toFixed(1);

        let html = `
        <div class="section-header">
            <h2 class="section-title">Kelly Criterion & Capital Allocation</h2>
        </div>

        <div class="kelly-grid">
            <div class="table-wrapper">
                <div class="table-header"><span class="table-title">Kelly Parameters</span></div>
                <table class="kelly-table"><tbody>
                    <tr><td>Backtest trades</td><td>${data.bt_trades}</td></tr>
                    <tr><td>Live trades</td><td>${data.live_trades}</td></tr>
                    <tr><td>Total trades</td><td style="color:var(--accent)">${data.n_trades}</td></tr>
                    <tr><td>Mean return (mu)</td><td>${(data.mu * 100).toFixed(2)}%</td></tr>
                    <tr><td>Std dev</td><td>${(data.std * 100).toFixed(2)}%</td></tr>
                    <tr><td>Variance</td><td>${data.var.toFixed(4)}</td></tr>
                    <tr><td>Full Kelly f*</td><td>${(data.kelly_full * 100).toFixed(2)}%</td></tr>
                    <tr><td>Half Kelly f/2</td><td style="color:var(--accent);font-size:18px">${(data.kelly_f * 100).toFixed(2)}%</td></tr>
                </tbody></table>
            </div>

            <div class="table-wrapper">
                <div class="table-header"><span class="table-title">Capital Allocation</span></div>
                <table class="kelly-table"><tbody>
                    <tr><td>Account value</td><td>${fmt.usd(data.account_value)}</td></tr>
                    <tr><td>Kelly target (spreads)</td><td style="color:var(--accent)">${fmt.usd(data.kelly_target)}</td></tr>
                    <tr><td>Alloc per position</td><td>${fmt.usd(data.alloc_per_pos)}</td></tr>
                    <tr><td>Cash reserve</td><td style="color:var(--green)">${fmt.usd(data.cash_reserve)}</td></tr>
                    <tr><td>Risk-free rate</td><td>${(data.rf_rate * 100).toFixed(1)}%</td></tr>
                    <tr><td>RF income / year</td><td style="color:var(--green)">${fmt.usd(data.rf_income)}/yr</td></tr>
                </tbody></table>
            </div>
        </div>

        <div class="alloc-bar">
            <div class="alloc-segment alloc-spreads" style="width: ${spreadPct}%">Spreads ${spreadPct}%</div>
            <div class="alloc-segment alloc-cash" style="width: ${cashPct}%">Cash @ ${(data.rf_rate*100).toFixed(1)}% = ${cashPct}%</div>
        </div>

        <div class="table-wrapper" style="margin-top: 24px;">
            <div class="table-header"><span class="table-title">Projected Annual Returns</span></div>
            <table>
                <thead><tr><th>Source</th><th>Allocation</th><th>Rate</th><th>Contribution</th><th>$ Amount</th></tr></thead>
                <tbody>
                    <tr>
                        <td>Calendar Spreads</td>
                        <td>${spreadPct}%</td>
                        <td class="cell-pos">${(data.spread_cagr * 100).toFixed(1)}%</td>
                        <td class="cell-pos">${(data.spread_cagr * 100).toFixed(1)}%</td>
                        <td class="cell-pos">${fmt.usd(data.spread_cagr * data.account_value)}</td>
                    </tr>
                    <tr>
                        <td>Risk-Free (T-Bills)</td>
                        <td>${cashPct}%</td>
                        <td>${(data.rf_rate * 100).toFixed(1)}%</td>
                        <td>${(data.rf_rate * (1 - data.kelly_f) * 100).toFixed(1)}%</td>
                        <td>${fmt.usd(data.rf_income)}</td>
                    </tr>
                    <tr style="font-weight:700;font-size:14px">
                        <td>Combined</td>
                        <td>100%</td>
                        <td></td>
                        <td class="cell-pos">${(data.combined_cagr * 100).toFixed(1)}%</td>
                        <td class="cell-pos">${fmt.usd(data.combined_cagr * data.account_value)}</td>
                    </tr>
                </tbody>
            </table>
        </div>`;

        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = `<div class="loading">Error: ${e.message}</div>`;
    }
}

// ── Risk & Analytics Tab ──
async function loadRisk() {
    const container = document.getElementById('risk-content');
    container.innerHTML = '<div class="loading"><div class="spinner"></div> Computing risk analytics...</div>';

    try {
        const data = await api(`/api/risk?account_value=${state.accountValue}`);

        if (data.error) {
            container.innerHTML = `<div class="loading">${data.error} (${data.n_trades} trades)</div>`;
            return;
        }

        const mc = data.monte_carlo;
        const risk = data.risk_metrics;
        const edge = data.edge;
        const dist = data.distribution;

        let html = `
        <div class="section-header">
            <h2 class="section-title">Risk & Analytics</h2>
            <div class="btn-group">
                <button class="btn" onclick="loadRisk()">Refresh</button>
            </div>
        </div>

        <!-- KPI Cards -->
        <div class="metrics-row">
            <div class="metric-card risk-kpi-green">
                <div class="metric-label">Prob(Profit)</div>
                <div class="metric-value green">${(mc.prob_profit * 100).toFixed(1)}%</div>
                <div class="metric-sub">${mc.n_sims.toLocaleString()} MC sims, ${mc.n_trades} trades</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Median CAGR</div>
                <div class="metric-value accent">${(mc.median_cagr * 100).toFixed(1)}%</div>
                <div class="metric-sub">5th: ${(mc.p5_cagr * 100).toFixed(1)}% | 95th: ${(mc.p95_cagr * 100).toFixed(1)}%</div>
            </div>
            <div class="metric-card risk-kpi-red">
                <div class="metric-label">VaR 95% (portfolio)</div>
                <div class="metric-value red">${(risk.var_95_portfolio * 100).toFixed(2)}%</div>
                <div class="metric-sub">${fmt.usd(risk.var_95_dollar)} per position (${(risk.var_95 * 100).toFixed(0)}% of cost)</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Max DD (median)</div>
                <div class="metric-value red">${(mc.max_dd_median * 100).toFixed(1)}%</div>
                <div class="metric-sub">5th: ${(mc.max_dd_p5 * 100).toFixed(1)}% | 95th: ${(mc.max_dd_p95 * 100).toFixed(1)}%</div>
            </div>
            <div class="metric-card ${edge.permutation_pvalue < 0.05 ? 'risk-kpi-green' : 'risk-kpi-red'}">
                <div class="metric-label">Edge p-value</div>
                <div class="metric-value ${edge.permutation_pvalue < 0.05 ? 'green' : 'red'}">${edge.permutation_pvalue.toFixed(4)}</div>
                <div class="metric-sub">${edge.permutation_pvalue < 0.05 ? 'Significant' : 'Not significant'} (permutation test)</div>
            </div>
        </div>

        <!-- Charts: 2x2 grid -->
        <div class="risk-charts-grid">`;

        if (data.charts) {
            for (const chart of data.charts) {
                const title = chart.includes('fan') ? 'Monte Carlo Fan Chart' :
                              chart.includes('terminal') ? 'Terminal Wealth Distribution' :
                              chart.includes('edge') ? 'Edge Persistence' : 'Return Distribution';
                html += `<div class="table-wrapper" style="padding: 12px;">
                    <div class="table-header"><span class="table-title">${title}</span></div>
                    <img src="${chart}?t=${Date.now()}" style="width: 100%; border-radius: 6px; margin-top: 8px;" alt="${title}">
                </div>`;
            }
        }

        html += `</div>

        <!-- Risk Metrics Table -->
        <div class="kelly-grid">
            <div class="table-wrapper">
                <div class="table-header"><span class="table-title">Value at Risk</span></div>
                <table class="kelly-table"><tbody>
                    <tr><td>Position allocation</td><td>${fmt.usd(risk.pos_alloc)}</td></tr>
                    <tr><td>VaR 95% (trade)</td><td class="cell-neg">${(risk.var_95 * 100).toFixed(1)}%</td></tr>
                    <tr><td>VaR 99% (trade)</td><td class="cell-neg">${(risk.var_99 * 100).toFixed(1)}%</td></tr>
                    <tr><td>VaR 95% (portfolio)</td><td class="cell-neg">${(risk.var_95_portfolio * 100).toFixed(3)}%</td></tr>
                    <tr><td>VaR 99% (portfolio)</td><td class="cell-neg">${(risk.var_99_portfolio * 100).toFixed(3)}%</td></tr>
                    <tr><td>CVaR 95% ($ per position)</td><td class="cell-neg">${fmt.usd(risk.cvar_95_dollar)}</td></tr>
                    <tr><td>CVaR 99% ($ per position)</td><td class="cell-neg">${fmt.usd(risk.cvar_99_dollar)}</td></tr>
                    <tr><td>Worst trade</td><td class="cell-neg">${(risk.worst_trade * 100).toFixed(1)}%</td></tr>
                    <tr><td>Best trade</td><td class="cell-pos">${(risk.best_trade * 100).toFixed(1)}%</td></tr>
                </tbody></table>
            </div>

            <div class="table-wrapper">
                <div class="table-header"><span class="table-title">Monte Carlo Summary</span></div>
                <table class="kelly-table"><tbody>
                    <tr><td>Prob(Profit)</td><td class="cell-pos">${(mc.prob_profit * 100).toFixed(1)}%</td></tr>
                    <tr><td>Prob(2x)</td><td class="cell-pos">${(mc.prob_double * 100).toFixed(1)}%</td></tr>
                    <tr><td>Prob(>20% loss)</td><td class="cell-neg">${(mc.prob_loss_20 * 100).toFixed(1)}%</td></tr>
                    <tr><td>Median terminal</td><td>${fmt.usd(mc.terminal_median)}</td></tr>
                    <tr><td>5th pctl terminal</td><td class="cell-neg">${fmt.usd(mc.terminal_p5)}</td></tr>
                    <tr><td>95th pctl terminal</td><td class="cell-pos">${fmt.usd(mc.terminal_p95)}</td></tr>
                    <tr><td>Median CAGR</td><td class="cell-pos">${(mc.median_cagr * 100).toFixed(1)}%</td></tr>
                    <tr><td>Max DD (median)</td><td class="cell-neg">${(mc.max_dd_median * 100).toFixed(1)}%</td></tr>
                </tbody></table>
            </div>
        </div>

        <!-- Distribution Stats -->
        <div class="kelly-grid">
            <div class="table-wrapper">
                <div class="table-header"><span class="table-title">Distribution</span></div>
                <table class="kelly-table"><tbody>
                    <tr><td>Mean return</td><td class="cell-pos">${(dist.mean * 100).toFixed(2)}%</td></tr>
                    <tr><td>Std dev</td><td>${(dist.std * 100).toFixed(2)}%</td></tr>
                    <tr><td>Skewness</td><td>${dist.skewness.toFixed(3)}</td></tr>
                    <tr><td>Excess kurtosis</td><td>${dist.kurtosis.toFixed(3)}</td></tr>
                    <tr><td>Jarque-Bera stat</td><td>${dist.jarque_bera.toFixed(1)}</td></tr>
                    <tr><td>JB p-value</td><td>${dist.jb_pvalue < 0.05 ? '<span class="cell-neg">'+dist.jb_pvalue.toFixed(4)+'</span> (non-normal)' : '<span class="cell-pos">'+dist.jb_pvalue.toFixed(4)+'</span> (normal)'}</td></tr>
                    <tr><td>Win rate</td><td>${(dist.win_rate * 100).toFixed(1)}%</td></tr>
                    <tr><td>N trades</td><td>${dist.n_trades}</td></tr>
                </tbody></table>
            </div>

            <div class="table-wrapper">
                <div class="table-header"><span class="table-title">Edge Persistence</span></div>
                <table class="kelly-table"><tbody>
                    <tr><td>Current Half Kelly f</td><td style="color:var(--accent)">${(edge.current_kelly * 100).toFixed(2)}%</td></tr>
                    <tr><td>Current Sharpe (${edge.window}-trade)</td><td>${edge.current_sharpe.toFixed(2)}</td></tr>
                    <tr><td>Current Win Rate (${edge.window}-trade)</td><td>${(edge.current_winrate * 100).toFixed(1)}%</td></tr>
                    <tr><td>Observed mean return</td><td class="cell-pos">${(edge.observed_mean * 100).toFixed(3)}%</td></tr>
                    <tr><td>Permutation p-value</td><td class="${edge.permutation_pvalue < 0.05 ? 'cell-pos' : 'cell-neg'}">${edge.permutation_pvalue.toFixed(4)}</td></tr>
                    <tr><td>Edge significant?</td><td class="${edge.permutation_pvalue < 0.05 ? 'cell-pos' : 'cell-neg'}">${edge.permutation_pvalue < 0.05 ? 'YES' : 'NO'}</td></tr>
                </tbody></table>
            </div>
        </div>

        <!-- Return Percentiles -->
        <div class="table-wrapper">
            <div class="table-header"><span class="table-title">Return Percentiles (per trade)</span></div>
            <table>
                <thead><tr>
                    ${Object.keys(dist.percentiles).map(p => '<th>P' + p + '</th>').join('')}
                </tr></thead>
                <tbody><tr>
                    ${Object.values(dist.percentiles).map(v => {
                        const cls = v >= 0 ? 'cell-pos' : 'cell-neg';
                        return '<td class="' + cls + '">' + (v * 100).toFixed(1) + '%</td>';
                    }).join('')}
                </tr></tbody>
            </table>
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
            timeWarning = `<div style="background:rgba(248,81,73,0.1);border:1px solid #f85149;border-radius:6px;padding:12px;margin-bottom:12px;">
                <strong style="color:#f85149;">Outside optimal window (11:00-14:30 ET)</strong><br>
                <span style="font-size:12px;">Spreads may be wider. Consider waiting for better fills.</span>
            </div>`;
        } else {
            timeWarning = `<div style="background:rgba(63,185,80,0.1);border:1px solid #3fb950;border-radius:6px;padding:12px;margin-bottom:12px;">
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
        <div style="margin-top:12px; padding:10px; background:var(--bg-secondary); border-radius:6px; font-size:12px; color:var(--text-muted);">
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
                html += `<div class="chart-card"><img src="/output/${c}?t=${Date.now()}" alt="${c}" style="width:100%;border-radius:6px;"></div>`;
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
