// Global State Management (No localStorage due to sandbox restrictions)
let appState = {
  currentUser: null,
  currentRole: null,
  isAuthenticated: false,
  currentPage: 'overview',
  mfaVerified: false,
  sessionTimeout: null,
  notifications: []
};

// Sample Data
const sampleData = {
  admin: {
    kpis: {
      total_revenue: '₹12,45,890',
      active_merchants: 248,
      total_customers: 15420,
      total_orders: 3847,
      commission_earned: '₹1,24,589'
    },
    merchants: [
      { id: 1, name: 'Tech Store India', status: 'pending', category: 'Electronics', registration_date: '2025-10-25', email: 'tech@store.com', revenue: '₹2,45,890' },
      { id: 2, name: 'Fashion Hub', status: 'active', category: 'Fashion', registration_date: '2025-10-24', email: 'fashion@hub.com', revenue: '₹1,89,450' },
      { id: 3, name: 'Home Decor Plus', status: 'active', category: 'Home & Garden', registration_date: '2025-10-20', email: 'home@decor.com', revenue: '₹3,12,670' },
      { id: 4, name: 'Beauty Bazaar', status: 'pending', category: 'Beauty', registration_date: '2025-10-26', email: 'beauty@bazaar.com', revenue: '₹0' }
    ],
    customers: [
      { id: 1, name: 'Rajesh Kumar', email: 'rajesh@email.com', total_orders: 23, total_spent: '₹45,670', status: 'active' },
      { id: 2, name: 'Priya Sharma', email: 'priya@email.com', total_orders: 15, total_spent: '₹32,450', status: 'active' },
      { id: 3, name: 'Amit Patel', email: 'amit@email.com', total_orders: 8, total_spent: '₹18,900', status: 'active' }
    ],
    revenue_trends: [
      { month: 'Jan', revenue: 800000 },
      { month: 'Feb', revenue: 950000 },
      { month: 'Mar', revenue: 1100000 },
      { month: 'Apr', revenue: 980000 },
      { month: 'May', revenue: 1150000 },
      { month: 'Jun', revenue: 1200000 },
      { month: 'Jul', revenue: 1050000 },
      { month: 'Aug', revenue: 1180000 },
      { month: 'Sep', revenue: 1220000 },
      { month: 'Oct', revenue: 1245890 }
    ],
    notifications: [
      'New merchant application from Tech Store India',
      'High value order alert: ₹25,000',
      'Monthly revenue target achieved',
      'System maintenance scheduled for tomorrow'
    ]
  },
  merchant: {
    kpis: {
      total_sales: '₹2,45,890',
      total_orders: 156,
      avg_order_value: '₹1,576',
      conversion_rate: '3.2%',
      customer_rating: 4.7,
      pending_orders: 8
    },
    products: [
      { id: 1, name: 'Wireless Headphones', sku: 'WH001', price: '₹2,999', stock: 25, status: 'in_stock', category: 'Electronics', views: 450, sales: 23 },
      { id: 2, name: 'Bluetooth Speaker', sku: 'BS001', price: '₹1,899', stock: 3, status: 'low_stock', category: 'Electronics', views: 320, sales: 18 },
      { id: 3, name: 'Phone Case', sku: 'PC001', price: '₹599', stock: 0, status: 'out_of_stock', category: 'Accessories', views: 290, sales: 45 },
      { id: 4, name: 'USB Cable', sku: 'UC001', price: '₹299', stock: 150, status: 'in_stock', category: 'Accessories', views: 380, sales: 67 },
      { id: 5, name: 'Power Bank', sku: 'PB001', price: '₹1,499', stock: 45, status: 'in_stock', category: 'Electronics', views: 410, sales: 34 }
    ],
    orders: [
      { id: 'ORD001', customer: 'Rajesh Kumar', total: '₹2,999', status: 'pending', date: '2025-10-26', items: 1 },
      { id: 'ORD002', customer: 'Priya Sharma', total: '₹1,899', status: 'shipped', date: '2025-10-25', items: 1 },
      { id: 'ORD003', customer: 'Amit Patel', total: '₹3,597', status: 'pending', date: '2025-10-26', items: 3 },
      { id: 'ORD004', customer: 'Sunita Reddy', total: '₹599', status: 'shipped', date: '2025-10-24', items: 1 },
      { id: 'ORD005', customer: 'Vikram Singh', total: '₹4,497', status: 'pending', date: '2025-10-26', items: 2 }
    ],
    sales_trends: [
      { day: 'Mon', sales: 12500 },
      { day: 'Tue', sales: 18900 },
      { day: 'Wed', sales: 15600 },
      { day: 'Thu', sales: 22300 },
      { day: 'Fri', sales: 28400 },
      { day: 'Sat', sales: 35200 },
      { day: 'Sun', sales: 25800 }
    ],
    notifications: [
      'New order received from Rajesh Kumar',
      'Low stock alert: Bluetooth Speaker (3 units)',
      'Customer review: 5 stars for Wireless Headphones',
      'Payment received: ₹2,999'
    ]
  },
  ai: {
    tokens_used: 15420,
    tokens_remaining: 34580,
    total_tokens: 50000,
    services: {
      voice_processing: 6200,
      image_analysis: 4800,
      text_generation: 4420
    },
    languages: ['Hindi', 'Telugu', 'English'],
    voice_commands: [
      'Bhaiya, 20 kg basmati rice aaya hai, 350 rupees per 5kg',
      'Stock mein 50 mobile covers add karo',
      'WhatsApp par customers ko diwali offer bhejo'
    ]
  }
};

// Initialize App
function init() {
  renderLoginPage();
  setupSessionTimeout();
}

// Session Timeout Management
function setupSessionTimeout() {
  const TIMEOUT_DURATION = 30 * 60 * 1000; // 30 minutes
  
  function resetTimeout() {
    if (appState.sessionTimeout) {
      clearTimeout(appState.sessionTimeout);
    }
    
    if (appState.isAuthenticated) {
      appState.sessionTimeout = setTimeout(() => {
        logout('Session expired due to inactivity');
      }, TIMEOUT_DURATION);
    }
  }
  
  ['click', 'keypress', 'scroll', 'mousemove'].forEach(event => {
    document.addEventListener(event, resetTimeout);
  });
  
  resetTimeout();
}

// Login Page
function renderLoginPage() {
  const app = document.getElementById('app');
  app.innerHTML = `
    <div class="login-container">
      <div class="login-box">
        <div class="login-header">
          <h1>VyaapaarAI</h1>
          <p>Comprehensive Dashboard Platform with MFA</p>
        </div>
        
        ${!appState.mfaVerified ? `
          <div class="role-selector">
            <button class="role-btn ${appState.currentRole === 'admin' ? 'active' : ''}" onclick="selectRole('admin')">Admin Login</button>
            <button class="role-btn ${appState.currentRole === 'merchant' ? 'active' : ''}" onclick="selectRole('merchant')">Merchant Login</button>
          </div>
          
          <form onsubmit="handleLogin(event)">
            <div class="form-group">
              <label class="form-label">Email Address</label>
              <input type="email" class="form-input" id="email" placeholder="Enter your email" required>
            </div>
            
            <div class="form-group">
              <label class="form-label">Password</label>
              <input type="password" class="form-input" id="password" placeholder="Enter your password" required>
            </div>
            
            <button type="submit" class="btn btn-primary">Send OTP</button>
            <div id="login-message"></div>
          </form>
          
          <div style="margin-top: 20px; padding: 16px; background: var(--color-bg-1); border-radius: var(--radius-base); font-size: var(--font-size-sm);">
            <strong>Demo Credentials:</strong><br>
            Admin: admin@vyapaar.ai / admin123<br>
            Merchant: merchant@store.com / merchant123
          </div>
        ` : `
          <div class="login-header">
            <h2 style="font-size: var(--font-size-xl); margin-bottom: 16px;">Enter OTP</h2>
            <p style="font-size: var(--font-size-sm);">We've sent a 6-digit code to your email</p>
          </div>
          
          <form onsubmit="verifyOTP(event)">
            <div class="form-group">
              <label class="form-label">6-Digit OTP</label>
              <input type="text" class="form-input" id="otp" placeholder="Enter OTP" maxlength="6" required>
            </div>
            
            <button type="submit" class="btn btn-primary">Verify OTP</button>
            <button type="button" class="btn btn-secondary" onclick="resetLogin()" style="margin-top: 12px;">Back to Login</button>
            <div id="otp-message"></div>
          </form>
          
          <div style="margin-top: 20px; padding: 16px; background: var(--color-bg-2); border-radius: var(--radius-base); font-size: var(--font-size-sm);">
            <strong>Demo OTP:</strong> 123456<br>
            (In production, this would be sent to your email)
          </div>
        `}
      </div>
    </div>
  `;
}

// Role Selection
function selectRole(role) {
  appState.currentRole = role;
  renderLoginPage();
}

// Handle Login
function handleLogin(event) {
  event.preventDefault();
  
  const email = document.getElementById('email').value;
  const password = document.getElementById('password').value;
  const messageDiv = document.getElementById('login-message');
  
  if (!appState.currentRole) {
    messageDiv.innerHTML = '<p class="error-message">Please select a role (Admin or Merchant)</p>';
    return;
  }
  
  // Demo validation
  const validCredentials = {
    admin: { email: 'admin@vyapaar.ai', password: 'admin123' },
    merchant: { email: 'merchant@store.com', password: 'merchant123' }
  };
  
  if (email === validCredentials[appState.currentRole].email && 
      password === validCredentials[appState.currentRole].password) {
    appState.currentUser = email;
    appState.mfaVerified = false;
    messageDiv.innerHTML = '<p class="success-message">✓ OTP sent to your email!</p>';
    
    setTimeout(() => {
      renderLoginPage();
    }, 1000);
  } else {
    messageDiv.innerHTML = '<p class="error-message">Invalid credentials. Please use demo credentials.</p>';
  }
}

// Verify OTP
function verifyOTP(event) {
  event.preventDefault();
  
  const otp = document.getElementById('otp').value;
  const messageDiv = document.getElementById('otp-message');
  
  // Demo OTP verification
  if (otp === '123456') {
    appState.isAuthenticated = true;
    appState.mfaVerified = true;
    appState.notifications = appState.currentRole === 'admin' ? 
      sampleData.admin.notifications : sampleData.merchant.notifications;
    
    messageDiv.innerHTML = '<p class="success-message">✓ OTP verified successfully!</p>';
    
    setTimeout(() => {
      renderDashboard();
    }, 500);
  } else {
    messageDiv.innerHTML = '<p class="error-message">Invalid OTP. Please use 123456 for demo.</p>';
  }
}

// Reset Login
function resetLogin() {
  appState.mfaVerified = false;
  appState.currentUser = null;
  renderLoginPage();
}

// Logout
function logout(message = 'Logged out successfully') {
  appState.isAuthenticated = false;
  appState.currentUser = null;
  appState.currentRole = null;
  appState.mfaVerified = false;
  appState.currentPage = 'overview';
  
  if (appState.sessionTimeout) {
    clearTimeout(appState.sessionTimeout);
  }
  
  alert(message);
  renderLoginPage();
}

// Render Dashboard
function renderDashboard() {
  const app = document.getElementById('app');
  
  app.innerHTML = `
    <div class="dashboard-layout">
      ${renderSidebar()}
      <div class="main-content">
        ${renderTopbar()}
        <div class="content-area" id="content-area">
          ${renderContent()}
        </div>
      </div>
    </div>
  `;
}

// Render Sidebar
function renderSidebar() {
  const navItems = appState.currentRole === 'admin' ? [
    { id: 'overview', label: 'Overview', icon: '📊' },
    { id: 'merchants', label: 'Merchants', icon: '🏪' },
    { id: 'customers', label: 'Customers', icon: '👥' },
    { id: 'revenue', label: 'Revenue & Analytics', icon: '💰' },
    { id: 'settings', label: 'Settings', icon: '⚙️' }
  ] : [
    { id: 'overview', label: 'Overview', icon: '📊' },
    { id: 'products', label: 'Products', icon: '📦' },
    { id: 'orders', label: 'Orders', icon: '🛒' },
    { id: 'analytics', label: 'Analytics', icon: '📈' },
    { id: 'inventory', label: 'Inventory', icon: '📋' },
    { id: 'ai-assistant', label: 'AI Assistant', icon: '🤖' },
    { id: 'settings', label: 'Settings', icon: '⚙️' }
  ];
  
  return `
    <div class="sidebar">
      <div class="sidebar-header">
        <div class="sidebar-logo">VyaapaarAI</div>
        <span class="sidebar-role">${appState.currentRole.toUpperCase()}</span>
      </div>
      
      <ul class="sidebar-nav">
        ${navItems.map(item => `
          <li class="nav-item">
            <a href="#" class="nav-link ${appState.currentPage === item.id ? 'active' : ''}" onclick="navigateTo('${item.id}')">
              <span class="nav-icon">${item.icon}</span>
              <span>${item.label}</span>
            </a>
          </li>
        `).join('')}
      </ul>
    </div>
  `;
}

// Render Topbar
function renderTopbar() {
  const pageTitle = appState.currentPage.charAt(0).toUpperCase() + appState.currentPage.slice(1);
  
  return `
    <div class="topbar">
      <div class="topbar-left">
        <h2>${pageTitle}</h2>
      </div>
      <div class="topbar-right">
        <button class="notification-btn" onclick="toggleNotifications()">
          🔔
          <span class="notification-badge">${appState.notifications.length}</span>
        </button>
        <div class="user-menu">
          <div class="user-avatar">${appState.currentUser ? appState.currentUser.charAt(0).toUpperCase() : 'U'}</div>
          <button class="logout-btn" onclick="logout()">Logout</button>
        </div>
      </div>
    </div>
  `;
}

// Navigate
function navigateTo(page) {
  appState.currentPage = page;
  renderDashboard();
}

// Toggle Notifications
function toggleNotifications() {
  alert('Notifications:\n\n' + appState.notifications.join('\n'));
}

// Render Content
function renderContent() {
  const role = appState.currentRole;
  const page = appState.currentPage;
  
  if (role === 'admin') {
    switch (page) {
      case 'overview':
        return renderAdminOverview();
      case 'merchants':
        return renderMerchants();
      case 'customers':
        return renderCustomers();
      case 'revenue':
        return renderRevenue();
      case 'settings':
        return renderSettings();
      default:
        return renderAdminOverview();
    }
  } else {
    switch (page) {
      case 'overview':
        return renderMerchantOverview();
      case 'products':
        return renderProducts();
      case 'orders':
        return renderOrders();
      case 'analytics':
        return renderAnalytics();
      case 'inventory':
        return renderInventory();
      case 'ai-assistant':
        return renderAIAssistant();
      case 'settings':
        return renderSettings();
      default:
        return renderMerchantOverview();
    }
  }
}

// Admin Overview
function renderAdminOverview() {
  const data = sampleData.admin;
  
  setTimeout(() => {
    renderRevenueChart();
    renderCategoryChart();
  }, 100);
  
  return `
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total Platform Revenue</div>
        <div class="kpi-value">${data.kpis.total_revenue}</div>
        <div class="kpi-trend">↑ 12.5% vs last month</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Active Merchants</div>
        <div class="kpi-value">${data.kpis.active_merchants}</div>
        <div class="kpi-trend">↑ 8 new this week</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Total Customers</div>
        <div class="kpi-value">${data.kpis.total_customers.toLocaleString()}</div>
        <div class="kpi-trend">↑ 234 new this week</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Total Orders</div>
        <div class="kpi-value">${data.kpis.total_orders.toLocaleString()}</div>
        <div class="kpi-trend">↑ 156 today</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Commission Earned</div>
        <div class="kpi-value">${data.kpis.commission_earned}</div>
        <div class="kpi-trend">↑ 10.2% vs last month</div>
      </div>
    </div>
    
    <div class="chart-grid">
      <div class="chart-card">
        <h3 class="chart-header">Revenue Trends (10 Months)</h3>
        <div class="chart-container">
          <canvas id="revenueChart"></canvas>
        </div>
      </div>
      
      <div class="chart-card">
        <h3 class="chart-header">Sales by Category</h3>
        <div class="chart-container">
          <canvas id="categoryChart"></canvas>
        </div>
      </div>
    </div>
    
    <div class="notification-panel">
      <h3 class="chart-header">Recent Activity & Alerts</h3>
      ${data.notifications.map(notif => `
        <div class="notification-item">📢 ${notif}</div>
      `).join('')}
    </div>
    
    <div class="quick-actions">
      <div class="quick-action-card" onclick="alert('Opening merchant approvals...')">
        <div class="quick-action-icon">✅</div>
        <div class="quick-action-label">Approve Merchants</div>
      </div>
      <div class="quick-action-card" onclick="alert('Opening dispute resolution...')">
        <div class="quick-action-icon">⚖️</div>
        <div class="quick-action-label">Resolve Disputes</div>
      </div>
      <div class="quick-action-card" onclick="alert('Generating reports...')">
        <div class="quick-action-icon">📄</div>
        <div class="quick-action-label">Generate Reports</div>
      </div>
      <div class="quick-action-card" onclick="alert('Opening system settings...')">
        <div class="quick-action-icon">🔧</div>
        <div class="quick-action-label">System Config</div>
      </div>
    </div>
  `;
}

// Render Merchants
function renderMerchants() {
  const merchants = sampleData.admin.merchants;
  
  return `
    <div class="data-table">
      <div class="table-header">
        <h3 class="table-title">Merchant Management</h3>
        <div class="table-actions">
          <input type="text" class="search-box" placeholder="Search merchants...">
          <button class="btn-secondary" onclick="alert('Exporting data...')">📥 Export</button>
          <button class="btn-secondary" onclick="alert('Adding new merchant...')">➕ Add Merchant</button>
        </div>
      </div>
      
      <table>
        <thead>
          <tr>
            <th>Merchant Name</th>
            <th>Category</th>
            <th>Status</th>
            <th>Registration Date</th>
            <th>Revenue</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${merchants.map(merchant => `
            <tr>
              <td><strong>${merchant.name}</strong><br><small>${merchant.email}</small></td>
              <td>${merchant.category}</td>
              <td><span class="status-badge status-${merchant.status}">${merchant.status.toUpperCase()}</span></td>
              <td>${merchant.registration_date}</td>
              <td><strong>${merchant.revenue}</strong></td>
              <td>
                <button class="action-btn" onclick="alert('Viewing ${merchant.name}')">View</button>
                ${merchant.status === 'pending' ? '<button class="action-btn" onclick="alert(\'Approving merchant...\')">Approve</button>' : ''}
                <button class="action-btn secondary" onclick="alert('Editing ${merchant.name}')">Edit</button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
    
    <div style="margin-top: 32px;" class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Pending Approvals</div>
        <div class="kpi-value">2</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Active Merchants</div>
        <div class="kpi-value">246</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Total Commission (MTD)</div>
        <div class="kpi-value">₹1,24,589</div>
      </div>
    </div>
  `;
}

// Render Customers
function renderCustomers() {
  const customers = sampleData.admin.customers;
  
  return `
    <div class="data-table">
      <div class="table-header">
        <h3 class="table-title">Customer Management</h3>
        <div class="table-actions">
          <input type="text" class="search-box" placeholder="Search customers...">
          <button class="btn-secondary" onclick="alert('Exporting data...')">📥 Export</button>
          <button class="btn-secondary" onclick="alert('Creating segment...')">🎯 Create Segment</button>
        </div>
      </div>
      
      <table>
        <thead>
          <tr>
            <th>Customer Name</th>
            <th>Email</th>
            <th>Total Orders</th>
            <th>Total Spent</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${customers.map(customer => `
            <tr>
              <td><strong>${customer.name}</strong></td>
              <td>${customer.email}</td>
              <td>${customer.total_orders}</td>
              <td><strong>${customer.total_spent}</strong></td>
              <td><span class="status-badge status-${customer.status}">${customer.status.toUpperCase()}</span></td>
              <td>
                <button class="action-btn" onclick="alert('Viewing ${customer.name}')">View</button>
                <button class="action-btn secondary" onclick="alert('Messaging ${customer.name}')">Message</button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
    
    <div style="margin-top: 32px;">
      <h3 class="chart-header">Customer Segmentation</h3>
      <div class="quick-actions">
        <div class="quick-action-card">
          <div class="quick-action-icon">⭐</div>
          <div class="quick-action-label">High Value<br><strong>847 customers</strong></div>
        </div>
        <div class="quick-action-card">
          <div class="quick-action-icon">🔄</div>
          <div class="quick-action-label">Repeat Buyers<br><strong>3,456 customers</strong></div>
        </div>
        <div class="quick-action-card">
          <div class="quick-action-icon">💤</div>
          <div class="quick-action-label">Dormant<br><strong>1,234 customers</strong></div>
        </div>
        <div class="quick-action-card">
          <div class="quick-action-icon">✨</div>
          <div class="quick-action-label">New Customers<br><strong>567 this month</strong></div>
        </div>
      </div>
    </div>
  `;
}

// Render Revenue
function renderRevenue() {
  setTimeout(() => {
    renderRevenueChart();
  }, 100);
  
  return `
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total Revenue</div>
        <div class="kpi-value">₹12,45,890</div>
        <div class="kpi-trend">↑ 12.5% vs last month</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Average Order Value</div>
        <div class="kpi-value">₹3,238</div>
        <div class="kpi-trend">↑ 8.2% improvement</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Conversion Rate</div>
        <div class="kpi-value">3.8%</div>
        <div class="kpi-trend">↑ 0.5% vs last month</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Customer Lifetime Value</div>
        <div class="kpi-value">₹18,450</div>
        <div class="kpi-trend">↑ 15.3% growth</div>
      </div>
    </div>
    
    <div class="chart-card" style="margin-top: 32px;">
      <h3 class="chart-header">Revenue Analytics (10 Months)</h3>
      <div class="chart-container">
        <canvas id="revenueChart"></canvas>
      </div>
    </div>
    
    <div style="margin-top: 32px;">
      <h3 class="chart-header">Financial Reports</h3>
      <div class="quick-actions">
        <div class="quick-action-card" onclick="alert('Generating P&L Statement...')">
          <div class="quick-action-icon">📊</div>
          <div class="quick-action-label">P&L Statement</div>
        </div>
        <div class="quick-action-card" onclick="alert('Generating Tax Report...')">
          <div class="quick-action-icon">🧾</div>
          <div class="quick-action-label">Tax Reports</div>
        </div>
        <div class="quick-action-card" onclick="alert('Generating Commission Report...')">
          <div class="quick-action-icon">💰</div>
          <div class="quick-action-label">Commission Report</div>
        </div>
        <div class="quick-action-card" onclick="alert('Exporting all data...')">
          <div class="quick-action-icon">📥</div>
          <div class="quick-action-label">Export All Data</div>
        </div>
      </div>
    </div>
  `;
}

// Merchant Overview
function renderMerchantOverview() {
  const data = sampleData.merchant;
  
  setTimeout(() => {
    renderSalesChart();
  }, 100);
  
  return `
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total Sales</div>
        <div class="kpi-value">${data.kpis.total_sales}</div>
        <div class="kpi-trend">↑ 15.2% vs last week</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Total Orders</div>
        <div class="kpi-value">${data.kpis.total_orders}</div>
        <div class="kpi-trend">↑ 12 new today</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Average Order Value</div>
        <div class="kpi-value">${data.kpis.avg_order_value}</div>
        <div class="kpi-trend">↑ 8.5% improvement</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Conversion Rate</div>
        <div class="kpi-value">${data.kpis.conversion_rate}</div>
        <div class="kpi-trend">↑ 0.4% vs last week</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Customer Rating</div>
        <div class="kpi-value">${data.kpis.customer_rating} ⭐</div>
        <div class="kpi-trend">Based on 234 reviews</div>
      </div>
    </div>
    
    <div class="chart-card" style="margin-top: 32px;">
      <h3 class="chart-header">Sales Trends (Last 7 Days)</h3>
      <div class="chart-container">
        <canvas id="salesChart"></canvas>
      </div>
    </div>
    
    <div class="notification-panel" style="margin-top: 32px;">
      <h3 class="chart-header">Recent Activity</h3>
      ${data.notifications.map(notif => `
        <div class="notification-item">📢 ${notif}</div>
      `).join('')}
    </div>
    
    <div class="quick-actions" style="margin-top: 32px;">
      <div class="quick-action-card" onclick="navigateTo('products')">
        <div class="quick-action-icon">➕</div>
        <div class="quick-action-label">Add Product</div>
      </div>
      <div class="quick-action-card" onclick="navigateTo('orders')">
        <div class="quick-action-icon">📦</div>
        <div class="quick-action-label">View Orders</div>
      </div>
      <div class="quick-action-card" onclick="navigateTo('analytics')">
        <div class="quick-action-icon">📊</div>
        <div class="quick-action-label">Analytics</div>
      </div>
      <div class="quick-action-card" onclick="navigateTo('ai-assistant')">
        <div class="quick-action-icon">🤖</div>
        <div class="quick-action-label">AI Assistant</div>
      </div>
    </div>
  `;
}

// Render Products
function renderProducts() {
  const products = sampleData.merchant.products;
  
  return `
    <div class="data-table">
      <div class="table-header">
        <h3 class="table-title">Product Management</h3>
        <div class="table-actions">
          <input type="text" class="search-box" placeholder="Search products...">
          <button class="btn-secondary" onclick="alert('Bulk import coming...')">📤 Bulk Import</button>
          <button class="btn-secondary" onclick="alert('Adding new product...')">➕ Add Product</button>
        </div>
      </div>
      
      <table>
        <thead>
          <tr>
            <th>Product Name</th>
            <th>SKU</th>
            <th>Price</th>
            <th>Stock</th>
            <th>Status</th>
            <th>Category</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${products.map(product => `
            <tr>
              <td><strong>${product.name}</strong></td>
              <td>${product.sku}</td>
              <td><strong>${product.price}</strong></td>
              <td>${product.stock} units</td>
              <td><span class="status-badge status-${product.status.replace('_', '-')}">${product.status.replace('_', ' ').toUpperCase()}</span></td>
              <td>${product.category}</td>
              <td>
                <button class="action-btn" onclick="alert('Editing ${product.name}')">Edit</button>
                <button class="action-btn secondary" onclick="alert('Restocking ${product.name}')">Restock</button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
    
    <div style="margin-top: 32px;" class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total Products</div>
        <div class="kpi-value">${products.length}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">In Stock</div>
        <div class="kpi-value">${products.filter(p => p.status === 'in_stock').length}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Low Stock</div>
        <div class="kpi-value">${products.filter(p => p.status === 'low_stock').length}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Out of Stock</div>
        <div class="kpi-value">${products.filter(p => p.status === 'out_of_stock').length}</div>
      </div>
    </div>
  `;
}

// Render Orders
function renderOrders() {
  const orders = sampleData.merchant.orders;
  
  return `
    <div class="data-table">
      <div class="table-header">
        <h3 class="table-title">Order Management</h3>
        <div class="table-actions">
          <input type="text" class="search-box" placeholder="Search orders...">
          <button class="btn-secondary" onclick="alert('Exporting orders...')">📥 Export</button>
          <button class="btn-secondary" onclick="alert('Bulk processing...')">⚡ Bulk Process</button>
        </div>
      </div>
      
      <table>
        <thead>
          <tr>
            <th>Order ID</th>
            <th>Customer</th>
            <th>Date</th>
            <th>Items</th>
            <th>Total</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${orders.map(order => `
            <tr>
              <td><strong>${order.id}</strong></td>
              <td>${order.customer}</td>
              <td>${order.date}</td>
              <td>${order.items}</td>
              <td><strong>${order.total}</strong></td>
              <td><span class="status-badge status-${order.status}">${order.status.toUpperCase()}</span></td>
              <td>
                <button class="action-btn" onclick="alert('Viewing order ${order.id}')">View</button>
                ${order.status === 'pending' ? '<button class="action-btn" onclick="alert(\'Processing order...\')">Process</button>' : ''}
                <button class="action-btn secondary" onclick="alert('Printing invoice...')">Print</button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
    
    <div style="margin-top: 32px;" class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Pending Orders</div>
        <div class="kpi-value">${orders.filter(o => o.status === 'pending').length}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Shipped Orders</div>
        <div class="kpi-value">${orders.filter(o => o.status === 'shipped').length}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Total Order Value</div>
        <div class="kpi-value">₹11,891</div>
      </div>
    </div>
  `;
}

// Render Analytics
function renderAnalytics() {
  setTimeout(() => {
    renderProductPerformanceChart();
  }, 100);
  
  return `
    <h3 class="chart-header">Sales & Product Analytics</h3>
    
    <div class="kpi-grid" style="margin-bottom: 32px;">
      <div class="kpi-card">
        <div class="kpi-label">Total Revenue</div>
        <div class="kpi-value">₹2,45,890</div>
        <div class="kpi-trend">↑ 15.2% vs last period</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Product Views</div>
        <div class="kpi-value">1,850</div>
        <div class="kpi-trend">↑ 234 this week</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Add to Cart Rate</div>
        <div class="kpi-value">12.5%</div>
        <div class="kpi-trend">↑ 2.1% improvement</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Total Sales</div>
        <div class="kpi-value">187</div>
        <div class="kpi-trend">↑ 23 this week</div>
      </div>
    </div>
    
    <div class="chart-card">
      <h3 class="chart-header">Top Performing Products</h3>
      <div class="chart-container">
        <canvas id="productChart"></canvas>
      </div>
    </div>
    
    <div style="margin-top: 32px;" class="data-table">
      <div class="table-header">
        <h3 class="table-title">Product Performance Details</h3>
      </div>
      <table>
        <thead>
          <tr>
            <th>Product</th>
            <th>Views</th>
            <th>Sales</th>
            <th>Revenue</th>
            <th>Conversion</th>
          </tr>
        </thead>
        <tbody>
          ${sampleData.merchant.products.map(product => `
            <tr>
              <td><strong>${product.name}</strong></td>
              <td>${product.views}</td>
              <td>${product.sales}</td>
              <td><strong>₹${(parseInt(product.price.replace(/[^0-9]/g, '')) * product.sales).toLocaleString()}</strong></td>
              <td>${((product.sales / product.views) * 100).toFixed(1)}%</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

// Render Inventory
function renderInventory() {
  const products = sampleData.merchant.products;
  
  return `
    <h3 class="chart-header">Inventory Management Dashboard</h3>
    
    <div class="kpi-grid" style="margin-bottom: 32px;">
      <div class="kpi-card">
        <div class="kpi-label">Total Inventory Value</div>
        <div class="kpi-value">₹4,56,234</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Low Stock Items</div>
        <div class="kpi-value" style="color: var(--color-warning);">${products.filter(p => p.status === 'low_stock').length}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Out of Stock</div>
        <div class="kpi-value" style="color: var(--color-error);">${products.filter(p => p.status === 'out_of_stock').length}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Inventory Turnover</div>
        <div class="kpi-value">4.2x</div>
        <div class="kpi-trend">Healthy turnover rate</div>
      </div>
    </div>
    
    <div class="data-table">
      <div class="table-header">
        <h3 class="table-title">Stock Status</h3>
        <div class="table-actions">
          <button class="btn-secondary" onclick="alert('Bulk restock initiated...')">📦 Bulk Restock</button>
          <button class="btn-secondary" onclick="alert('Stock report generated...')">📊 Stock Report</button>
        </div>
      </div>
      
      <table>
        <thead>
          <tr>
            <th>Product</th>
            <th>SKU</th>
            <th>Current Stock</th>
            <th>Status</th>
            <th>Reorder Point</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${products.map(product => `
            <tr>
              <td><strong>${product.name}</strong></td>
              <td>${product.sku}</td>
              <td>${product.stock} units</td>
              <td><span class="status-badge status-${product.status.replace('_', '-')}">${product.status.replace('_', ' ').toUpperCase()}</span></td>
              <td>10 units</td>
              <td>
                <button class="action-btn" onclick="alert('Restocking ${product.name}')">Restock</button>
                <button class="action-btn secondary" onclick="alert('Adjusting stock...')">Adjust</button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
    
    <div class="notification-panel" style="margin-top: 32px;">
      <h3 class="chart-header">⚠️ Stock Alerts</h3>
      <div class="notification-item">🔴 Phone Case is OUT OF STOCK - Immediate action required</div>
      <div class="notification-item">🟡 Bluetooth Speaker has LOW STOCK (3 units remaining)</div>
      <div class="notification-item">✅ Wireless Headphones restocked successfully (25 units added)</div>
    </div>
  `;
}

// Render AI Assistant
function renderAIAssistant() {
  const aiData = sampleData.ai;
  const tokensUsedPercent = (aiData.tokens_used / aiData.total_tokens) * 100;
  
  return `
    <h3 class="chart-header">VyaapaarAI Assistant - Conversational Commerce</h3>
    
    <div class="language-selector">
      <button class="lang-btn active">🇬🇧 English</button>
      <button class="lang-btn">🇮🇳 हिंदी (Hindi)</button>
      <button class="lang-btn">🇮🇳 తెలుగు (Telugu)</button>
    </div>
    
    <div class="token-usage">
      <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
        <span><strong>AI Token Usage</strong></span>
        <span>${aiData.tokens_used.toLocaleString()} / ${aiData.total_tokens.toLocaleString()} tokens</span>
      </div>
      <div class="token-bar">
        <div class="token-fill" style="width: ${tokensUsedPercent}%;"></div>
      </div>
      <div style="font-size: var(--font-size-xs); color: var(--color-text-secondary); margin-top: 8px;">
        Voice: ${aiData.services.voice_processing.toLocaleString()} | 
        Images: ${aiData.services.image_analysis.toLocaleString()} | 
        Text: ${aiData.services.text_generation.toLocaleString()}
      </div>
    </div>
    
    <div class="quick-actions" style="margin-top: 24px;">
      <div class="quick-action-card" onclick="simulateVoiceCommand()">
        <div class="quick-action-icon">🎤</div>
        <div class="quick-action-label">Voice Input</div>
      </div>
      <div class="quick-action-card" onclick="simulatePhotoUpload()">
        <div class="quick-action-icon">📸</div>
        <div class="quick-action-label">Photo Analysis</div>
      </div>
      <div class="quick-action-card" onclick="generateMarketing()">
        <div class="quick-action-icon">📢</div>
        <div class="quick-action-label">Generate Marketing</div>
      </div>
      <div class="quick-action-card" onclick="whatsappIntegration()">
        <div class="quick-action-icon">💬</div>
        <div class="quick-action-label">WhatsApp Campaign</div>
      </div>
    </div>
    
    <div class="ai-chat-container" style="margin-top: 32px;">
      <h3 class="chart-header">AI Chat Assistant (GPT-4o-mini)</h3>
      <div class="chat-messages" id="chatMessages">
        <div class="chat-message assistant">
          Namaste! 🙏 I'm your VyaapaarAI assistant. How can I help you manage your business today?
          <br><br>
          You can ask me to:
          <ul style="margin-top: 8px; padding-left: 20px;">
            <li>Update inventory (voice or text)</li>
            <li>Generate marketing content</li>
            <li>Create WhatsApp campaigns</li>
            <li>Analyze sales data</li>
            <li>Draft customer responses</li>
          </ul>
        </div>
      </div>
      <div class="chat-input-group">
        <input type="text" class="chat-input" id="chatInput" placeholder="Type your message or business command..." onkeypress="handleChatKeypress(event)">
        <button class="btn btn-primary" onclick="sendChatMessage()" style="width: auto; padding: 12px 24px;">Send</button>
      </div>
    </div>
    
    <div class="notification-panel" style="margin-top: 32px;">
      <h3 class="chart-header">📝 Example Voice Commands (Hinglish)</h3>
      ${aiData.voice_commands.map(cmd => `
        <div class="notification-item" style="cursor: pointer;" onclick="useSampleCommand('${cmd}')">
          🎤 "${cmd}"
        </div>
      `).join('')}
    </div>
  `;
}

// AI Assistant Functions
function simulateVoiceCommand() {
  const commands = sampleData.ai.voice_commands;
  const randomCommand = commands[Math.floor(Math.random() * commands.length)];
  alert('🎤 Voice Input Simulated\n\nCommand: "' + randomCommand + '"\n\nAI Processing: ✓\nAction: Inventory updated successfully!');
}

function simulatePhotoUpload() {
  alert('📸 Photo Analysis\n\nAI detected: Basmati Rice Package\nSuggested Price: ₹350 for 5kg\nCategory: Groceries\nStock Status: Ready to add\n\n✓ Product ready to add to inventory');
}

function generateMarketing() {
  alert('📢 AI Marketing Generator\n\n✨ Generated Content:\n\n🎉 Diwali Dhamaka Offer! 🎉\n\nBasmati Rice - Special Price!\n₹350 for 5kg (Save ₹50)\n\nLimited time offer!\nOrder now on WhatsApp: [Your Number]\n\n#DiwaliOffer #GroceryDeals\n\n✓ Ready to share on WhatsApp!');
}

function whatsappIntegration() {
  alert('💬 WhatsApp Business Integration\n\nActive Customers: 234\nResponse Rate: 92%\n\nQuick Actions:\n• Send new stock alerts\n• Share festival offers\n• Follow up on orders\n\n✓ Ready to send campaigns!');
}

function useSampleCommand(command) {
  const chatMessages = document.getElementById('chatMessages');
  if (chatMessages) {
    chatMessages.innerHTML += `
      <div class="chat-message user">${command}</div>
      <div class="chat-message assistant">
        ✓ Command processed successfully!<br><br>
        I've updated your inventory as requested. Here's what I did:<br>
        • Added product to inventory<br>
        • Set pricing automatically<br>
        • Updated stock levels<br><br>
        Would you like me to send a WhatsApp message to your customers about this new stock?
      </div>
    `;
    chatMessages.scrollTop = chatMessages.scrollHeight;
  }
}

function handleChatKeypress(event) {
  if (event.key === 'Enter') {
    sendChatMessage();
  }
}

function sendChatMessage() {
  const input = document.getElementById('chatInput');
  const message = input.value.trim();
  
  if (!message) return;
  
  const chatMessages = document.getElementById('chatMessages');
  chatMessages.innerHTML += `
    <div class="chat-message user">${message}</div>
    <div class="chat-message assistant">
      I understand you want help with: "${message}"<br><br>
      I'm processing your request using AI. In a production environment, I would:<br>
      • Analyze your business data<br>
      • Provide actionable insights<br>
      • Execute the requested action<br><br>
      Is there anything else I can help you with?
    </div>
  `;
  
  input.value = '';
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

// Render Settings
function renderSettings() {
  const role = appState.currentRole;
  
  return `
    <h3 class="chart-header">${role === 'admin' ? 'Admin' : 'Merchant'} Settings & Configuration</h3>
    
    <div class="data-table">
      <div class="table-header">
        <h3 class="table-title">🔐 Security Settings</h3>
      </div>
      <div style="padding: 20px;">
        <div class="form-group">
          <label class="form-label">Multi-Factor Authentication (MFA)</label>
          <div style="display: flex; align-items: center; gap: 12px;">
            <span class="status-badge status-active">✓ ENABLED</span>
            <button class="btn-secondary" onclick="alert('MFA settings updated')">Configure MFA</button>
          </div>
        </div>
        
        <div class="form-group">
          <label class="form-label">Password Management</label>
          <button class="btn-secondary" onclick="alert('Password change initiated')">Change Password</button>
        </div>
        
        <div class="form-group">
          <label class="form-label">Session Timeout</label>
          <select class="form-input" style="width: 200px;">
            <option>30 minutes</option>
            <option>1 hour</option>
            <option>4 hours</option>
          </select>
        </div>
      </div>
    </div>
    
    ${role === 'merchant' ? `
      <div class="data-table" style="margin-top: 24px;">
        <div class="table-header">
          <h3 class="table-title">🏪 Store Profile</h3>
        </div>
        <div style="padding: 20px;">
          <div class="form-grid">
            <div class="form-group">
              <label class="form-label">Store Name</label>
              <input type="text" class="form-input" value="Tech Store India" placeholder="Enter store name">
            </div>
            <div class="form-group">
              <label class="form-label">Contact Email</label>
              <input type="email" class="form-input" value="merchant@store.com" placeholder="Enter email">
            </div>
            <div class="form-group">
              <label class="form-label">Phone Number</label>
              <input type="tel" class="form-input" value="+91 98765 43210" placeholder="Enter phone">
            </div>
            <div class="form-group">
              <label class="form-label">Business Category</label>
              <select class="form-input">
                <option>Electronics</option>
                <option>Fashion</option>
                <option>Home & Garden</option>
                <option>Beauty</option>
              </select>
            </div>
          </div>
          <button class="btn btn-primary" onclick="alert('Store profile updated!')">Save Changes</button>
        </div>
      </div>
      
      <div class="data-table" style="margin-top: 24px;">
        <div class="table-header">
          <h3 class="table-title">💳 Payment Settings</h3>
        </div>
        <div style="padding: 20px;">
          <div class="form-grid">
            <div class="form-group">
              <label class="form-label">Bank Account Number</label>
              <input type="text" class="form-input" value="XXXX XXXX 1234" placeholder="Account number">
            </div>
            <div class="form-group">
              <label class="form-label">IFSC Code</label>
              <input type="text" class="form-input" value="SBIN0001234" placeholder="IFSC code">
            </div>
            <div class="form-group">
              <label class="form-label">Payout Frequency</label>
              <select class="form-input">
                <option>Weekly</option>
                <option>Bi-weekly</option>
                <option>Monthly</option>
              </select>
            </div>
            <div class="form-group">
              <label class="form-label">Minimum Payout</label>
              <input type="text" class="form-input" value="₹1,000" placeholder="Minimum amount">
            </div>
          </div>
          <button class="btn btn-primary" onclick="alert('Payment settings updated!')">Save Changes</button>
        </div>
      </div>
    ` : `
      <div class="data-table" style="margin-top: 24px;">
        <div class="table-header">
          <h3 class="table-title">💰 Commission Configuration</h3>
        </div>
        <div style="padding: 20px;">
          <div class="form-grid">
            <div class="form-group">
              <label class="form-label">Default Commission Rate (%)</label>
              <input type="number" class="form-input" value="10" placeholder="Enter percentage">
            </div>
            <div class="form-group">
              <label class="form-label">Platform Fee (₹)</label>
              <input type="number" class="form-input" value="50" placeholder="Fixed fee">
            </div>
            <div class="form-group">
              <label class="form-label">Payout Frequency</label>
              <select class="form-input">
                <option>Weekly</option>
                <option>Bi-weekly</option>
                <option>Monthly</option>
              </select>
            </div>
            <div class="form-group">
              <label class="form-label">Minimum Threshold</label>
              <input type="number" class="form-input" value="5000" placeholder="Minimum amount">
            </div>
          </div>
          <button class="btn btn-primary" onclick="alert('Commission settings updated!')">Save Changes</button>
        </div>
      </div>
      
      <div class="data-table" style="margin-top: 24px;">
        <div class="table-header">
          <h3 class="table-title">👥 Admin User Management</h3>
        </div>
        <div style="padding: 20px;">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Email</th>
                <th>Role</th>
                <th>Status</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Admin User</td>
                <td>admin@vyapaar.ai</td>
                <td><span class="status-badge status-active">Super Admin</span></td>
                <td><span class="status-badge status-active">Active</span></td>
                <td><button class="action-btn secondary">Edit</button></td>
              </tr>
              <tr>
                <td>Support Staff</td>
                <td>support@vyapaar.ai</td>
                <td><span class="status-badge status-pending">Moderator</span></td>
                <td><span class="status-badge status-active">Active</span></td>
                <td><button class="action-btn secondary">Edit</button></td>
              </tr>
            </tbody>
          </table>
          <button class="btn btn-primary" onclick="alert('Adding new admin user...')" style="margin-top: 16px;">➕ Add Admin User</button>
        </div>
      </div>
    `}
    
    <div class="data-table" style="margin-top: 24px;">
      <div class="table-header">
        <h3 class="table-title">📋 Audit Log</h3>
      </div>
      <div style="padding: 20px;">
        <div class="notification-item">✓ Login successful from IP: 192.168.1.1 - Oct 26, 2025 11:45 AM</div>
        <div class="notification-item">✓ Settings updated - Oct 26, 2025 11:30 AM</div>
        <div class="notification-item">✓ ${role === 'admin' ? 'Merchant approved' : 'Product added'} - Oct 26, 2025 10:15 AM</div>
        <div class="notification-item">✓ Password changed - Oct 25, 2025 3:20 PM</div>
      </div>
    </div>
  `;
}

// Chart Rendering Functions
function renderRevenueChart() {
  const canvas = document.getElementById('revenueChart');
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d');
  const data = sampleData.admin.revenue_trends;
  
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.map(d => d.month),
      datasets: [{
        label: 'Revenue (₹)',
        data: data.map(d => d.revenue),
        borderColor: '#21808D',
        backgroundColor: 'rgba(33, 128, 141, 0.1)',
        tension: 0.4,
        fill: true
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              return '₹' + (value / 100000).toFixed(1) + 'L';
            }
          }
        }
      }
    }
  });
}

function renderCategoryChart() {
  const canvas = document.getElementById('categoryChart');
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d');
  
  new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['Electronics', 'Fashion', 'Home & Garden', 'Beauty', 'Others'],
      datasets: [{
        data: [35, 25, 20, 12, 8],
        backgroundColor: [
          '#1FB8CD',
          '#FFC185',
          '#B4413C',
          '#5D878F',
          '#DB4545'
        ]
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'bottom'
        }
      }
    }
  });
}

function renderSalesChart() {
  const canvas = document.getElementById('salesChart');
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d');
  const data = sampleData.merchant.sales_trends;
  
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => d.day),
      datasets: [{
        label: 'Sales (₹)',
        data: data.map(d => d.sales),
        backgroundColor: '#21808D',
        borderRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              return '₹' + (value / 1000).toFixed(0) + 'K';
            }
          }
        }
      }
    }
  });
}

function renderProductPerformanceChart() {
  const canvas = document.getElementById('productChart');
  if (!canvas) return;
  
  const ctx = canvas.getContext('2d');
  const products = sampleData.merchant.products;
  
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: products.map(p => p.name),
      datasets: [{
        label: 'Sales',
        data: products.map(p => p.sales),
        backgroundColor: '#21808D',
        borderRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: {
        legend: {
          display: false
        }
      },
      scales: {
        x: {
          beginAtZero: true
        }
      }
    }
  });
}

// Initialize the app
init();