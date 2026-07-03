/* =========================================================
   STEP — Shared engine: mock data, role/theme state, shell render
   ========================================================= */
const STEP = (function () {

  /* ---- Navigation config (RBAC source of truth for the prototype) ---- */
  const ALL_ROLES = ['spv', 'area_manager', 'distributor_manager', 'regional_sales', 'ho_admin', 'distributor_admin'];
  const HQ_ROLES  = ['spv', 'area_manager', 'distributor_manager', 'regional_sales', 'ho_admin'];

  /* ================================================================
     PERMISSIONS — single source of truth for all RBAC.
     Each entry: { roles: string[], scopes: { [role]: 'area'|'distributor'|'national' } }
     'area'        = data limited to user's assigned territory
     'distributor' = data limited to user's distributor
     'national'    = full nationwide access
     Pages use STEP.guardPage(pageId, fn) — fn receives the resolved scope string.
     ================================================================ */
  const PERMISSIONS = {
    'dashboard':            { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
    'store-opportunity':    { roles: ALL_ROLES, scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national', distributor_admin:'distributor' } },
    'route-evaluation':     { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
    'route-planner':        { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
    'outlet-salesman':      { roles: ALL_ROLES, scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national', distributor_admin:'distributor' } },
    'target-management':    { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
    'master-data':          { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
    'master-data-salesman': { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
    'master-data-pjp':      { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
    'administration':       { roles: ['ho_admin'], scopes: { ho_admin:'national' } },
    'approvals':            { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
    'import-export':        { roles: HQ_ROLES,  scopes: { spv:'area', area_manager:'area', distributor_manager:'national', regional_sales:'national', ho_admin:'national' } },
  };

  /* Derive roles from PERMISSIONS so NAV and PERMISSIONS never diverge */
  const NAV = [
    { id: 'dashboard',         href: 'dashboard.html',            icon: '📊', label: 'Dashboard',                 roles: PERMISSIONS['dashboard'].roles },
    { id: 'store-opportunity', href: 'store-opportunity.html',    icon: '🎯', label: 'Store Opportunity',         roles: PERMISSIONS['store-opportunity'].roles },
    { id: 'route-evaluation',  href: 'route-evaluation.html',     icon: '🧭', label: 'Route Evaluate',            roles: PERMISSIONS['route-evaluation'].roles },
    { id: 'route-planner',     href: 'route-planner.html',        icon: '🗺️', label: 'Route Planner',             roles: PERMISSIONS['route-planner'].roles },
    { id: 'outlet-salesman',   href: 'outlet-salesman.html',      icon: '🏬', label: 'Store & Salesman Evaluate', roles: PERMISSIONS['outlet-salesman'].roles },
    { id: 'target-management', href: 'target-management.html',    icon: '🎯', label: 'Manajemen Target',          roles: PERMISSIONS['target-management'].roles },
    { id: 'master-data',       href: 'master-data-salesman.html', icon: '🗂️', label: 'Master Data Salesman',      roles: PERMISSIONS['master-data'].roles },
    { id: 'administration',    href: 'administration.html',       icon: '⚙️', label: 'Administrasi',              roles: PERMISSIONS['administration'].roles },
  ];

  const CROSS = [
    { id: 'approvals', href: 'approvals.html', icon: '📥', label: 'Approval' },
    { id: 'import-export', href: 'import-export.html', icon: '⇅', label: 'Import & Export' },
  ];

  const BOTTOM_NAV = [
    { id: 'dashboard', href: 'dashboard.html', icon: '📊', label: 'Dashboard' },
    { id: 'route-planner', href: 'route-planner.html', icon: '🗺️', label: 'Planner' },
    { id: 'notifications', href: 'notifications.html', icon: '🔔', label: 'Notification' },
    { id: 'import-export', href: 'import-export.html', icon: '⇅', label: 'Import' },
    { id: 'more', href: 'administration.html', icon: '⋯', label: 'Lainnya' },
  ];

  /* ---- Multi-brand / multi-tenant support ---- */
  const BRAND_GROUPS = {
    skintific_group: {
      id: 'skintific_group', label: 'Skintific Group',
      logo: 'assets/img/logo-skintific-group.png',
      brands: ['Skintific', 'Timephoria', 'Facerinna'],
      tenant: { appTitle: 'STEP — Skintific Group', primaryHue: '203', accentColor: '#0EA5E9', navAccent: '#38BDF8', shortName: 'SKT' },
    },
    g2g_group: {
      id: 'g2g_group', label: 'G2G Group',
      logo: 'assets/img/logo-g2g-group.jpg',
      brands: ['Glad2Glow', 'Bodibreeze', 'Next Prime'],
      tenant: { appTitle: 'STEP — G2G Group', primaryHue: '348', accentColor: '#F43F5E', navAccent: '#FDA4AF', shortName: 'G2G' },
    },
  };

  /* Apply tenant branding (logo already handled in renderShell; this updates CSS vars + title) */
  function applyTenantBranding() {
    const group = BRAND_GROUPS[getBrandGroup()];
    if (!group?.tenant) return;
    document.title = group.tenant.appTitle;
    const r = document.documentElement.style;
    r.setProperty('--primary-hue', group.tenant.primaryHue);
    r.setProperty('--brand-accent', group.tenant.accentColor);
    r.setProperty('--nav-accent',   group.tenant.navAccent);
  }
  const ALL_BRAND_GROUP_IDS = Object.keys(BRAND_GROUPS);
  const ALL_BRANDS = ALL_BRAND_GROUP_IDS.flatMap(g => BRAND_GROUPS[g].brands);
  function brandGroupOfBrand(brand) { return ALL_BRAND_GROUP_IDS.find(g => BRAND_GROUPS[g].brands.includes(brand)); }

  const ROLES = {
    spv:                  { label: 'SPV', name: 'Ahmad Setiawan', initials: 'AS', territory: 'Branch Jakarta Selatan 3', brandGroups: ['skintific_group'] },
    area_manager:         { label: 'Area Manager', name: 'Rina Wulandari', initials: 'RW', territory: 'Area Jakarta Selatan', brandGroups: ['skintific_group'] },
    distributor_manager:  { label: 'Distributor Manager', name: 'Budi Santoso', initials: 'BS', territory: 'Nasional — Head Office', brandGroups: ['skintific_group'] },
    regional_sales:       { label: 'Regional Sales', name: 'Dewi Anggraini', initials: 'DA', territory: 'Nasional — Head Office', brandGroups: ['skintific_group', 'g2g_group'] },
    ho_admin:             { label: 'Head Office Admin', name: 'Sari Indrawati', initials: 'SI', territory: 'Nasional', brandGroups: ['skintific_group', 'g2g_group'] },
    distributor_admin:    { label: 'Distributor Admin', name: 'Budi Distributor', initials: 'BD', territory: 'PT Mitra Jaya Distribusi', distributor_code: 'DST171', brandGroups: ['skintific_group', 'g2g_group'] },
  };

  /* ---- State ---- */
  function getRole() { return localStorage.getItem('step_role') || 'spv'; }
  function setRole(r) { localStorage.setItem('step_role', r); setBrandGroup(ROLES[r].brandGroups[0], { silent: true }); location.reload(); }
  function getTheme() { return localStorage.getItem('step_theme') || 'light'; }
  function applyTheme() { document.documentElement.setAttribute('data-theme', getTheme()); }
  function toggleTheme() {
    localStorage.setItem('step_theme', getTheme() === 'dark' ? 'light' : 'dark');
    applyTheme();
  }

  function availableBrandGroups() { return ROLES[getRole()].brandGroups; }
  function getBrandGroup() {
    const avail = availableBrandGroups();
    const stored = localStorage.getItem('step_brand_group');
    return avail.includes(stored) ? stored : avail[0];
  }
  function setBrandGroup(groupId, opts) {
    localStorage.setItem('step_brand_group', groupId);
    if (!(opts && opts.silent)) location.reload();
  }
  function brandsInCurrentGroup() { return BRAND_GROUPS[getBrandGroup()].brands; }
  function salesmenInCurrentGroup() { const brands = brandsInCurrentGroup(); return DATA.salesmen.filter(sm => brands.includes(sm.brand)); }

  /* ---- Sidebar collapse (desktop) ---- */
  function isSidebarCollapsed() { return localStorage.getItem('step_sidebar_collapsed') === '1'; }
  function setSidebarCollapsed(val) { localStorage.setItem('step_sidebar_collapsed', val ? '1' : '0'); }

  /* ---- ISO-8601 week helpers: Mon-Sun, week 1 = week containing the year's first Thursday ---- */
  function isoWeekInfo(date) {
    const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
    const dayNum = d.getUTCDay() || 7; // Mon=1 ... Sun=7
    d.setUTCDate(d.getUTCDate() + 4 - dayNum); // move to this week's Thursday
    const isoYearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
    const week = Math.ceil(((d - isoYearStart) / 86400000 + 1) / 7);
    return { week, isoYear: d.getUTCFullYear() };
  }
  function isoWeekMonday(date) {
    const d = new Date(date);
    const dayNum = d.getDay() || 7;
    d.setDate(d.getDate() - dayNum + 1);
    d.setHours(0, 0, 0, 0);
    return d;
  }
  function isoWeekMondayForOffset(baseDate, offset) {
    const monday = isoWeekMonday(baseDate);
    monday.setDate(monday.getDate() + offset * 7);
    return monday;
  }
  function fmtDateID(d) { return d.toLocaleDateString('id-ID', { day: '2-digit', month: 'short' }); }
  function isoWeekLabel(baseDate, offset) {
    const monday = isoWeekMondayForOffset(baseDate, offset);
    const sunday = new Date(monday); sunday.setDate(sunday.getDate() + 6);
    const { week } = isoWeekInfo(monday);
    return { week, monday, sunday, label: `Week ${week}`, range: `${fmtDateID(monday)} - ${fmtDateID(sunday)}` };
  }

  function dataAsOf() {
    try {
      const d = new Date();
      const parts = new Intl.DateTimeFormat('id-ID', {
        timeZone: 'Asia/Jakarta', day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false
      }).formatToParts(d).reduce((a, p) => (a[p.type] = p.value, a), {});
      return `${parts.day} ${parts.month} ${parts.year} ${parts.hour}.${parts.minute} WIB`;
    } catch (e) { return '25 Jun 2026 14.32 WIB'; }
  }

  /* =========================================================
     DETERMINISTIC GENERATORS — same data every load, no server needed
     ========================================================= */
  function mulberry32(seed) {
    return function () {
      seed |= 0; seed = (seed + 0x6D2B79F5) | 0;
      let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  function pick(rng, arr) { return arr[Math.floor(rng() * arr.length)]; }
  function pickWeighted(rng, weighted) {
    const total = weighted.reduce((a, w) => a + w[1], 0);
    let r = rng() * total;
    for (const [val, w] of weighted) { if ((r -= w) <= 0) return val; }
    return weighted[0][0];
  }

  const REGIONS = ['Jabodetabek', 'Jawa Barat', 'Jawa Tengah', 'Jawa Timur', 'Sumatera Utara'];
  const AREAS_BY_REGION = {
    'Jabodetabek': ['Jakarta Selatan', 'Jakarta Utara', 'Tangerang', 'Bekasi'],
    'Jawa Barat': ['Bandung', 'Bogor', 'Cirebon'],
    'Jawa Tengah': ['Semarang', 'Solo'],
    'Jawa Timur': ['Surabaya', 'Malang'],
    'Sumatera Utara': ['Medan'],
  };
  const DISTRIBUTORS = ['PT Mitra Jaya Distribusi', 'PT Sinar Abadi Sejahtera', 'PT Cahaya Nusantara', 'PT Berkah Sentosa', 'PT Anugerah Makmur', 'PT Karya Utama Niaga'];
  const SPVS = ['Ahmad Setiawan', 'Rizal Ramadhan', 'Fitriani Putri', 'Sandi Kurniawan', 'Lina Marlina', 'Hendra Wijaya'];
  const FIRST_NAMES = ['Yusuf', 'Putri', 'Agus', 'Maya', 'Budi', 'Siti', 'Andi', 'Rina', 'Dedi', 'Sri', 'Hendra', 'Fitri', 'Bambang', 'Wulan', 'Eko', 'Indah', 'Joko', 'Lestari', 'Rudi', 'Dewi', 'Anton', 'Ratna', 'Wahyu', 'Yuni', 'Bayu', 'Sari', 'Iwan', 'Nina', 'Hadi', 'Tika', 'Arif', 'Mira', 'Doni', 'Lina', 'Surya', 'Fani', 'Teguh', 'Rani', 'Adi', 'Vina'];
  const LAST_NAMES = ['Maulana', 'Lestari', 'Setiadi', 'Kusuma', 'Santoso', 'Wulandari', 'Pratama', 'Saputra', 'Anggraini', 'Nugroho', 'Wibowo', 'Permata', 'Hidayat', 'Susanti', 'Firmansyah', 'Ramadhani', 'Kurniawan', 'Safitri', 'Gunawan', 'Wijaya'];

  function generateSalesmen() {
    const rng = mulberry32(20260622);
    const base = [
      { id: 'SLM-01', name: 'Yusuf Maulana', code: 'SF-021', region: 'Jabodetabek', area: 'Jakarta Selatan', territory: 'Jakarta Selatan', distributor: 'PT Mitra Jaya Distribusi', spv: 'Ahmad Setiawan', status: 'active', capacityPerDay: 12, routeCompliancePct: 88, achievementPct: 81, coveragePct: 94, brand: 'Skintific' },
      { id: 'SLM-02', name: 'Putri Lestari', code: 'SF-022', region: 'Jabodetabek', area: 'Jakarta Selatan', territory: 'Jakarta Selatan', distributor: 'PT Mitra Jaya Distribusi', spv: 'Ahmad Setiawan', status: 'active', capacityPerDay: 10, routeCompliancePct: 95, achievementPct: 104, coveragePct: 98, brand: 'Skintific' },
      { id: 'SLM-03', name: 'Agus Setiadi', code: 'SF-023', region: 'Jabodetabek', area: 'Jakarta Selatan', territory: 'Jakarta Selatan', distributor: 'PT Mitra Jaya Distribusi', spv: 'Ahmad Setiawan', status: 'active', capacityPerDay: 12, routeCompliancePct: 71, achievementPct: 62, coveragePct: 80, brand: 'Skintific' },
      { id: 'SLM-04', name: 'Maya Kusuma', code: 'SF-024', region: 'Jabodetabek', area: 'Jakarta Selatan', territory: 'Jakarta Selatan', distributor: 'PT Mitra Jaya Distribusi', spv: 'Ahmad Setiawan', status: 'active', capacityPerDay: 11, routeCompliancePct: 90, achievementPct: 89, coveragePct: 91, brand: 'Skintific' },
    ];
    const generated = [];
    let n = 5;
    for (const region of REGIONS) {
      for (const area of AREAS_BY_REGION[region]) {
        const countForArea = 2 + Math.floor(rng() * 3); // 2-4 salesmen per area
        for (let i = 0; i < countForArea; i++) {
          generated.push({
            id: 'SLM-' + String(n).padStart(2, '0'),
            name: `${pick(rng, FIRST_NAMES)} ${pick(rng, LAST_NAMES)}`,
            code: 'SF-' + (20 + n),
            region, area, territory: area,
            distributor: pick(rng, DISTRIBUTORS),
            spv: pick(rng, SPVS),
            brand: pick(rng, ALL_BRANDS),
            status: pickWeighted(rng, [['active', 85], ['on_leave', 10], ['inactive', 5]]),
            capacityPerDay: 9 + Math.floor(rng() * 4),
            routeCompliancePct: 65 + Math.floor(rng() * 33),
            achievementPct: 55 + Math.floor(rng() * 65),
            coveragePct: 70 + Math.floor(rng() * 29),
          });
          n++;
        }
      }
    }
    return base.concat(generated);
  }

  const STORE_TYPES = ['Toko', 'Apotek', 'Store', 'Mini Market', 'Swalayan', 'Klinik Estetika'];
  const STORE_NAMES = ['Maju Jaya', 'Cantik Bersama', 'Sejahtera', 'Harmoni', 'Sumber Rejeki', 'Berkah', 'Indah', 'Sentosa', 'Makmur', 'Bahagia', 'Mulia', 'Abadi', 'Sukses', 'Cahaya', 'Mitra', 'Permata', 'Anugerah', 'Sentral', 'Bersinar', 'Nusantara', 'Melati', 'Kencana', 'Damai', 'Lestari', 'Utama'];
  const TIER_WEIGHTS = [['S', 8], ['A', 27], ['B', 35], ['C', 22], ['D', 8]];
  const VISIT_START_MIN = 8 * 60; // 08:00

  function fmtTime(mins) { const h = Math.floor(mins / 60), m = mins % 60; return String(h).padStart(2, '0') + '.' + String(m).padStart(2, '0'); }

  function makeOutletStub(rng, areaName, idx) {
    return {
      code: 'STR' + String(idx).padStart(5, '0'),
      name: `${pick(rng, STORE_TYPES)} ${pick(rng, STORE_NAMES)} ${areaName}`,
      tier: pickWeighted(rng, TIER_WEIGHTS),
      area: areaName,
      lastSellIn: 800000 + Math.floor(rng() * 4200000),
      lastVisitDays: 1 + Math.floor(rng() * 30),
    };
  }

  /** Deterministic Mon–Sat weekly route for a salesman: 10-12 outlets/day,
   *  with 2-3 "anchor" outlets recurring on Mon/Wed/Fri (MWF visit cycle).
   *  Pure function of (salesmanId, weekOffset) — same inputs always produce the same route,
   *  no shared mutable state, so re-rendering the same salesman/week never reshuffles outlets. */
  function generateWeeklyRoute(salesmanId, areaName, weekOffset) {
    const seedNum = hashStr(salesmanId + '|' + weekOffset);
    const rng = mulberry32(seedNum);
    let codeCounter = 0;
    const nextStub = () => makeOutletStub(rng, areaName, ++codeCounter);
    const dayKeys = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
    const anchorCount = 2 + Math.floor(rng() * 2); // 2-3
    const anchors = Array.from({ length: anchorCount }, nextStub);
    const route = {};
    dayKeys.forEach((day) => {
      const count = 10 + Math.floor(rng() * 3); // 10-12
      const isMWF = day === 'mon' || day === 'wed' || day === 'fri';
      const dayOutlets = isMWF ? anchors.slice() : [];
      while (dayOutlets.length < count) dayOutlets.push(nextStub());
      let mins = VISIT_START_MIN;
      route[day] = dayOutlets.map((o, i) => {
        mins += 35 + Math.floor(rng() * 20);
        return { seq: i + 1, code: o.code, name: o.name, tier: o.tier, area: o.area, plannedTime: fmtTime(mins), lastSellIn: o.lastSellIn, lastVisitDays: o.lastVisitDays };
      });
    });
    route.sun = [];
    return route;
  }
  function hashStr(s) { let h = 0; for (let i = 0; i < s.length; i++) { h = (Math.imul(31, h) + s.charCodeAt(i)) | 0; } return h; }

  /* =========================================================
     MOCK DATA
     ========================================================= */
  const DATA = {
    outlets: [
      { id: 'OUT-001', name: 'Toko Sumber Jaya', code: 'JKS-0142', tier: 'S', territory: 'Jakarta Selatan', lastVisitDays: 18, sellInGrowthPct: -32, potentialScore: 88, isNew: false, achievementPct: 64, target: 18000000, achieved: 11500000 },
      { id: 'OUT-002', name: 'Apotek Cahaya Sehat', code: 'JKS-0091', tier: 'A', territory: 'Jakarta Selatan', lastVisitDays: 6, sellInGrowthPct: 12, potentialScore: 71, isNew: false, achievementPct: 92, target: 9000000, achieved: 8280000 },
      { id: 'OUT-003', name: 'Beauty Corner Kemang', code: 'JKS-0203', tier: 'A', territory: 'Jakarta Selatan', lastVisitDays: 27, sellInGrowthPct: -8, potentialScore: 65, isNew: false, achievementPct: 58, target: 12000000, achieved: 6960000 },
      { id: 'OUT-004', name: 'Toko Kosmetik Melati', code: 'JKS-0177', tier: 'B', territory: 'Jakarta Selatan', lastVisitDays: 3, sellInGrowthPct: 5, potentialScore: 54, isNew: false, achievementPct: 101, target: 6000000, achieved: 6060000 },
      { id: 'OUT-005', name: 'Indah Skincare Store', code: 'JKS-0218', tier: 'B', territory: 'Jakarta Selatan', lastVisitDays: 41, sellInGrowthPct: -19, potentialScore: 49, isNew: false, achievementPct: 39, target: 7000000, achieved: 2730000 },
      { id: 'OUT-006', name: 'Toko Baru Pondok Indah', code: 'JKS-0301', tier: 'C', territory: 'Jakarta Selatan', lastVisitDays: 9, sellInGrowthPct: 0, potentialScore: 76, isNew: true, achievementPct: 21, target: 4000000, achieved: 840000 },
      { id: 'OUT-007', name: 'Glow Beauty Mart', code: 'JKS-0254', tier: 'A', territory: 'Jakarta Selatan', lastVisitDays: 14, sellInGrowthPct: -4, potentialScore: 70, isNew: false, achievementPct: 77, target: 10000000, achieved: 7700000 },
      { id: 'OUT-008', name: 'Toko Wangi Sentosa', code: 'JKS-0066', tier: 'C', territory: 'Jakarta Selatan', lastVisitDays: 22, sellInGrowthPct: -12, potentialScore: 41, isNew: false, achievementPct: 45, target: 3500000, achieved: 1575000 },
      { id: 'OUT-009', name: 'Skintific Corner Senopati', code: 'JKS-0288', tier: 'S', territory: 'Jakarta Selatan', lastVisitDays: 2, sellInGrowthPct: 24, potentialScore: 91, isNew: false, achievementPct: 113, target: 20000000, achieved: 22600000 },
      { id: 'OUT-010', name: 'Toko Bunga Skincare', code: 'JKS-0199', tier: 'D', territory: 'Jakarta Selatan', lastVisitDays: 35, sellInGrowthPct: -22, potentialScore: 30, isNew: false, achievementPct: 33, target: 2500000, achieved: 825000 },
    ],

    salesmen: generateSalesmen(),

    recommendations: [
      { outletId: 'OUT-001', category: 'critical', score: 0.91, reasons: ['Belum dikunjungi 18 hari', 'Sell-In turun 32%', 'Tier S', 'Skor potensi tinggi (88)'], suggestedDay: 'Tue' },
      { outletId: 'OUT-005', category: 'critical', score: 0.83, reasons: ['Belum dikunjungi 41 hari', 'Sell-In turun 19%', 'Coverage gap — sudah lewat jadwal'], suggestedDay: 'Mon' },
      { outletId: 'OUT-009', category: 'recommended', score: 0.61, reasons: ['Tier S', 'Sell-In naik 24% — pertahankan momentum', 'Terakhir dikunjungi 2 hari lalu'], suggestedDay: 'Thu' },
      { outletId: 'OUT-003', category: 'recommended', score: 0.58, reasons: ['Belum dikunjungi 27 hari', 'Sell-In turun 8%', 'Tier A'], suggestedDay: 'Wed' },
      { outletId: 'OUT-006', category: 'recommended', score: 0.52, reasons: ['Store baru (< 30 hari)', 'Skor potensi tinggi (76)'], suggestedDay: 'Fri' },
      { outletId: 'OUT-008', category: 'optional', score: 0.33, reasons: ['Belum dikunjungi 22 hari', 'Tier C — prioritas lebih rendah'], suggestedDay: 'Fri' },
      { outletId: 'OUT-010', category: 'optional', score: 0.29, reasons: ['Tier D', 'Skor potensi rendah (30)'], suggestedDay: 'Fri' },
    ],

    recommendationRules: [
      { factor: 'Hari Sejak Kunjungan Terakhir', weight: 25 },
      { factor: 'Pertumbuhan Sell-In', weight: 20 },
      { factor: 'Tier Store', weight: 20 },
      { factor: 'Skor Potensi', weight: 15 },
      { factor: 'Store Baru', weight: 10 },
      { factor: 'Kedekatan Route', weight: 10 },
    ],

    areaTarget: { period: 'Jul 2026', territory: 'Jakarta Selatan', amount: 92000000 },

    approvals: [
      { id: 'APR-1042', type: 'target_adjustment', title: 'Penyesuaian Target Store — Toko Sumber Jaya', requestedBy: 'Ahmad Setiawan (SPV)', submittedAt: '2026-06-23 09:14', status: 'pending_l1', sla: 'ontrack', slaDueAt: '2026-06-24 09:14', current: 'Rp18.000.000', proposed: 'Rp22.000.000', deltaPct: '+22.2%', reason: 'Promo distributor Q3 diperkirakan meningkatkan penjualan — store sudah konfirmasi ikut kampanye Juli.', comments: [{ by: 'Ahmad Setiawan', at: '2026-06-23 09:14', text: 'Mengajukan sebelum periode promo Juli dimulai.' }] },
      { id: 'APR-1041', type: 'tier_override', title: 'Override Tier — Toko Baru Pondok Indah', requestedBy: 'Ahmad Setiawan (SPV)', submittedAt: '2026-06-22 16:02', status: 'pending_l2', sla: 'atrisk', slaDueAt: '2026-06-24 16:02', current: 'Tier C', proposed: 'Tier B', autoEngine: 'Tier C', reason: 'Store menunjukkan sell-through awal yang kuat; mengajukan upgrade manual sebelum siklus klasifikasi otomatis berikutnya.', comments: [{ by: 'Ahmad Setiawan', at: '2026-06-22 16:02', text: 'Performa minggu 1-3 kuat, lihat data Sell-In terlampir.' }, { by: 'Rina Wulandari', at: '2026-06-23 10:20', text: 'Disetujui di level Area Manager — angkanya sudah sesuai.' }] },
      { id: 'APR-1039', type: 'reopen_request', title: 'Permintaan Buka Kembali — Target Juni (Toko Beauty Corner Kemang)', requestedBy: 'Ahmad Setiawan (SPV)', submittedAt: '2026-06-21 11:30', status: 'rejected_back_to_submitter', sla: 'breached', slaDueAt: '2026-06-22 11:30', current: 'Terkunci', proposed: 'Buka kembali untuk koreksi', reason: 'Ditemukan kesalahan input data pada mapping kode store setelah periode terkunci.', comments: [{ by: 'Ahmad Setiawan', at: '2026-06-21 11:30', text: 'Kode store yang salah ter-mapping saat import bulan Mei — perlu dikoreksi.' }, { by: 'Rina Wulandari', at: '2026-06-22 14:10', text: 'Ditolak — mohon lampirkan file error dari import awal sebelum mengajukan ulang.' }] },
      { id: 'APR-1037', type: 'target_adjustment', title: 'Penyesuaian Target Store — Glow Beauty Mart', requestedBy: 'Ahmad Setiawan (SPV)', submittedAt: '2026-06-18 08:40', status: 'approved', sla: 'ontrack', slaDueAt: '2026-06-19 08:40', current: 'Rp8.500.000', proposed: 'Rp10.000.000', deltaPct: '+17.6%', reason: 'Listing SKU baru disetujui untuk store ini mulai Juli.', comments: [{ by: 'Ahmad Setiawan', at: '2026-06-18 08:40', text: 'Listing SKU baru sudah dikonfirmasi oleh distributor.' }, { by: 'Rina Wulandari', at: '2026-06-18 15:00', text: 'Disetujui.' }, { by: 'Budi Santoso', at: '2026-06-19 09:12', text: 'Disetujui — silakan lanjutkan.' }] },
      { id: 'APR-1033', type: 'tier_override', title: 'Override Tier — Toko Wangi Sentosa', requestedBy: 'Ahmad Setiawan (SPV)', submittedAt: '2026-06-15 13:00', status: 'rejected_back_to_submitter', sla: 'breached', slaDueAt: '2026-06-16 13:00', current: 'Tier C', proposed: 'Tier B', autoEngine: 'Tier D', reason: 'Mengajukan upgrade berdasarkan peningkatan foot traffic terbaru.', comments: [{ by: 'Ahmad Setiawan', at: '2026-06-15 13:00', text: 'Foot traffic meningkat sejak sayap mal baru dibuka.' }, { by: 'Rina Wulandari', at: '2026-06-16 09:45', text: 'Ditolak — sistem rekomendasi otomatis justru menyarankan downgrade ke D, bukan upgrade ke B. Perlu data Sell-In yang lebih kuat.' }] },
      { id: 'APR-1029', type: 'target_adjustment', title: 'Realokasi Target Area — Jakarta Selatan', requestedBy: 'Ahmad Setiawan (SPV)', submittedAt: '2026-06-10 10:00', status: 'approved', sla: 'ontrack', slaDueAt: '2026-06-11 10:00', current: 'Rp88.000.000', proposed: 'Rp92.000.000', deltaPct: '+4.5%', reason: 'Realokasi tengah bulan menyusul revisi target area dari Head Office.', comments: [{ by: 'Ahmad Setiawan', at: '2026-06-10 10:00', text: 'Melakukan realokasi sesuai target area revisi dari Head Office.' }, { by: 'Rina Wulandari', at: '2026-06-10 16:00', text: 'Disetujui.' }, { by: 'Budi Santoso', at: '2026-06-11 08:30', text: 'Disetujui.' }] },
    ],

    notifications: [
      { id: 'N-1', category: 'approval', title: 'Approval baru menunggu review Anda', body: 'Override Tier — Toko Baru Pondok Indah menunggu di tahap Anda.', at: '2026-06-23 10:21', read: false, deepLink: 'approvals.html' },
      { id: 'N-2', category: 'routing', title: 'Route berhasil disimpan', body: 'Yusuf Maulana — minggu 23 Jun 2026 tersimpan tanpa konflik.', at: '2026-06-23 09:02', read: false, deepLink: 'route-planner.html' },
      { id: 'N-3', category: 'target', title: 'Versi target disetujui', body: 'Penyesuaian Target Store untuk Glow Beauty Mart kini berlaku.', at: '2026-06-19 09:12', read: true, deepLink: 'target-management.html' },
      { id: 'N-4', category: 'system', title: 'Sinkronisasi SFA sebagian — 2 salesman terdampak', body: 'Agus Setiadi dan Maya Kusuma memiliki kunjungan yang masih menunggu sinkronisasi.', at: '2026-06-23 07:45', read: false, deepLink: 'administration.html' },
      { id: 'N-5', category: 'announcement', title: 'Materi training baru dipublikasikan', body: 'Sell-In Playbook Juli kini tersedia di Help Center.', at: '2026-06-22 17:00', read: true, deepLink: 'announcements.html' },
      { id: 'N-6', category: 'approval', title: 'Permintaan Anda ditolak', body: 'Permintaan Buka Kembali untuk Beauty Corner Kemang perlu direvisi — lihat komentar.', at: '2026-06-22 14:10', read: false, deepLink: 'approvals.html' },
      { id: 'N-7', category: 'target', title: 'SLA berisiko terlambat', body: 'Override Tier APR-1041 berisiko melewati batas SLA 24 jam.', at: '2026-06-23 12:00', read: false, deepLink: 'approvals.html' },
    ],

    announcements: [
      { id: 'AN-1', type: 'training', title: 'Sell-In Playbook Juli dipublikasikan', body: 'Panduan positioning baru dan cara menjawab keberatan pelanggan untuk kampanye Juli kini tersedia di Help Center.', publishedAt: '2026-06-22 17:00', audience: 'Semua role' },
      { id: 'AN-2', type: 'campaign', title: 'Promo Distributor Q3 — pendaftaran dibuka', body: 'Store kini dapat mendaftar untuk promo co-op Q3. SPV harap menandai store Tier S/A yang memenuhi syarat.', publishedAt: '2026-06-20 09:00', audience: 'SPV, Area Manager' },
      { id: 'AN-3', type: 'policy', title: 'Pembaruan syarat bukti Override Tier', body: 'Permintaan override tier kini wajib menyertakan bukti Sell-In sebelum diajukan, berlaku efektif segera.', publishedAt: '2026-06-18 11:00', audience: 'Semua role' },
      { id: 'AN-4', type: 'meeting', title: 'Review Area Bulanan — 30 Jun, 10.00 WIB', body: 'Seluruh Area Manager dan Distributor Manager diharapkan hadir dalam review performa bulanan.', publishedAt: '2026-06-17 08:00', audience: 'Area Manager, Distributor Manager' },
      { id: 'AN-5', type: 'distributor', title: 'PT Mitra Jaya — alokasi SKU baru', body: 'Alokasi SKU baru telah dikonfirmasi untuk PT Mitra Jaya Distribusi, berlaku mulai siklus pengiriman Juli.', publishedAt: '2026-06-15 14:00', audience: 'Distributor Manager' },
    ],

    exceptions: [
      { id: 'EX-1', type: 'missing_route', detail: 'Agus Setiadi belum memiliki route untuk minggu 30 Jun', territory: 'Jakarta Selatan', status: 'open' },
      { id: 'EX-2', type: 'under_coverage', detail: 'Toko Bunga Skincare belum dikunjungi selama 35 hari', territory: 'Jakarta Selatan', status: 'open' },
      { id: 'EX-3', type: 'failed_sync', detail: 'Maya Kusuma — 4 kunjungan gagal sinkronisasi sejak 22 Jun', territory: 'Jakarta Selatan', status: 'open' },
      { id: 'EX-4', type: 'duplicate_assignment', detail: 'Toko Apotek Cahaya Sehat ter-mapping ke dua salesman (SF-021, SF-022)', territory: 'Jakarta Selatan', status: 'open' },
      { id: 'EX-5', type: 'missing_mapping', detail: 'Toko Baru Pondok Indah belum memiliki salesman', territory: 'Jakarta Selatan', status: 'open' },
      { id: 'EX-6', type: 'low_conversion', detail: 'Toko Wangi Sentosa — 3 kunjungan minggu ini tanpa order (Effective Call 0%)', territory: 'Jakarta Selatan', status: 'open' },
      { id: 'EX-7', type: 'low_conversion', detail: 'Beauty Corner Kemang — Effective Call Rate di bawah 20% selama 2 minggu berturut-turut', territory: 'Jakarta Selatan', status: 'open' },
    ],

    sfaSync: [
      { salesman: 'Yusuf Maulana', status: 'healthy', lastSync: '2026-06-23 13:40', successRate: 100 },
      { salesman: 'Putri Lestari', status: 'healthy', lastSync: '2026-06-23 13:38', successRate: 99 },
      { salesman: 'Agus Setiadi', status: 'partial', lastSync: '2026-06-23 09:10', successRate: 84 },
      { salesman: 'Maya Kusuma', status: 'failed', lastSync: '2026-06-22 18:02', successRate: 61 },
    ],

    auditLogs: [
      { at: '2026-06-23 10:20', actor: 'Rina Wulandari', action: 'Menyetujui Override Tier (L1)', entity: 'APR-1041' },
      { at: '2026-06-22 14:10', actor: 'Rina Wulandari', action: 'Menolak Permintaan Buka Kembali', entity: 'APR-1039' },
      { at: '2026-06-19 09:12', actor: 'Budi Santoso', action: 'Menyetujui Penyesuaian Target (L2)', entity: 'APR-1037' },
      { at: '2026-06-18 09:00', actor: 'Sari Indrawati', action: 'Memperbarui bobot Recommendation Rule', entity: 'Hari Sejak Kunjungan Terakhir 20% → 25%' },
      { at: '2026-06-15 13:00', actor: 'Ahmad Setiawan', action: 'Mengajukan Override Tier', entity: 'APR-1033' },
      { at: '2026-06-10 16:00', actor: 'Rina Wulandari', action: 'Menyetujui Realokasi Target (L1)', entity: 'APR-1029' },
    ],
  };

  /* =========================================================
     TARGET COMPLY (SPV-proposed target vs Management top-down target)
     Shared by target-management.html (editable, page-local override map)
     and dashboard.html (read-only baseline KPI cards) so both always
     agree on Management Target figures and Comply thresholds.
     ========================================================= */
  const MANAGEMENT_TARGET_BY_BRAND = {
    'Skintific': 1700000000, 'Timephoria': 540000000, 'Facerinna': 500000000,
    'Glad2Glow': 450000000, 'Bodibreeze': 610000000, 'Next Prime': 560000000,
  };
  function salesmanTargetRp(sm) { return Math.round(sm.capacityPerDay * 6 * 1.7 / 5) * 5 * 1000000; }
  function salesmenForBrand(brand) { return DATA.salesmen.filter(sm => sm.brand === brand); }
  function baselineSpvTargetForBrand(brand) { return salesmenForBrand(brand).reduce((a, sm) => a + salesmanTargetRp(sm), 0); }
  function complyPct(mgmtTarget, spvTarget) { return mgmtTarget ? (spvTarget / mgmtTarget) * 100 : 0; }
  function complyStatus(pct) {
    const rounded = Math.round(pct * 10) / 10;
    if (rounded === 100) return { label: 'Comply', cls: 'comply-ok' };
    if (rounded < 100) return { label: 'Under Comply', cls: 'comply-under' };
    return { label: 'Over Target', cls: 'comply-over' };
  }
  function fmtCompliancePct(pct) { const r = Math.round(pct * 10) / 10; return (r % 1 === 0 ? r.toFixed(0) : r.toFixed(1)) + '%'; }

  /* =========================================================
     RENDER HELPERS
     ========================================================= */
  function tierBadge(t) { return `<span class="tier-badge tier-${t}">${t}</span>`; }

  /** Hand-built SVG radar/hexagon chart — "FIFA player card" style overall-status visual.
   *  No charting library (project has no build step / external deps), so the polygon geometry
   *  is computed directly: N axes evenly spaced starting from the top, going clockwise. */
  function hexChartSvg(axes, opts) {
    opts = opts || {};
    const size = opts.size || 280;
    const cx = size / 2, cy = size / 2;
    const maxR = size / 2 - (opts.labelPad || 46);
    const n = axes.length;
    const angleFor = i => (-90 + i * (360 / n)) * Math.PI / 180;
    const ptAt = (i, frac) => { const a = angleFor(i); return [cx + Math.cos(a) * maxR * frac, cy + Math.sin(a) * maxR * frac]; };
    const rings = [0.2, 0.4, 0.6, 0.8, 1].map(frac => {
      const pts = axes.map((_, i) => ptAt(i, frac).join(',')).join(' ');
      return `<polygon points="${pts}" fill="none" stroke="var(--line)" stroke-width="1"/>`;
    }).join('');
    const spokes = axes.map((_, i) => { const [x, y] = ptAt(i, 1); return `<line x1="${cx}" y1="${cy}" x2="${x}" y2="${y}" stroke="var(--line)" stroke-width="1"/>`; }).join('');
    const clamp = v => Math.max(0, Math.min(100, v));
    const dataPts = axes.map((a, i) => ptAt(i, clamp(a.value) / 100).join(',')).join(' ');
    const dataDots = axes.map((a, i) => { const [x, y] = ptAt(i, clamp(a.value) / 100); return `<circle cx="${x}" cy="${y}" r="3.5" fill="${opts.color || 'var(--primary-500)'}"/>`; }).join('');
    const labels = axes.map((a, i) => {
      const [x, y] = ptAt(i, 1.30);
      return `<text x="${x}" y="${y}" text-anchor="middle" dominant-baseline="middle" font-size="11" font-weight="700" fill="var(--ink-soft)">${a.label}</text>
              <text x="${x}" y="${y + 14}" text-anchor="middle" font-size="12" font-weight="800" fill="${opts.color || 'var(--primary-600)'}">${Math.round(a.value)}</text>`;
    }).join('');
    return `<svg viewBox="0 0 ${size} ${size}" width="100%" height="${size}" style="max-width:${size}px; display:block; margin:0 auto;">
      ${rings}${spokes}
      <polygon points="${dataPts}" fill="${opts.color || 'var(--primary-500)'}" fill-opacity="0.22" stroke="${opts.color || 'var(--primary-500)'}" stroke-width="2"/>
      ${dataDots}${labels}
    </svg>`;
  }

  function slaChip(sla) {
    const map = { ontrack: ['sla-ontrack', 'Tepat Waktu'], atrisk: ['sla-atrisk', 'Berisiko'], breached: ['sla-breached', 'Terlambat'] };
    const [cls, label] = map[sla] || map.ontrack;
    return `<span class="sla-chip ${cls}">${label}</span>`;
  }

  function statusPill(status) {
    const map = {
      pending_l1: ['pill-warning', 'Menunggu — Area Manager'],
      pending_l2: ['pill-warning', 'Menunggu — Distributor Manager'],
      approved: ['pill-success', 'Disetujui'],
      rejected_back_to_submitter: ['pill-danger', 'Ditolak — perlu revisi'],
      draft: ['pill-neutral', 'Draft'],
      submitted: ['pill-info', 'Submitted'],
      locked: ['pill-neutral', 'Terkunci'],
      archived: ['pill-neutral', 'Diarsipkan'],
      healthy: ['pill-success', 'Normal'],
      partial: ['pill-warning', 'Sebagian'],
      failed: ['pill-danger', 'Gagal'],
      open: ['pill-warning', 'Terbuka'],
      active: ['pill-success', 'Aktif'],
      on_leave: ['pill-warning', 'Cuti'],
      inactive: ['pill-neutral', 'Nonaktif'],
    };
    const [cls, label] = map[status] || ['pill-neutral', status];
    return `<span class="pill ${cls}">${label}</span>`;
  }

  function typeLabel(type) {
    return { target_adjustment: 'Penyesuaian Target', tier_override: 'Override Tier', reopen_request: 'Permintaan Buka Kembali' }[type] || type;
  }

  function exceptionTypeLabel(type) {
    const map = {
      missing_route: 'Route Belum Dibuat', under_coverage: 'Coverage Kurang', failed_sync: 'Sinkronisasi Gagal',
      duplicate_assignment: 'Penugasan Duplikat', missing_mapping: 'Mapping Belum Ada', low_conversion: 'Konversi Rendah',
    };
    return map[type] || type.replace(/_/g, ' ');
  }

  function announcementTypeLabel(type) {
    const map = { training: 'Training', campaign: 'Campaign', policy: 'Policy', meeting: 'Meeting', distributor: 'Distributor' };
    return map[type] || type;
  }

  function fmtIDR(n) { return 'Rp' + n.toLocaleString('id-ID'); }

  function outletById(id) { return DATA.outlets.find(o => o.id === id); }

  /* ---- Toast ---- */
  function toast(msg, icon = '✓') {
    let wrap = document.querySelector('.toast-wrap');
    if (!wrap) { wrap = document.createElement('div'); wrap.className = 'toast-wrap'; document.body.appendChild(wrap); }
    const el = document.createElement('div');
    el.className = 'toast';
    el.innerHTML = `<span>${icon}</span><span>${msg}</span>`;
    wrap.appendChild(el);
    setTimeout(() => el.remove(), 3200);
  }

  /* ---- Drawer ---- */
  function openDrawer(html) {
    let ov = document.getElementById('stepDrawerOverlay');
    if (!ov) {
      ov = document.createElement('div');
      ov.id = 'stepDrawerOverlay';
      ov.className = 'drawer-overlay';
      ov.innerHTML = `<div class="drawer" id="stepDrawer"></div>`;
      ov.addEventListener('click', (e) => { if (e.target === ov) closeDrawer(); });
      document.body.appendChild(ov);
    }
    document.getElementById('stepDrawer').innerHTML = html;
    ov.classList.add('open');
  }
  function closeDrawer() { const ov = document.getElementById('stepDrawerOverlay'); if (ov) ov.classList.remove('open'); }

  /* ---- Modal ---- */
  function openModal(html) {
    let ov = document.getElementById('stepModalOverlay');
    if (!ov) {
      ov = document.createElement('div');
      ov.id = 'stepModalOverlay';
      ov.className = 'overlay';
      ov.innerHTML = `<div class="modal" id="stepModal"></div>`;
      ov.addEventListener('click', (e) => { if (e.target === ov) closeModal(); });
      document.body.appendChild(ov);
    }
    document.getElementById('stepModal').innerHTML = html;
    ov.classList.add('open');
  }
  function closeModal() { const ov = document.getElementById('stepModalOverlay'); if (ov) ov.classList.remove('open'); }

  /** Opens a modal that goes full-bleed (edge-to-edge) on mobile widths — used for
   *  flows the brief calls out as "full-screen searchable drawer" on mobile (e.g. Add Outlet). */
  function openResponsiveModal(html) {
    openModal(html);
    const m = document.getElementById('stepModal');
    if (m) m.classList.toggle('mobile-full', isMobile());
  }

  function isMobile() { return window.innerWidth <= 767; }
  function isTablet() { return window.innerWidth > 767 && window.innerWidth < 1280; }

  /* ---- Bottom Sheet (mobile filters / actions) ---- */
  function openSheet(html, opts) {
    opts = opts || {};
    let ov = document.getElementById('stepSheetOverlay');
    if (!ov) {
      ov = document.createElement('div');
      ov.id = 'stepSheetOverlay';
      ov.className = 'bottom-sheet-overlay';
      ov.innerHTML = `<div class="bottom-sheet" id="stepSheet"><div class="bottom-sheet-handle"></div><div id="stepSheetContent"></div></div>`;
      ov.addEventListener('click', (e) => { if (e.target === ov) closeSheet(); });
      document.body.appendChild(ov);
    }
    document.getElementById('stepSheetContent').innerHTML = html;
    document.getElementById('stepSheet').classList.toggle('full', !!opts.full);
    ov.classList.add('open');
  }
  function closeSheet() { const ov = document.getElementById('stepSheetOverlay'); if (ov) ov.classList.remove('open'); }

  /** Renders the same dataset as a desktop <table> AND a mobile card list in one call —
   *  CSS (.table-desktop / .report-cards) decides which is visible per breakpoint. */
  function tableOrCards(columns, rows, opts) {
    opts = opts || {};
    if (!rows.length) {
      return `<div class="empty-state"><div class="ic">${opts.emptyIcon || '🔍'}</div><div class="h3">${opts.emptyTitle || 'No results'}</div><p>${opts.emptyText || 'Try adjusting your search or filters.'}</p></div>`;
    }
    const thead = columns.map(c => `<th>${c.label}</th>`).join('');
    const tbody = rows.map(r => `<tr>${columns.map(c => `<td class="${c.numeric ? 'num' : ''}">${c.render(r)}</td>`).join('')}</tr>`).join('');
    const cards = rows.map(r => {
      const titleCol = columns[0];
      const rest = columns.slice(1);
      return `<div class="card report-card">
        <div class="rc-row rc-title"><span class="rc-val">${titleCol.render(r)}</span></div>
        ${rest.map(c => `<div class="rc-row"><span class="rc-label">${c.label}</span><span class="rc-val">${c.render(r)}</span></div>`).join('')}
      </div>`;
    }).join('');
    return `<table class="table table-desktop"><thead><tr>${thead}</tr></thead><tbody>${tbody}</tbody></table>
      <div class="report-cards">${cards}</div>`;
  }

  /* =========================================================
     RBAC GUARD HELPERS
     ========================================================= */

  /* Returns { allowed, scope, role } for a given page ID.
     scope: 'area' | 'distributor' | 'national' | null (when denied) */
  function checkPageAccess(pageId) {
    const role = getRole();
    const perm = PERMISSIONS[pageId];
    if (!perm) return { allowed: true, scope: 'national', role };
    const allowed = perm.roles.includes(role);
    const scope   = allowed ? (perm.scopes[role] || 'national') : null;
    return { allowed, scope, role };
  }

  /* Returns the scope string for the current role + page */
  function getScope(pageId) { return checkPageAccess(pageId).scope; }

  /* Scope badge HTML — injected near the page title */
  function scopeBadgeHtml(scope, territory) {
    if (!scope || scope === 'national') return '';
    const icons = { area: '📍', distributor: '🏢' };
    const labels = { area: `Area: ${territory || '—'}`, distributor: `Distributor: ${territory || '—'}` };
    return `<span class="pill" style="margin-left:10px; font-size:11.5px; vertical-align:middle;">${icons[scope]} ${labels[scope]}</span>`;
  }

  /* Access-denied HTML block — used by guardPage */
  function _accessDeniedHtml(role, pageId) {
    const nav = NAV.find(n => n.id === pageId);
    const moduleName = nav ? nav.label : pageId;
    return `
      <div class="card card-pad" style="max-width:540px; margin:40px auto;">
        <div class="empty-state">
          <div class="ic">🔒</div>
          <div class="h3">Akses Dibatasi</div>
          <p>Role <strong>${ROLES[role]?.label || role}</strong> tidak memiliki izin untuk mengakses modul <strong>${moduleName}</strong>.</p>
          <p class="text-sm ink-faint" style="margin-top:8px;">Pembatasan ini diterapkan di sisi server — tidak hanya di UI. Hubungi HO Admin jika Anda memerlukan akses.</p>
          <div style="display:flex; gap:10px; margin-top:16px; justify-content:center;">
            <a class="btn btn-outline btn-sm" href="dashboard.html">← Dashboard</a>
            <a class="btn btn-outline btn-sm" href="index.html">Ganti Role</a>
          </div>
        </div>
      </div>`;
  }

  /* Main guard function. Call immediately after renderShell(pageId).
     If allowed: calls fn(scope) and returns true.
     If denied:  injects access-denied UI into .content and returns false.
     Usage:
       STEP.renderShell('dashboard');
       STEP.guardPage('dashboard', function(scope) {
         // all page logic here — scope = 'area'|'distributor'|'national'
       }); */
  function guardPage(pageId, fn) {
    // Production mode: verify JWT session, use real role from token
    if (api.enabled) {
      if (!api.session.isLoggedIn()) {
        window.location.href = 'login.html?from=' + encodeURIComponent(window.location.pathname);
        return false;
      }
      const u = api.session.user;
      const perm = PERMISSIONS[pageId];
      const role = u.role;
      const allowed = !perm || perm.roles.includes(role);
      if (!allowed) {
        const content = document.querySelector('.content');
        if (content) content.innerHTML = _accessDeniedHtml(role, pageId);
        return false;
      }
      const scope = perm ? (perm.scopes[role] || 'national') : 'national';
      if (typeof fn === 'function') fn(scope);
      return true;
    }
    // Mock mode: existing behavior
    const { allowed, scope, role } = checkPageAccess(pageId);
    if (!allowed) {
      const content = document.querySelector('.content');
      if (content) content.innerHTML = _accessDeniedHtml(role, pageId);
      return false;
    }
    if (typeof fn === 'function') fn(scope);
    return true;
  }

  /* =========================================================
     SHELL (sidebar + topbar + bottom nav)
     ========================================================= */
  function pendingCountForRole(role) {
    if (role === 'area_manager') return DATA.approvals.filter(a => a.status === 'pending_l1').length;
    if (role === 'distributor_manager') return DATA.approvals.filter(a => a.status === 'pending_l2').length;
    if (role === 'spv') return DATA.approvals.filter(a => a.status === 'rejected_back_to_submitter').length;
    return 0;
  }
  function unreadNotifCount() { return DATA.notifications.filter(n => !n.read).length; }

  function sidebarNavHtml(role, activeId) {
    const navHtml = NAV.filter(n => n.roles.includes(role)).map(n => `
      <a href="${n.href}" class="${n.id === activeId ? 'active' : ''}"><span class="ic">${n.icon}</span><span>${n.label}</span><span class="tooltip">${n.label}</span></a>
    `).join('');
    const crossHtml = CROSS.map(n => {
      const count = n.id === 'approvals' ? pendingCountForRole(role) : 0;
      return `<a href="${n.href}" class="${n.id === activeId ? 'active' : ''}"><span class="ic">${n.icon}</span><span>${n.label}</span>${count ? `<span class="nav-count">${count}</span>` : ''}<span class="tooltip">${n.label}</span></a>`;
    }).join('');
    return `
      <nav class="sidebar-nav">
        <span class="sidebar-section-label">Modul</span>
        ${navHtml}
        <span class="sidebar-section-label">Workflow</span>
        ${crossHtml}
      </nav>
      <div class="sidebar-foot">
        <a href="notifications.html">🔔 Pusat Notification</a>
        <a href="announcements.html">📣 Pusat Pengumuman</a>
        <a href="index.html">← Keluar</a>
      </div>`;
  }

  function renderShell(activeId) {
    applyTheme();
    applyTenantBranding();
    const role = getRole();
    const roleInfo = ROLES[role];
    const collapsed = isSidebarCollapsed();

    const sidebarMount = document.getElementById('sidebarMount');
    if (sidebarMount) {
      sidebarMount.innerHTML = `
        <aside class="sidebar ${collapsed ? 'collapsed' : ''}" id="mainSidebar">
          <div class="sidebar-brand">
            <a href="dashboard.html" style="display:flex; align-items:center; gap:10px; flex:1; min-width:0;">
              <span class="logo-mark">ST</span>
              <span><span class="brand-name">STEP</span><br><span class="brand-tag">Plan. Execute. Monitor.</span></span>
            </a>
            <button class="sidebar-collapse-toggle" id="sidebarCollapseBtn" title="${collapsed ? 'Perluas menu' : 'Ciutkan menu'}">${collapsed ? '»' : '«'}</button>
          </div>
          ${sidebarNavHtml(role, activeId)}
        </aside>`;
      document.getElementById('sidebarCollapseBtn').addEventListener('click', () => {
        setSidebarCollapsed(!isSidebarCollapsed());
        document.getElementById('mainSidebar').classList.toggle('collapsed');
        const btn = document.getElementById('sidebarCollapseBtn');
        btn.textContent = isSidebarCollapsed() ? '»' : '«';
        btn.title = isSidebarCollapsed() ? 'Perluas menu' : 'Ciutkan menu';
      });
    }

    const topbarMount = document.getElementById('topbarMount');
    if (topbarMount) {
      const roleButtons = ALL_ROLES.map(r => `<button data-role="${r}" class="${r === role ? 'active' : ''}">${ROLES[r].label}</button>`).join('');
      const notifCount = unreadNotifCount();
      const apprCount = pendingCountForRole(role);
      const currentGroup = BRAND_GROUPS[getBrandGroup()];
      /* Group switcher — always a dropdown using ALL_BRAND_GROUP_IDS so any role can switch.
         The 'avail' role-filter is retained for data scoping only, not for hiding the switcher. */
      const brandSwitchHtml = `<div class="dropdown-wrap">
          <div class="brand-switch" id="brandSwitchBtn" style="cursor:pointer;">
            <img src="${currentGroup.logo}" alt="${currentGroup.label}" style="height:22px; width:auto; object-fit:contain; border-radius:3px;">
            <span class="full" style="font-weight:600;">${currentGroup.label}</span>
            <span style="font-size:10px; opacity:.6; margin-left:2px;">▾</span>
          </div>
          <div class="dropdown-menu" id="brandSwitchMenu" style="min-width:200px;">
            <div class="dd-head">Pilih Brand Group</div>
            ${ALL_BRAND_GROUP_IDS.map(g => `<button class="dd-item ${g === currentGroup.id ? 'active' : ''}" data-brand-group="${g}" style="display:flex; align-items:center; gap:10px;">
              <img src="${BRAND_GROUPS[g].logo}" alt="" style="height:20px; width:32px; object-fit:contain; border-radius:3px; background:var(--soft-white); padding:1px;">
              <span>${BRAND_GROUPS[g].label}</span>
              ${g === currentGroup.id ? '<span style="margin-left:auto; color:var(--primary-600);">✓</span>' : ''}
            </button>`).join('')}
          </div>
        </div>`;
      topbarMount.innerHTML = `
        <div class="topbar">
          <button class="icon-btn hamburger-btn" id="hamburgerBtn" title="Menu">☰</button>
          <div class="topbar-search"><span>🔎</span><input placeholder="Cari store, salesman, route... (⌘K)"></div>
          <div class="role-switch" id="roleSwitch">${roleButtons}</div>
          ${brandSwitchHtml}
          <div class="territory-switch">📍 <span class="full">${roleInfo.territory}</span></div>
          <div class="topbar-actions">
            <button class="icon-btn" id="themeToggle" title="Mode gelap">${getTheme() === 'dark' ? '☀️' : '🌙'}</button>
            <a class="icon-btn" href="approvals.html" title="Approval">📥${apprCount ? `<span class="badge-dot">${apprCount}</span>` : ''}</a>
            <a class="icon-btn" href="notifications.html" title="Notification">🔔${notifCount ? `<span class="badge-dot">${notifCount}</span>` : ''}</a>
            <div class="dropdown-wrap">
              <button class="avatar profile-trigger" id="profileMenuBtn" title="${roleInfo.name}">${roleInfo.initials}</button>
              <div class="dropdown-menu" id="profileMenu" style="min-width:240px;">
                <div class="dd-head">${roleInfo.name} · ${roleInfo.label}</div>
                <a href="account-settings.html">👤 My Profile</a>
                <a href="account-settings.html?tab=password">🔒 Change Password</a>
                <a href="notifications.html">🔔 Notification Preferences</a>
                <hr>
                <a href="index.html">↪ Sign Out</a>
              </div>
            </div>
          </div>
        </div>`;
      topbarMount.querySelectorAll('#roleSwitch button').forEach(b => b.addEventListener('click', () => setRole(b.dataset.role)));
      const tt = document.getElementById('themeToggle');
      if (tt) tt.addEventListener('click', () => { toggleTheme(); tt.textContent = getTheme() === 'dark' ? '☀️' : '🌙'; });

      wireDropdown('profileMenuBtn', 'profileMenu');
      wireDropdown('brandSwitchBtn', 'brandSwitchMenu');
      document.querySelectorAll('[data-brand-group]').forEach(b => b.addEventListener('click', () => setBrandGroup(b.dataset.brandGroup)));

      const hb = document.getElementById('hamburgerBtn');
      if (hb) hb.addEventListener('click', () => {
        openDrawer(`
          <div class="drawer-head"><strong>Menu</strong><button class="icon-btn" onclick="STEP.closeDrawer()">✕</button></div>
          <div class="drawer-body" style="padding:0;">
            <aside class="sidebar" style="position:static; height:auto; width:100%; border-right:none;">
              ${sidebarNavHtml(role, activeId)}
            </aside>
          </div>`);
      });
    }

    const bottomMount = document.getElementById('bottomNavMount');
    if (bottomMount) {
      bottomMount.innerHTML = `
        <nav class="bottom-nav">
          ${BOTTOM_NAV.map(n => `<a href="${n.href}" class="${n.id === activeId ? 'active' : ''}"><span class="ic">${n.icon}</span><span>${n.label}</span></a>`).join('')}
        </nav>`;
    }
  }

  function wireDropdown(btnId, menuId) {
    const btn = document.getElementById(btnId);
    const menu = document.getElementById(menuId);
    if (!btn || !menu) return;
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      document.querySelectorAll('.dropdown-menu.open').forEach(m => { if (m !== menu) m.classList.remove('open'); });
      menu.classList.toggle('open');
    });
    document.addEventListener('click', (e) => { if (!menu.contains(e.target) && e.target !== btn) menu.classList.remove('open'); });
  }

  /* =========================================================
     PRODUCTION API CLIENT
     Set window.STEP_API_BASE = 'https://your-cloud-run-url' before loading step.js
     to switch from mock data to the real FastAPI backend.
     In mock mode (default, STEP_API_BASE not set): api.enabled = false and all
     api.get/post calls return null — pages fall back to STEP.DATA.
     ========================================================= */
  const API_BASE = (window.STEP_API_BASE || localStorage.getItem('step_api_base') || '').replace(/\/$/, '');

  const _session = {
    _key: 'step_jwt_session',
    _stored() { try { return JSON.parse(localStorage.getItem(this._key)); } catch { return null; } },
    get token() { return this._stored()?.token || null; },
    get user()  { return this._stored()?.user  || null; },
    save(token, user) { localStorage.setItem(this._key, JSON.stringify({ token, user })); },
    clear() { localStorage.removeItem(this._key); },
    isLoggedIn() { return !!this.token; },
  };

  async function _apiFetch(method, path, data) {
    if (!API_BASE) return null;
    const headers = { 'Content-Type': 'application/json' };
    if (_session.token) headers['Authorization'] = 'Bearer ' + _session.token;
    let res;
    try {
      res = await fetch(API_BASE + path, {
        method, headers,
        body: data !== undefined ? JSON.stringify(data) : undefined,
      });
    } catch (e) {
      console.error('STEP API fetch error:', e);
      throw e;
    }
    if (res.status === 401) {
      _session.clear();
      window.location.href = 'login.html?from=' + encodeURIComponent(window.location.pathname);
      return null;
    }
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${res.status}`);
    }
    return res.json();
  }

  const api = {
    enabled: !!API_BASE,
    session: _session,
    get: (path, params) => {
      const qs = params ? '?' + new URLSearchParams(
        Object.fromEntries(Object.entries(params).filter(([, v]) => v != null))
      ).toString() : '';
      return _apiFetch('GET', path + qs, undefined);
    },
    post:   (path, data) => _apiFetch('POST',   path, data),
    put:    (path, data) => _apiFetch('PUT',    path, data),
    delete: (path)       => _apiFetch('DELETE', path, undefined),
    login: async (username, password) => {
      const res = await _apiFetch('POST', '/api/v1/auth/login', { username, password });
      if (res) _session.save(res.access_token, res.user);
      return res;
    },
    logout: () => { _session.clear(); window.location.href = 'login.html'; },
  };

  return {
    NAV, CROSS, ROLES, ALL_ROLES, HQ_ROLES, PERMISSIONS, DATA, REGIONS, AREAS_BY_REGION, DISTRIBUTORS, SPVS,
    getRole, setRole, getTheme, toggleTheme, applyTheme, dataAsOf,
    tierBadge, slaChip, statusPill, typeLabel, exceptionTypeLabel, announcementTypeLabel, fmtIDR, fmtTime, outletById, hexChartSvg,
    toast, openDrawer, closeDrawer, openModal, closeModal, openResponsiveModal,
    openSheet, closeSheet, isMobile, isTablet, tableOrCards, generateWeeklyRoute,
    pendingCountForRole, unreadNotifCount, renderShell, wireDropdown,
    BRAND_GROUPS, ALL_BRAND_GROUP_IDS, ALL_BRANDS, brandGroupOfBrand,
    availableBrandGroups, getBrandGroup, setBrandGroup, brandsInCurrentGroup, salesmenInCurrentGroup,
    applyTenantBranding,
    checkPageAccess, getScope, guardPage, scopeBadgeHtml,
    isSidebarCollapsed, setSidebarCollapsed,
    isoWeekInfo, isoWeekMonday, isoWeekMondayForOffset, isoWeekLabel, fmtDateID,
    MANAGEMENT_TARGET_BY_BRAND, salesmanTargetRp, salesmenForBrand, baselineSpvTargetForBrand,
    complyPct, complyStatus, fmtCompliancePct,
    api,
  };
})();
