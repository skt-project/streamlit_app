import { lazy, Suspense } from "react";
import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import { useAuthStore } from "@/store/authStore";
import { Toaster } from "@/components/ui";
import Layout from "@/components/layout/Layout";
import Login from "@/pages/Login";

// ── Lazy-loaded pages (code-split per route) ───────────────────────────────
const Dashboard         = lazy(() => import("@/pages/Dashboard"));
const RoutePlanner      = lazy(() => import("@/pages/RoutePlanner"));
const RouteEvaluate     = lazy(() => import("@/pages/RouteEvaluate"));
const TargetManagement  = lazy(() => import("@/pages/TargetManagement"));
const Approvals         = lazy(() => import("@/pages/Approvals"));
const Announcements     = lazy(() => import("@/pages/Announcements"));
const Reports           = lazy(() => import("@/pages/Reports"));
const MasterDataPjp     = lazy(() => import("@/pages/MasterDataPjp"));
const MasterDataSalesman = lazy(() => import("@/pages/MasterDataSalesman"));
const OutletSalesman    = lazy(() => import("@/pages/OutletSalesman"));
const Store360          = lazy(() => import("@/pages/Store360"));
const Salesman360       = lazy(() => import("@/pages/Salesman360"));
const StoreOpportunity  = lazy(() => import("@/pages/StoreOpportunity"));
const Visits            = lazy(() => import("@/pages/Visits"));
const VisitDetail       = lazy(() => import("@/pages/VisitDetail"));
const Administration    = lazy(() => import("@/pages/Administration"));
const ImportExport      = lazy(() => import("@/pages/ImportExport"));
const Notifications     = lazy(() => import("@/pages/Notifications"));

// ── Per-page loading skeleton ──────────────────────────────────────────────
function PageFallback() {
  return (
    <div className="flex-1 overflow-y-auto p-6 space-y-5 animate-pulse" aria-hidden="true">
      <div className="flex items-center justify-between">
        <div className="h-6 w-44 bg-slate-200 rounded" />
        <div className="h-9 w-28 bg-slate-200 rounded-lg" />
      </div>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="card h-24 bg-slate-50" />
        ))}
      </div>
      <div className="card h-72 bg-slate-50" />
    </div>
  );
}

// ── Auth guard ─────────────────────────────────────────────────────────────
function AuthGuard({ children }: { children: React.ReactNode }) {
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);
  const location = useLocation();

  // No useEffect rehydrate — the store initializes synchronously from
  // localStorage (see authStore.ts loadInitialState), so isAuthenticated is
  // already correct on the first render.  The old async rehydrate caused a
  // race where the login page flashed and then redirected back to dashboard.
  if (!isAuthenticated) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }
  return <>{children}</>;
}

// ── Routes ─────────────────────────────────────────────────────────────────
function AppRoutes() {
  const { isAuthenticated } = useAuthStore();

  return (
    <Routes>
      <Route
        path="/login"
        element={isAuthenticated ? <Navigate to="/dashboard" replace /> : <Login />}
      />

      <Route
        element={
          <AuthGuard>
            <Layout />
          </AuthGuard>
        }
      >
        <Route index element={<Navigate to="/dashboard" replace />} />

        <Route
          path="dashboard"
          element={<Suspense fallback={<PageFallback />}><Dashboard /></Suspense>}
        />
        <Route
          path="route-planner"
          element={<Suspense fallback={<PageFallback />}><RoutePlanner /></Suspense>}
        />
        <Route
          path="route-evaluate"
          element={<Suspense fallback={<PageFallback />}><RouteEvaluate /></Suspense>}
        />
        <Route
          path="target-management"
          element={<Suspense fallback={<PageFallback />}><TargetManagement /></Suspense>}
        />
        <Route
          path="approvals"
          element={<Suspense fallback={<PageFallback />}><Approvals /></Suspense>}
        />
        <Route
          path="announcements"
          element={<Suspense fallback={<PageFallback />}><Announcements /></Suspense>}
        />
        <Route
          path="reports"
          element={<Suspense fallback={<PageFallback />}><Reports /></Suspense>}
        />

        {/* Master Data */}
        <Route
          path="master-data-pjp"
          element={<Suspense fallback={<PageFallback />}><MasterDataPjp /></Suspense>}
        />
        <Route
          path="master-data-salesman"
          element={<Suspense fallback={<PageFallback />}><MasterDataSalesman /></Suspense>}
        />
        <Route
          path="outlet-salesman"
          element={<Suspense fallback={<PageFallback />}><OutletSalesman /></Suspense>}
        />

        {/* 360° Views */}
        <Route
          path="store360"
          element={<Suspense fallback={<PageFallback />}><Store360 /></Suspense>}
        />
        <Route
          path="salesman360"
          element={<Suspense fallback={<PageFallback />}><Salesman360 /></Suspense>}
        />
        <Route
          path="store-opportunity"
          element={<Suspense fallback={<PageFallback />}><StoreOpportunity /></Suspense>}
        />

        {/* Visits & Demand */}
        <Route
          path="visits"
          element={<Suspense fallback={<PageFallback />}><Visits /></Suspense>}
        />
        <Route
          path="visits/:visitId"
          element={<Suspense fallback={<PageFallback />}><VisitDetail /></Suspense>}
        />

        {/* Admin */}
        <Route
          path="administration"
          element={<Suspense fallback={<PageFallback />}><Administration /></Suspense>}
        />
        <Route
          path="import-export"
          element={<Suspense fallback={<PageFallback />}><ImportExport /></Suspense>}
        />
        <Route
          path="notifications"
          element={<Suspense fallback={<PageFallback />}><Notifications /></Suspense>}
        />
      </Route>

      {/* Fallback */}
      <Route
        path="*"
        element={
          isAuthenticated
            ? <Navigate to="/dashboard" replace />
            : <Navigate to="/login" replace />
        }
      />
    </Routes>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AppRoutes />
      <Toaster />
    </BrowserRouter>
  );
}
