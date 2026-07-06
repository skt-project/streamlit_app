import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import { useAuthStore } from "@/store/authStore";
import Layout from "@/components/layout/Layout";
import Login from "@/pages/Login";
import Dashboard from "@/pages/Dashboard";
import RoutePlanner from "@/pages/RoutePlanner";
import TargetManagement from "@/pages/TargetManagement";
import RouteEvaluate from "@/pages/RouteEvaluate";
import Approvals from "@/pages/Approvals";
import Announcements from "@/pages/Announcements";
import Reports from "@/pages/Reports";
import MasterDataPjp from "@/pages/MasterDataPjp";
import MasterDataSalesman from "@/pages/MasterDataSalesman";
import OutletSalesman from "@/pages/OutletSalesman";
import Store360 from "@/pages/Store360";
import Salesman360 from "@/pages/Salesman360";
import StoreOpportunity from "@/pages/StoreOpportunity";
import Administration from "@/pages/Administration";
import ImportExport from "@/pages/ImportExport";
import Notifications from "@/pages/Notifications";
import Visits from "@/pages/Visits";
import VisitDetail from "@/pages/VisitDetail";

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

function AppRoutes() {
  const { isAuthenticated } = useAuthStore();

  return (
    <Routes>
      <Route path="/login" element={isAuthenticated ? <Navigate to="/dashboard" replace /> : <Login />} />

      <Route
        element={
          <AuthGuard>
            <Layout />
          </AuthGuard>
        }
      >
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="dashboard"            element={<Dashboard />} />
        <Route path="route-planner"        element={<RoutePlanner />} />
        <Route path="route-evaluate"       element={<RouteEvaluate />} />
        <Route path="target-management"    element={<TargetManagement />} />
        <Route path="approvals"            element={<Approvals />} />
        <Route path="announcements"        element={<Announcements />} />
        <Route path="reports"              element={<Reports />} />

        {/* Master Data */}
        <Route path="master-data-pjp"      element={<MasterDataPjp />} />
        <Route path="master-data-salesman" element={<MasterDataSalesman />} />
        <Route path="outlet-salesman"      element={<OutletSalesman />} />

        {/* 360° Views */}
        <Route path="store360"             element={<Store360 />} />
        <Route path="salesman360"          element={<Salesman360 />} />
        <Route path="store-opportunity"    element={<StoreOpportunity />} />

        {/* Visits & Demand */}
        <Route path="visits"               element={<Visits />} />
        <Route path="visits/:visitId"      element={<VisitDetail />} />

        {/* Admin */}
        <Route path="administration"       element={<Administration />} />
        <Route path="import-export"        element={<ImportExport />} />
        <Route path="notifications"        element={<Notifications />} />
      </Route>

      {/* Fallback: authenticated → dashboard, unauthenticated → login */}
      <Route
        path="*"
        element={isAuthenticated ? <Navigate to="/dashboard" replace /> : <Navigate to="/login" replace />}
      />
    </Routes>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>
  );
}
