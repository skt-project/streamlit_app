// ── Auth ──────────────────────────────────────────────────────────────────────
export interface User {
  user_id: string;
  username: string;
  full_name: string;
  role: Role;
  email: string | null;
  territory: string | null;
  distributor_code: string | null;
  brand_group: string | null;
  salesman_sk: string | null;
  is_active: boolean;
}

export type Role = "se" | "spv" | "asm" | "dm" | "rsm" | "ho_admin";

// ── Dashboard ─────────────────────────────────────────────────────────────────
export interface DashboardKpi {
  label: string;
  value: string | number;
  sub?: string;
  trend?: "up" | "down" | "neutral";
  color?: "blue" | "green" | "yellow" | "red";
}

export interface ComplyBrand {
  brand: string;
  management_target: number;
  spv_target: number;
  comply_pct: number;
  comply_status: "Comply" | "Under Comply" | "Over Target" | "No Data";
}

export interface LeaderboardRow {
  rank: number;
  salesman_name: string;
  salesman_sk: string;
  achievement_pct: number;
  route_compliance_pct: number;
  coverage_pct: number;
}

// ── Route ─────────────────────────────────────────────────────────────────────
export type DayId = "Senin" | "Selasa" | "Rabu" | "Kamis" | "Jumat" | "Sabtu";

export interface RouteStore {
  route_plan_sk: string;
  outlet_sk: string;
  store_name: string;
  source_outlet_code: string;
  store_grade: string | null;
  address: string | null;
  last_visit_date: string | null;
  visit_day_of_week: DayId;
  visit_week_pattern: string;
  sequence_no: number;
}

export interface SalesmanRoute {
  salesman_sk: string;
  salesman_name: string;
  source_salesman_code: string;
  region: string | null;
  distributor_code: string | null;
  stores_per_day: Record<DayId, RouteStore[]>;
  total_stores: number;
  achievement_pct: number | null;
  compliance_pct: number | null;
}

// ── Target ────────────────────────────────────────────────────────────────────
export interface SpvTargetRow {
  spv_target_id: string;
  salesman_sk: string;
  salesman_name: string;
  brand: string;
  period_month: string;
  spv_target_amount: number;
  approval_status: "draft" | "submitted" | "approved" | "rejected";
}

export interface TargetComply {
  brand: string;
  period_month: string;
  management_target_total: number;
  spv_target_total: number;
  comply_pct: number;
  comply_status: string;
}

// ── Route Evaluate ────────────────────────────────────────────────────────────
export interface EvaluateTeamRow {
  salesman_sk: string;
  salesman_name: string;
  planned: number;
  call_count: number;
  effective_call_count: number;
  ec_rate_pct: number;
}

export interface EvaluateStoreRow {
  outlet_sk: string;
  store_name: string;
  store_grade: string | null;
  planned: boolean;
  is_call: boolean;
  is_effective: boolean | null;
  status: "OK" | "Low Conversion" | "Belum Terlaksana";
}

// ── Approval ──────────────────────────────────────────────────────────────────
export type ApprovalType = "target_adjust" | "tier_override" | "reopen";
export type ApprovalStatus = "pending" | "approved" | "rejected" | "revision";

export interface ApprovalRequest {
  approval_id: string;
  type: ApprovalType;
  title: string;
  submitted_by: string;
  submitted_at: string;
  current_value: number | string | null;
  proposed_value: number | string;
  reason: string;
  status: ApprovalStatus;
  sla_hours: number;
  comments: ApprovalComment[];
}

export interface ApprovalComment {
  author: string;
  role: string;
  body: string;
  created_at: string;
}

// ── Announcement ──────────────────────────────────────────────────────────────
export type AnnouncementType = "Campaign" | "Policy" | "Meeting" | "Distributor" | "Training";

export interface Announcement {
  announcement_id: string;
  type: AnnouncementType;
  title: string;
  body: string;
  audience: string;
  created_by: string;
  created_at: string;
}

// ── Salesman / Outlet ─────────────────────────────────────────────────────────
export interface Salesman {
  salesman_sk: string;
  source_salesman_code: string;
  salesman_name: string;
  salesman_type: string;
  brand_group: string | null;
  distributor_code: string | null;
  region: string | null;
  spv_name: string | null;
  asm_name: string | null;
  is_active: boolean;
}

export interface Outlet {
  outlet_sk: string;
  outlet_id: string;
  source_outlet_code: string;
  store_name: string;
  store_grade: string | null;
  tier: string | null;
  brand: string | null;
  channel: string | null;
  region: string | null;
  kecamatan: string | null;
  city: string | null;
  address: string | null;
  spv_name: string | null;
  salesman_name: string | null;
  salesman_code: string | null;
  salesman_sk: string | null;
  is_active: boolean;
}

// ── Notification ──────────────────────────────────────────────────────────────
export type NotificationType = "approval" | "announcement" | "compliance" | "target" | "system";

export interface Notification {
  notification_id: string;
  type: NotificationType;
  title: string;
  body: string;
  is_read: boolean;
  deep_link: string | null;
  created_at: string;
}
