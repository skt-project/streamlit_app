import { api } from "./client";
import type { Visit, VisitListResponse, SkippedStore } from "@/types";

export const listVisits = (params: {
  salesman_sk?: string;
  visit_date?: string;
  status?: string;
  store_name?: string;
  page?: number;
  page_size?: number;
}) => api.get<VisitListResponse>("/visit", { params }).then((r) => r.data);

export const getVisit = (visitId: string) =>
  api.get<Visit>(`/visit/${visitId}`).then((r) => r.data);

export const approveVisit = (visitId: string, notes?: string) =>
  api.put<Visit>(`/visit/${visitId}/approve`, { notes }).then((r) => r.data);

export const rejectVisit = (visitId: string, rejectionNotes: string) =>
  api.put<Visit>(`/visit/${visitId}/reject`, { rejection_notes: rejectionNotes }).then((r) => r.data);

export const updateFinalQty = (
  visitId: string,
  items: { sku_id: string; final_qty: number }[],
) => api.put<Visit>(`/visit/${visitId}/final-qty`, { items }).then((r) => r.data);

export const updateStorePrice = (
  visitId: string,
  items: { sku_id: string; price_for_store: number }[],
) => api.put<Visit>(`/visit/${visitId}/store-price`, { items }).then((r) => r.data);

export const updateAdjustment = (
  visitId: string,
  adjustment_amount: number,
  adjustment_note: string | null,
) => api.put<Visit>(`/visit/${visitId}/adjustment`, { adjustment_amount, adjustment_note }).then((r) => r.data);

export const downloadVisitPdf = async (visitId: string, filename?: string): Promise<void> => {
  const response = await api.get(`/visit/${visitId}/pdf`, {
    responseType: "blob",
  });
  const url = window.URL.createObjectURL(new Blob([response.data], { type: "application/pdf" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename ?? `Order_${visitId}.pdf`;
  a.click();
  window.URL.revokeObjectURL(url);
};

// ── Skipped Stores ────────────────────────────────────────────────────────────

export const listSkippedStores = (params: {
  week_iso?: string;
  status?: string;
  brand_group?: string;
  page?: number;
  page_size?: number;
}) => api.get<SkippedStore[]>("/skipped-stores", { params }).then((r) => r.data);

export const returnSkippedStore = (id: string, notes?: string) =>
  api.put<SkippedStore>(`/skipped-stores/${id}/return`, { notes }).then((r) => r.data);

export const executeSkippedStore = (id: string, notes?: string) =>
  api.put<SkippedStore>(`/skipped-stores/${id}/execute`, { notes }).then((r) => r.data);

export const getSkippedStoreSummary = (weekIso?: string) =>
  api.get("/skipped-stores/summary", { params: { week_iso: weekIso } }).then((r) => r.data);
