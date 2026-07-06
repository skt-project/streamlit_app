import { api } from "./client";
import type { Visit, VisitListResponse } from "@/types";

export const listVisits = (params: {
  salesman_sk?: string;
  visit_date?: string;
  status?: string;
  page?: number;
  page_size?: number;
}) => api.get<VisitListResponse>("/visit", { params }).then((r) => r.data);

export const getVisit = (visitId: string) =>
  api.get<Visit>(`/visit/${visitId}`).then((r) => r.data);

export const approveVisit = (visitId: string, notes?: string) =>
  api.put<Visit>(`/visit/${visitId}/approve`, { notes }).then((r) => r.data);

export const rejectVisit = (visitId: string, rejectionNotes: string) =>
  api.put<Visit>(`/visit/${visitId}/reject`, { rejection_notes: rejectionNotes }).then((r) => r.data);
