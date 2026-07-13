import { api } from "./client";
export const fetchNotifications = () => api.get("/notifications").then((r) => r.data);
