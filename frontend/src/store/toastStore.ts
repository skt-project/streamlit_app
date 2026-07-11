import { create } from "zustand";

export type ToastType = "success" | "error" | "warning" | "info";

export interface ToastItem {
  id: string;
  type: ToastType;
  message: string;
  duration?: number;
}

interface ToastStore {
  toasts: ToastItem[];
  show: (type: ToastType, message: string, duration?: number) => void;
  dismiss: (id: string) => void;
}

export const useToastStore = create<ToastStore>((set) => ({
  toasts: [],

  show: (type, message, duration = 3500) => {
    const id = Math.random().toString(36).slice(2);
    set((s) => ({ toasts: [...s.toasts, { id, type, message, duration }] }));
    setTimeout(() => {
      set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) }));
    }, duration);
  },

  dismiss: (id) =>
    set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),
}));

// Convenience helpers — import these instead of the store directly
export const toast = {
  success: (msg: string, dur?: number) => useToastStore.getState().show("success", msg, dur),
  error:   (msg: string, dur?: number) => useToastStore.getState().show("error",   msg, dur),
  warning: (msg: string, dur?: number) => useToastStore.getState().show("warning", msg, dur),
  info:    (msg: string, dur?: number) => useToastStore.getState().show("info",    msg, dur),
};
