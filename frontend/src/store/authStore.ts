import { create } from "zustand";
import { jwtDecode } from "jwt-decode";
import type { User } from "@/types";
import { clearToken, getToken, saveToken } from "@/api/client";

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  login: (token: string, user: User) => void;
  logout: () => void;
  rehydrate: () => void;
}

export const useAuthStore = create<AuthState>((set) => ({
  user: null,
  token: null,
  isAuthenticated: false,

  login: (token, user) => {
    saveToken(token);
    set({ token, user, isAuthenticated: true });
  },

  logout: () => {
    clearToken();
    set({ token: null, user: null, isAuthenticated: false });
  },

  rehydrate: () => {
    const token = getToken();
    if (!token) return;
    try {
      const decoded = jwtDecode<User & { exp: number; sub: string }>(token);
      if (decoded.exp * 1000 < Date.now()) { clearToken(); return; }
      set({
        token,
        isAuthenticated: true,
        user: {
          user_id: decoded.sub,
          username: decoded.username,
          full_name: decoded.full_name ?? decoded.username,
          role: decoded.role,
          email: decoded.email ?? null,
          territory: decoded.territory,
          distributor_code: decoded.distributor_code,
          brand_group: decoded.brand_group,
          salesman_sk: decoded.salesman_sk ?? null,
          is_active: decoded.is_active ?? true,
        },
      });
    } catch { clearToken(); }
  },
}));
