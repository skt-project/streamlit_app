import { Outlet } from "react-router-dom";
import Sidebar from "./Sidebar";

export default function Layout() {
  return (
    <div className="flex min-h-screen">
      <a href="#main-content" className="skip-link">
        Lewati ke konten utama
      </a>
      <Sidebar />
      <div id="main-content" tabIndex={-1} className="flex-1 flex flex-col min-w-0 overflow-hidden outline-none">
        <Outlet />
      </div>
    </div>
  );
}
