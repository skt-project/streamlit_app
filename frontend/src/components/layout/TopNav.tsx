import { Link } from "react-router-dom";

interface Props {
  title: string;
  subtitle?: string;
  actions?: React.ReactNode;
}

export default function TopNav({ title, subtitle, actions }: Props) {
  return (
    <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-6 shrink-0 sticky top-0 z-10">
      <div className="flex flex-col justify-center min-w-0">
        <h1 className="text-lg font-semibold text-slate-900 tracking-tight leading-tight truncate">
          {title}
        </h1>
        {subtitle && (
          <p className="text-xs text-slate-500 mt-0.5 truncate">{subtitle}</p>
        )}
      </div>
      <div className="flex items-center gap-2 shrink-0 ml-4">
        {actions}
        <Link
          to="/notifications"
          className="w-9 h-9 flex items-center justify-center rounded-lg text-slate-500
                     hover:text-slate-700 hover:bg-slate-100 transition-colors duration-150"
          title="Notifikasi"
        >
          <span className="text-lg leading-none">🔔</span>
        </Link>
      </div>
    </header>
  );
}
