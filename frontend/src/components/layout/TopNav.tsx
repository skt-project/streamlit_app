import { Link } from "react-router-dom";

interface Props {
  title: string;
  actions?: React.ReactNode;
}

export default function TopNav({ title, actions }: Props) {
  return (
    <header className="h-14 bg-white border-b border-slate-200 flex items-center justify-between px-6 shrink-0">
      <h1 className="text-base font-semibold text-slate-800">{title}</h1>
      <div className="flex items-center gap-3">
        {actions}
        <Link to="/notifications" className="relative p-2 text-slate-500 hover:text-slate-700">
          <span className="text-xl">🔔</span>
        </Link>
      </div>
    </header>
  );
}
