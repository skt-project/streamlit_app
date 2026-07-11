import { Component, type ErrorInfo, type ReactNode } from "react";
import Icon from "@/components/ui/Icon";

interface Props  { children: ReactNode; }
interface State  { error: Error | null; }

function ErrorFallback({ error, onReset }: { error: Error; onReset: () => void }) {
  return (
    <div className="flex-1 flex flex-col items-center justify-center p-8 text-center gap-4">
      <span className="icon-badge icon-badge-red w-14 h-14 !rounded-full">
        <Icon name="exclamation-triangle" className="w-6 h-6" />
      </span>
      <div className="space-y-1">
        <h2 className="text-lg font-semibold text-slate-800">Terjadi Kesalahan</h2>
        <p className="text-sm text-slate-500 max-w-sm">
          Halaman ini mengalami error yang tidak terduga.
          {error.message ? ` (${error.message})` : ""}
        </p>
      </div>
      <button
        className="btn-primary"
        onClick={() => { onReset(); window.location.reload(); }}
      >
        Muat Ulang Halaman
      </button>
    </div>
  );
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("[ErrorBoundary]", error.message, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <ErrorFallback
          error={this.state.error}
          onReset={() => this.setState({ error: null })}
        />
      );
    }
    return this.props.children;
  }
}
