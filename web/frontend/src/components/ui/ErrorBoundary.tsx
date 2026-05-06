import { Component, type ReactNode } from "react";
import { AlertTriangle } from "lucide-react";

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex flex-col items-center justify-center min-h-[400px] p-8">
          <div className="border border-terminal-red rounded-sm p-6 max-w-md text-center bg-terminal-red-glow">
            <AlertTriangle className="w-6 h-6 text-terminal-red mx-auto mb-3" />
            <p className="text-xs font-mono text-terminal-red mb-2">RUNTIME ERROR</p>
            <p className="text-xs font-mono text-terminal-text-dim break-all">
              {this.state.error?.message || "Unknown error"}
            </p>
            <button
              onClick={() => this.setState({ hasError: false, error: null })}
              className="mt-4 px-3 py-1.5 border border-terminal-border text-xs font-mono text-terminal-text-dim hover:text-terminal-text hover:border-terminal-text-dim transition-colors"
            >
              RETRY
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
