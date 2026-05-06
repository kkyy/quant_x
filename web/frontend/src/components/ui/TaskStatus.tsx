import { useEffect } from "react";
import { useSSE } from "../../hooks/useSSE";
import { Badge } from "./Badge";

interface TaskStatusProps {
  taskId: string | null;
  onComplete?: () => void;
  onError?: (message: string) => void;
}

export function TaskStatus({ taskId, onComplete, onError }: TaskStatusProps) {
  const { status, error, events } = useSSE(taskId);

  useEffect(() => {
    if (status === "done" && onComplete) {
      onComplete();
    }
    if (status === "error" && onError && error) {
      onError(error);
    }
  }, [status, onComplete, onError, error]);

  const statusVariant = (() => {
    switch (status) {
      case "idle":
        return "neutral" as const;
      case "streaming":
        return "info" as const;
      case "done":
        return "success" as const;
      case "error":
        return "error" as const;
    }
  })();

  const statusLabel = (() => {
    switch (status) {
      case "idle":
        return "idle";
      case "streaming":
        return "running";
      case "done":
        return "done";
      case "error":
        return "error";
    }
  })();

  const lastEvent = events[events.length - 1];

  return (
    <div className="flex items-center gap-3 text-xs">
      {taskId ? (
        <span className="font-mono text-zinc-400">task:{taskId.slice(0, 8)}</span>
      ) : (
        <span className="text-zinc-600">no task</span>
      )}
      <Badge variant={statusVariant}>{statusLabel}</Badge>
      {lastEvent && (
        <span className="text-zinc-500 truncate max-w-xs">
          {lastEvent.type}: {JSON.stringify(lastEvent.data).slice(0, 60)}
        </span>
      )}
      {error && status === "error" && (
        <span className="text-red-400 truncate max-w-xs">{error}</span>
      )}
    </div>
  );
}
