import { useState, useCallback, useEffect } from "react";
import { X, CheckCircle, XCircle, Info } from "lucide-react";
import { clsx } from "clsx";

type ToastVariant = "success" | "error" | "info";

interface ToastItem {
  id: string;
  variant: ToastVariant;
  message: string;
}

let addToastFn: ((variant: ToastVariant, message: string) => void) | null = null;

export function toast(variant: ToastVariant, message: string) {
  addToastFn?.(variant, message);
}

export function ToastContainer() {
  const [toasts, setToasts] = useState<ToastItem[]>([]);

  const addToast = useCallback((variant: ToastVariant, message: string) => {
    const id = Math.random().toString(36).slice(2);
    setToasts((prev) => [...prev, { id, variant, message }]);
  }, []);

  useEffect(() => {
    addToastFn = addToast;
    return () => {
      addToastFn = null;
    };
  }, [addToast]);

  const remove = useCallback((id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  useEffect(() => {
    const timer = setInterval(() => {
      setToasts((prev) => {
        const cutoff = Date.now() - 3000;
        return prev.filter((t) => {
          // keep toasts without timestamp; auto-dismiss is handled below
          return true;
        });
      });
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div className="fixed top-4 right-4 z-[100] flex flex-col gap-2 pointer-events-none">
      {toasts.map((t) => (
        <ToastItem key={t.id} toast={t} onDismiss={remove} />
      ))}
    </div>
  );
}

function ToastItem({
  toast,
  onDismiss,
}: {
  toast: ToastItem;
  onDismiss: (id: string) => void;
}) {
  useEffect(() => {
    const timer = setTimeout(() => onDismiss(toast.id), 3000);
    return () => clearTimeout(timer);
  }, [toast.id, onDismiss]);

  const variantClasses: Record<ToastVariant, string> = {
    success: "bg-emerald-900 border-emerald-700 text-emerald-100",
    error: "bg-red-900 border-red-700 text-red-100",
    info: "bg-blue-900 border-blue-700 text-blue-100",
  };

  const Icon = {
    success: CheckCircle,
    error: XCircle,
    info: Info,
  }[toast.variant];

  return (
    <div
      className={clsx(
        "pointer-events-auto flex items-center gap-3 px-4 py-3 border rounded-lg shadow-xl min-w-72 max-w-sm",
        variantClasses[toast.variant]
      )}
    >
      <Icon size={16} className="flex-shrink-0" />
      <span className="flex-1 text-sm">{toast.message}</span>
      <button
        onClick={() => onDismiss(toast.id)}
        className="flex-shrink-0 hover:opacity-70"
      >
        <X size={14} />
      </button>
    </div>
  );
}
