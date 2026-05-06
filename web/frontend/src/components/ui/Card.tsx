interface CardProps {
  title?: string;
  children: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
}

export function Card({ title, children, actions, className = '' }: CardProps) {
  return (
    <div className={`bg-zinc-900 border border-zinc-800 rounded-lg ${className}`}>
      {(title || actions) && (
        <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800">
          {title && (
            <h3 className="text-sm font-semibold text-zinc-200">{title}</h3>
          )}
          {actions && <div className="flex items-center gap-2">{actions}</div>}
        </div>
      )}
      <div className="p-4">{children}</div>
    </div>
  );
}
