import { clsx } from "clsx";

interface SkeletonProps {
  className?: string;
  rows?: number;
}

export function Skeleton({ className }: SkeletonProps) {
  return (
    <div
      className={clsx(
        "skeleton-shimmer rounded-sm",
        className
      )}
    />
  );
}

export function SkeletonTable({ rows = 5 }: SkeletonProps) {
  return (
    <div className="space-y-2">
      <div className="flex gap-4 px-3 py-2">
        <Skeleton className="h-3 w-20" />
        <Skeleton className="h-3 w-16" />
        <Skeleton className="h-3 w-24" />
        <Skeleton className="h-3 w-16" />
      </div>
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="flex gap-4 px-3 py-2">
          <Skeleton className="h-3 w-20" />
          <Skeleton className="h-3 w-16" />
          <Skeleton className="h-3 w-24" />
          <Skeleton className="h-3 w-16" />
        </div>
      ))}
    </div>
  );
}

export function SkeletonCard({ rows = 3 }: SkeletonProps) {
  return (
    <div className="bg-terminal-surface border border-terminal-border rounded-sm p-4 space-y-3">
      <Skeleton className="h-3 w-32" />
      {Array.from({ length: rows }).map((_, i) => (
        <Skeleton key={i} className="h-3 w-full" />
      ))}
    </div>
  );
}
