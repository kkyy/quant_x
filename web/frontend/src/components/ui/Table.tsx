import { useState, useCallback } from 'react';
import { ChevronUp, ChevronDown } from 'lucide-react';

export interface TableColumn<T> {
  key: keyof T | string;
  label: string;
  render?: (row: T) => React.ReactNode;
  align?: 'left' | 'center' | 'right';
  sortable?: boolean;
  mono?: boolean;
}

interface TableProps<T extends Record<string, unknown>> {
  columns: TableColumn<T>[];
  data: T[];
  pageSize?: number;
  onRowClick?: (row: T) => void;
  rowKey?: keyof T;
  emptyMessage?: string;
}

type SortDir = 'asc' | 'desc' | null;

export function Table<T extends Record<string, unknown>>({
  columns,
  data,
  pageSize = 20,
  onRowClick,
  rowKey,
  emptyMessage = 'NO DATA',
}: TableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>(null);
  const [page, setPage] = useState(0);

  const handleSort = useCallback(
    (key: string) => {
      if (sortKey === key) {
        if (sortDir === 'asc') {
          setSortDir('desc');
        } else if (sortDir === 'desc') {
          setSortKey(null);
          setSortDir(null);
        }
      } else {
        setSortKey(key);
        setSortDir('asc');
      }
      setPage(0);
    },
    [sortKey, sortDir],
  );

  const sorted = sortKey && sortDir
    ? [...data].sort((a, b) => {
        const av = a[sortKey];
        const bv = b[sortKey];
        const cmp =
          typeof av === 'number' && typeof bv === 'number'
            ? av - bv
            : String(av).localeCompare(String(bv));
        return sortDir === 'asc' ? cmp : -cmp;
      })
    : data;

  const totalPages = Math.ceil(sorted.length / pageSize);
  const paged = sorted.slice(page * pageSize, (page + 1) * pageSize);
  const start = data.length === 0 ? 0 : page * pageSize + 1;
  const end = Math.min((page + 1) * pageSize, sorted.length);

  const alignClass = (align?: 'left' | 'center' | 'right') => {
    if (align === 'center') return 'text-center';
    if (align === 'right') return 'text-right';
    return 'text-left';
  };

  return (
    <div className="flex flex-col border border-terminal-border rounded-sm overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-terminal-raised border-b border-terminal-border">
              {columns.map((col) => (
                <th
                  key={String(col.key)}
                  className={`px-3 py-2 font-mono font-medium text-terminal-text-dim text-xs uppercase tracking-wider whitespace-nowrap ${alignClass(col.align)} ${
                    col.sortable ? 'cursor-pointer select-none hover:text-terminal-text-bright' : ''
                  }`}
                  onClick={col.sortable ? () => handleSort(String(col.key)) : undefined}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.label}
                    {col.sortable && sortKey === String(col.key) && (
                      sortDir === 'asc' ? (
                        <ChevronUp className="w-3 h-3 text-terminal-green" />
                      ) : (
                        <ChevronDown className="w-3 h-3 text-terminal-green" />
                      )
                    )}
                    {col.sortable && sortKey !== String(col.key) && (
                      <span className="w-3 h-3 opacity-20">
                        <ChevronUp className="w-3 h-3" />
                      </span>
                    )}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paged.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length}
                  className="px-4 py-10 text-center text-terminal-text-dim font-mono text-xs"
                >
                  {emptyMessage}
                </td>
              </tr>
            ) : (
              paged.map((row, i) => {
                const key =
                  rowKey !== undefined
                    ? (row[rowKey] as string | number)
                    : String(i);
                return (
                  <tr
                    key={key}
                    onClick={onRowClick ? () => onRowClick(row) : undefined}
                    className={`border-b border-terminal-border-dim last:border-b-0 transition-colors ${
                      onRowClick
                        ? 'cursor-pointer hover:bg-terminal-raised hover:border-l-2 hover:border-l-terminal-green'
                        : 'hover:bg-terminal-raised/50'
                    }`}
                  >
                    {columns.map((col) => {
                      const raw = row[col.key as keyof T];
                      return (
                        <td
                          key={String(col.key)}
                          className={`px-3 py-2 text-terminal-text ${alignClass(col.align)} ${col.mono !== false ? 'font-mono text-xs' : ''}`}
                        >
                          {col.render ? col.render(row) : String(raw ?? '')}
                        </td>
                      );
                    })}
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {data.length > pageSize && (
        <div className="flex items-center justify-between px-3 py-2 bg-terminal-surface border-t border-terminal-border font-mono text-xs text-terminal-text-dim">
          <span>{start}-{end} / {sorted.length}</span>
          <div className="flex gap-1">
            <button
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              className="px-2 py-0.5 border border-terminal-border hover:border-terminal-text-dim disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            >
              PREV
            </button>
            <span className="px-2 py-0.5">{page + 1}/{totalPages}</span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              className="px-2 py-0.5 border border-terminal-border hover:border-terminal-text-dim disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            >
              NEXT
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
