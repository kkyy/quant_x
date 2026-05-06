import { useState, useCallback } from 'react';
import { ChevronUp, ChevronDown } from 'lucide-react';

export interface TableColumn<T> {
  key: keyof T | string;
  label: string;
  render?: (value: unknown, row: T) => React.ReactNode;
  align?: 'left' | 'center' | 'right';
  sortable?: boolean;
}

interface TableProps<T extends Record<string, unknown>> {
  columns: TableColumn<T>[];
  data: T[];
  pageSize?: number;
  onRowClick?: (row: T) => void;
  rowKey?: keyof T;
}

type SortDir = 'asc' | 'desc' | null;

export function Table<T extends Record<string, unknown>>({
  columns,
  data,
  pageSize = 20,
  onRowClick,
  rowKey,
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
    <div className="flex flex-col gap-0 rounded-lg border border-zinc-800 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-zinc-800 border-b border-zinc-700">
              {columns.map((col) => (
                <th
                  key={String(col.key)}
                  className={`px-4 py-3 font-medium text-zinc-300 whitespace-nowrap ${alignClass(col.align)} ${
                    col.sortable ? 'cursor-pointer select-none hover:text-white' : ''
                  }`}
                  onClick={col.sortable ? () => handleSort(String(col.key)) : undefined}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.label}
                    {col.sortable && sortKey === String(col.key) && (
                      sortDir === 'asc' ? (
                        <ChevronUp className="w-3.5 h-3.5" />
                      ) : (
                        <ChevronDown className="w-3.5 h-3.5" />
                      )
                    )}
                    {col.sortable && sortKey !== String(col.key) && (
                      <span className="w-3.5 h-3.5 opacity-30">
                        <ChevronUp className="w-3.5 h-3.5" />
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
                  className="px-4 py-8 text-center text-zinc-500"
                >
                  No data
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
                    className={`border-b border-zinc-800 last:border-b-0 transition-colors ${
                      onRowClick ? 'cursor-pointer hover:bg-zinc-800/60' : ''
                    }`}
                  >
                    {columns.map((col) => {
                      const raw = row[col.key as keyof T];
                      return (
                        <td
                          key={String(col.key)}
                          className={`px-4 py-2.5 text-zinc-300 ${alignClass(col.align)}`}
                        >
                          {col.render ? col.render(raw, row) : String(raw ?? '')}
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
        <div className="flex items-center justify-between px-4 py-2 bg-zinc-900 border-t border-zinc-800 text-xs text-zinc-400">
          <span>
            {start}-{end} of {sorted.length}
          </span>
          <div className="flex gap-1">
            <button
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              className="px-3 py-1 rounded bg-zinc-800 hover:bg-zinc-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-zinc-300"
            >
              Prev
            </button>
            <button
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              className="px-3 py-1 rounded bg-zinc-800 hover:bg-zinc-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors text-zinc-300"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
