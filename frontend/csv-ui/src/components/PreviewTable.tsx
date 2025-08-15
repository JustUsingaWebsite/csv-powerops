import type { TableData } from "../types";

type Props = {
  table: TableData | null;
  maxRows?: number;
  onDownloadCSV?: (csv: string) => void;
};

export default function PreviewTable({ table, maxRows = 50 }: Props) {
  if (!table) return <div className="p-4 text-gray-500">No dataset loaded.</div>;
  const rows = table.rows.slice(0, maxRows);
  return (
    <div className="bg-white rounded shadow-sm p-4">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h4 className="font-semibold">Preview</h4>
          <div className="text-sm text-gray-500">{table.hasHeader ? "Header detected" : "No header"}</div>
        </div>
        <div className="text-sm text-gray-500">Rows: {table.rows.length}</div>
      </div>

      <div className="overflow-auto max-h-96">
        <table className="min-w-full text-sm">
          <thead className="bg-gray-50 sticky top-0">
            <tr>
              {(table.header || []).map((h, i) => (
                <th key={i} className="px-2 py-1 text-left border-b">{h || `col-${i}`}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, ri) => (
              <tr key={ri} className={ri % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                {table.header.map((_, ci) => (
                  <td key={ci} className="px-2 py-1 border-b align-top">{r[ci] ?? ""}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {table.rows.length > maxRows && <div className="text-xs text-gray-500 mt-2">Showing first {maxRows} rows.</div>}
    </div>
  );
}
