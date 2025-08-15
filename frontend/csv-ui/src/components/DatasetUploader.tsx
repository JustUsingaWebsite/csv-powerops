import React, { useState } from "react";
import type { TableData } from "../types";
import { parseFileToTableData } from "../utils/csv";

type Props = {
  onLoad: (t: TableData) => void;
};

export default function DatasetUploader({ onLoad }: Props) {
  const [hasHeader, setHasHeader] = useState(true);
  const [parsing, setParsing] = useState(false);

  async function handleFile(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files && e.target.files[0];
    if (!f) return;
    setParsing(true);
    try {
      const table = await parseFileToTableData(f, hasHeader);
      onLoad(table);
    } catch (err) {
      alert("Failed to parse CSV: " + (err as Error).message);
    } finally {
      setParsing(false);
      // reset input
      e.currentTarget.value = "";
    }
  }

  return (
    <div className="p-4 bg-white rounded shadow-sm">
      <h3 className="font-semibold mb-2">Upload CSV</h3>
      <div className="flex items-center gap-3">
        <input type="file" accept=".csv,text/csv" onChange={handleFile} className="border p-2 rounded" />
        <label className="flex items-center gap-2">
          <input type="checkbox" checked={hasHeader} onChange={e => setHasHeader(e.target.checked)} />
          <span className="text-sm text-gray-600">Has header</span>
        </label>
        <button className="ml-auto bg-blue-600 text-white px-3 py-1 rounded" disabled>
          {parsing ? "Parsing..." : "Upload file"}
        </button>
      </div>
      <p className="text-xs text-gray-500 mt-2">CSV is parsed client-side. You can preview and download JSON/CSV.</p>
    </div>
  );
}
