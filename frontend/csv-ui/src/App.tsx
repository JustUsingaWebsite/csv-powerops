import { useMemo, useState } from "react";
import DatasetUploader from "./components/DatasetUploader";
import PreviewTable from "./components/PreviewTable";
import type { TableData } from "./types";
import { tableDataToCSV } from "./utils/csv";
import { v4 as uuidv4 } from "uuid";

/**
 * Simple in-memory dataset model used by the UI.
 */
type DatasetItem = {
  id: string;
  name: string;
  table: TableData;
  isMaster: boolean;
};

export default function App() {
  const [datasets, setDatasets] = useState<DatasetItem[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [viewJson, setViewJson] = useState(false);

  // When uploader calls onLoad it gives a TableData object. We'll auto-name the dataset and add it.
  function handleLoad(table: TableData) {
    const suggested = `dataset-${datasets.length + 1}`;
    const name = window.prompt("Name this dataset", suggested) || suggested;
    const id = uuidv4();
    const item: DatasetItem = { id, name, table, isMaster: datasets.length === 0 }; // first uploaded = master by default
    setDatasets((d) => {
      const next = [...d, item];
      // set active to newest
      setActiveId(id);
      return next;
    });
  }

  function removeDataset(id: string) {
    setDatasets((d) => d.filter((x) => x.id !== id));
    setActiveId((cur) => (cur === id ? null : cur));
  }

  function setMaster(id: string) {
    setDatasets((d) => d.map((x) => ({ ...x, isMaster: x.id === id })));
  }

  function selectDataset(id: string | null) {
    setActiveId(id);
  }

  const active = useMemo(() => datasets.find((d) => d.id === activeId) ?? null, [datasets, activeId]);

  // download active dataset as CSV / JSON
  function downloadCSV(item?: DatasetItem) {
    const ds = item ?? active;
    if (!ds) return alert("No dataset selected");
    const csv = tableDataToCSV(ds.table);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${ds.name}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  function downloadJSON(item?: DatasetItem) {
    const ds = item ?? active;
    if (!ds) return alert("No dataset selected");
    const blob = new Blob([JSON.stringify(ds.table, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${ds.name}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // Quick UI-only functions placeholders (we'll call backend later)
  function quickTrim() {
    alert("Quick trim (client-side) — placeholder for a real action.");
  }
  function quickTitleCase() {
    alert("Quick title-case (client-side) — placeholder for a real action.");
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-md bg-gradient-to-br from-blue-600 to-indigo-500 flex items-center justify-center text-white font-bold">
              CP
            </div>
            <div>
              <div className="text-lg font-semibold">CSV PowerOps</div>
              <div className="text-xs text-gray-500">Powerful CSV ops — browser sandbox</div>
            </div>
          </div>

          <nav className="flex items-center gap-4 text-sm text-gray-600">
            <button className="px-3 py-1 rounded hover:bg-gray-100">Home</button>
            <button className="px-3 py-1 rounded hover:bg-gray-100">Quick Functions</button>
            <button className="px-3 py-1 rounded hover:bg-gray-100">Jobs</button>
            <button className="px-3 py-1 rounded hover:bg-gray-100">Settings</button>
          </nav>
        </div>
      </header>

      {/* Main */}
      <main className="max-w-7xl mx-auto p-6 grid grid-cols-12 gap-6">
        {/* Left column: uploader + dataset list */}
        <aside className="col-span-3 space-y-4">
          <div className="sticky top-6">
            <DatasetUploader onLoad={handleLoad} />

            <div className="mt-4 bg-white rounded shadow p-3">
              <div className="flex items-center justify-between mb-2">
                <div className="text-sm font-semibold">Datasets</div>
                <div className="text-xs text-gray-500">{datasets.length} saved</div>
              </div>

              <div className="space-y-2">
                {datasets.length === 0 && <div className="text-sm text-gray-500">No datasets uploaded yet.</div>}
                {datasets.map((ds) => (
                  <div
                    key={ds.id}
                    className={`p-2 rounded border ${activeId === ds.id ? "border-blue-300 bg-blue-50" : "border-transparent bg-white"}`}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div>
                        <div className="text-sm font-medium">{ds.name}</div>
                        <div className="text-xs text-gray-500">{ds.table.rows.length} rows • {ds.table.header.length} cols</div>
                        {ds.isMaster && <div className="mt-1 inline-block text-xs text-green-700 bg-green-100 px-2 py-0.5 rounded">Master</div>}
                      </div>

                      <div className="flex flex-col items-end gap-1">
                        <button onClick={() => selectDataset(ds.id)} className="text-xs px-2 py-1 rounded hover:bg-gray-100">Preview</button>
                        <button onClick={() => setMaster(ds.id)} className="text-xs px-2 py-1 rounded hover:bg-gray-100">Set master</button>
                        <button onClick={() => downloadCSV(ds)} className="text-xs px-2 py-1 bg-green-600 text-white rounded">CSV</button>
                        <button onClick={() => removeDataset(ds.id)} className="text-xs px-2 py-1 rounded text-red-600 hover:bg-red-50">Remove</button>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="mt-4 bg-white rounded shadow p-3">
              <div className="text-sm font-semibold mb-2">Quick Actions</div>
              <div className="flex flex-col gap-2">
                <button onClick={() => quickTrim()} className="w-full text-sm px-3 py-2 rounded bg-gray-100 hover:bg-gray-200 text-gray-700">Trim Whitespace</button>
                <button onClick={() => quickTitleCase()} className="w-full text-sm px-3 py-2 rounded bg-gray-100 hover:bg-gray-200 text-gray-700">Title Case</button>
              </div>
            </div>
          </div>
        </aside>

        {/* Center: main function area */}
        <section className="col-span-6">
          <div className="bg-white rounded shadow p-4">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold">Workspace</h2>
                <p className="text-sm text-gray-500">Select a dataset on the left to preview and run functions.</p>
              </div>

              <div className="flex items-center gap-2">
                <select
                  value={activeId ?? ""}
                  onChange={(e) => selectDataset(e.target.value || null)}
                  className="border px-3 py-1 rounded bg-white text-sm"
                >
                  <option value="">-- Select dataset --</option>
                  {datasets.map((d) => <option key={d.id} value={d.id}>{d.name}</option>)}
                </select>

                <button onClick={() => downloadCSV()} className="px-3 py-1 bg-green-600 text-white rounded text-sm">Download CSV</button>
                <button onClick={() => downloadJSON()} className="px-3 py-1 bg-gray-800 text-white rounded text-sm">Download JSON</button>
              </div>
            </div>

            <div className="mt-4 border-t pt-4">
              <div className="grid grid-cols-3 gap-3">
                <div className="col-span-2">
                  <div className="text-sm font-medium text-gray-700 mb-2">Preview</div>
                  {/* If no active dataset show an empty state */}
                  {!active ? (
                    <div className="rounded border border-dashed border-gray-200 p-6 text-center text-gray-500">
                      <div className="text-sm">No dataset selected</div>
                      <div className="mt-3 text-xs">Upload a CSV on the left or choose a dataset to begin.</div>
                    </div>
                  ) : (
                    <PreviewTable table={active.table} maxRows={10} />
                  )}
                </div>

                <div className="col-span-1">
                  <div className="text-sm font-medium text-gray-700 mb-2">Summary</div>
                  <div className="bg-gray-50 rounded p-3 text-sm">
                    <div className="mb-2">Datasets: <strong>{datasets.length}</strong></div>
                    <div className="mb-2">Active: <strong>{active?.name ?? "—"}</strong></div>
                    <div className="mb-2">Rows: <strong>{active?.table.rows.length ?? 0}</strong></div>
                    <div className="mb-4">Cols: <strong>{active ? active.table.header.length : 0}</strong></div>

                    <div className="text-xs text-gray-600">Quick run:</div>
                    <div className="flex flex-col gap-2 mt-2">
                      <button className="px-2 py-1 text-sm rounded bg-blue-600 text-white">Run CrossRef</button>
                      <button className="px-2 py-1 text-sm rounded bg-blue-600 text-white">Run Extract</button>
                      <button className="px-2 py-1 text-sm rounded bg-blue-600 text-white">Run Sort</button>
                    </div>
                  </div>
                </div>
              </div>

              {/* Placeholder for function forms or results */}
              <div className="mt-6">
                <div className="text-sm font-semibold mb-2">Actions & Workflow</div>
                <div className="text-sm text-gray-600">
                  Use the action buttons above to run operations on the currently selected dataset. Results will appear in the right-side panel where you can download or chain quick functions.
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Right: results / JSON view */}
        <aside className="col-span-3">
          <div className="bg-white rounded shadow p-4 sticky top-6">
            <div className="flex items-center justify-between mb-3">
              <div>
                <h3 className="text-md font-semibold">Result</h3>
                <div className="text-xs text-gray-500">Preview / JSON</div>
              </div>

              <div className="flex items-center gap-1">
                <button onClick={() => setViewJson(false)} className={`px-2 py-1 rounded text-sm ${viewJson ? "bg-white border" : "bg-blue-600 text-white"}`}>Table</button>
                <button onClick={() => setViewJson(true)} className={`px-2 py-1 rounded text-sm ${viewJson ? "bg-blue-600 text-white" : "bg-white border"}`}>JSON</button>
              </div>
            </div>

            {!active && <div className="text-sm text-gray-500">No results to show.</div>}

            {active && !viewJson && <PreviewTable table={active.table} maxRows={20} />}

            {active && viewJson && (
              <div className="mt-2">
                <pre className="bg-gray-100 p-3 rounded text-xs overflow-auto max-h-96">
                  {JSON.stringify(active.table, null, 2)}
                </pre>
              </div>
            )}

            <div className="mt-3 flex gap-2">
              <button onClick={() => active && downloadCSV()} className="flex-1 px-3 py-2 rounded bg-green-600 text-white text-sm">Download CSV</button>
              <button onClick={() => active && downloadJSON()} className="flex-1 px-3 py-2 rounded bg-gray-800 text-white text-sm">Download JSON</button>
            </div>
          </div>
        </aside>
      </main>

      <footer className="max-w-7xl mx-auto px-6 py-6 text-xs text-gray-400">
        CSV PowerOps — local UI sandbox — prototype
      </footer>
    </div>
  );
}
