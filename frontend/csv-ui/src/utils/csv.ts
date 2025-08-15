import Papa from "papaparse";
import type { TableData } from "../types";

/**
 * parseFileToTableData(file, opts)
 * - hasHeader: boolean (if true, first row treated as header)
 * - delim: string (optional)
 */
export function parseFileToTableData(file: File, hasHeader = true, delim?: string): Promise<TableData> {
  return new Promise((resolve, reject) => {
    Papa.parse(file, {
      delimiter: delim || "",
      skipEmptyLines: false,
      complete: (res) => {
        const data = res.data as string[][];
        if (!data || data.length === 0) {
          resolve({ hasHeader, header: [], rows: []});
          return;
        }
        if (hasHeader) {
          const header = (data[0] as string[]).map(h => (h ?? "").toString());
          const rows = (data.slice(1) as string[][]).map(r => r.map(c => (c ?? "").toString()));
          resolve({ hasHeader: true, header, rows });
        } else {
          // create numeric headers 0..n-1
          const maxCols = Math.max(...data.map(r => r.length));
          const header = Array.from({length: maxCols}, (_, i) => `col-${i}`);
          const rows = data.map(r => {
            const copy = new Array(maxCols).fill("");
            r.forEach((c, i) => copy[i] = (c ?? "").toString());
            return copy;
          });
          resolve({ hasHeader: false, header, rows });
        }
      },
      error: (err) => reject(err)
    });
  });
}

export function tableDataToCSV(table: TableData, delim = ","): string {
  const data: string[][] = [];
  if (table.hasHeader && table.header.length > 0) {
    data.push(table.header);
  }
  for (const r of table.rows) data.push(r);
  return Papa.unparse(data, { delimiter: delim });
}
