import React, { createContext, useContext, useState, ReactNode } from 'react';
import { useApi } from "@/src/useApi";

export interface Column {
  name: string;
  type: string;
}

export interface Table {
  name: string;
  columns: Column[];
}

export type TablesContextType =
  { type: 'loading' } |
  { type: 'error', message: string } |
  { type: 'result', result: readonly Table[] };

const TablesContext = createContext<TablesContextType | undefined>(undefined);

export const useTables = (): TablesContextType => {
  const context = useContext(TablesContext);
  if (context === undefined) {
    throw new Error('useTables must be used within a TablesProvider');
  }
  return context;
};

interface TablesProviderProps {
  children: ReactNode;
}

export const TablesProvider: React.FC<TablesProviderProps> = ({ children }) => {
  const api = useApi()
  const [state, setState] = useState<TablesContextType>({ type: 'loading' });

  React.useEffect(() => {
    api.execute("SELECT table_name, column_name, data_type FROM information_schema.columns")
      .then(res => {
        switch (res.type) {
          case 'result':
            setState(parseTables(res.result.rows))
            return
          case 'error':
            setState(res)
        }
      })
  }, [])

  return (
    <TablesContext.Provider value={state}>
      {children}
    </TablesContext.Provider>
  );
};

function parseTables (rows: string[][]): TablesContextType {
  const tables = new Map<string, Table>()
  for (const row of rows) {
    if (row.length != 3) {
      return { type: 'error', message: "Invalid return from information_schema.columns" };
    }
    const [tbl, col, type] = row
    let table = tables.get(tbl)
    if (table === undefined) {
      const newTable: Table = { name: tbl, columns: [] }
      tables.set(tbl, newTable)
      table = newTable
    }
    table.columns.push({ name: col, type })
  }
  const tableList = [...tables.values()]
  tableList.sort((a, b) => a.name > b.name ? 1 : -1)
  return { type: 'result', result: tableList }
}