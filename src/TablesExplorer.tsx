import { HTMLProps, useState } from "react";
import { useTables, Table } from "@/src/TablesContext";

export interface TablesExplorerProps extends HTMLProps<HTMLDivElement> {

}

export function TablesExplorer ({ className, ...props }: TablesExplorerProps) {
  const tables = useTables();
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  const toggleTable = (tableName: string) => {
    setExpandedTables(prev => {
      const newSet = new Set(prev);
      if (newSet.has(tableName)) {
        newSet.delete(tableName);
      } else {
        newSet.add(tableName);
      }
      return newSet;
    });
  };

  const renderContent = () => {
    switch (tables.type) {
      case 'loading':
        return <div className="p-4 text-text-secondary">Loading tables...</div>;

      case 'error':
        return <div className="p-4 text-red-500">Error: {tables.message}</div>;

      case 'result':
        if (tables.result.length === 0) {
          return <div className="p-4 text-text-secondary">No tables found</div>;
        }

        return (
          <div className="p-2">
            <h3 className="text-sm font-semibold text-text-primary mb-2 px-2">Tables</h3>
            <div className="space-y-1">
              {tables.result.map(table => (
                <TableItem
                  key={table.name}
                  table={table}
                  isExpanded={expandedTables.has(table.name)}
                  onToggle={() => toggleTable(table.name)}
                />
              ))}
            </div>
          </div>
        );
    }
  };

  return (
    <div className={`bg-secondary-surface border-r border-border ${className || ''}`} {...props}>
      {renderContent()}
    </div>
  );
}

interface TableItemProps {
  table: Table;
  isExpanded: boolean;
  onToggle: () => void;
}

function TableItem ({ table, isExpanded, onToggle }: TableItemProps) {
  return (
    <div>
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between px-2 py-1 text-left hover:bg-secondary-surface rounded text-sm"
      >
        <span className="flex items-center gap-1">
          <span className={`text-text-primary transition-transform ${isExpanded ? 'rotate-90' : ''}`}>
            ▶
          </span>
          <span className="text-text-primary font-medium">{table.name}</span>
        </span>
        <span className="text-xs text-text-secondary">
          {table.columns.length} cols
        </span>
      </button>

      {isExpanded && (
        <div className="ml-6 mt-1 space-y-1">
          {table.columns.map(column => (
            <div
              key={column.name}
              className="flex items-center justify-between px-2 py-1 text-xs"
            >
              <span className="text-text-primary">{column.name}</span>
              <span className="text-text-secondary font-mono">{column.type}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}