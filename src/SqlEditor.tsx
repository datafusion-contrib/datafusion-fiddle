import { useVimMode } from "@/src/useVimMode";
import { LoadingSpinner } from "@/components/LoadingSpinner";
import React, { CSSProperties } from "react";
import { Editor, loader } from "@monaco-editor/react";
import {
  EntityContextType,
  ICompletionItem,
  LanguageIdEnum,
  setupLanguageFeatures,
  vsPlusTheme
} from 'monaco-sql-languages';
import * as monaco from "monaco-editor";
import { languages } from "monaco-editor";

import pgWorker from 'monaco-sql-languages/esm/languages/pgsql/pgsql.worker?worker'
import { Table, useTables } from "@/src/TablesContext";

self.MonacoEnvironment = {
  getWorker: function () {
    return new pgWorker()
  }
};

// https://github.com/resetsix/react-monaco-sql-language-demo/issues/1#issuecomment-2178101800
loader.config({ monaco: monaco })

monaco.editor.defineTheme('sql-dark', vsPlusTheme.darkThemeData);
monaco.editor.setTheme('sql-dark');

const tableRe = /\bcreate\s+table\s+([a-zA-Z_][a-zA-Z0-9_]+)\b/gi
const columnRe = /^ +([a-zA-Z_][a-zA-Z0-9_]+)\s+/gm

// Helper function to quote SQL identifiers if needed
function quoteIdentifierIfNeeded(identifier: string): string {
  // Check if identifier is a normal identifier (lowercase letters, numbers, underscore)
  // and doesn't start with a number
  const normalIdentifierRe = /^[a-z_][a-z0-9_]*$/;
  
  if (normalIdentifierRe.test(identifier)) {
    return identifier;
  } else {
    return `"${identifier}"`;
  }
}

// FIXME: I don't like global variables, but I don't want to think.
const TABLES: Table[] = []

setupLanguageFeatures(LanguageIdEnum.PG, {
  completionItems: {
    completionService: async (model, __, ___, suggestions) => {
      if (!suggestions) {
        return [];
      }
      const { keywords, syntax } = suggestions;
      // keyword completions are always there.
      const keywordsCompletionItems: ICompletionItem[] = keywords.map((kw) => ({
        label: kw,
        kind: languages.CompletionItemKind.Keyword,
        detail: 'keyword',
        sortText: '2' + kw
      }));

      let syntaxCompletionItems: ICompletionItem[] = [];

      syntax.forEach((item) => {
        let completions
        switch (item.syntaxContextType) {
          // if we are suggesting for columns...
          case EntityContextType.COLUMN:
            completions = TABLES
              .flatMap(tbl => model.getValue().includes(tbl.name) ? tbl.columns : [])
              .map(col => quoteIdentifierIfNeeded(col.name))
            break
          // ...or for tables...
          case EntityContextType.CATALOG:
          case EntityContextType.DATABASE:
          case EntityContextType.TABLE:
            completions = TABLES
              .map(tbl => quoteIdentifierIfNeeded(tbl.name))
            break
        }
        if (completions) {
          const words = completions
            .map(v => ({
              label: v,
              kind: languages.CompletionItemKind.Class,
              detail: item.syntaxContextType + ' suggestion',
              // top priority completion.
              sortText: '1' + v
            }))

          // these gathered column/table names will be added to the completions.
          syntaxCompletionItems.push(...words);
        }
      });

      return [...syntaxCompletionItems, ...keywordsCompletionItems];
    }
  }
});

export interface SqlEditorProps {
  height: CSSProperties['height'];
  width: CSSProperties['width'];
  vim: boolean;
  value: string;
  onChange: (code: string) => void;
  onMount?: (editor: monaco.editor.IStandaloneCodeEditor) => void;
  // Optional: add a handler for Cmd+Enter if needed
  onSubmit?: () => void;
}

export function SqlEditor ({ onChange, onSubmit, vim, onMount, ...props }: SqlEditorProps) {
  // Need to pass the onSubmit callback by reference, otherwise, only
  // the first onSubmit value ever passed will be captured by the onMount closure.
  const onSubmitRef = React.useRef(onSubmit)
  onSubmitRef.current = onSubmit

  const tables = useTables()
  React.useEffect(() => {
    if (tables.type === 'result') {
      TABLES.splice(0)
      TABLES.push(...tables.result)
    }
  }, [tables])

  const vimMode = useVimMode({ enabled: vim });

  return (
    <Editor
      onMount={(editor) => {
        vimMode.onMount(editor)
        onMount?.(editor);
      }}
      theme={'sql-dark'}
      language={LanguageIdEnum.PG}
      loading={<LoadingSpinner/>}
      onChange={(e) => onChange(e ?? "")}
      options={{ fontSize: 14, minimap: { enabled: false } }}
      {...props}
    />
  );
}
