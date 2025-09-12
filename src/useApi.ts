import React, { useState } from "react";
import * as Viz from "@viz-js/viz";

export interface SqlRequest {
  stmts: string[];
  distributed: boolean;
}

export interface SqlResponse {
  columns: Array<[string, string]>;
  rows: Array<Array<string>>;
  logical_plan: string;
  physical_plan: string;
  graphviz_svg: string;
  graphviz: string;
}

export async function executeStatements(
  stmts: string[],
  distributed: boolean,
): Promise<SqlResponse> {
  const req: SqlRequest = {
    stmts,
    distributed,
  };
  const res = await fetch("/api/main", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });
  if (res.status === 200) {
    return await res.json();
  } else if (res.status === 400) {
    const { message } = await res.json();
    throw new Error(message);
  } else {
    const msg = await res.text();
    throw new Error(`unexpected status ${res.status}: ${msg}`);
  }
}

export type ApiState =
  | { type: "nothing" }
  | { type: "loading" }
  | { type: "error"; message: string }
  | { type: "result"; result: SqlResponse };

export interface ApiRequest {
  statement: string;
  distributed: boolean;
}

export function useApi() {
  const [state, setState] = useState<ApiState>({ type: "nothing" });

  const execute = React.useCallback(async (req: ApiRequest) => {
    setState({ type: "loading" });
    try {
      const result = await executeStatements(
        req.statement
          .split(";")
          .map((_) => _.trim())
          .filter((_) => _.length > 0),
        req.distributed,
      );

      if (result.graphviz.length > 0) {
        const viz = await Viz.instance();
        try {
          const svg = viz.renderSVGElement(result.graphviz);
          result.graphviz_svg = svg.outerHTML;
        } catch (e) {
          console.error("failed to render graphviz plan", e);
        }
      }
      setState({ type: "result", result });
      return {
        type: "result" as const,
        result,
      };
    } catch (error) {
      const err = error as Error;
      return { type: "error" as const, message: err.toString() };
    }
  }, []);

  return { state, execute };
}
