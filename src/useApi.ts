import React, { useState } from 'react';

export interface SqlRequest {
  stmts: string[]
}

export interface SqlResponse {
  columns: Array<[string, string]>,
  rows: Array<Array<string>>,
}

export async function executeStatements (stmts: string[]): Promise<SqlResponse> {
  const req: SqlRequest = {
    stmts
  }
  const res = await fetch(
    '/api/main',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req),
    }
  )
  if (res.status === 200) {
    return await res.json()
  } else if (res.status === 400) {
    const { message } = await res.json()
    throw new Error(message)
  } else {
    const msg = await res.text()
    throw new Error(`unexpected status ${res.status}: ${msg}`)
  }
}

export type ApiState =
  { type: 'nothing' } |
  { type: 'loading' } |
  { type: 'error', message: string } |
  { type: 'result', result: SqlResponse }

export function useApi () {
  const [state, setState] = useState<ApiState>({ type: 'nothing' });

  const execute = React.useCallback(async (...statements: string[]) => {
    setState({ type: 'loading' });
    const result = await executeStatements(
      statements.flatMap((statement) =>
        statement.split(';').map(_ => _.trim()).filter(_ => _.length > 0),
      )
    )
      .then((result) => ({ type: 'result' as const, result }))
      .catch((err) => ({ type: 'error' as const, message: err.toString() }));
    setState(result)
    return result
  }, []);

  return { state, execute };
}