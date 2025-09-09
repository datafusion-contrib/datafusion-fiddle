use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt::Display;
use std::fs;
use std::sync::Arc;
use vercel_runtime::{run, Body, Error, Request, RequestPayloadExt, Response, StatusCode};

const MAX_RESULTS: usize = 500;

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(handler).await
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct SqlRequest {
    stmts: Vec<String>,
}

pub async fn handler(req: Request) -> Result<Response<Body>, Error> {
    let req = match req.payload::<SqlRequest>()? {
        Some(req) => req,
        None => return throw_error("No sql request was passed", None, StatusCode::BAD_REQUEST),
    };

    let res = match execute_statements(req.stmts, "api/parquet").await {
        Ok(res) => res,
        Err(err) => {
            return throw_error(
                &err.to_string(),
                Some(Box::new(err)),
                StatusCode::BAD_REQUEST,
            )
        }
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            "Cache-Control",
            format!(
                "public, max-age=0, must-revalidate, s-maxage={s_maxage}",
                s_maxage = 60 * 60
            ),
        )
        .header("Content-Type", "application/json")
        .body(json!(res).to_string().into())?)
}

pub fn throw_error(
    message: &str,
    error: Option<Error>,
    status_code: StatusCode,
) -> Result<Response<Body>, Error> {
    if let Some(error) = error {
        eprintln!("error: {error}");
    }

    Ok(Response::builder()
        .status(status_code)
        .header("Content-Type", "application/json")
        .body(json!({ "message": message }).to_string().into())?)
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct SqlResult {
    columns: Vec<(String, String)>,
    rows: Vec<Vec<String>>,
}

async fn execute_statements(
    stmts: Vec<String>,
    path: impl Display,
) -> datafusion::error::Result<SqlResult> {
    let options = FormatOptions::default().with_display_error(true);
    let cfg = SessionConfig::new().with_information_schema(true);
    let ctx = Arc::new(SessionContext::new_with_config(cfg));
    load_parquet_files(path.to_string(), &ctx).await?;

    if stmts.is_empty() {
        return Ok(SqlResult::default());
    }

    for i in 0..stmts.len() - 1 {
        ctx.sql(stmts.get(i).unwrap()).await?.collect().await?;
    }
    let df = ctx.sql(stmts.last().unwrap()).await?;

    let record_batches = df.collect().await?;

    let mut columns: Vec<(String, String)> = vec![];
    let mut rows: Vec<Vec<String>> = vec![];
    for record_batch in record_batches {
        if columns.is_empty() {
            columns = record_batch
                .schema()
                .fields
                .iter()
                .map(|e| (e.name().to_string(), e.data_type().to_string()))
                .collect()
        }

        let per_column_formatters = record_batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for i in 0..record_batch.num_rows() {
            let mut row: Vec<String> = vec![];
            for formatter in &per_column_formatters {
                row.push(formatter.value(i).to_string());
            }
            rows.push(row);
        }
    }
    if rows.len() > MAX_RESULTS {
        rows.truncate(MAX_RESULTS);
        rows.push(vec!["...".to_string(); columns.len()]);
    }

    Ok(SqlResult { columns, rows })
}

async fn load_parquet_files(base: String, ctx: &SessionContext) -> Result<(), DataFusionError> {
    let mut futures = vec![];
    for entry in fs::read_dir(&base)? {
        let entry_path = entry?.path();
        let file_name = entry_path.file_name().unwrap().display().to_string();
        let file_path = format!("{base}/{file_name}");

        let fut = ctx.register_parquet(file_name, file_path, ParquetReadOptions::default());
        futures.push(fut);
    }

    for result in futures::future::join_all(futures).await {
        result?
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{execute_statements, SqlResult};

    #[tokio::test]
    async fn test_create_table() -> datafusion::error::Result<()> {
        let result = execute_statements(
            vec![
                "CREATE TABLE book (str text)".to_string(),
                "INSERT INTO book (str) VALUES ('foo')".to_string(),
                "SELECT * FROM book".to_string(),
            ],
            format!("{}/api/parquet", env!("CARGO_MANIFEST_DIR")),
        )
        .await?;

        insta::assert_snapshot!(result, @r"
        +----------------+
        | str [Utf8View] |
        +----------------+
        | foo            |
        +----------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_parquet() -> datafusion::error::Result<()> {
        let result = execute_statements(
            vec!["SELECT * FROM weather LIMIT 10".to_string()],
            format!("{}/api/parquet", env!("CARGO_MANIFEST_DIR")),
        )
        .await?;

        insta::assert_snapshot!(result, @r"
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | MinTemp [Float64] | MaxTemp [Float64] | Rainfall [Float64] | Evaporation [Float64] | Sunshine [Utf8View] | WindGustDir [Utf8View] | WindGustSpeed [Utf8View] | WindDir9am [Utf8View] | WindDir3pm [Utf8View] | WindSpeed9am [Utf8View] | WindSpeed3pm [Int64] | Humidity9am [Int64] | Humidity3pm [Int64] | Pressure9am [Float64] | Pressure3pm [Float64] | Cloud9am [Int64] | Cloud3pm [Int64] | Temp9am [Float64] | Temp3pm [Float64] | RainToday [Utf8View] | RISK_MM [Float64] | RainTomorrow [Utf8View] |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 8.0               | 24.3              | 0.0                | 3.4                   | 6.3                 | NW                     | 30                       | SW                    | NW                    | 6                       | 20                   | 68                  | 29                  | 1019.7                | 1015.0                | 7                | 7                | 14.4              | 23.6              | No                   | 3.6               | Yes                     |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 14.0              | 26.9              | 3.6                | 4.4                   | 9.7                 | ENE                    | 39                       | E                     | W                     | 4                       | 17                   | 80                  | 36                  | 1012.4                | 1008.4                | 5                | 3                | 17.5              | 25.7              | Yes                  | 3.6               | Yes                     |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 13.7              | 23.4              | 3.6                | 5.8                   | 3.3                 | NW                     | 85                       | N                     | NNE                   | 6                       | 6                    | 82                  | 69                  | 1009.5                | 1007.2                | 8                | 7                | 15.4              | 20.2              | Yes                  | 39.8              | Yes                     |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 13.3              | 15.5              | 39.8               | 7.2                   | 9.1                 | NW                     | 54                       | WNW                   | W                     | 30                      | 24                   | 62                  | 56                  | 1005.5                | 1007.0                | 2                | 7                | 13.5              | 14.1              | Yes                  | 2.8               | Yes                     |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 7.6               | 16.1              | 2.8                | 5.6                   | 10.6                | SSE                    | 50                       | SSE                   | ESE                   | 20                      | 28                   | 68                  | 49                  | 1018.3                | 1018.5                | 7                | 7                | 11.1              | 15.4              | Yes                  | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 6.2               | 16.9              | 0.0                | 5.8                   | 8.2                 | SE                     | 44                       | SE                    | E                     | 20                      | 24                   | 70                  | 57                  | 1023.8                | 1021.7                | 7                | 5                | 10.9              | 14.8              | No                   | 0.2               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 6.1               | 18.2              | 0.2                | 4.2                   | 8.4                 | SE                     | 43                       | SE                    | ESE                   | 19                      | 26                   | 63                  | 47                  | 1024.6                | 1022.2                | 4                | 6                | 12.4              | 17.3              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 8.3               | 17.0              | 0.0                | 5.6                   | 4.6                 | E                      | 41                       | SE                    | E                     | 11                      | 24                   | 65                  | 57                  | 1026.2                | 1024.2                | 6                | 7                | 12.1              | 15.5              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 8.8               | 19.5              | 0.0                | 4.0                   | 4.1                 | S                      | 48                       | E                     | ENE                   | 19                      | 17                   | 70                  | 48                  | 1026.1                | 1022.7                | 7                | 7                | 14.1              | 18.9              | No                   | 16.2              | Yes                     |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 8.4               | 22.8              | 16.2               | 5.4                   | 7.7                 | E                      | 31                       | S                     | ESE                   | 7                       | 6                    | 82                  | 32                  | 1024.1                | 1020.7                | 7                | 1                | 13.3              | 21.7              | Yes                  | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        ");
        Ok(())
    }

    impl std::fmt::Display for SqlResult {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let mut builder = tabled::builder::Builder::new();
            for (i, (name, typ)) in self.columns.iter().enumerate() {
                let values = self
                    .rows
                    .iter()
                    .map(|v| v.get(i).unwrap())
                    .collect::<Vec<_>>();
                builder.push_column([vec![&format!("{name} [{typ}]")], values].concat())
            }
            let table = builder.build();
            write!(f, "{}", table)
        }
    }
}
