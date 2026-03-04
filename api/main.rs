use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_distributed::{
    create_flight_client, display_plan_ascii, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedPhysicalOptimizerRule, Worker, WorkerQueryContext, WorkerResolver,
};
use futures::TryStreamExt;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env::current_dir;
use std::fmt::Display;
use std::fs;
use std::sync::Arc;
use tonic::transport::{Endpoint, Server};
use url::Url;
use vercel_runtime::{run, Body, Error, Request, RequestPayloadExt, Response, StatusCode};

const MAX_RESULTS: usize = 500;

const DUMMY_URL: &str = "http://localhost:50051";

struct InMemoryWorkerResolver;

#[derive(Clone)]
struct InMemoryChannelResolver {
    channel: BoxCloneSyncChannel,
}

impl InMemoryChannelResolver {
    fn new() -> Self {
        let (client, server) = tokio::io::duplex(1024 * 1024);

        let mut client = Some(client);
        let channel = Endpoint::try_from(DUMMY_URL)
            .expect("Invalid dummy URL for building an endpoint. This should never happen")
            .connect_with_connector_lazy(tower::service_fn(move |_| {
                let client = client
                    .take()
                    .expect("Client taken twice. This should never happen");
                async move { Ok::<_, std::io::Error>(TokioIo::new(client)) }
            }));

        let this = Self {
            channel: BoxCloneSyncChannel::new(channel),
        };
        let this_clone = this.clone();

        let endpoint = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
            let this = this.clone();
            async move {
                let builder = ctx.builder.with_distributed_channel_resolver(this);
                Ok(builder.build())
            }
        });

        tokio::spawn(async move {
            Server::builder()
                .add_service(endpoint.into_flight_server())
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });

        this_clone
    }
}

impl WorkerResolver for InMemoryWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(vec![Url::parse(DUMMY_URL).unwrap(); 16])
    }
}

#[async_trait]
impl ChannelResolver for InMemoryChannelResolver {
    async fn get_flight_client_for_url(
        &self,
        _: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        Ok(create_flight_client(self.channel.clone()))
    }
}

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
    logical_plan: String,
    physical_plan: String,
}

async fn execute_statements(
    stmts: Vec<String>,
    path: impl Display,
) -> datafusion::error::Result<SqlResult> {
    let options = FormatOptions::default().with_display_error(true);
    let cfg = SessionConfig::new().with_information_schema(true);

    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(cfg)
        .with_distributed_worker_resolver(InMemoryWorkerResolver)
        .with_distributed_channel_resolver(InMemoryChannelResolver::new());
    if stmts.iter().any(|v| v.contains("distributed.")) {
        builder = builder.with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
    }
    let ctx = Arc::new(SessionContext::new_with_state(builder.build()));
    load_parquet_files(path.to_string(), &ctx).await?;

    if stmts.is_empty() {
        return Ok(SqlResult::default());
    }

    for i in 0..stmts.len() - 1 {
        ctx.sql(stmts.get(i).unwrap()).await?.collect().await?;
    }
    let df = ctx.sql(stmts.last().unwrap()).await?;
    let logical_plan_str = df.logical_plan().display_indent().to_string();

    let physical_plan = df.create_physical_plan().await?;

    let record_batches = execute_stream(physical_plan.clone(), ctx.task_ctx())?
        .try_collect::<Vec<_>>()
        .await?;

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

    Ok(SqlResult {
        columns,
        rows,
        logical_plan: logical_plan_str,
        physical_plan: display_physical_plan(&physical_plan).unwrap_or_else(|err| err.to_string()),
    })
}

fn display_physical_plan(physical_plan: &Arc<dyn ExecutionPlan>) -> Result<String, Error> {
    let physical_plan_str = display_plan_ascii(physical_plan.as_ref(), false);
    let curr_dir = current_dir()?.display().to_string();
    let curr_dir = curr_dir.trim_start_matches("/");
    let physical_plan_str = physical_plan_str.replace(curr_dir, "");
    Ok(physical_plan_str)
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
        | 0.5               | 17.1              | 0.0                | 4.0                   | 9.4                 | NW                     | 31                       | ESE                   | W                     | 6                       | 13                   | 74                  | 42                  | 1020.8                | 1017.4                | 1                | 1                | 7.4               | 16.2              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | -0.9              | 16.7              | 0.0                | 2.4                   | 9.3                 | NNW                    | 30                       | SW                    | NNW                   | 2                       | 15                   | 76                  | 42                  | 1022.7                | 1018.5                | 5                | 2                | 6.2               | 15.4              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 0.4               | 19.0              | 0.0                | 3.4                   | 8.3                 | NW                     | 39                       | NE                    | WNW                   | 2                       | 19                   | 76                  | 41                  | 1019.8                | 1015.8                | 6                | 5                | 7.7               | 18.5              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 7.5               | 16.8              | 0.0                | 2.8                   | 3                   | NW                     | 41                       | W                     | NW                    | 7                       | 26                   | 70                  | 53                  | 1018.0                | 1013.8                | 7                | 7                | 12.5              | 15.4              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 8.3               | 17.6              | 0.0                | 3.4                   | 9.4                 | WNW                    | 43                       | NW                    | WNW                   | 17                      | 30                   | 73                  | 43                  | 1015.8                | 1013.5                | 1                | 1                | 12.4              | 16.5              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | -0.2              | 18.1              | 0.0                | 4.4                   | 9.4                 | NW                     | 24                       | NA                    | NW                    | 0                       | 9                    | 80                  | 44                  | 1021.4                | 1018.9                | 1                | 1                | 6.7               | 16.9              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 0.1               | 21.0              | 0.0                | 2.2                   | 9.2                 | NNW                    | 17                       | WNW                   | N                     | 2                       | 9                    | 78                  | 36                  | 1023.2                | 1020.3                | 0                | 1                | 7.6               | 20.7              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 1.5               | 20.9              | 0.0                | 2.4                   | 9.3                 | NW                     | 20                       | NW                    | NNW                   | 2                       | 9                    | 80                  | 41                  | 1023.2                | 1020.0                | 1                | 1                | 8.4               | 20.9              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 8.3               | 17.4              | 0.0                | 2.0                   | 1.6                 | E                      | 20                       | WSW                   | NE                    | 6                       | 11                   | 80                  | 52                  | 1024.4                | 1021.5                | 7                | 7                | 13.5              | 17.2              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        | 9.4               | 19.2              | 0.0                | 2.2                   | 7.7                 | NA                     | 24                       | E                     | NNW                   | 4                       | 15                   | 73                  | 47                  | 1024.2                | 1020.3                | 7                | 1                | 12.1              | 18.8              | No                   | 0.0               | No                      |
        +-------------------+-------------------+--------------------+-----------------------+---------------------+------------------------+--------------------------+-----------------------+-----------------------+-------------------------+----------------------+---------------------+---------------------+-----------------------+-----------------------+------------------+------------------+-------------------+-------------------+----------------------+-------------------+-------------------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_distributed() -> datafusion::error::Result<()> {
        let result = execute_statements(
            // TPCH 17
            vec![
                "SET distributed.files_per_task = 1;".into(),
                r#"
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
        p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
            0.2 * avg(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
);
            "#
                .into(),
            ],
            format!("{}/api/parquet", env!("CARGO_MANIFEST_DIR")),
        )
        .await?;

        insta::assert_snapshot!(result.physical_plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[CAST(sum(lineitem.l_extendedprice)@0 AS Float64) / 7 as avg_yearly]
        │   AggregateExec: mode=Final, gby=[], aggr=[sum(lineitem.l_extendedprice)]
        │     CoalescePartitionsExec
        │       AggregateExec: mode=Partial, gby=[], aggr=[sum(lineitem.l_extendedprice)]
        │         HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@2, l_partkey@1)], filter=CAST(l_quantity@0 AS Decimal128(30, 15)) < Float64(0.2) * avg(lineitem.l_quantity)@1, projection=[l_extendedprice@1]
        │           CoalescePartitionsExec
        │             ProjectionExec: expr=[l_quantity@1 as l_quantity, l_extendedprice@2 as l_extendedprice, p_partkey@0 as p_partkey]
        │               HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, l_partkey@0)], projection=[p_partkey@0, l_quantity@2, l_extendedprice@3]
        │                 CoalescePartitionsExec
        │                   [Stage 1] => NetworkCoalesceExec: output_partitions=64, input_tasks=4
        │                 DataSourceExec: file_groups={4 groups: [[/api/parquet/lineitem/1.parquet], [/api/parquet/lineitem/2.parquet], [/api/parquet/lineitem/3.parquet], [/api/parquet/lineitem/4.parquet]]}, projection=[l_partkey, l_quantity, l_extendedprice], file_type=parquet, predicate=DynamicFilter [ empty ]
        │           ProjectionExec: expr=[CAST(0.2 * CAST(avg(lineitem.l_quantity)@1 AS Float64) AS Decimal128(30, 15)) as Float64(0.2) * avg(lineitem.l_quantity), l_partkey@0 as l_partkey]
        │             AggregateExec: mode=FinalPartitioned, gby=[l_partkey@0 as l_partkey], aggr=[avg(lineitem.l_quantity)]
        │               [Stage 2] => NetworkShuffleExec: output_partitions=16, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p15] t1:[p16..p31] t2:[p32..p47] t3:[p48..p63] 
          │ FilterExec: p_brand@1 = Brand#23 AND p_container@2 = MED BOX, projection=[p_partkey@0]
          │   RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1
          │     PartitionIsolatorExec: t0:[p0,__,__,__] t1:[__,p0,__,__] t2:[__,__,p0,__] t3:[__,__,__,p0]
          │       DataSourceExec: file_groups={4 groups: [[/api/parquet/part/1.parquet], [/api/parquet/part/2.parquet], [/api/parquet/part/3.parquet], [/api/parquet/part/4.parquet]]}, projection=[p_partkey, p_brand, p_container], file_type=parquet, predicate=p_brand@3 = Brand#23 AND p_container@6 = MED BOX, pruning_predicate=p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#23 AND Brand#23 <= p_brand_max@1 AND p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED BOX AND MED BOX <= p_container_max@5, required_guarantees=[p_brand in (Brand#23), p_container in (MED BOX)]
          └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p15] t1:[p0..p15] t2:[p0..p15] t3:[p0..p15] 
          │ RepartitionExec: partitioning=Hash([l_partkey@0], 16), input_partitions=1
          │   AggregateExec: mode=Partial, gby=[l_partkey@0 as l_partkey], aggr=[avg(lineitem.l_quantity)]
          │     PartitionIsolatorExec: t0:[p0,__,__,__] t1:[__,p0,__,__] t2:[__,__,p0,__] t3:[__,__,__,p0]
          │       DataSourceExec: file_groups={4 groups: [[/api/parquet/lineitem/1.parquet], [/api/parquet/lineitem/2.parquet], [/api/parquet/lineitem/3.parquet], [/api/parquet/lineitem/4.parquet]]}, projection=[l_partkey, l_quantity], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
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
