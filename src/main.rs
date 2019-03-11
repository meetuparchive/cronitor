use chrono::{prelude::*, Duration};
use futures::{future::join_all, Future};
use rusoto_cloudwatch::{CloudWatch, CloudWatchClient, Dimension, GetMetricStatisticsInput};
use rusoto_core::{credential::ChainProvider, request::HttpClient};
use rusoto_ecs::{DescribeTasksRequest, Ecs, EcsClient, ListTasksRequest};
use rusoto_events::{CloudWatchEvents, CloudWatchEventsClient, ListRulesRequest};
use std::time::Duration as StdDuration;
use structopt::StructOpt;
use tokio::runtime::Runtime;

#[derive(StructOpt)]
#[structopt(name = "cronitor", about = "tool for introspecting AWS ECS crons")]
struct Options {
    #[structopt(
        short = "r",
        long = "rule",
        help = "name of Cloud Watch event rule or rule prefix"
    )]
    rule: String,
    #[structopt(short = "c", long = "cluster", help = "ECS cluster name")]
    cluster: String,
}

fn credentials() -> ChainProvider {
    let mut chain = ChainProvider::new();
    chain.set_timeout(StdDuration::from_millis(200));
    chain
}

/// get the timestamp of the last time a given rule triggered an event
/// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cwe-metricscollected.html
fn get_last_trigger(
    metrics: std::sync::Arc<CloudWatchClient>,
    rule: &str,
    since: Duration,
) -> impl Future<Item = Option<String>, Error = String> {
    let now = Utc::now();
    let start = now - since;
    metrics
        .get_metric_statistics(GetMetricStatisticsInput {
            dimensions: Some(vec![Dimension {
                name: "RuleName".into(),
                value: rule.into(),
            }]),
            end_time: now.to_rfc3339(),
            metric_name: "TriggeredRules".into(),
            namespace: "AWS/Events".into(),
            period: Duration::days(1).num_seconds(),
            start_time: start.to_rfc3339(),
            statistics: Some(vec!["Sum".into()]),
            ..GetMetricStatisticsInput::default()
        })
        .map_err(|e| e.to_string())
        .map(|response| {
            response
                .datapoints
                .unwrap_or_default()
                .into_iter()
                .last()
                .and_then(move |dp| dp.timestamp)
        })
}

fn main() {
    let Options { rule, cluster } = Options::from_args();
    let mut rt = Runtime::new().expect("failed to create runtime");
    let creds = credentials();

    let events = CloudWatchEventsClient::new_with(
        HttpClient::new().expect("failed to create request dispatcher"),
        creds.clone(),
        Default::default(),
    );
    let metrics = CloudWatchClient::new_with(
        HttpClient::new().expect("failed to create request dispatcher"),
        creds.clone(),
        Default::default(),
    );
    let ecs = EcsClient::new_with(
        HttpClient::new().expect("failed to create request dispatcher"),
        creds,
        Default::default(),
    );

    let rules = events
        .list_rules(ListRulesRequest {
            name_prefix: Some(rule.clone()),
            ..ListRulesRequest::default()
        })
        .map_err(|e| e.to_string())
        .map(|result| {
            result
                .rules
                .unwrap_or_default()
                .into_iter()
                .map(|rule| rule.name.unwrap_or_default())
                .collect::<Vec<_>>()
        });

    let last_triggers = rules.and_then(move |names| {
        let mets = std::sync::Arc::new(metrics);
        join_all(names.into_iter().map(move |name| {
            get_last_trigger(mets.clone(), name.as_str(), Duration::weeks(1)).map(|ts| (name, ts))
        }))
    });

    let stopped_tasks = last_triggers.and_then(move |triggers| {
        let ecss = std::sync::Arc::new(ecs);
        join_all(triggers.into_iter().map(move |(rule, last)| {
            let cluster = cluster.clone();
            let ecs = ecss.clone();
            let ecs2 = ecss.clone();
            ecs.list_tasks(ListTasksRequest {
                cluster: Some(cluster.clone()),
                desired_status: Some("STOPPED".into()),
                started_by: Some(format!("events-rule/{}", rule).chars().take(36).collect()),
                ..ListTasksRequest::default()
            })
            .map_err(|e| e.to_string())
            .and_then(move |response| {
                ecs2.describe_tasks(DescribeTasksRequest {
                    cluster: Some(cluster),
                    tasks: response.task_arns.unwrap_or_default(),
                })
                .map_err(|e| e.to_string())
                .map(|result| (rule, last, result.tasks.unwrap_or_default()))
            })
        }))
    });

    println!("{:#?}", rt.block_on(stopped_tasks));
}
