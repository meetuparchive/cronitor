use chrono::prelude::*;
use rusoto_cloudwatch::{CloudWatch, CloudWatchClient, Dimension, GetMetricStatisticsInput};
use rusoto_core::{credential::ChainProvider, request::HttpClient};
use rusoto_ecs::{DescribeTasksRequest, Ecs, EcsClient, ListTasksRequest};
use rusoto_events::{CloudWatchEvents, CloudWatchEventsClient, ListTargetsByRuleRequest};
use std::{env, time::Duration as StdDuration};

use chrono::Duration;

fn credentials() -> ChainProvider {
    let mut chain = ChainProvider::new();
    chain.set_timeout(StdDuration::from_millis(200));
    chain
}

fn get_ecs_task_def_arn(
    events: &CloudWatchEvents,
    rule_name: &str,
) -> Option<String> {
    events
        .list_targets_by_rule(ListTargetsByRuleRequest {
            rule: rule_name.into(),
            ..ListTargetsByRuleRequest::default()
        })
        .sync()
        .map(|response| {
            response
                .targets
                .unwrap_or_default()
                .into_iter()
                .filter_map(|target| target.ecs_parameters.map(|ecs| ecs.task_definition_arn))
                .next()
        })
        .ok()
        .unwrap_or_default()
}

/// get the timestamp of the last time a given rule triggered an event
/// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cwe-metricscollected.html
fn get_last_trigger(
    metrics: &CloudWatch,
    rule_name: &str,
    since: Duration,
) -> Option<String> {
    let now = Utc::now();
    let start = now - since;
    metrics
        .get_metric_statistics(GetMetricStatisticsInput {
            dimensions: Some(vec![Dimension {
                name: "RuleName".into(),
                value: rule_name.into(),
            }]),
            end_time: now.to_rfc3339(),
            metric_name: "TriggeredRules".into(),
            namespace: "AWS/Events".into(),
            period: Duration::days(1).num_seconds(),
            start_time: start.to_rfc3339(),
            statistics: Some(vec!["Sum".into()]),
            ..GetMetricStatisticsInput::default()
        })
        .sync()
        .map(|response| {
            response
                .datapoints
                .unwrap_or_default()
                .into_iter()
                .last()
                .and_then(move |dp| dp.timestamp)
        })
        .ok()
        .unwrap_or_default()
}

fn main() {
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

    match (env::args().nth(1), env::args().nth(2)) {
        (Some(rule), Some(cluster)) => {
            println!(
                "{:#?}",
                get_last_trigger(&metrics, rule.as_str(), Duration::weeks(1))
            );
            let task_def = get_ecs_task_def_arn(&events, rule.as_str());
            if let Some(arn) = task_def {
                println!("inspecting tasks for {}", arn);
                let tasks = ecs
                    .list_tasks(ListTasksRequest {
                        cluster: Some(cluster.clone()),
                        desired_status: Some("STOPPED".into()),
                        started_by: Some(format!("events-rule/{}", rule)[..36].into()),
                        ..ListTasksRequest::default()
                    })
                    .sync()
                    .ok()
                    .and_then(|response| {
                        ecs.describe_tasks(DescribeTasksRequest {
                            cluster: Some(cluster.clone()),
                            tasks: response.task_arns.unwrap_or_default(),
                        })
                        .sync()
                        .map(|response| response.tasks.unwrap_or_default())
                        .ok()
                    })
                    .unwrap_or_default();

                println!(
                    "matched tasks {:#?}",
                    tasks
                        .into_iter()
                        .map(|task| task.task_definition_arn.clone().unwrap_or_default())
                        .collect::<Vec<_>>()
                );
            }
        }
        _ => {
            eprintln!(
                "usage: {} <rule_name> <ecs_cluster_name>",
                env::args().next().unwrap_or_default()
            );
        }
    }
}
