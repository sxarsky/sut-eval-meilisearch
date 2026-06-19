use std::collections::BTreeMap;

use actix_web::web::{self, Data, Path};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::dynamic_search_rules::{
    DynamicSearchRule, DynamicSearchRuleUpdateRequest, RuleUid,
};
use meilisearch_types::error::deserr_codes::{
    InvalidDynamicSearchRuleFilter, InvalidDynamicSearchRuleFilterActive,
    InvalidDynamicSearchRuleFilterAttributePatterns, InvalidDynamicSearchRuleLimit,
    InvalidDynamicSearchRuleOffset,
};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::{AttributePatterns, PatternMatch};
use meilisearch_types::tasks::{DsrUpdate, KindWithContent};
use serde::Serialize;
use wip::WipOptionExt as _;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::proxy::{proxy, task_network_and_check_leader_and_version, Body};
use crate::routes::{Pagination, PaginationView, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT};

#[routes::routes(
    routes(
        "" => [post(list_rules)],
        "/{uid}" => [get(get_rule), patch(update_or_create_rule), delete(delete_rule)],
    ),
    tag = "Search rules",
    tags((
        name = "Search rules",
        description = "The `/dynamic-search-rules` route allows you to configure search rules.",
    ))
)]
pub struct DynamicSearchRulesApi;

#[routes::request(override_error = DeserrJsonError<InvalidDynamicSearchRuleFilter>)]
#[derive(Debug)]
pub struct ListRulesFilter {
    /// Only include rules whose names match these patterns (e.g. `["black-friday", "promo*"]`).
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilterAttributePatterns>)]
    pub attribute_patterns: Option<AttributePatterns>,
    /// Only include rules that are active (true) or not active (false).
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilterActive>)]
    pub active: Option<bool>,
}

#[routes::request]
#[derive(Debug)]
pub struct ListRules {
    /// Number of rules to skip. Defaults to 0.
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleOffset>)]
    pub offset: usize,
    /// Maximum number of rules to return. Default to 20.
    #[request(default = PAGINATION_DEFAULT_LIMIT, error = DeserrJsonError<InvalidDynamicSearchRuleLimit>)]
    pub limit: usize,
    /// Optional filter to restrict which rules are returned (e.g. by attribute patterns or by properties like if the rule is active or not)
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilter>)]
    pub filter: Option<ListRulesFilter>,
}

impl ListRules {
    fn apply_filter(&self, rule: &DynamicSearchRule) -> bool {
        if let Some(filter) = &self.filter {
            if let Some(patterns) = &filter.attribute_patterns {
                if matches!(
                    patterns.match_str(&rule.uid),
                    PatternMatch::NoMatch | PatternMatch::Parent
                ) {
                    return false;
                }
            }

            if let Some(active) = &filter.active {
                if *active != rule.active {
                    return false;
                }
            }
        }

        true
    }
}

#[derive(Debug, thiserror::Error)]
enum DynamicSearchRulesError {
    #[error("Dynamic search rule `{0}` not found.")]
    NotFound(RuleUid),
}

impl ErrorCode for DynamicSearchRulesError {
    fn error_code(&self) -> Code {
        match self {
            DynamicSearchRulesError::NotFound(_) => Code::DynamicSearchRuleNotFound,
        }
    }
}

#[derive(Serialize, Default)]
struct UpdateDynamicSearchRuleAnalytics;

impl Aggregate for UpdateDynamicSearchRuleAnalytics {
    fn event_name(&self) -> &'static str {
        "Dynamic Search Rules Created or Updated"
    }

    fn aggregate(self: Box<Self>, _new: Box<Self>) -> Box<Self> {
        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

#[derive(Serialize, Default)]
struct DeleteDynamicSearchRuleAnalytics;

impl Aggregate for DeleteDynamicSearchRuleAnalytics {
    fn event_name(&self) -> &'static str {
        "Dynamic Search Rules Deleted"
    }

    fn aggregate(self: Box<Self>, _new: Box<Self>) -> Box<Self> {
        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// List search rules
///
/// Return all search rules configured on the instance.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.get", "dynamicSearchRules.*", "*.get", "*"])),
    request_body = ListRules,
    responses(
        (status = OK, description = "Dynamic search rules are returned.", body = PaginationView<DynamicSearchRule>, content_type = "application/json", example = json!({
            "results": [
                {
                    "uid": "black-friday",
                    "description": "Black Friday 2025 rules",
                    "priority": 10,
                    "active": true,
                    "conditions": [
                        { "scope": "query", "isEmpty": true },
                        { "scope": "time", "start": "2025-11-28T00:00:00Z", "end": "2025-11-28T23:59:59Z" }
                    ],
                    "actions": [
                        {
                            "selector": { "indexUid": "products", "id": "123" },
                            "action": { "type": "pin", "position": 1 }
                        }
                    ]
                }
            ],
            "offset": 0,
            "limit": 20,
            "total": 1
        })),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        })),
    ),
)]
async fn list_rules(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_GET }>,
        Data<IndexScheduler>,
    >,
    body: AwebJson<ListRules, DeserrJsonError>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let rules: BTreeMap<String, DynamicSearchRule> = wip::wip!();
    let pagination = Pagination { offset: body.0.offset, limit: body.0.limit };
    let pagination_view =
        pagination.auto_paginate_counting(rules.values().filter(|rule| body.0.apply_filter(rule)));

    Ok(HttpResponse::Ok().json(pagination_view))
}

/// Get a search rule
///
/// Retrieve a single search rule by its unique identifier.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.get", "dynamicSearchRules.*", "*.get", "*"])),
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the search rule.", nullable = false)),
    responses(
        (status = OK, description = "Dynamic search rule returned.", body = DynamicSearchRule, content_type = "application/json", example = json!({
            "uid": "black-friday",
            "description": "Black Friday 2025 rules",
            "priority": 10,
            "active": true,
            "conditions": [
                { "scope": "query", "isEmpty": true },
                { "scope": "time", "start": "2025-11-28T00:00:00Z", "end": "2025-11-28T23:59:59Z" }
            ],
            "actions": [
                {
                    "selector": { "indexUid": "products", "id": "123" },
                    "action": { "type": "pin", "position": 1 }
                }
            ]
        })),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        })),
        (status = 404, description = "Dynamic search rule not found.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "Dynamic search rule `black-friday` not found.",
            "code": "dynamic_search_rule_not_found",
            "type": "invalid_request",
            "link": "https://docs.meilisearch.com/errors#dynamic_search_rule_not_found"
        })),
    ),
)]
async fn get_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_GET }>,
        Data<IndexScheduler>,
    >,
    uid: Path<RuleUid>,
) -> Result<HttpResponse, ResponseError> {
    let features = index_scheduler.features();
    features.check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let uid = uid.into_inner();
    let rules = index_scheduler.dynamic_search_rules(features).unwrap_wip();
    let rule = rules.get(&uid)?.ok_or(DynamicSearchRulesError::NotFound(uid))?;

    Ok(HttpResponse::Ok().json(rule))
}

/// Create or update a search rule
///
/// Partially update a search rule by replacing the provided fields. If the rule doesn't exist, it will be created.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.update", "dynamicSearchRules.*", "*"])),
    request_body = DynamicSearchRuleUpdateRequest,
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the search rule.", nullable = false)),
    responses(
        (status = OK, description = "Dynamic search rule updated.", body = DynamicSearchRule, content_type = "application/json", example = json!({
            "uid": "black-friday",
            "description": "Black Friday 2025 rules",
            "priority": 5,
            "active": true,
            "conditions": [
                { "scope": "query", "isEmpty": true }
            ],
            "actions": [
                {
                    "selector": { "indexUid": "products", "id": "123" },
                    "action": { "type": "pin", "position": 1 }
                }
            ]
        })),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        })),
        (status = 404, description = "Dynamic search rule not found.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "Dynamic search rule `black-friday` not found.",
            "code": "dynamic_search_rule_not_found",
            "type": "invalid_request",
            "link": "https://docs.meilisearch.com/errors#dynamic_search_rule_not_found"
        })),
    ),
)]
async fn update_or_create_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_UPDATE }>,
        Data<IndexScheduler>,
    >,
    uid: Path<RuleUid>,
    body: AwebJson<DynamicSearchRuleUpdateRequest, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;
    let network = index_scheduler.network();

    let uid = uid.into_inner();
    let rule = body.into_inner();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    wip::fixme!("consider supporting custom metadata");
    let mut task = {
        let kind = KindWithContent::DsrUpdate(DsrUpdate::CreateOrUpdate {
            rule_id: uid,
            update: rule.clone(),
        });
        index_scheduler.register_with_custom_metadata(kind, None, None, false, task_network)
    }?;

    if let Some(task_network) = task.network.take() {
        proxy(&index_scheduler, None, &req, task_network, network, Body::inline(rule), &task)
            .await?;
    }

    let task: SummarizedTaskView = task.into();

    analytics.publish(UpdateDynamicSearchRuleAnalytics, &req);
    tracing::debug!(returns = ?task, "Update DSR");

    Ok(HttpResponse::Accepted().json(task))
}

/// Delete a search rule
///
/// Delete a search rule by its unique identifier.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.delete", "dynamicSearchRules.*", "*.delete", "*"])),
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the search rule.", nullable = false)),
    responses(
        (status = NO_CONTENT, description = "Dynamic search rule deleted."),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        })),
        (status = 404, description = "Dynamic search rule not found.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "Dynamic search rule `black-friday` not found.",
            "code": "dynamic_search_rule_not_found",
            "type": "invalid_request",
            "link": "https://docs.meilisearch.com/errors#dynamic_search_rule_not_found"
        })),
    ),
)]
async fn delete_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_DELETE }>,
        Data<IndexScheduler>,
    >,
    uid: Path<RuleUid>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;
    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    let uid = uid.into_inner();

    wip::fixme!("metadata");
    let mut task = {
        let kind = KindWithContent::DsrUpdate(DsrUpdate::Deletion(uid));
        index_scheduler.register_with_custom_metadata(kind, None, None, false, task_network)?
    };

    if let Some(task_network) = task.network.take() {
        proxy(&index_scheduler, None, &req, task_network, network, Body::none(), &task).await?;
    }

    analytics.publish(DeleteDynamicSearchRuleAnalytics, &req);

    let task: SummarizedTaskView = task.into();

    tracing::debug!(returns = ?task, "Delete DSR");
    Ok(HttpResponse::Accepted().json(task))
}
