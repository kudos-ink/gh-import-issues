use chrono::{DateTime, Utc};
use lambda_http::{
    run, service_fn,
    tracing::{self, error},
    Body, Error, Request, Response,
};
use octocrab::{models::issues::Issue, params::State, Octocrab};
use serde::{Deserialize, Serialize};
use serde_json;

use sqlx::postgres::PgPool;
use sqlx::Row;
use std::env;

#[derive(Deserialize, Debug)]
struct ProjectLinks {
    repository: Vec<Repository>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ProjectAttributes {
    purposes: Vec<String>,
    stack_levels: Vec<String>,
    technologies: Vec<String>,
    types: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct Project {
    name: String,
    slug: String,
    attributes: ProjectAttributes,
    links: ProjectLinks,
}
impl Project {
    fn new_project_query(&self) -> &str {
        let query_string = r#"
        INSERT INTO projects (name, slug, categories, purposes, stack_levels, technologies)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id;
        "#;
        return query_string;
    }
}

#[derive(Deserialize, Debug)]
struct Repository {
    label: String,
    url: String,
}

impl Repository {
    fn insert_respository_query(&self) -> &str {
        let query_string = r#"
        INSERT INTO repositories (slug, project_id)
        VALUES ($1, $2)
        RETURNING id;
        "#;
        return query_string;
    }
}

#[derive(Deserialize, Debug)]
struct RepoInfo {
    owner: String,
    name: String,
}

impl RepoInfo {
    fn from_url(url: &str) -> Option<Self> {
        let parts: Vec<&str> = url.trim_end_matches('/').split('/').collect();
        if parts.len() >= 2 {
            Some(RepoInfo {
                owner: parts[parts.len() - 2].to_string(),
                name: parts[parts.len() - 1].to_string(),
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct KudosIssue {
    number: i64,
    title: String,
    html_url: String,
    issue_created_at: DateTime<Utc>,
    issue_updated_at: DateTime<Utc>,
    user: String,
    labels: Vec<String>,
}

impl From<Issue> for KudosIssue {
    fn from(value: Issue) -> Self {
        KudosIssue {
            number: value.number as i64,
            title: value.title,
            html_url: value.html_url.to_string(),
            issue_created_at: value.created_at,
            issue_updated_at: value.updated_at,
            user: value.user.login,
            labels: value
                .labels
                .iter()
                .map(|label| label.name.clone())
                .collect::<Vec<String>>(),
        }
    }
}

async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    let request_body = event.body();
    let json_string = (match request_body {
        Body::Text(json) => Some(json),
        _ => None,
    })
    .ok_or_else(|| Error::from("Invalid request body type"))?;

    let project: Project = serde_json::from_str(&json_string).map_err(|e| {
        error!("Error parsing JSON: {}", e);
        Error::from("Error parsing JSON")
    })?;

    let pool = PgPool::connect(&env::var("DATABASE_URL")?).await?;

    let query = project.new_project_query();

    let project_row = sqlx::query(query)
        .bind(&project.name)
        .bind(&project.slug)
        .bind(&project.attributes.types)
        .bind(&project.attributes.purposes)
        .bind(&project.attributes.stack_levels)
        .bind(&project.attributes.technologies)
        .fetch_one(&pool)
        .await?;

    let project_id: i32 = project_row.get("id");

    let token = env::var("GITHUB_TOKEN")?;
    let octocrab = Octocrab::builder().personal_token(token).build()?;

    let mut total_issues_imported = 0;

    for repo in project.links.repository {
        let repo_info = RepoInfo::from_url(&repo.url)
            .ok_or_else(|| Error::from("Couldn't extract repo info from url"))?;

        let repo_query = repo.insert_respository_query();

        let repo_row = sqlx::query(repo_query)
            .bind(&repo.label)
            .bind(project_id)
            .fetch_one(&pool)
            .await?;

        let repo_id: i32 = repo_row.get("id");

        let page = octocrab
            .issues(repo_info.owner, repo_info.name)
            .list()
            .state(State::Open)
            .per_page(100)
            .send()
            .await?;

        let filtered_issues: Vec<KudosIssue> = page
            .items
            .into_iter()
            .filter_map(|issue| {
                issue
                    .pull_request
                    .is_none()
                    .then(|| KudosIssue::from(issue))
            })
            .collect();

        if filtered_issues.is_empty() {
            continue;
        }

        let placeholders = filtered_issues
            .iter()
            .enumerate()
            .map(|(i, _)| {
                format!(
                    "(${}, ${}, ${}, ${}, ${})",
                    i * 5 + 1,
                    i * 5 + 2,
                    i * 5 + 3,
                    i * 5 + 4,
                    i * 5 + 5
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let query_string = format!(
            "INSERT INTO issues (number, title, labels, repository_id, issue_created_at) VALUES {}",
            placeholders
        );

        let mut insert_issues_query = sqlx::query(&query_string);

        for issue in filtered_issues {
            insert_issues_query = insert_issues_query
                .bind(issue.number)
                .bind(issue.title)
                .bind(issue.labels)
                .bind(repo_id)
                .bind(issue.issue_created_at)
        }

        let issues_inserted_count = insert_issues_query.execute(&pool).await?.rows_affected();

        total_issues_imported += issues_inserted_count;
    }

    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(Body::Text(format!(
            "Total issues imported: {}",
            total_issues_imported
        )))
        .map_err(Box::new)?;
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
