use lambda_http::{
    run, service_fn,
    tracing::{self, error, info},
    Body, Error, Request, Response,
};
use octocrab::{models::issues::Issue, params::State, Octocrab};
use serde::{Deserialize, Serialize};
use serde_json;
use std::env;

#[derive(Deserialize, Debug)]
struct Repository {
    label: String,
    url: String,
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
    title: String,
    html_url: String,
    created_at: String,
    updated_at: String,
    user: String,
}

impl From<Issue> for KudosIssue {
    fn from(value: Issue) -> Self {
        KudosIssue {
            title: value.title,
            html_url: value.html_url.to_string(),
            created_at: value.created_at.to_string(),
            updated_at: value.updated_at.to_string(),
            user: value.user.login,
        }
    }
}

#[derive(Deserialize, Debug)]
struct Payload {
    repository: Vec<Repository>,
}

async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    let request_body = event.body();
    let json_string = (match request_body {
        Body::Text(json) => Some(json),
        _ => None,
    })
    .ok_or_else(|| Error::from("Invalid request body type"))?;

    let data: Payload = serde_json::from_str(&json_string).map_err(|e| {
        error!("Error parsing JSON: {}", e);
        Error::from("Error parsing JSON")
    })?;

    let token = env::var("GITHUB_TOKEN")?;
    let octocrab = Octocrab::builder().personal_token(token).build()?;

    let mut issue_data: Vec<KudosIssue> = vec![];

    for repo in data.repository {
        let repo_info = RepoInfo::from_url(&repo.url)
            .ok_or_else(|| Error::from("Couldn't extract repo info from url"))?;

        let mut page = octocrab
            .issues(repo_info.owner, repo_info.name)
            .list()
            .state(State::Open)
            .per_page(100)
            .send()
            .await?;

        let mut filtered_issues: Vec<KudosIssue> = page
            .items
            .into_iter()
            .filter_map(|issue| {
                issue
                    .pull_request
                    .is_none()
                    .then(|| KudosIssue::from(issue))
            })
            .collect();

        issue_data.append(&mut filtered_issues)
    }

    let response_body = serde_json::to_string(&issue_data)?;

    let resp = Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(response_body.into())
        .map_err(Box::new)?;
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
