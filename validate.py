"""
Validate TDS 2025 Jan Project 1 GitHub repos and Docker Hub images from
TDS*Project 1 Submission*Form Responses*.csv in current directory.
"""

# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "httpx",
#     "pandas",
# ]
# ///

from base64 import b64decode
from pathlib import Path
from subprocess import run
from urllib.parse import urlparse
import httpx
import os
import pandas as pd

DEADLINE = pd.Timestamp("2025-02-17", tz="Asia/Kolkata")
REPOS = Path("repos")
github_token = os.environ.get("GITHUB_TOKEN")
github_headers = {"Authorization": f"Bearer {github_token}"} if github_token else None


def github(path: str) -> dict:
    return httpx.get(f"https://api.github.com/repos/{path}", headers=github_headers).json()


def main(paths: list[str]) -> None:
    # Get the latest submissions
    dfs = []
    for file in paths:
        df = pd.read_csv(file)
        df.columns = ["timestamp", "email", "github_url", "dockerhub_image"]
        df["timestamp"] = pd.to_datetime(df["timestamp"], dayfirst=False)
        dfs.append(df)
    submissions = pd.concat(dfs)
    latest_submissions = submissions.sort_values("timestamp").drop_duplicates("email", keep="last")

    # Skip submissions that have already been processed
    if os.path.exists("validate.csv"):
        past_submissions = pd.read_csv("validate.csv")
    else:
        past_submissions = pd.DataFrame(columns=["email", "url", "result", "type", "message"])
        past_submissions.to_csv("validate.csv", index=False)
    checks_done = {(row["email"], row["url"]) for _, row in past_submissions.iterrows()}
    validate_file = open("validate.csv", "a", encoding="utf-8")
    images_file = open("images.txt", "a", encoding="utf-8", newline="")

    # Log the results to the console and validate.csv
    def log(email: str, url: str, result: bool, type: str, message: str) -> None:
        msg = f"{email},{url},{'OK' if result else 'FAIL'},{type},{message}"
        print(msg)
        validate_file.write(msg + "\n")
        validate_file.flush()

    # Check the Docker Hub image
    for _, row in latest_submissions.iterrows():
        email = row["email"]
        url = row["dockerhub_image"].strip()
        if (email, url) in checks_done:
            continue

        username, repo = url.split("/", 1)
        tags = httpx.get(
            f"https://hub.docker.com/v2/repositories/{username}/{repo}/tags?ordering=last_updated"
        ).json()
        tag, size = next(
            (
                (tag["name"], tag["full_size"])
                for tag in tags.get("results", [])
                if pd.Timestamp(tag["last_updated"]) <= DEADLINE
            ),
            (None, 0),
        )
        msg = f"Tag: {tag} ({size})" if tag else "No public DockerHub image"
        log(email, url, tag, "Docker", msg)
        if tag:
            images_file.write(f"{email}\t{url}:{tag}\n")

    # Check the GitHub repo
    for _, row in latest_submissions.iterrows():
        email = row["email"]
        url = row["github_url"].strip()
        if (email, url) in checks_done:
            continue

        owner, repo = urlparse(url).path.strip("/").split("/")[:2]
        if repo.endswith(".git"):
            repo = repo[:-4]
        repo_data = github(f"{owner}/{repo}")
        license = github(f"{owner}/{repo}/contents/LICENSE")
        dockerfile_data = github(f"{owner}/{repo}/contents/Dockerfile")

        exists = "id" in repo_data
        log(email, url, exists, "Repo", "exists" if exists else "No such repo")

        mit = license.get("encoding") == "base64" and b"MIT" in b64decode(license.get("content"))
        log(email, url, mit, "License", "LICENSE" if mit else 'No "MIT" in LICENSE')

        dockerfile = dockerfile_data.get("type") == "file"
        log(email, url, dockerfile, "Dockerfile", "Dockerfile" if dockerfile else "No Dockerfile")

        if exists:
            # Clone the latest commit into ./repos/[base email]
            path = REPOS / email.split("@")[0]
            cmd = ["git", "clone", "--depth", "1", f"https://github.com/{owner}/{repo}.git", path]
            result = run(cmd, capture_output=True, text=True)
            code = result.returncode
            log(email, url, code == 0, result.stderr if code else "Cloned")


if __name__ == "__main__":
    main(Path().glob("TDS*Project 1 Submission*Form Responses*.csv"))
