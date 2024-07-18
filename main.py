import requests
import os
import shutil
import schedule
import time
import logging
from dateutil.parser import parse as parse_date
from config import GITHUB_TOKENS, TELEGRAM_TOKEN, TELEGRAM_GROUP_ID, TELEGRAM_TOPIC_ID

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_last_commit_date(repo_url):
    try:
        with open("last_commit_dates.txt", "r") as file:
            for line in file:
                url, date_str = line.split(maxsplit=1)
                if repo_url == url:
                    return parse_date(date_str.strip())
    except FileNotFoundError:
        logging.info("Last commit dates file not found. Creating a new one.")
        open("last_commit_dates.txt", "w").close()
    return None

def update_last_commit_date(repo_url, last_commit_date):
    updated = False
    lines = []
    try:
        with open("last_commit_dates.txt", "r") as file:
            lines = file.readlines()
        with open("last_commit_dates.txt", "w") as file:
            for line in lines:
                if repo_url in line:
                    line = f"{repo_url} {last_commit_date}\n"
                    updated = True
                file.write(line)
            if not updated:
                file.write(f"{repo_url} {last_commit_date}\n")
    except Exception as e:
        logging.error(f"Error updating last commit dates file: {e}")

def check_repository_updates(repo_url, tokens):
    api_url = f"https://api.github.com/repos/{repo_url.split('github.com/')[1]}"
    for token in tokens:
        headers = {'Authorization': f'token {token}'}
        response = requests.get(f"{api_url}/commits", headers=headers)
        if response.status_code == 200:
            last_commit = response.json()[0]
            last_commit_date = last_commit['commit']['committer']['date']
            last_commit_message = last_commit['commit']['message']
            return parse_date(last_commit_date), last_commit_message
        elif response.status_code == 404:
            logging.warning(f"Repository not found with token {token}. Trying next token.")
        else:
            logging.warning(f"Access error with token {token}. Status code: {response.status_code}")

    logging.error(f"Failed to access repository {repo_url} with any provided tokens.")
    return None, None

def download_repository(repo_url, tokens):
    repo_name = repo_url.split('/')[-1]
    api_url = f"https://api.github.com/repos/{repo_url.split('github.com/')[1]}/zipball"
    for token in tokens:
        headers = {'Authorization': f'token {token}'}
        with requests.get(api_url, headers=headers, stream=True) as r:
            if r.status_code == 200:
                zip_path = f"{repo_name}.zip"
                with open(zip_path, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
                logging.info(f"Repository {repo_name} downloaded as {zip_path} using token: {token}")
                return zip_path
            else:
                logging.warning(f"Failed to download repository {repo_name} with token {token}. Status code: {r.status_code}")
    logging.error(f"Failed to download repository {repo_name} using available tokens.")
    return None

def send_file_telegram(file_path, commit_date, commit_message, repo_url):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    message = f"Commit date: {commit_date.strftime('%Y-%m-%d %H:%M:%S')}\nCommit message: {commit_message}\nRepository link: {repo_url}"
    with open(file_path, 'rb') as f:
        files = {'document': f}
        data = {
            'chat_id': TELEGRAM_GROUP_ID,
            'message_thread_id': TELEGRAM_TOPIC_ID,
            'caption': message
        }
        response = requests.post(url, files=files, data=data)
        if response.status_code == 200:
            logging.info(f"File {file_path} successfully sent to Telegram topic with commit info and repository link.")
        else:
            logging.warning(f"Failed to send file {file_path} to Telegram topic. Status code: {response.status_code}")


def run_task():
    logging.info("Task started.")
    try:
        with open("repositories.txt", "r") as file:
            for line in file:
                repo_url = line.strip()
                last_commit_date, last_commit_message = check_repository_updates(repo_url, GITHUB_TOKENS)
                saved_commit_date = get_last_commit_date(repo_url)
                if last_commit_date and (not saved_commit_date or last_commit_date > saved_commit_date):
                    file_path = download_repository(repo_url, GITHUB_TOKENS)
                    if file_path:
                        send_file_telegram(file_path, last_commit_date, last_commit_message, repo_url)
                        os.remove(file_path)
                        update_last_commit_date(repo_url, last_commit_date.isoformat())
                    else:
                        logging.warning(f"File not downloaded for {repo_url}.")
                else:
                    logging.info(f"No updates found for {repo_url}.")
    except Exception as e:
        logging.error(f"Error executing task: {e}")

schedule.every(10).minutes.do(run_task)

if __name__ == "__main__":
    logging.info("Bot started.")
    run_task()  # Execute task immediately on start
    while True:
        schedule.run_pending()
        time.sleep(1)
