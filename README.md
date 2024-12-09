# Slack Scraper
## 1.0 Overview
In this project I implement an `ELT (Extract Load Transform)` automation that accesses a<br>
given Slack workspace using the Slack Web API and does the following:
- Downloads all messages and threads in private and public channels.
- Downloads files and attachments related to each message or thread.

### 1.1 Extract, Load, Transform
The automation extracts data from Slack using the Web API, it then loads the data to Google<br>
Cloud Storage. This includes the files and a JSONL file containing the messages. From GCS<br>
the data is loaded to Google BigQuery where it can be transformed and put to use.<br>
In case you are wondering why I did ELT instead of ETL, allow me to quote my manager on this:<br>
"ETL is a bad practice. ELT is a good practice..."<br><br>
Slack Web API has a limit of 999 on the number of messages that can be downloaded with a single<br>
API call to `conversations.history`. For channels with large conversations or for workspaces<br>
that have been around for long that limit is not enough to extrac all messages.<br>
With a few modifications to this implementation, this automation is capable of downloading all<br>
messages in channels even if the number of messages is larger than 999. For example in the use<br>
case at our office it was able to download all messages going back to 2019 till date. This was<br>
about `285,934` messages and their related files in a span of `4 days`.<br><br>
At that level of magnitude; scaling, runtime and robustness are important factors to put into<br>
consideration. Runtime because the automation could end up taking unreasonably long to download<br>
the content if inefficiently designed. Robustness because the automation needs to handle errors<br>
gracefully, record progress, and resume from the last checkpoint when interrupted. Other <br>
considerations include storage, network bandwidth, and compute resources, enter scaling.<br>
The Dockerfile included computes a small file (~133MB) that can be deployed to the cloud.<br>

## 2.0 Setup and Running Instructions
### 2.1 Command-line Script
1. Clone the repository to your local machine in an appropriate folder.<br>
    ```bash

    git clone https://github.com/cmmasaba/SlackScraper.git

    ```
2. Navigate to the root folder.
    ```bash

    cd SlackScraper/

    ```
3. Create the virtual environment for managing the project and it's dependencies.
    ```bash

    python -m venv .venv

    ```
4. Activate the virtual environment.
    ```bash

    source .venv/bin/avtivate

    ```
5. Install project dependencies.
    ```bash

    pip install -r requirements.txt

    ```
6. If you don't have an account on Google Cloud Platform you can sign up and create a project
   [here](https://cloud.google.com/?hl=en).<br>
   Follow the instructions at this [link](https://cloud.google.com/iam/docs/service-accounts-create#iam-service-accounts-create-console) to create a service account and assign the following roles to the SA:<br>
   ```

   - Storage Object Creator
   - Storage Object User
   - Storage Object Viewer
   - BigQuery Admin
   - BigQuery Data Editor
   - BigQuery Data Owner
   - BigQuery Data Viewer
   - BigQuery Job User

    ```
7. Follow the instructions at this [link](https://cloud.google.com/iam/docs/keys-create-delete) to download the service account key created above and save it in<br> the root folder. You will use it in step 9.
8. Follow the instructions at this [link](https://api.slack.com/quickstart) to create a Slack App and download the bot token. Only the first<br>
three steps are relevant for this project. When requesting scopes, select for the following:
    ```
    
    channels:history
    channels:read
    groups:history
    groups:read
    users:read

    ```
9. Create a .env file at the root folder and set the following environment varibles.
    ```

    GOOGLE_APPLICATION_CREDENTIALS=path-to-your-service-account-key-file
    SLACK_BOT_TOKEN=slack-bot-token
    GCP_PROJECT=project-name-in-gcp
    GCP_STORAGE_BUCKET=bucket-name-in-gcp
    TABLE_ID=table-id-in-bigquery

    ```
10. Start the automation.
    ```bash

    python src/main.py

    ```

### 2.2 Docker Container
1. Follow steps `1, 2, 6, 7, 8, 9` above.
2. Sign up for a Docker account at this [link](https://app.docker.com/signup) if you don't have an account. Download Docker Desktop<br>
 at this [link](https://docs.docker.com/get-started/get-docker/).
4. Build the Docker image. More instructions can be found at this [link](https://docs.docker.com/get-started/docker-concepts/building-images/build-tag-and-publish-an-image/).
    ```

    docker build -t your-username/scraper .

    ```
5. Find the ID of the Docker image you built.
    ```

    docker image ls

    ```
    Copy the ID of the image tagged `scraper` or whichever name you used as the tag in step 5<br>above.
6. Start the Docker container.
    ```

    docker run image-id

    ```
    or run it in background mode
    ```

    docker run -d image-id

    ```

As the automation executes, it outputs log information to stdout that you can use to follow along.<br>
The downloaded messages and files are stored in `SlackDownloads` folder inside `Messages` and <br>
`Files` subfolders respectively.

### 3.0 Adjustments
If you don't need to store your scraped data on Google Cloud Platform, comment out the following<br>
lines to disable the feature of saving to Google Cloud Storage:
```

#81, #82, #340, #350, #469, #479, #501, #500

```
In the setup instructions, skip the parts relevant to Google Cloud Platform.<br>
Set the boolean variable in line 4 of [src/main.py](src/main.py) to `False`

### 4.0 License
Refer to the [LICENSE](LICENSE) file for terms of usage. 

### 5.0 Contact
For inquiries, issues or source contributions, get in touch via `mmasabacollins9@gmail.com`.

&copy; Collins Mmasaba 2024