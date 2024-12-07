# Slack Scraper
In this project I implement an `ELT (Extract Load Transform)` automation that accesses a<br>
given Slack workspace using the Slack Web API and does the following:
- Downloads all messages and threads in private and public channels.
- Downloads files and attachments related to each message or thread.

### Extract, Load, Transform
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

## Setup and Running Instructions
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

   - BigQuery Job User
   - Storage Object Creator
   - Storage Object User
   - Storage Object Viewer

    ```
7. Follow the instructions at this [link](https://cloud.google.com/iam/docs/keys-create-delete) to download the service account key created above and save it in<br> the root folder. You can rename it to a more friendly name. You will use it in step 9.
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
    SLACK_BOT_TOKEN=xxxxxx

    ```
10. Start the automation.
    ```bash

    python src/main.py

    ```

As the automation executes, it outputs log information to stdout that you can use to follow along.<br>
The downloaded messages and files are stored in `SlackDownloads` folder inside `Messages` and <br>
`Files` subfolders respectively.

### Adjustments
If you don't need to store your scraped data on Google Cloud Platform, comment out the following<br>
lines to disable the feature of saving to Google Cloud Storage:
```

#73, #74, #214, #223, #236, #245, #260, #261

```
In the setup instructions, skip the parts relevant to Google Cloud Platform.<br>
Set the boolean variable in line 4 of [src/main.py](src/main.py) to `False`

### License
Refer to the [LICENSE](LICENSE) file for terms of usage. 

### Contact
For inquiries, issues or source contributions, get in touch via `mmasabacollins9@gmail.com`.

&copy; Collins Mmasaba 2024