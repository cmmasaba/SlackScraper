# Slack Scraper
In this project I implement an `ELT (Extract Load Transform)` automation that accesses a<br>
given Slack workspace and does the following:
- Downloads all messages in private and public channels
- Downloads threads related to every message and nests them inside the related message
- Downloads files and attachments related to each message or thread.

### Extract, Load, Transform
The automation extracts data from Slack using the Web API, it then loads the data to Google<br>
Cloud Storage. This includes the files and a JSONL file containing the messages. From GCS<br>
the data is loaded to Google BigQuery where it can be transformed and put to use.<br>
"ETL is a bad practice. ELT is a good practice..." ~ my manager. In case you're wondering why<br>
I did ELT instead of ETL.<br><br>
Slack Web API has a limit of 999 on the number of messages that can be downloaded with a single<br>
API call to `conversations.history`. For channels with large conversations or for workspaces<br>
that have been around for long that limit is not enough to extrac all messages.<br>
With a few modifications to this implementation, this automation is capable of downloading all<br>
messages in channels even if the number of messages is larger than 999. For example in the use<br>
case at our office it was able to download all messages going back to 2019 till date. This was<br>
about 100k+ messages, threads and related files.<br>
At that level of magnitude; scaling, runtime and robustness are important factors to put into<br>
consideration. Runtime because the automation could end up taking unreasonably long to download<br>
the content if inefficiently designed. Robustness because the automation needs to handle errors<br>
gracefully, record progress, and resume from the last checkpoint when interrupted. Other <br>
considerations include storage, network bandwidth, and compute resources, enter scaling.<br>
The Dockerfile included computes a small file (~133MB) that can be deployed to the cloud.<br>

## Installation and Running.
