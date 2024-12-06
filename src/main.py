# TODO: implement the entrypoint
from slack_bot.slack_elt_automation import SlackScraper

def main():
    saving_to_cloud_storage = True
    app = SlackScraper(saving_to_cloud=saving_to_cloud_storage)

    print('Starting automation...', end='\n')
    app.start()
    print('Finished successfully.')