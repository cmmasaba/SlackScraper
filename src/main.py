from slack_bot.slack_elt_automation import SlackScraper

def main():
    save_to_cloud_storage = True
    app = SlackScraper(saving_to_cloud=save_to_cloud_storage)

    print('Starting automation...', end='\n')
    app.start()
    print('Finished successfully.', end='\n')

if __name__ == '__main__':
    main()