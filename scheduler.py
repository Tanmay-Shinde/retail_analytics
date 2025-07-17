import schedule
import time
import change_data_capture


def run_cdc():
    change_data_capture.main()


schedule.every().day.at("00:00").do(run_cdc)

while True:
    schedule.run_pending()
    time.sleep(1)