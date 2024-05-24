import schedule
import time
import logging

def setup_schedule(main_function):
    """
    Set up the schedule to run the given main function at specified intervals.
    This function configures the scheduler to run 'main_function' every 4 hours.

    Args:
    - main_function (function): The main function to be scheduled.
    """
    schedule.every(4).hours.do(main_function)  
    print("Scheduler set up to run the main function every 4 hours.")
    logging.info("Scheduler set up to run the main function every 4 hours.")

    # Continuously run the schedule in a loop
    while True:
        schedule.run_pending()
        time.sleep(300)  # Sleep for 5 min between checks to minimize resource usage

