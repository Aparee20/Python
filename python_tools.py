from apscheduler.schedulers.blocking import BlockingScheduler

def some_job():
    print "Decorated job"

scheduler = BlockingScheduler()
scheduler.add_job(some_job,'cron', hour='0-23',minute='28')
scheduler.start()
