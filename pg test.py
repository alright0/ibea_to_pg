import select
import datetime

import psycopg2
import psycopg2.extensions
from settings import conn

curs = conn.cursor()

curs.execute("LISTEN ibea_cont;")

seconds_passed = 0
print("Waiting for notifications on channel 'ibea_cont'")
while 1:
    conn.commit()
    if select.select([conn], [], [], 5) == ([], [], []):
        seconds_passed += 5
        print("{} seconds passed without a notification...".format(seconds_passed))
    else:
        seconds_passed = 0
        conn.poll()
        conn.commit()
        while conn.notifies:
            notify = conn.notifies.pop()
            print(
                "Got NOTIFY:",
                datetime.datetime.now(),
                notify.pid,
                notify.channel,
                notify.payload,
            )
