import logging
import os
import threading
from pathlib import Path
from time import ctime, sleep
import psycopg2
import pandas as pd

from datetime import datetime
from settings import IBEA_ADDRESS, conn


script_path = Path(__file__).parents[0]
threads = []

cursor = conn.cursor()

logging.basicConfig(
    level=logging.DEBUG,
    filename=script_path / "app.csv",
    filemode="a",
    format="%(asctime)s; %(levelname)s; %(message)s;",
)


def readfile(line, cursor):

    filename = "//{}/ibea/statistics/actual.csv".format(IBEA_ADDRESS.get(line))

    try:
        last_update = ctime(
            os.stat(filename)[-2]
        )  # дата и время последнего обновления файла
    except Exception as e:
        e = str(e).replace("\n", " ")
        logging.debug(
            f"{line};{IBEA_ADDRESS.get(line)}; {e}",
        )
        last_update = 0  # дата и время последнего обновления файла

    while True:

        try:

            if last_update != ctime(os.stat(filename)[-2]):

                # try:
                df = pd.read_csv(filename, sep=";", index_col=0)  # запись файла в df

                # название линии по типу "LZ-01"
                if "ST" in line:
                    line_name = f"{line[:4]} ST"
                else:
                    line_name = line[:4]

                # дата обновления файла
                date_now = df.loc["dated"].iloc[0]

                date_now_sys = str(datetime.today())[:-7]

                # номер заказа / название работы
                job = df.loc["Job"].iloc[0]

                # дата и время начала смены
                start_time = df.loc["Starttime"].iloc[0]

                # дата и время прохождения последней крышки
                lastpart_time = df.loc["last part"].iloc[0]

                # всего просмотрено
                total = pd.to_numeric(df.loc["Total"].iloc[0])

                # всего выброшено
                rejected = pd.to_numeric(df.loc["rejected"].iloc[0])

                # дата и время последнего обновления файла
                last_update = ctime(os.stat(filename)[-2])

                # запись в БД
                cursor.execute(
                    f"""
                    INSERT INTO ibea_agregate (line, line_side, date_now, date_now_sys, job, start_time, last_part, total, rejected)
                    VALUES {line_name, line, date_now, date_now_sys, job, start_time, lastpart_time, total, rejected};
                """
                )

                conn.commit()

                # для отслеживания из командной строки
                print(f"{date_now} Добавлена линия {line}")

        except Exception as e:
            e = str(e).replace("\n", " ")
            logging.debug(
                f"{line};{IBEA_ADDRESS.get(line)}; {e}",
            )

        sleep(15)


if __name__ == "__main__":

    for line in IBEA_ADDRESS:
        xthread = threading.Thread(
            target=readfile,
            name=line,
            args=(
                line,
                cursor,
            ),
        )

        threads.append(xthread)
        xthread.start()
