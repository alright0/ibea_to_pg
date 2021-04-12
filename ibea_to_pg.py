import logging
import os
import threading
from pathlib import Path
from time import ctime, sleep
import psycopg2
import pandas as pd

from datetime import datetime
from settings import IBEA_ADDRESS, VM

script_path = Path(__file__).parents[0]
threads = []

ISOLEVEL = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT


logging.basicConfig(
    level=logging.DEBUG,
    filename=script_path / "app.csv",
    filemode="a",
    format="%(asctime)s; %(levelname)s; %(message)s;",
)

sleep_time = 15  # секунды


def connect():
    conn = psycopg2.connect(
        database=VM["database"],
        user=VM["user"],
        password=VM["password"],
        host=VM["host"],
        port=VM["port"],
    )

    cursor = conn.cursor()

    return conn, cursor


def readfile(line, conn, cursor):

    filename = "//{}/ibea/statistics/actual.csv".format(IBEA_ADDRESS.get(line))

    last_update = 0

    while True:

        try:

            if last_update != ctime(os.stat(filename)[-2]):

                df = pd.read_csv(filename, sep=";", index_col=0)  # запись файла в df

                # название линии по типу "LZ-01"
                if "ST" in line:
                    line_name = f"{line[:5]} ST"
                else:
                    line_name = line[:5]

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

                try:

                    cursor.execute(
                        f"""
                        INSERT INTO ibea_agregate (line, line_side, date_now, date_now_sys, job, start_time, last_part, total, rejected)
                        VALUES {line_name, line, date_now, date_now_sys, job, start_time, lastpart_time, total, rejected};
                    """
                    )

                    conn.commit()

                    print(f"{date_now} Добавлена линия {line}")

                # реализация переподключения
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:

                    logging.debug(
                        f"{line};{IBEA_ADDRESS.get(line)};{e}",
                    )

                    print(f"{date_now} {line} подключение закрыто. Переподключение...")

                    while cursor.closed:

                        conn, cursor = connect()

                        if not cursor.closed:
                            print(f"{date_now} {line} успешно переподключено")
                        else:
                            print(
                                f"{date_now} {line} не удалось переподключиться. Переподключение через {sleep_time} секунд(ы)"
                            )

                            sleep(sleep_time)

                except Exception as e:
                    print(f"{line} {e}")

        except Exception as e:
            e = str(e).replace("\n", " ")

            logging.debug(
                f"{line};{IBEA_ADDRESS.get(line)}; {e}",
            )

        sleep(sleep_time)


if __name__ == "__main__":

    connection, cursor = connect()

    # readfile("LZ-04", connection, cursor)

    for line in IBEA_ADDRESS:
        xthread = threading.Thread(
            target=readfile,
            name=line,
            args=(
                line,
                connection,
                cursor,
            ),
        )

        threads.append(xthread)
        xthread.start()
