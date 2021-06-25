import os
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import csv
from io import StringIO


# project paths
project_root_dir = os.path.normpath(os.getcwd() + os.sep + os.pardir)
db_path = os.path.join(project_root_dir, "database")

# function for faster data insertion
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


#### create survey table


def create_survey_table():
    print()
    print("Creating survey table...")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS survey(
            question TEXT   NOT NULL,
            user_id  VARCHAR(255) NOT NULL,
            response TEXT   NOT NULL)
        """
    )
    conn.commit()

    # read the data
    survey_df = pd.read_csv(os.path.join(db_path, "survey.csv"))

    # insert the data into survey table
    survey_df.to_sql(
        "survey", engine, if_exists="append", index=False, method=psql_insert_copy
    )
    conn.commit()


# def insert_into_survey():
#     # insert command
#     insert_command = """
#         INSERT INTO survey (
#             user_id, question, response
#         )
#         VALUES (%s, %s, %s)
#         """
#     # read the data
#     survey_df = pd.read_csv(os.path.join(db_path, "survey.csv"))

#     # iterate through each row and insert into table
#     for i, row in survey_df.iterrows():
#         row_to_insert = (row["user_id"], row["question"], row["response"])
#         curr.execute(insert_command, row_to_insert)
#     conn.commit()


###### create quiz table
def create_quiz_table():
    print()
    print("Creating quiz table...")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS quiz(
            user_id VARCHAR(255) NOT NULL,
            style   TEXT    NOT NULL,
            fit     TEXT    NOT NULL,
            shape   TEXT    NOT NULL,
            color   TEXT    NOT NULL)
        """
    )
    conn.commit()

    # read the data
    quiz_df = pd.read_csv(os.path.join(db_path, "quiz.csv"))

    # insert the data into quiz table
    quiz_df.to_sql(
        "quiz", engine, if_exists="append", index=False, method=psql_insert_copy
    )
    conn.commit()


# def insert_into_quiz():
#     # insert command
#     insert_command = """
#         INSERT INTO quiz (
#             user_id, style, fit, shape, color
#         )
#         VALUES (%s, %s, %s, %s, %s)
#         """
#     # read the data
#     quiz_df = pd.read_csv(os.path.join(db_path, "quiz.csv"))

#     # iterate through each row and insert into table
#     for i, row in quiz_df.iterrows():
#         row_to_insert = (
#             row["user_id"],
#             row["style"],
#             row["fit"],
#             row["shape"],
#             row["color"],
#         )
#         curr.execute(insert_command, row_to_insert)
#     conn.commit()


#### create home_try_on table


def create_home_try_on_table():
    print()
    print("Creating Home Try On table...")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS home_try_on(
            user_id       VARCHAR(255)  NOT NULL,
            number_of_pairs     TEXT    NOT NULL,
            address             TEXT    NOT NULL)
        """
    )
    conn.commit()

    try_on_df = pd.read_csv(os.path.join(db_path, "home_try_on.csv"))

    # insert the data into home try on table
    try_on_df.to_sql(
        "home_try_on", engine, if_exists="append", index=False, method=psql_insert_copy
    )
    conn.commit()


# def insert_into_home_try_on():
#     # insert command
#     insert_command = """
#         INSERT INTO home_try_on (
#             user_id, number_of_pairs, address
#         )
#         VALUES (%s, %s, %s)
#         """
#     # read the data
#     try_on_df = pd.read_csv(os.path.join(db_path, "home_try_on.csv"))

#     # iterate through each row and insert into table
#     for i, row in try_on_df.iterrows():
#         row_to_insert = (
#             row["user_id"],
#             row["number_of_pairs"],
#             row["address"],
#         )
#         curr.execute(insert_command, row_to_insert)
#     conn.commit()


#### create purchase table


def create_purchase_table():
    print()
    print("Creating Purchase table...")
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS purchase(
            user_id    VARCHAR(255) NOT NULL,
            product_id      INTEGER NOT NULL,
            style           TEXT    NOT NULL,
            model_name      TEXT    NOT NULL,
            color           TEXT    NOT NULL,
            price           INTEGER NOT NULL)
        """
    )
    conn.commit()

    # read the data
    pur_df = pd.read_csv(os.path.join(db_path, "purchase.csv"))

    # insert the data into purchase table
    pur_df.to_sql(
        "purchase", engine, if_exists="append", index=False, method=psql_insert_copy
    )
    conn.commit()


# def insert_into_purchase():
#     # insert command
#     insert_command = """
#         INSERT INTO purchase (
#             user_id, product_id, style, model_name, color, price
#         )
#         VALUES (%s, %s, %s, %s, %s, %s)
#         """
#     # read the data
#     pur_df = pd.read_csv(os.path.join(db_path, "purchase.csv"))

#     # iterate through each row and insert into table
#     for i, row in pur_df.iterrows():
#         row_to_insert = (
#             row["user_id"],
#             row["product_id"],
#             row["style"],
#             row["model_name"],
#             row["color"],
#             row["price"],
#         )
#         curr.execute(insert_command, row_to_insert)
#     conn.commit()


if __name__ == "__main__":
    print()
    print("Creating Warby Parker Database...")
    # create and connect to the database
    try:
        # connect to the databse
        conn = psycopg2.connect(
            dbname="warby_parker",
            user="postgres",
            password=os.environ.get("DB_PASSWORD"),
            host="localhost",
        )
        # create a cursor
        curr = conn.cursor()

        # create SQLAlchemy engine
        engine = create_engine(
            "postgresql://postgres:"
            + os.environ["DB_PASSWORD"]
            + "@localhost:5432/warby_parker"
        )

        # create and insert into survey table
        create_survey_table()
        # insert_into_survey()

        # create and insert into quiz table
        create_quiz_table()
        # insert_into_quiz()

        # create and insert into home try on table
        create_home_try_on_table()
        # insert_into_home_try_on()

        # create and insert into purchase table
        create_purchase_table()
        # insert_into_purchase()

    except psycopg2.OperationalError as e:
        raise e
    else:
        print("connected")

    finally:
        # close cursor and connection
        curr.close()
        conn.close()
        print("Process completed.")
