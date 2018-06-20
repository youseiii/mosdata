import psycopg2
from datetime import timedelta, date

CONN_POSTGRES = "host='localhost' dbname='db' user='postgres' password='postgres'"


def read_mvalidation_on_date(file_path, d_from=date.today() - timedelta(days=1)):
    """ function load list of rows of table mdata.mvalidation to csv file
    params: file_path - path of target file
            d_from - date for selecting rows (default = yesterday)"""

    # determine end point of date interval
    d_to = d_from + timedelta(days=+1)

    # set connection to db
    with psycopg2.connect(CONN_POSTGRES) as conn:
        with conn.cursor() as cur:
            query = 'SELECT * FROM mdata.mvalidation where validation_ts >= %s and validation_ts <= %s'

            query_bind = cur.mogrify(query, (d_from, d_to)).decode("utf-8")
            outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query_bind)
            with open(file_path, 'w') as f:
                cur.copy_expert(outputquery, f)
            print('{} rows were written to csv file'.format(cur.rowcount))


def update_mvalidation_on_date(file_path, d_from=date.today() - timedelta(days=1)):
    """ function delete rows of table mdata.mvalidation on selected date and then load csv file to table
    params: file_path - path of target file
            d_from - date for selecting rows (default = yesterday)"""

    d_to = d_from + timedelta(days=+1)

    with psycopg2.connect(CONN_POSTGRES) as conn:
        with conn.cursor() as cur:
            # !!! open file before deleting rows from table
            # because if file doesn't exist exception will be raised and rows will not be deleted
            with open(file_path, 'r') as f:
                # read first row for column names
                colnames = f.readline().split(',')

                # delete rows from yesterday
                query = "DELETE FROM mdata.mvalidation where validation_ts >=  %s and validation_ts <= %s"
                cur.execute(query, (d_from, d_to))
                print('{} rows were deleted from table'.format(cur.rowcount))

                # insert data from csv file
                cur.copy_from(f, 'mdata.mvalidation', columns=colnames, sep=',')
                print('{} rows were loaded from csv file'.format(cur.rowcount))


if __name__ == "__main__":
    read_mvalidation_on_date(file_path='yesterday.csv')
    update_mvalidation_on_date(file_path='yesterday.csv')
