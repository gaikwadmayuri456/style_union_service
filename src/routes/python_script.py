from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
from fastapi import FastAPI
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from uvicorn import run
from loguru import logger
import influxdb_client
from starlette.middleware.cors import CORSMiddleware
from os import getenv
from datetime import datetime
import psycopg2.extras

router = APIRouter()


def get_influx2_client():
    try:
        client = influxdb_client.InfluxDBClient(
            url=getenv("INFLUX_URL"),
            username=getenv("INFLUX_USERNAME"),
            password=getenv("INFLUX_PASSWORD"),
            token=getenv("INFLUX_TOKEN"),
            org=getenv("INFLUX_ORG")
        )
        # print("Hii")
        return client
    except HTTPException as e:
        raise e

    except Exception as e:
        logger.debug(f"{e}")
        raise HTTPException(status_code=500, detail=f"{e}")


def move_to_postgres(df):
    username = "postgres"
    password = "password"
    database = "su_report_testing"
    table_name = "nagarbhavi"

    # Create an SQLAlchemy engine
    engine = create_engine(f'postgresql://{username}:{password}@10.129.2.205:5432/{database}')

    # Store the DataFrame in the PostgreSQL table
    df.to_sql(table_name, con=engine, if_exists="append", index=False)

    try:
        rdb = engine.raw_connection()
        cursor = rdb.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Execute the SQL query
        query = f'''SELECT * FROM "{table_name}"'''
        cursor.execute(query)
        result = cursor.fetchall()
        for row in result:
            print(row)
        return result
    except Exception as e:
        print("Error:", e)
    finally:
        # Close the database connection
        rdb.close()

    # Close the database connection
    engine.dispose()


@router.get("/data")
def get_all_data(
):
    mydata = []
    try:
        client = get_influx2_client()
        start_time = datetime.now().date() - timedelta(days=9)
        end_time = datetime.now().date() - timedelta(days=2)
        q = f'''

          from(bucket: "data")
          |> range(start: {start_time.strftime("%Y-%m-%dT00:00:00Z")} , stop: {end_time.strftime("%Y-%m-%dT23:59:59Z")})
          |> filter(fn: (r) => r["_measurement"] == "energy")
          |> filter(fn: (r) => r["panel_no"] == "80660037_1")
          |> filter(fn: (r) => r["device_code"] == "MOD02")
          |> filter(fn: (r) => r["zone"] == "KWH")
          |> timeShift(duration: 330m, columns: [ "_time"])
          |> aggregateWindow(every:10m, fn: last, createEmpty: false)
          |> difference(nonNegative: false, columns: ["_value"])


        '''

        query_api = client.query_api()
        result = client.query_api().query(query=q)

        for x in result:
            for records in x:
                mydata.append(records.values)
        df = pd.DataFrame(mydata)
        res = move_to_postgres(df)
        # print(df)
        return res
    except Exception as e:
        # logger.critical(f'{e}')
        print("Exception is", e)
    # finally:
    #     # print(devicecode,zone,measurment)
    #     return mydata
