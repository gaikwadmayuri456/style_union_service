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
import psycopg2


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

    # Read the DataFrame (assuming you have all_dataframes and 'df_Vizianagaram' inside it)

    # df = get_all_data()
    # Store the DataFrame in the PostgreSQL table
    df.to_sql(table_name, con=engine, if_exists="append", index=False)

    # Count the rows in the table
    try:
        connection = engine.connect()
        result = connection.execute(f'''SELECT * FROM "{table_name}"''')
        print(f"Number of rows in the table: {result}")
        for rslt in result:
            print(rslt)
    except Exception as e:
        print("Error:", e)
    finally:
        connection.close()

    # Close the database connection
    engine.dispose()


@router.get("/data")
def get_all_data(
        # data: GetInputData
):
    mydata = []
    try:
        client = get_influx2_client()
        start_time = datetime.now().date() - timedelta(days=7)
        end_time = datetime.now().date()
        q = f'''

          from(bucket: "data")
          |> range(start: {start_time.strftime("%Y-%m-%dT00:00:00Z")} , stop: {end_time.strftime("%Y-%m-%dT23:59:59Z")})
          |> filter(fn: (r) => r["_measurement"] == "energy")
          |> filter(fn: (r) => r["panel_no"] == "80660001_1")
          |> filter(fn: (r) => r["device_code"] == "MOD02")
          |> filter(fn: (r) => r["zone"] == "KWH")
          |> timeShift(duration: 330m, columns: [ "_time"])
          |> aggregateWindow(every: 1d, fn: last, createEmpty: false)
          |> difference(nonNegative: false, columns: ["_value"])


        '''

        query_api = client.query_api()
        result = client.query_api().query(query=q)

        for x in result:
            for records in x:
                mydata.append(records.values)
        df = pd.DataFrame(mydata)
        move_to_postgres(df)
        print(df)
        return df
    except Exception as e:
        # logger.critical(f'{e}')
        print("Exception is", e)
    # finally:
    #     # print(devicecode,zone,measurment)
    #     return mydata
