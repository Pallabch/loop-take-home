import os
from dotenv import load_dotenv
from fastapi import FastAPI,BackgroundTasks
from utilities import database
import io
from fastapi.responses import StreamingResponse
from utilities import util
import pandas as pd
import string
import random

load_dotenv()
app = FastAPI()


def start_data_processing(report_id):
    compute = util.compute()
    res = compute.calculateWeeklyDayStatusForAllStores(compute.store_status)
    compute.saveDataToCSV(res, os.getenv('CACHE')+'/'+report_id+'.csv')

@app.get("/")
async def root():
    return {"message":"Hello World"}

@app.get("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    '''
    We Collect Data from the database and put it up for processing using a background task
    '''
    db = database.Database()
    if(not os.path.exists(os.getenv('FILE_MENU_HOURS'))):
        background_tasks.add_task(db.fetch_menu_hours,os.getenv('FILE_MENU_HOURS'))

    if(not os.path.exists(os.getenv('FILE_STORE_STATUS'))):
        background_tasks.add_task(db.fetch_store_status,os.getenv('FILE_STORE_STATUS')) 
    
    if(not os.path.exists(os.getenv('FILE_TIME_ZONES'))):
        background_tasks.add_task(db.fetch_time_zones,os.getenv('FILE_TIME_ZONES'))    

    # if os.path.exists(os.getenv('FILE_RESULT')):
    #     os.remove(os.getenv('FILE_RESULT'))
    report_id = ''.join(random.choices(string.ascii_uppercase +string.digits, k=7))
    background_tasks.add_task(start_data_processing,report_id)
    db.close_connection()
    return {"message": "The task is submitted for computation, report id is "+report_id}


@app.get("/get_report/{report_id}",status_code=200)
async def send_report(report_id):
    print(os.getenv('CACHE')+'/'+report_id+'.csv')
    if not os.path.exists(os.getenv('CACHE')+'/'+report_id+'.csv'):
        return {"status": "task is running or has not been triggered"}
    result = pd.read_csv(os.getenv('CACHE')+'/'+report_id+'.csv')
    fl_name = report_id + '.csv'
    headers = {"Content-Disposition": "attachment; filename="+fl_name}
    csv_bytes = result.to_csv(index=False).encode("utf-8")

    csv_file = io.BytesIO(csv_bytes)
    return StreamingResponse(
        iter(csv_file.readline, b""), media_type="text/csv", headers=headers)





