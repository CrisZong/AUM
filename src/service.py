import json
import os
from generate_sheet import autoPilot 
from env_setup import getPassword
def handler(event, context):
    try:
        service_password = os.environ['SERVICE_PASS']
    except KeyError:
        # path not yet set
        getPassword()
        service_password = os.environ['SERVICE_PASS']
    try:
        input_pass = json.loads(event["body"] or "{}").get("password","wrong")
        input_date = json.loads(event["body"] or "{}").get("date",None)
    except:
        input_pass = event["body"].get("password","wrong")
        input_date = event["body"].get("date",None)
    # TODO Check date format
    message,status_code = "Succesfully Updated",200
    if  input_pass != service_password:
        message = "Wrong Password"
        status_code = 403
    else:
        result = autoPilot('Results_for_test',input_date)
        print(result)
        if not result: 
            message = "Fail to update. Here might be why: 1. maybe we haven't uploaded today's file 2. we enter the wrong date(correct format: 1/1/21 rather than 01/01/21)"
            status_code = 500
    return {
        "statusCode": status_code,
        "headers": { "Content-Type": "application/json"},
        "body": message
    }