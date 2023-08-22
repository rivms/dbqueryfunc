import azure.functions as func
import azure.durable_functions as df
import os
import logging

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# An HTTP-Triggered Function with a Durable Functions Client binding
# @myApp.route(route="orchestrators/{functionName}")
@myApp.route(route="orchestrators/dbqueryfunc")
@myApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    json_data = req.get_json()
    
    start_key = json_data["startKey"]
    end_key = json_data["stopKey"]
    activity_delay = json_data.get("activityDelay", 0)


    instance_id = await client.start_new("dbqueryfunc", client_input={"start_key": start_key, 
                                                                      "stop_key": end_key,
                                                                      "activity_delay": activity_delay})
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def dbqueryfunc(context: df.DurableOrchestrationContext):
    input = context.get_input()
    result1 = yield context.call_activity("retrieve_invoke", input)

    return result1

# Activity
@myApp.activity_trigger(input_name="input")
def retrieve_invoke(input):
    # Get data from Snowflake
    import snowflake.connector
    import time
    #import pandas
    
    start_key = input["start_key"]
    stop_key = input["stop_key"]
    activity_delay = input["activity_delay"]

    USER = os.environ["snowflake_user"]
    PASSWORD = os.environ["snowflake_password"]
    ACCOUNT = os.environ["snowflake_account"]
    WAREHOUSE = os.environ["snowflake_warehouse"]
    DATABASE = os.environ["snowflake_database"]
    SCHEMA = os.environ["snowflake_schema"]

    conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )

    cur = conn.cursor()


    sql = f"SELECT * FROM ITEM WHERE I_ITEM_SK >= {start_key} AND I_ITEM_SK <= {stop_key}"

    #sql = f"SELECT * FROM ITEM"


    cur.execute(sql)

    df = cur.fetch_pandas_all()
    json_result =  df.to_json()

    # Make Rest API call
    logging.info(f"Starting Delay of {activity_delay} seconds")
    if activity_delay>0:
        time.sleep(activity_delay)
    logging.info(f"Ending Delay")

    return {"rows": len(df), 
            "jsonSize": len(json_result), 
            "status": "OK"}
    # return "Hello " + city