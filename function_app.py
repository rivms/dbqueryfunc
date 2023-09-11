import azure.functions as func
import azure.durable_functions as df
import os
import logging
import uuid

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
    method = json_data.get("method", 0)


    instance_id = await client.start_new("dbqueryfunc", client_input={"start_key": start_key, 
                                                                      "stop_key": end_key,
                                                                      "activity_delay": activity_delay,
                                                                      "method": method})
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def dbqueryfunc(context: df.DurableOrchestrationContext):
    input = context.get_input()
    method = input["method"]

    if method==0:
        result1 = yield context.call_activity("retrieve_invoke", input)
    else:
        result1 = yield context.call_activity("stage_retrieve_invoke", input)
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



@myApp.activity_trigger(input_name="input")
def stage_retrieve_invoke(input):
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

    staging_folder = str(uuid.uuid4().hex)

    sub_query = f"SELECT * FROM LINEITEM WHERE L_ORDERKEY >= {start_key} AND L_ORDERKEY <= {stop_key}"  # 40000000

    staging_query_lines = [f"copy into @~/{staging_folder}",
        f"from ({sub_query})",
        "FILE_FORMAT=(TYPE=parquet, COMPRESSION=SNAPPY)",
        "MAX_FILE_SIZE = 268435456",
        "INCLUDE_QUERY_ID=true;"
    ]

    staging_query = " ".join(staging_query_lines)
    
    base_folder = f"/snow/staging"
    local_folder = f"{base_folder}/{staging_folder}"
    get_query = f"GET @~/{staging_folder} file://{local_folder};"

    if not os.path.exists(local_folder):
        os.mkdir(local_folder)
    
    rm_query = f"RM @~/{staging_folder}"

    cur = conn.cursor()

    conn.cursor().execute(staging_query)

    conn.cursor().execute(get_query)

    conn.cursor().execute(rm_query)


    return {"rows": -1, 
            "jsonSize": -1, 
            "stagingFolder": staging_folder,
            "local_path": local_folder, 
            "status": "OK"}


