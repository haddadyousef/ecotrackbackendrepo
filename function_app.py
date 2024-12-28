# function_app.py
import azure.functions as func
import json
import datetime
import logging
from azure.cosmos import CosmosClient
import os

app = func.FunctionApp()

@app.function_name(name="UpdateWeeklyScore")
@app.route(route="updateweeklyscore", methods=["POST"])
def update_weekly_score(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('userId')
        weekly_score = req_body.get('weeklyScore')

        if not user_id or weekly_score is None:
            return func.HttpResponse(
                "Please pass userId and weeklyScore in the request body",
                status_code=400
            )

        # Initialize Cosmos DB client
        connection_string = os.environ["CosmosDBConnection"]
        client = CosmosClient.from_connection_string(connection_string)
        database = client.get_database_client("LeaderboardDB")
        container = database.get_container_client("Scores")

        # Get current date for the week number
        current_date = datetime.datetime.utcnow()
        week_number = current_date.isocalendar()[1]
        
        # Create or update user score document
        document = {
            'id': f"{user_id}_{week_number}",
            'userId': user_id,
            'weekNumber': week_number,
            'weeklyScore': weekly_score,
            'lastUpdated': current_date.isoformat()
        }

        container.upsert_item(document)
        
        return func.HttpResponse(
            json.dumps({
                "message": f"Score updated for user {user_id}",
                "score": weekly_score
            }),
            mimetype="application/json",
            status_code=200
        )
    except ValueError:
        return func.HttpResponse(
            "Invalid request body",
            status_code=400
        )
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.function_name(name="GetLeaderboard")
@app.route(route="leaderboard", methods=["GET"])

def get_leaderboard(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Initialize Cosmos DB client
        connection_string = os.environ["CosmosDBConnection"]
        client = CosmosClient.from_connection_string(connection_string)
        database = client.get_database_client("LeaderboardDB")
        container = database.get_container_client("Scores")

        # Get current week number
        current_week = datetime.datetime.utcnow().isocalendar()[1]

        # Query for current week's scores - Changed ORDER BY to ASC instead of DESC
        query = "SELECT c.userId, c.weeklyScore FROM c WHERE c.weekNumber = @week ORDER BY c.weeklyScore ASC"
        parameters = [{"name": "@week", "value": current_week}]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        return func.HttpResponse(
            json.dumps(items),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error fetching leaderboard: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.function_name(name="ResetDailyScores")
@app.schedule(schedule="0 0 0 * * *", arg_name="timer", run_on_startup=False)
def reset_daily_scores(timer: func.TimerRequest) -> None:
    try:
        utc_timestamp = datetime.datetime.utcnow().replace(
            tzinfo=datetime.timezone.utc).isoformat()
        
        # Initialize Cosmos DB client
        connection_string = os.environ["CosmosDBConnection"]
        client = CosmosClient.from_connection_string(connection_string)
        database = client.get_database_client("LeaderboardDB")
        container = database.get_container_client("Scores")

        # Get current week number
        current_week = datetime.datetime.utcnow().isocalendar()[1]

        # Query for current week's scores
        query = "SELECT * FROM c WHERE c.weekNumber = @week"
        parameters = [{"name": "@week", "value": current_week}]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        # Reset daily scores for each user
        for item in items:
            item['dailyScore'] = 0
            container.upsert_item(item)

        logging.info(f'Reset daily scores function completed at: {utc_timestamp}')
    except Exception as e:
        logging.error(f"Error resetting daily scores: {str(e)}")
