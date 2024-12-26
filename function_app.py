# function_app.py
import azure.functions as func
import json
import datetime
import logging

app = func.FunctionApp()

@app.function_name(name="UpdateWeeklyScore")
@app.route(route="updateweeklyscore", methods=["POST"])
def update_weekly_score(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('userId')
        weekly_score = req_body.get('weeklyScore')
        
        # Here we'll add database operations later
        # For now, just logging the received data
        logging.info(f'Received score update: User {user_id}, Score {weekly_score}')
        
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

@app.function_name(name="GetLeaderboard")
@app.route(route="leaderboard", methods=["GET"])
def get_leaderboard(req: func.HttpRequest) -> func.HttpResponse:
    # For now, returning dummy data
    # We'll replace this with database queries later
    leaderboard_data = [
        {"userId": "user1", "weeklyScore": 100},
        {"userId": "user2", "weeklyScore": 90}
    ]
    
    return func.HttpResponse(
        json.dumps(leaderboard_data),
        mimetype="application/json",
        status_code=200
    )

@app.function_name(name="ResetDailyScores")
@app.schedule(schedule="0 0 0 * * *", arg_name="timer", run_on_startup=False)
def reset_daily_scores(timer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    logging.info(f'Reset daily scores function ran at: {utc_timestamp}')
    # Here we'll add code to reset daily scores
