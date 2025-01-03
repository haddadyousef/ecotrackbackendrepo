import azure.functions as func
import json
import datetime
import logging
from azure.cosmos import CosmosClient, exceptions
import os
from datetime import timedelta
from typing import Optional, Dict, List
import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import time

app = func.FunctionApp()

# Configuration
database_name = "LeaderboardDB"
scores_container_name = "Scores"
historical_container_name = "HistoricalData"
users_container_name = "Users"

class DatabaseManager:
    def __init__(self):
        self.client = None
        self.database = None
        self.scores_container = None
        self.historical_container = None
        self.users_container = None
        self.initialize_client()

    def ensure_containers_exist(self):
        try:
            # Create database if it doesn't exist
            database = self.client.create_database_if_not_exists(database_name)
            
            # Create containers if they don't exist
            database.create_container_if_not_exists(
                id=scores_container_name,
                partition_key=PartitionKey(path="/userId")
            )
            database.create_container_if_not_exists(
                id=historical_container_name,
                partition_key=PartitionKey(path="/userId")
            )
            database.create_container_if_not_exists(
                id=users_container_name,
                partition_key=PartitionKey(path="/userId")
            )
        except Exception as e:
            logging.error(f"Error ensuring containers exist: {str(e)}")
            raise
    
    def initialize_client(self):
        try:
            connection_string = os.environ["CosmosDBConnection"]
            self.client = CosmosClient.from_connection_string(connection_string)
            self.database = self.client.get_database_client(database_name)
            self.ensure_containers_exist()  # Add this line
            self.scores_container = self.database.get_container_client(scores_container_name)
            self.historical_container = self.database.get_container_client(historical_container_name)
            self.users_container = self.database.get_container_client(users_container_name)
        except Exception as e:
            logging.error(f"Failed to initialize database connection: {str(e)}")
            raise

    def get_container(self, container_name: str):
        if container_name == scores_container_name:
            return self.scores_container
        elif container_name == historical_container_name:
            return self.historical_container
        elif container_name == users_container_name:
            return self.users_container
        else:
            raise ValueError(f"Unknown container name: {container_name}")

# Initialize database manager
db_manager = DatabaseManager()

# Helper Functions
def create_empty_emissions():
    return {
        "carEmissions": 0,
        "food": 0,
        "energy": 0,
        "goods": 0
    }

def calculate_weekly_score(daily_history, current_day_emissions, offset_grams=0):
    try:
        total = sum(sum(day.values()) for day in daily_history) + sum(current_day_emissions.values())
        return max(0, total - offset_grams)  # Ensure score doesn't go below 0
    except Exception as e:
        logging.error(f"Error calculating weekly score: {str(e)}")
        return 0

def get_or_create_user(user_id: str, current_week: int) -> Dict:
    try:
        container = db_manager.get_container(scores_container_name)
        
        # Query for existing user
        query = """
        SELECT * FROM c 
        WHERE c.userId = @userId 
        AND c.weekNumber = @week
        """
        parameters = [
            {"name": "@userId", "value": user_id},
            {"name": "@week", "value": current_week}
        ]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        if items:
            return items[0]
        
        # Create new user document
        new_user = {
            'id': f"{user_id}_{current_week}",
            'userId': user_id,
            'weekNumber': current_week,
            'weeklyScore': 0,
            'dailyHistory': [create_empty_emissions() for _ in range(7)],
            'currentDayEmissions': create_empty_emissions(),
            'offsetGrams': 0,
            'drivingHours': 0,
            'carDetails': {},
            'lastUpdated': datetime.datetime.utcnow().isoformat()
        }
        
        container.upsert_item(new_user)
        return new_user
    except Exception as e:
        logging.error(f"Error in get_or_create_user: {str(e)}")
        raise

def update_user_emissions(user_doc: Dict, emission_type: str, value: int) -> Dict:
    try:
        user_doc['currentDayEmissions'][emission_type] += value
        user_doc['weeklyScore'] = calculate_weekly_score(
            user_doc['dailyHistory'],
            user_doc['currentDayEmissions'],
            user_doc.get('offsetGrams', 0)
        )
        return user_doc
    except Exception as e:
        logging.error(f"Error updating user emissions: {str(e)}")
        raise
# Timer Triggered Functions
@app.function_name(name="HourlyUpdate")
@app.schedule(schedule="0 0 * * * *", arg_name="timer", run_on_startup=False)
def hourly_update(timer: func.TimerRequest) -> None:
    try:
        container = db_manager.get_container(scores_container_name)
        current_week = datetime.datetime.utcnow().isocalendar()[1]
        
        # Get all active users
        query = "SELECT * FROM c WHERE c.weekNumber = @week"
        parameters = [{"name": "@week", "value": current_week}]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        for item in items:
            try:
                # Update passive emissions
                item['currentDayEmissions']['food'] += 400
                item['currentDayEmissions']['goods'] += 460
                
                # Update weekly score
                item['weeklyScore'] = calculate_weekly_score(
                    item['dailyHistory'],
                    item['currentDayEmissions'],
                    item.get('offsetGrams', 0)
                )
                
                # Update timestamp
                item['lastUpdated'] = datetime.datetime.utcnow().isoformat()
                
                container.upsert_item(item)
            except Exception as e:
                logging.error(f"Error updating user {item.get('userId')}: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Error in hourly update: {str(e)}")

@app.function_name(name="EnergyUpdate")
@app.schedule(schedule="0 30 21 * * *", arg_name="timer", run_on_startup=False)
def energy_update(timer: func.TimerRequest) -> None:
    try:
        container = db_manager.get_container(scores_container_name)
        current_week = datetime.datetime.utcnow().isocalendar()[1]
        
        query = "SELECT * FROM c WHERE c.weekNumber = @week"
        parameters = [{"name": "@week", "value": current_week}]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        for item in items:
            try:
                driving_hours = float(item.get('drivingHours', 0))
                non_driving_hours = max(0, 24 - driving_hours)  # Ensure non-negative
                energy_emissions = int(non_driving_hours * 600)
                
                item['currentDayEmissions']['energy'] = energy_emissions
                
                # Update weekly score
                item['weeklyScore'] = calculate_weekly_score(
                    item['dailyHistory'],
                    item['currentDayEmissions'],
                    item.get('offsetGrams', 0)
                )
                
                item['lastUpdated'] = datetime.datetime.utcnow().isoformat()
                
                container.upsert_item(item)
            except Exception as e:
                logging.error(f"Error updating energy for user {item.get('userId')}: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Error in energy update: {str(e)}")

@app.function_name(name="DailyReset")
@app.schedule(schedule="0 0 0 * * *", arg_name="timer", run_on_startup=False)
def daily_reset(timer: func.TimerRequest) -> None:
    try:
        container = db_manager.get_container(scores_container_name)
        historical_container = db_manager.get_container(historical_container_name)
        
        current_date = datetime.datetime.utcnow()
        current_week = current_date.isocalendar()[1]
        is_monday = current_date.weekday() == 0

        query = "SELECT * FROM c WHERE c.weekNumber = @week"
        parameters = [{"name": "@week", "value": current_week}]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        for item in items:
            try:
                # Archive current day's emissions
                item['dailyHistory'] = item['dailyHistory'][1:] + [item['currentDayEmissions']]
                
                if is_monday:
                    # Archive current week's data
                    archive_item = item.copy()
                    archive_item['id'] = f"{item['userId']}_{current_week}_archive"
                    historical_container.upsert_item(archive_item)
                    
                    # Create new week entry
                    new_item = {
                        'id': f"{item['userId']}_{current_week + 1}",
                        'userId': item['userId'],
                        'weekNumber': current_week + 1,
                        'weeklyScore': 0,
                        'dailyHistory': [create_empty_emissions() for _ in range(7)],
                        'currentDayEmissions': create_empty_emissions(),
                        'offsetGrams': item.get('offsetGrams', 0),
                        'drivingHours': 0,
                        'carDetails': item.get('carDetails', {}),
                        'lastUpdated': current_date.isoformat()
                    }
                    container.upsert_item(new_item)
                else:
                    # Reset current day emissions
                    item['currentDayEmissions'] = create_empty_emissions()
                    item['drivingHours'] = 0
                    item['lastUpdated'] = current_date.isoformat()
                    container.upsert_item(item)
                    
            except Exception as e:
                logging.error(f"Error processing daily reset for user {item.get('userId')}: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Error in daily reset: {str(e)}")

# HTTP Triggered Functions
@app.route(route="updatedriving", methods=["POST"])
@app.function_name(name="UpdateDriving")

def update_driving(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('userId')
        driving_hours = req_body.get('drivingHours')
        driving_emissions = req_body.get('drivingEmissions', 0)
        
        if not all([user_id, driving_hours is not None]):
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields"}),
                status_code=400
            )

        current_week = datetime.datetime.utcnow().isocalendar()[1]
        user_doc = get_or_create_user(user_id, current_week)
        
        # Update driving data
        user_doc['drivingHours'] = driving_hours
        user_doc['currentDayEmissions']['carEmissions'] += driving_emissions
        
        # Update weekly score
        user_doc['weeklyScore'] = calculate_weekly_score(
            user_doc['dailyHistory'],
            user_doc['currentDayEmissions'],
            user_doc.get('offsetGrams', 0)
        )
        
        # Update timestamp
        user_doc['lastUpdated'] = datetime.datetime.utcnow().isoformat()
        
        # Save updates
        container = db_manager.get_container(scores_container_name)
        container.upsert_item(user_doc)
        
        return func.HttpResponse(
            json.dumps({
                "message": "Driving data updated successfully",
                "currentScore": user_doc['weeklyScore']
            }),
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error updating driving data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )
@app.route(route="updatecarinfo", methods=["POST"])
@app.function_name(name="UpdateCarInfo")

def update_car_info(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('userId')
        car_year = req_body.get('carYear')
        car_make = req_body.get('carMake')
        car_model = req_body.get('carModel')
        
        if not all([user_id, car_year, car_make, car_model]):
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields"}),
                status_code=400
            )

        current_week = datetime.datetime.utcnow().isocalendar()[1]
        user_doc = get_or_create_user(user_id, current_week)
        
        user_doc['carDetails'] = {
            'year': car_year,
            'make': car_make,
            'model': car_model
        }
        
        container = db_manager.get_container(scores_container_name)
        container.upsert_item(user_doc)
        
        return func.HttpResponse(
            json.dumps({"message": "Car information updated successfully"}),
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error updating car information: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )
    
@app.route(route="updateoffsets", methods=["POST"])
@app.function_name(name="UpdateCarbonOffsets")
def update_carbon_offsets(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('userId')
        offset_grams = req_body.get('offsetGrams')
        
        if not all([user_id, offset_grams is not None]):
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields"}),
                status_code=400
            )

        current_week = datetime.datetime.utcnow().isocalendar()[1]
        user_doc = get_or_create_user(user_id, current_week)
        
        # Update offset grams
        current_offsets = user_doc.get('offsetGrams', 0)
        user_doc['offsetGrams'] = current_offsets + offset_grams
        
        # Update weekly score
        user_doc['weeklyScore'] = calculate_weekly_score(
            user_doc['dailyHistory'],
            user_doc['currentDayEmissions'],
            user_doc['offsetGrams']
        )
        
        container = db_manager.get_container(scores_container_name)
        container.upsert_item(user_doc)
        
        return func.HttpResponse(
            json.dumps({
                "message": "Carbon offsets updated successfully",
                "newScore": user_doc['weeklyScore'],
                "totalOffsets": user_doc['offsetGrams']
            }),
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error updating carbon offsets: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )

@app.function_name(name="GetLeaderboard")
@app.route(route="leaderboard", methods=["GET"])
def get_leaderboard(req: func.HttpRequest) -> func.HttpResponse:
    try:
        container = db_manager.get_container(scores_container_name)
        current_week = datetime.datetime.utcnow().isocalendar()[1]
        
        query = """
        SELECT 
            c.userId,
            c.weeklyScore,
            c.offsetGrams,
            c.carDetails
        FROM c
        WHERE c.weekNumber = @week
        """
        
        items = list(container.query_items(
            query=query,
            parameters=[{"name": "@week", "value": current_week}],
            enable_cross_partition_query=True
        ))
        
        # Calculate net scores and sort
        for item in items:
            item['netScore'] = item['weeklyScore'] - item.get('offsetGrams', 0)
        
        sorted_items = sorted(items, key=lambda x: x['netScore'])
        
        return func.HttpResponse(
            json.dumps({
                "leaderboard": sorted_items,
                "totalUsers": len(sorted_items),
                "lastUpdated": datetime.datetime.utcnow().isoformat()
            }),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error fetching leaderboard: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )

@app.route(route="userstats/{user_id}", methods=["GET"])
@app.function_name(name="GetUserStats") 
def get_user_stats(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('user_id')
        if not user_id:
            return func.HttpResponse(
                json.dumps({"error": "User ID is required"}),
                status_code=400
            )

        current_week = datetime.datetime.utcnow().isocalendar()[1]
        user_doc = get_or_create_user(user_id, current_week)
        
        # Format response to match frontend expectations exactly
        response_data = {
            "userId": user_id,  # Add this
            "weeklyScore": user_doc['weeklyScore'],
            "dailyHistory": user_doc['dailyHistory'],
            "currentDayEmissions": user_doc['currentDayEmissions'],  # Change from currentDay to currentDayEmissions
            "offsetGrams": user_doc.get('offsetGrams', 0),
            "carDetails": user_doc.get('carDetails', {}),
            "lastUpdated": user_doc.get('lastUpdated', datetime.datetime.utcnow().isoformat())
        }
        
        return func.HttpResponse(
            json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error fetching user stats: {str(e)}")
        # Return error response in the same format
        return func.HttpResponse(
            json.dumps({
                "userId": user_id,
                "weeklyScore": 0,
                "dailyHistory": [create_empty_emissions() for _ in range(7)],
                "currentDayEmissions": create_empty_emissions(),
                "offsetGrams": 0,
                "carDetails": {},
                "lastUpdated": datetime.datetime.utcnow().isoformat()
            }),
            status_code=200  # Return 200 with empty data instead of 500
        )

@app.function_name(name="GetUserPosition")
@app.route(route="userposition/{user_id}", methods=["GET"])
def get_user_position(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('user_id')
        if not user_id:
            return func.HttpResponse(
                json.dumps({"error": "User ID is required"}),
                status_code=400
            )

        # Get leaderboard data
        container = db_manager.get_container(scores_container_name)
        current_week = datetime.datetime.utcnow().isocalendar()[1]
        
        query = """
        SELECT 
            c.userId,
            c.weeklyScore,
            c.offsetGrams
        FROM c
        WHERE c.weekNumber = @week
        """
        
        items = list(container.query_items(
            query=query,
            parameters=[{"name": "@week", "value": current_week}],
            enable_cross_partition_query=True
        ))
        
        # Calculate net scores and sort
        for item in items:
            item['netScore'] = item['weeklyScore'] - item.get('offsetGrams', 0)
        
        sorted_items = sorted(items, key=lambda x: x['netScore'])
        
        # Find user's position
        position = next((i + 1 for i, item in enumerate(sorted_items) 
                        if item['userId'] == user_id), None)
        
        if position is None:
            return func.HttpResponse(
                json.dumps({"error": "User not found in leaderboard"}),
                status_code=404
            )
        
        return func.HttpResponse(
            json.dumps({
                "position": position,
                "totalUsers": len(sorted_items),
                "percentile": ((len(sorted_items) - position) / len(sorted_items)) * 100
            }),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error fetching user position: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )
@app.route(route="createuser/{user_id}", methods=["POST"])
@app.function_name(name="CreateUser")
def create_user(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('user_id')
        if not user_id:
            return func.HttpResponse(
                json.dumps({"error": "User ID is required"}),
                status_code=400
            )

        current_week = datetime.datetime.utcnow().isocalendar()[1]
        user_doc = get_or_create_user(user_id, current_week)
        
        return func.HttpResponse(
            json.dumps({
                "message": "User created successfully",
                "userId": user_id
            }),
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error creating user: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )
@app.route(route="updateweeklyscore", methods=["POST"])
@app.function_name(name="UpdateWeeklyScore")
def update_weekly_score(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('userId')
        weekly_score = req_body.get('weeklyScore')
        daily_history = req_body.get('dailyHistory')
        current_day_emissions = req_body.get('currentDayEmissions')
        
        if not all([user_id, weekly_score is not None, daily_history, current_day_emissions]):
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields"}),
                status_code=400
            )

        current_week = datetime.datetime.utcnow().isocalendar()[1]
        user_doc = get_or_create_user(user_id, current_week)
        
        # Update user data
        user_doc['weeklyScore'] = weekly_score
        user_doc['dailyHistory'] = daily_history
        user_doc['currentDayEmissions'] = current_day_emissions
        user_doc['lastUpdated'] = datetime.datetime.utcnow().isoformat()
        
        # Save updates
        container = db_manager.get_container(scores_container_name)
        container.upsert_item(user_doc)
        
        return func.HttpResponse(
            json.dumps({
                "message": "Weekly score updated successfully",
                "currentScore": user_doc['weeklyScore']
            }),
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error updating weekly score: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )
