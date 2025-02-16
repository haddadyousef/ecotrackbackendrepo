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

# Data Models
class EmissionsBreakdown:
    def __init__(self):
        self.car = 0
        self.food = 0
        self.goods = 0
        self.energy = 0

    def to_dict(self):
        return {
            "car": self.car,
            "food": self.food,
            "goods": self.goods,
            "energy": self.energy
        }

    @classmethod
    def from_dict(cls, data):
        breakdown = cls()
        breakdown.car = data.get('car', 0)
        breakdown.food = data.get('food', 0)
        breakdown.goods = data.get('goods', 0)
        breakdown.energy = data.get('energy', 0)
        return breakdown

class DailyEntry:
    def __init__(self):
        self.breakdown = EmissionsBreakdown()
        self.total = 0
        self.date = datetime.datetime.utcnow().date().isoformat()

    def calculate_total(self):
        self.total = (self.breakdown.car + 
                     self.breakdown.food + 
                     self.breakdown.goods + 
                     self.breakdown.energy)

    def to_dict(self):
        return {
            "breakdown": self.breakdown.to_dict(),
            "total": self.total,
            "date": self.date
        }

    @classmethod
    def from_dict(cls, data):
        daily = cls()
        daily.breakdown = EmissionsBreakdown.from_dict(data.get('breakdown', {}))
        daily.total = data.get('total', 0)
        daily.date = data.get('date', datetime.datetime.utcnow().date().isoformat())
        return daily

class UserCalendar:
    def __init__(self):
        self.entries = {}  # Dictionary with dates as keys
        self.offset_grams = 0


    
    def get_or_create_entry(self, date_str: str) -> DailyEntry:
        if date_str not in self.entries:
            entry = DailyEntry()
            entry.date = date_str
            self.entries[date_str] = entry
        return self.entries[date_str]

    def get_weekly_total(self):
        today = datetime.datetime.utcnow().date()
        current_weekday = today.weekday()  # 0 is Monday, 6 is Sunday
        week_start = today - timedelta(days=current_weekday)  # This gets us back to Monday
        
        # Calculate total emissions from Monday up to today
        total = 0
        for i in range(current_weekday + 1):  # +1 to include today
            date = week_start + timedelta(days=i)
            date_str = date.isoformat()
            if date_str in self.entries:
                total += self.entries[date_str].total
        return total

    def get_previous_week_total(self, week_start_date: datetime.date):
        """Calculate total for a complete previous week"""
        total = 0
        for i in range(7):  # Always 7 days for previous weeks
            date = week_start_date + timedelta(days=i)
            date_str = date.isoformat()
            if date_str in self.entries:
                total += self.entries[date_str].total
        return total

    def to_dict(self):
        return {
            "entries": {date: entry.to_dict() for date, entry in self.entries.items()},
            "offset_grams": self.offset_grams
        }

    @classmethod
    def from_dict(cls, data):
        calendar = cls()
        entries_data = data.get('entries', {})
        calendar.entries = {
            date: DailyEntry.from_dict(entry_data) 
            for date, entry_data in entries_data.items()
        }
        calendar.offset_grams = data.get('offset_grams', 0)
        return calendar
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
            database = self.client.create_database_if_not_exists(database_name)
            
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
            self.ensure_containers_exist()
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
def get_current_date():
    return datetime.datetime.utcnow().date().isoformat()

def get_or_create_user(user_id: str) -> Dict:
    try:
        container = db_manager.get_container(scores_container_name)
        
        # Query for existing user
        query = "SELECT * FROM c WHERE c.userId = @userId"
        parameters = [{"name": "@userId", "value": user_id}]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        if items:
            return items[0]
        
        # Create new user document with calendar
        new_user = {
            'id': user_id,
            'userId': user_id,
            'calendar': UserCalendar().to_dict(),
            'carDetails': {},
            'lastUpdated': datetime.datetime.utcnow().isoformat()
        }
        
        container.upsert_item(new_user)
        return new_user
        
    except Exception as e:
        logging.error(f"Error in get_or_create_user: {str(e)}")
        raise

@app.function_name(name="GetUserEmissionsHistory") 
@app.route(route="emissions/history/{user_id}", methods=["GET"])
def get_date_range(days: int):
    end_date = datetime.datetime.utcnow().date()
    start_date = end_date - timedelta(days=days-1)
    return start_date, end_date
def get_user_emissions_history(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('user_id')
        days = int(req.params.get('days', 30))  # Default to 30 days of history
        
        if not user_id:
            return func.HttpResponse(
                json.dumps({"error": "User ID is required"}),
                status_code=400
            )
        
        user_doc = get_or_create_user(user_id)
        calendar = UserCalendar.from_dict(user_doc['calendar'])
        
        # Get date range
        start_date, end_date = get_date_range(days)
        
        # Filter entries within date range
        filtered_entries = {}
        current = start_date
        while current <= end_date:
            date_str = current.isoformat()
            entry = calendar.get_or_create_entry(date_str)
            filtered_entries[date_str] = entry.to_dict()
            current += timedelta(days=1)
        
        return func.HttpResponse(
            json.dumps({
                "userId": user_id,
                "entries": filtered_entries,
                "offset_grams": calendar.offset_grams
            }),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error fetching emissions history: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )

@app.function_name(name="UpdateEmissions")
@app.route(route="emissions/update", methods=["POST"])
def update_emissions(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('userId')
        car_emissions = req_body.get('carEmissions')
        date_str = req_body.get('date', get_current_date())
        
        if not all([user_id, car_emissions is not None]):
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields"}),
                status_code=400
            )
        
        user_doc = get_or_create_user(user_id)
        calendar = UserCalendar.from_dict(user_doc['calendar'])
        
        # Update car emissions for specified date
        daily_entry = calendar.get_or_create_entry(date_str)
        daily_entry.breakdown.car = car_emissions
        
        # Recalculate total
        daily_entry.calculate_total()
        
        # Update document
        user_doc['calendar'] = calendar.to_dict()
        user_doc['lastUpdated'] = datetime.datetime.utcnow().isoformat()
        
        container = db_manager.get_container(scores_container_name)
        container.upsert_item(user_doc)
        
        return func.HttpResponse(
            json.dumps({
                "message": "Emissions updated successfully",
                "dailyTotal": daily_entry.total,
                "weeklyTotal": calendar.get_weekly_total()
            }),
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error updating emissions: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )

@app.route(route="emissions/offsets", methods=["POST"])
@app.function_name(name="UpdateOffsets")
def update_offsets(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('userId')
        offset_amount = req_body.get('offsetGrams')
        
        if not all([user_id, offset_amount is not None]):
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields"}),
                status_code=400
            )
        
        user_doc = get_or_create_user(user_id)
        calendar = UserCalendar.from_dict(user_doc['calendar'])
        
        # Update offset amount
        calendar.offset_grams += offset_amount
        
        # Update document
        user_doc['calendar'] = calendar.to_dict()
        user_doc['lastUpdated'] = datetime.datetime.utcnow().isoformat()
        
        container = db_manager.get_container(scores_container_name)
        container.upsert_item(user_doc)
        
        return func.HttpResponse(
            json.dumps({
                "message": "Offsets updated successfully",
                "totalOffsets": calendar.offset_grams
            }),
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error updating offsets: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )
# Scheduled Tasks

@app.function_name(name="HourlyUpdate")
@app.schedule(schedule="0 0 * * * *", arg_name="timer", run_on_startup=False)
def hourly_update(timer: func.TimerRequest) -> None:
    try:
        container = db_manager.get_container(scores_container_name)
        current_date = get_current_date()
        
        query = "SELECT * FROM c"
        items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))

        for item in items:
            try:
                calendar = UserCalendar.from_dict(item.get('calendar', {}))
                daily_entry = calendar.get_or_create_entry(current_date)
                
                # Update passive emissions
                daily_entry.breakdown.food += 400  # Hourly food emissions
                daily_entry.breakdown.goods += 460  # Hourly goods emissions
                
                # Recalculate total
                daily_entry.calculate_total()
                
                # Update document
                item['calendar'] = calendar.to_dict()
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
        current_date = get_current_date()
        
        query = "SELECT * FROM c"
        items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))

        for item in items:
            try:
                calendar = UserCalendar.from_dict(item.get('calendar', {}))
                daily_entry = calendar.get_or_create_entry(current_date)
                
                # Calculate energy emissions based on driving time
                driving_hours = item.get('drivingHours', 0)
                non_driving_hours = max(0, 24 - driving_hours)
                energy_emissions = int(non_driving_hours * 600)
                
                # Update energy emissions for current day
                daily_entry.breakdown.energy = energy_emissions
                
                # Recalculate total
                daily_entry.calculate_total()
                
                # Update document
                item['calendar'] = calendar.to_dict()
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
        
        query = "SELECT * FROM c"
        items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))

        for item in items:
            try:
                # Reset driving hours
                item['drivingHours'] = 0
                item['lastUpdated'] = datetime.datetime.utcnow().isoformat()
                
                container.upsert_item(item)
                
            except Exception as e:
                logging.error(f"Error in daily reset for user {item.get('userId')}: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Error in daily reset: {str(e)}")

@app.route(route="leaderboard", methods=["GET"])
@app.function_name(name="GetLeaderboard")
def get_leaderboard(req: func.HttpRequest) -> func.HttpResponse:
    try:
        container = db_manager.get_container(scores_container_name)
        
        query = "SELECT c.userId, c.calendar FROM c"
        items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        
        # Calculate net scores for current week
        leaderboard_entries = []
        today = datetime.datetime.utcnow().date()
        current_weekday = today.weekday()
        week_start = today - timedelta(days=current_weekday)
        
        for item in items:
            calendar = UserCalendar.from_dict(item['calendar'])
            weekly_total = calendar.get_weekly_total()  # This now only counts from Monday to today
            net_score = weekly_total - calendar.offset_grams
            
            leaderboard_entries.append({
                "userId": item['userId'],
                "weeklyScore": weekly_total,
                "offsetGrams": calendar.offset_grams,
                "netScore": net_score,
                "daysCount": current_weekday + 1  # Number of days included in the total
            })
        
        # Sort by net score (lower is better)
        leaderboard_entries.sort(key=lambda x: x['netScore'])
        
        return func.HttpResponse(
            json.dumps({
                "leaderboard": leaderboard_entries,
                "totalUsers": len(leaderboard_entries),
                "weekStart": week_start.isoformat(),
                "lastUpdated": datetime.datetime.utcnow().isoformat(),
                "daysInWeek": current_weekday + 1
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

@app.route(route="userposition/{user_id}", methods=["GET"])
@app.function_name(name="GetUserPosition")
def calculate_current_position(user_id: str):
    container = db_manager.get_container(scores_container_name)
    query = "SELECT c.userId, c.calendar FROM c"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    
    # Calculate net scores for all users
    scores = []
    for item in items:
        calendar = UserCalendar.from_dict(item['calendar'])
        weekly_total = calendar.get_weekly_total()
        net_score = weekly_total - calendar.offset_grams
        scores.append((item['userId'], net_score))
    
    # Sort by net score (lower is better)
    scores.sort(key=lambda x: x[1])
    
    # Find position
    for i, (uid, _) in enumerate(scores):
        if uid == user_id:
            return i + 1, len(scores)
    
    return None, len(scores)
def get_user_position(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('user_id')
        if not user_id:
            return func.HttpResponse(
                json.dumps({"error": "User ID is required"}),
                status_code=400
            )
        
        position, total_users = calculate_current_position(user_id)
        
        if position is None:
            return func.HttpResponse(
                json.dumps({"error": "User not found"}),
                status_code=404
            )
        
        return func.HttpResponse(
            json.dumps({
                "position": position,
                "totalUsers": total_users,
                "percentile": ((total_users - position) / total_users) * 100
            }),
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error getting user position: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500
        )
