import pandas as pd
from datetime import datetime, date, time, timedelta
import streamlit as st


# ==========================================================
# LOAD LOOKUP TABLES
# ==========================================================

LOOKUP_FOLDER = "lookup"


@st.cache_data
def load_airline_lookup():
    return pd.read_csv(
        f"{LOOKUP_FOLDER}/AIRLINE_LOOKUP.csv"
    )


@st.cache_data
def load_airport_lookup():
    return pd.read_csv(
        f"{LOOKUP_FOLDER}/AIRPORT_LOOKUP.csv"
    )


@st.cache_data
def load_route_lookup():
    return pd.read_csv(
        f"{LOOKUP_FOLDER}/ROUTE_LOOKUP.csv"
    )


@st.cache_data
def load_date_lookup():
    return pd.read_csv(
        f"{LOOKUP_FOLDER}/DATE_LOOKUP.csv"
    )


airline_lookup = load_airline_lookup()

airport_lookup = load_airport_lookup()

route_lookup = load_route_lookup()

date_lookup = load_date_lookup()


# ==========================================================
# HELPER FUNCTIONS
# ==========================================================

def get_departure_period(hour):
    """
    Convert departure hour into
    Morning / Afternoon / Evening / Night
    """

    if 5 <= hour < 12:
        return "Morning"

    elif 12 <= hour < 17:
        return "Afternoon"

    elif 17 <= hour < 21:
        return "Evening"

    else:
        return "Night"


def get_peak_hour(hour):
    """
    Peak hour indicator
    """

    return int(hour in [
        7,
        8,
        9,
        16,
        17,
        18,
        19
    ])


def safe_float(value):

    if pd.isna(value):
        return 0.0

    return float(value)


def safe_int(value):

    if pd.isna(value):
        return 0

    return int(value)


# ==========================================================
# DATE FEATURE GENERATION
# ==========================================================

def generate_date_features(
        departure_date,
        departure_time
):
    """
    Creates

    Year
    Quarter
    Month
    DayofMonth
    DayOfWeek
    WeekendIndicator
    SeasonIndicator
    DepartureHour
    DeparturePeriod
    PeakHourIndicator
    """

    dt = datetime.combine(
        departure_date,
        departure_time
    )

    year = dt.year

    quarter = (dt.month - 1) // 3 + 1

    month = dt.month

    day = dt.day

    weekday = dt.weekday() + 1

    departure_hour = dt.hour

    weekend = int(weekday in [6, 7])

    peak = get_peak_hour(
        departure_hour
    )

    departure_period = get_departure_period(
        departure_hour
    )

    # -----------------------------
    # Season
    # -----------------------------

    if month in [12, 1, 2]:
        season = "Winter"

    elif month in [3, 4, 5]:
        season = "Spring"

    elif month in [6, 7, 8]:
        season = "Summer"

    else:
        season = "Fall"

    return {

        "Year": year,

        "Quarter": quarter,

        "Month": month,

        "DayofMonth": day,

        "DayOfWeek": weekday,

        "DepartureHour": departure_hour,

        "PeakHourIndicator": peak,

        "WeekendIndicator": weekend,

        "SeasonIndicator": season,

        "DeparturePeriod": departure_period
    }


# ==========================================================
# ARRIVAL FEATURE GENERATION
# ==========================================================

def generate_arrival_features(
        departure_hour,
        scheduled_minutes
):
    """
    Calculate

    ArrivalHour

    ArrivalPeriod
    """

    arrival_hour = (
        departure_hour +
        scheduled_minutes // 60
    ) % 24

    arrival_period = get_departure_period(
        arrival_hour
    )

    return {

        "ArrivalHour": arrival_hour,

        "ArrivalPeriod": arrival_period

    }


# ==========================================================
# LOOKUP HELPERS
# ==========================================================

def airline_record(
        airline_code
):

    row = airline_lookup[
        airline_lookup["AirlineCode"] == airline_code
    ]

    if row.empty:
        raise Exception(
            f"Unknown Airline : {airline_code}"
        )

    return row.iloc[0]


def airport_record(
        airport_code
):

    row = airport_lookup[
        airport_lookup["AirportCode"] == airport_code
    ]

    if row.empty:
        raise Exception(
            f"Unknown Airport : {airport_code}"
        )

    return row.iloc[0]


def route_record(
        origin,
        destination
):

    row = route_lookup[
        (route_lookup["Origin"] == origin) &
        (route_lookup["Dest"] == destination)
    ]

    if row.empty:
        raise Exception(
            f"Route {origin}-{destination} not found."
        )

    return row.iloc[0]

# ==========================================================
# MAIN FEATURE BUILDER
# ==========================================================

def build_features(
    airline_code: str,
    origin_airport: str,
    destination_airport: str,
    departure_date,
    departure_time
):
    """
    Generates all model features required for prediction.

    Parameters
    ----------
    airline_code : str
        Airline code (e.g. AA, DL)

    origin_airport : str
        Origin airport code (e.g. ATL)

    destination_airport : str
        Destination airport code (e.g. LAX)

    departure_date : datetime.date

    departure_time : datetime.time

    Returns
    -------
    dict
        Dictionary ready for PipelineModel.transform()
    """

    # --------------------------------------------------
    # Lookup Records
    # --------------------------------------------------

    airline = airline_record(airline_code)

    origin = airport_record(origin_airport)

    destination = airport_record(destination_airport)

    route = route_record(
        origin_airport,
        destination_airport
    )

    # --------------------------------------------------
    # Date Features
    # --------------------------------------------------

    date_features = generate_date_features(
        departure_date,
        departure_time
    )

    # --------------------------------------------------
    # Arrival Features
    # --------------------------------------------------

    arrival_features = generate_arrival_features(
        departure_hour=date_features["DepartureHour"],
        scheduled_minutes=safe_int(
            route["ScheduledElapsedTimeMinutes"]
        )
    )

    # --------------------------------------------------
    # Final Feature Dictionary
    # --------------------------------------------------

    features = {

        # ============================
        # Date
        # ============================

        "Year":
            date_features["Year"],

        "Quarter":
            date_features["Quarter"],

        "Month":
            date_features["Month"],

        "DayofMonth":
            date_features["DayofMonth"],

        "DayOfWeek":
            date_features["DayOfWeek"],

        # ============================
        # Time
        # ============================

        "DepartureHour":
            date_features["DepartureHour"],

        "ArrivalHour":
            arrival_features["ArrivalHour"],

        "PeakHourIndicator":
            date_features["PeakHourIndicator"],

        "WeekendIndicator":
            date_features["WeekendIndicator"],

        # ============================
        # Route
        # ============================

        "Distance":
            safe_float(route["Distance"]),

        "ScheduledElapsedTimeMinutes":
            safe_float(route["ScheduledElapsedTimeMinutes"]),

        "CodeshareFlag":
            safe_int(route["CodeshareFlag"]),

        "IntraStateRouteFlag":
            safe_int(route["IntraStateRouteFlag"]),

        # ============================
        # Airline Statistics
        # ============================

        "AirlineReliabilityScore":
            safe_float(
                airline["AirlineReliabilityScore"]
            ),

        "AirlineFlightCount":
            safe_float(
                airline["AirlineFlightCount"]
            ),

        "AirlineMonthlyDelayRate":
            safe_float(
                airline["AirlineMonthlyDelayRate"]
            ),

        # ============================
        # Origin Airport
        # ============================

        "OriginAirportReliabilityScore":
            safe_float(
                origin["ReliabilityScore"]
            ),

        "OriginAirportFlightCount":
            safe_float(
                origin["FlightCount"]
            ),

        "OriginMonthlyDelayRate":
            safe_float(
                origin["MonthlyDelayRate"]
            ),

        # ============================
        # Destination Airport
        # ============================

        "DestAirportReliabilityScore":
            safe_float(
                destination["ReliabilityScore"]
            ),

        "DestAirportFlightCount":
            safe_float(
                destination["FlightCount"]
            ),

        "DestMonthlyDelayRate":
            safe_float(
                destination["MonthlyDelayRate"]
            ),

        # ============================
        # Route Statistics
        # ============================

        "RouteReliabilityScore":
            safe_float(
                route["RouteReliabilityScore"]
            ),

        "RouteFlightCount":
            safe_float(
                route["RouteFlightCount"]
            ),

        "RouteAvgDistance":
            safe_float(
                route["RouteAvgDistance"]
            ),

        "RouteAvgElapsedTime":
            safe_float(
                route["RouteAvgElapsedTime"]
            ),

        "RouteHistoricalDelayRate":
            safe_float(
                route["RouteHistoricalDelayRate"]
            ),

        # ============================
        # Original Categorical Features
        # ============================

        "SeasonIndicator":
            date_features["SeasonIndicator"],

        "DeparturePeriod":
            date_features["DeparturePeriod"],

        "ArrivalPeriod":
            arrival_features["ArrivalPeriod"],

        "DistanceCategory":
            route["DistanceCategory"]

    }

    return features
