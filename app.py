# Import Dependencies
import numpy as np
import datetime as dt
from datetime import timedelta
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

# Create connection to hawaii.sqlite
engine = create_engine("sqlite:////Users/meganconnelly/desktop/Resources/hawaii.sqlite")

# Reflect an existing database into a new model
Base = automap_base()

# Reflect the Tables
Base.prepare(engine, reflect=True)

# Save references to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Flask
app = Flask(__name__)

# Flask routes
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/startdate<br/>"
        f"/api/v1.0/startdate/enddate"
    )

def to_dict(results, keys):
    data = []
    for result in results:
        row_dict = {}
        for i in range(len(keys)):
            row_dict[keys[i]] = result[i]
        data.append(row_dict)
    return data

def query_db(query, keys):
    session = Session(engine)
    results = session.query(*query).all()
    session.close()
    return jsonify(to_dict(results, keys))

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return a list of all dates and precipitation data"""
    query = [Measurement.date, Measurement.prcp]
    keys = ["date", "prcp"]
    return query_db(query, keys)

@app.route("/api/v1.0/stations")
def stations():
    """Return a list of all stations names"""
    query = [Station.station, Station.name]
    keys = ["station", "name"]
    return query_db(query, keys)

@app.route("/api/v1.0/tobs")
def tobs():
    #find last date in database from Measurements
    session = Session(engine)
    last_day = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    query_date = (dt.datetime.strptime(last_day[0], "%Y-%m-%d") - dt.timedelta(days=365)).strftime('%Y-%m-%d')

    #find the most active station in database from Measurements
    active_station = session.query(Measurement.station,func.count(Measurement.station)).group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()
    session.close()

    """Return a list of dates and temperature of the most active station for the last year"""
    query = [Measurement.tobs]
    keys = ["tobs"]
    results = session.query(*query).filter(Measurement.date >= query_date, Measurement.station == active_station[0]).all()
    return jsonify(to_dict(results, keys))

@app.route("/api/v1.0/<start>")
def start_date(start):
    """Return a list of minimum, average and max temperature for a given date"""
    session = Session(engine)
    query = [Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    keys = ["date", "TMIN", "TAVG", "TMAX"]
    results = session.query(*query).filter(Measurement.date >= start).all()
    session.close()
    return jsonify(to_dict(results, keys))