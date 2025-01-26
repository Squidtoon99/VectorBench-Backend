from flask import Flask, request, jsonify
from flask_cors import CORS
from db import get_db, close_db
import pandas as pd 

app = Flask(__name__)

CORS(app)

def init_db():
    db = get_db()
    with open("schema.sql") as f:
        db.cursor().execute(f.read())
    db.commit()
    

with app.app_context():
    init_db()


@app.route("/api/v1/stocks", methods=["GET"])
def get_stocks():
    stocks = pd.read_csv("static/stock_info.csv", header=0)
    
    query = request.args.get("query")
    
    if query:
        stocks = stocks[stocks["Ticker"].str.contains(query, case=False)]
    
    return jsonify(stocks.to_dict(orient="records")[:100])

@app.route("/api/v1/stocks/<ticker>", methods=["GET"])
def get_stock(ticker):
    db = get_db()
    
    stock = db.cursor().execute("SELECT * FROM stocks WHERE ticker = ?", (ticker,)).fetchall()
    
    return jsonify(stock.to_dict(orient="records")[0])
@app.route("/api/v1/bot-overview", methods=["GET"])
def get_bot_overview():
    bot = request.args.get("bot")
    time_range = request.args.get("time_range")
    
    
    if bot is None:
        return jsonify({"error": "No bot specified"}), 400

    match time_range:
        case "1m":
            filter = "trade_time >= date('now', '-1 month')"
        case "6m":
            filter = "trade_time >= date('now', '-6 month')"
        case "1y":
            filter = "trade_time >= date('now', '-1 year')"
        case None | "":
            filter = "trade_time >= date('now', '-1 year')"
        case _:
            return jsonify({"error": "Invalid time range", "provided": time_range}), 400
        
    db = get_db()
    
    query = f"""
    SELECT * FROM stocks
    """
    
    cur = db.cursor()
    cur.execute(query, (bot,))
    
    
    result = cur.fetchall()
    return jsonify(result)

if __name__ == "__main__":
    app.run(port=8080, debug=True)