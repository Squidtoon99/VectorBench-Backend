import requests
import traceback
from openai import OpenAI
import os 
import json
import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass 

TS_HOST = os.getenv('TS_HOST')
TS_PORT = os.getenv('TS_PORT', 5432)
TS_NAME = os.getenv('TS_DATABASE')
TS_USER = os.getenv('TS_USER')
TS_PASSWORD = os.getenv('TS_PASSWORD')

conn = psycopg2.connect(
        host=TS_HOST,
        port=TS_PORT,
        dbname=TS_NAME,
        user=TS_USER,
        password=TS_PASSWORD
    )
cursor = conn.cursor()


EODHD_API_KEY = os.getenv("EODHD_API_KEY", None)
OPENAI_API_KEY = os.getenv("YOUR_OPENAI_API_KEY", None)

openai_client = OpenAI(api_key=OPENAI_API_KEY)

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", None)

deepseek_client = OpenAI(api_key=os.getenv("DEEPSEEK_API_KEY", None), base_url="https://api.deepseek.com")

GPT_MODEL = "gpt-4o-2024-08-06"

REASON_MODEL = "deepseek-reasoner"


def get_articles(ticker, date):
    try:
        url = f"https://eodhd.com/api/news"
        params = {
            "api_token": EODHD_API_KEY,
            "s": ticker,
            "from": date,
            "to": date,
            "limit": 100
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching articles: {e}")
        traceback.print_exc()
        return []
    

def classify(date, ticker, articles, stock_del):
    if not articles or articles == []:
        articles_content = "No articles found."
    else:
        articles_content = "\n\n".join(
            f"Title: {a.get('title', 'No title')}\n"
            f"Date: {a.get('date', 'Unknown Date')}\n"
            f"Content: {a.get('content', 'No content')}\n" 
            for a in articles
        )


    truncated_string = articles_content[:29000]
    
    sys_msg = {
        "role": "system",
        "content": (
            "You are an expert financial analyst. Classify stock price anomalies into one of the following categories and explain your reasoning."
            ""
            "- **Categories**:"
            "  - Market-wide change"
            "  - Industry-specific change"
            "  - Company-specific change"
            ""
            "# Steps"
            "1. Analyze the stock price anomaly based on the provided data."
            "2. Determine which of the three categories the anomaly fits into by considering factors such as market trends, industry developments, and company-specific news or events."
            "3. Explain the rationale for your classification, mentioning the relevant factors and evidence that support your choice."
            ""
            "# Output Format"
            "- Start the response with the classification."
            "- Follow with one to two sentences explaining why this classification was chosen."
            ""
            "# Examples"
            ""
            "### Example 1"
            "**Input**: [Description of a stock price anomaly and relevant factors]"
            "**Output**:"
            "\"Market-wide change. The anomaly aligns with a recent economic report indicating a recession, affecting all stocks similarly.\""
            ""
            "### Example 2"
            "**Input**: [Description of a stock price anomaly and relevant factors]"
            "**Output**:"
            "\"Industry-specific change. The anomaly is due to a recent technological breakthrough in the sector, affecting the entire industry.\""
            ""
            "### Example 3"
            "**Input**: [Description of a stock price anomaly and relevant factors]"
            "**Output**:"
            "\"Company-specific change. The anomaly results from the company's recent legal issues, directly impacting its stock price.\""
            ""
            "# Notes"
            "- Consider factors such as global economic indicators for market-wide changes."
            "- Look for industry trends or sector reports for industry-specific changes."
            "- Pay attention to company earnings reports, news releases, or executive changes for company-specific changes."
        )
    }

    usr_msg = {
        "role": "user",
        "content": (
            f"Date of anomaly: {date}\n"
            f"Stock price change: {stock_del}%\n\n"
            f"Here are the news articles from EODHD for {ticker} on this day:\n\n"
            f"{truncated_string}\n\n"
            "Question: What was the primary driver of this anomaly (market, industry, or company)? "
        )
    }

    try:
        response = openai_client.chat.completions.create(
            model=GPT_MODEL,
            messages=[sys_msg, usr_msg],
            max_tokens=100,
            temperature=1.00,
            frequency_penalty=0.24,
            presence_penalty=0,
            top_p=1.00,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "stock_category",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "required": ["category", "explanation"],
                        "properties": {
                            "category": {
                                "enum": ["Market", "Industry", "Company"],
                                "type": "string",
                                "description": "Category under which the stock price anomaly has happened."
                            },
                            "explanation": {
                                "type": "string",
                                "description": "Explanation of why content matches category."
                            }
                        },
                        "additionalProperties": False
                    }
                }
            }
        )
        
        return response
    
    except Exception as e:
        print(f"ChatGPT error: {e}")
        return "Error"

def deepseek(data):
    explanation_message = {
        "role": "assistant",
        "content": (
            "You are tasked with analyzing prediction performance data to provide feedback on improving the bot's accuracy. The points provided to you are isolated information where the bot failed to predict major flucuations in the stock price."
            "The data includes:\n"
            "Bot Score: A measure of prediction error that is based on the distance between the observed and predicted values, where 0 means perfect prediction.\n"
            "Classification: The scope of the issue causing the stock change ('Company', 'Industry', or 'Market').\n"
            "Explanation: Contextual information about the stock movement derived from market analysis or news articles for the current data point.\n\n"
            "Below is the data for analysis:"
        )
    }

    data_messages = [
        {
            "role": "user",
            "content": (
                f"Bot Score: {score}\n"
                f"Classification: {entry['category']}\n"
                f"Explanation: {entry['explanation']}\n"
            )
        }
        for score, entry in data
    ]

    response = deepseek_client.chat.completions.create(
        model="deepseek-reasoner",
        messages=[explanation_message] + data_messages,
    )

    reasoning_content = response.choices[0].message.reasoning_content
    content = response.choices[0].message.content
    
    return reasoning_content, content


def load_anomalies_from_db(ticker):
    query = "SELECT trade_time, distance FROM anomaly WHERE ticker = %s ORDER BY trade_time;"
    cursor.execute(query, (ticker,))
    results = cursor.fetchall()

    anomalies = [
        {"trade_time": row[0], "distance": row[1]} for row in results
    ]

    return anomalies


def update_anomalies_in_db(classification_data, ticker):
    rows = [
        (entry['classification'], entry['explaination'], entry['trade_time'], ticker)
        for entry in classification_data
    ]

    query = """
    UPDATE anomaly
    SET classification = %s, explaination = %s
    WHERE trade_time = %s AND ticker = %s;
    """

    cursor.executemany(query, rows)
    conn.commit()
    print(f"Updated {len(rows)} anomalies in the database.")

def main():
    # Define the ticker
    ticker = "TSLA"

    # Load anomalies from the database
    anomalies = load_anomalies_from_db(ticker)

    # Initialize lists for valid classification data
    classification_data = []
    valid_dels = []

    # Loop through each anomaly
    for anomaly in anomalies:
        trade_time = anomaly['trade_time']
        date = trade_time.date()  # Extract the date from trade_time
        stock_del = anomaly['distance']  # Get the anomaly distance

        # Fetch related articles for the anomaly date
        articles = get_articles(ticker, date)

        # Classify the anomaly
        try:
            classification_result = classify(date, ticker, articles, stock_del)

            # Extract classification and explanation
            parsed = json.loads(classification_result.choices[0].message.content)
            classification = parsed.get("category", "Unknown")
            explanation = parsed.get("explanation", "No explanation provided.")

            classification_data.append({
                "classification": classification,
                "explanation": explanation,
                "trade_time": trade_time
            })

            valid_dels.append(stock_del)
        except Exception as e:
            print(f"Error classifying anomaly at {trade_time}: {e}")
            classification_data.append({
                "classification": "Error",
                "explanation": "Classification failed.",
                "trade_time": trade_time
            })
            valid_dels.append(stock_del)

    # Pass only classification and explanation into deepseek
    try:
        feedback = deepseek(valid_dels, classification_data)
        print("DeepSeek Feedback:", feedback)
    except Exception as e:
        print(f"Error during DeepSeek feedback generation: {e}")

    # Log classification results for debugging
    for entry in classification_data:
        print(f"Classification: {entry['classification']}")
        print(f"Explanation: {entry['explanation']}")
        print("---")



if __name__ == '__main__':
    main()
