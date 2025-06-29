import os
import requests
import pandas as pd
from appwrite.client import Client
from appwrite.services.databases import Databases
import io

def main(context):
    try:
        # Appwrite client
        client = Client()
        client.set_endpoint(os.environ["APPWRITE_ENDPOINT"])
        client.set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])
        client.set_key(os.environ["APPWRITE_API_KEY"])

        # #setup the database
        databases = Databases(client)
        database_id = os.environ["NIFTY_DATABASE_ID"]
        collection_id = os.environ["NIFTY_COLLECTION_ID"]

        #Send Request to the nifty website for the Nifty 500 stocks
        url = os.environ["NIFTY_URL"]
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        #Parse CSV content
        df = pd.read_csv(io.StringIO(response.text))
        df.columns = df.columns.str.strip()

        # Download Angel One token master
        token_url = os.environ["ANGEL_ONE_OPEN_SCRIPT_MASTER"]
        token_data = requests.get(token_url, headers=headers).json()
        nse_tokens = {
            item["name"].strip(): item["token"]
            for item in token_data
            if item.get("exch_seg") == "NSE" and item.get("name") and item.get("token")
        }

        # Clear existing documents (optional: only if overwrite is needed)
        try:
            docs = databases.delete_documents(database_id, collection_id)
        except Exception as e:
            print({"status": "error", "message": str(e)})

        inserted_count = 0

        for _,item in df.iterrows():
            symbol = item["Symbol"].strip()
            token = nse_tokens.get(symbol)
            doc = {
                "symbol": symbol,
                "company": item["Company Name"].strip(),
                "industry": item["Industry"].strip(),
                "isin": item["ISIN Code"].strip(),
                "token": token
            }

            try:
                databases.create_document(
                    database_id=database_id,
                    collection_id=collection_id,
                    document_id="unique()",
                    data=doc
                )
                inserted_count += 1
            except Exception as e:
                print(f"⚠️ Insert failed for {symbol}: {str(e)}")

        return {"success": True, "inserted": inserted_count}

    except Exception as e:
        return {"success": False, "error": str(e)}