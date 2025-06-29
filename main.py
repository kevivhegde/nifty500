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

        #Send Request to the nifty website for the Nifty 500 stocks
        url = os.environ["NIFTY_URL"]
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        #Parse CSV content
        df = pd.read_csv(io.StringIO(response.text))
        df.columns = df.columns.str.strip()

        #setup the database
        databases = Databases(client)
        database_id = os.environ["NIFTY_DATABASE_ID"]
        collection_id = os.environ["NIFTY_COLLECTION_ID"]

        # Clear existing documents (optional: only if overwrite is needed)
        try:
            docs = databases.delete_documents(database_id, collection_id)
        except Exception as e:
            print({"status": "error", "message": str(e)})

        for _, row in df.iterrows():
            doc = {
                "symbol": row["Symbol"].strip(),
                "company": row["Company Name"].strip(),
                "industry": row["Industry"].strip(),
                "isin": row["ISIN Code"].strip()
            }
            print(doc)
            databases.create_document(
                database_id,
                collection_id,
                document_id="unique()",
                data=doc
            )

        return {"success": True, "inserted": len(df)}

    except Exception as e:
        print({"status": "error", "message": str(e)})
