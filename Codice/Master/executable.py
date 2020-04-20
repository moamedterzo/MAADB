import requests
import json
import argparse
import loadMongoDBResources, loadSQLResources

def main():
    url = "http://localhost:4000/jsonrpc"

    # Example echo method
    payload = {
        "method": "diocane",
        "params": ["cane!"],
        "jsonrpc": "2.0",
        "id": 0,
    }

    response = requests.post(url, json=payload).json()
    print(response)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--initialize-databases', help='Initialize databases with default data', default=False)
    parser.add_argument('--execute-operation', help='', default=False)
    args = parser.parse_args()

    if args.initialize_databases:
        print('Initializing mongoDB cluster')
        loadMongoDBResources.initialise_cluster()

        print('Initializing relational db')
        #loadSQLResources.initialise_database()

        print('Initialization finished')

    if args.execute_operation:
        print('stocazzo')
