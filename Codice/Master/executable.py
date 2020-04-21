import requests
import json
import argparse
import loadMongoDBResources, loadSQLResources, slave_application


def mongodb_mapreduce(setting_data):

    for slave_setting_data in setting_data['MongoDB']['SlaveNodes']:

        url = "http://" + slave_setting_data['Address'] + ":" + str(slave_setting_data['ServicePort']) + "/jsonrpc"

        payload = {
            "method": "diocane",
            "params": ["cane!"],
            "jsonrpc": "2.0",
            "id": 0,
        }

        response = requests.post(url, json=payload).json()
        print("Risposta dal nodo con porta: " + str(slave_setting_data['ServicePort']))
        print(response)
        print()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--database-type', help='Type of database: M = mongoDB, S = SqlServer')
    parser.add_argument('--mongodb-nodetype', help='Type of node: M = master, S = slave')
    parser.add_argument('--mongodb-slave-number', help='Number of slave node')

    args = parser.parse_args()

    #lettura file setting
    with open('setting.json') as json_file:
        setting_data = json.load(json_file)

    if args.database_type == "S" or (args.database_type == "M" and args.mongodb_nodetype == "M"):

        selectedOperation = ""
        while selectedOperation != "-1":
            print("\nSelect operation to do:")
            print("\t1 to initialize database")
            print("\t2 to run Twitter analisys")
            print("\t-1 to exit")

            selectedOperation = input()

            if selectedOperation == "1":

                #inizializzazione database
                if args.database_type == "S":
                    loadSQLResources.initialise_database()
                elif args.database_type == "M":
                    loadMongoDBResources.initialise_cluster()
                print('Initialization completed!')

            elif selectedOperation == "2":
                if args.database_type == "S":
                    print("Niente ancora")
                elif args.database_type == "M":
                    mongodb_mapreduce(setting_data)


    elif args.database_type == "M" and args.mongodb_nodetype == "S":

        #se sono slave, avvio il servizio che rimane in ascolto di eventuali richieste
        slave_number = int(args.mongodb_slave_number)
        slave_setting_data = setting_data['MongoDB']['SlaveNodes'][slave_number]

        slave_application.start_slave_application(slave_setting_data['ServicePort'],
                                                  slave_setting_data['Address'],
                                                  slave_setting_data['DBPort'])


