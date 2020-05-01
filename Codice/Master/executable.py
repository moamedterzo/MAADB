import json
import argparse
import relational_db_utils as r_du, mongo_db_utils as m_du
import mongo_primary_node as m_pa, mongo_secondary_node as m_sa


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--database-type', help='Type of database: M = mongoDB, S = SqlServer')
    parser.add_argument('--mongodb-nodetype', help='Type of node: P = primary, S = secondary')
    parser.add_argument('--mongodb-secondary-index', help='Secondary node index')

    args = parser.parse_args()

    #lettura file setting
    with open('resources/setting.json') as json_file:
        setting_data = json.load(json_file)

    if args.database_type == "S" or (args.database_type == "M" and args.mongodb_nodetype == "P"):

        # primary node
        selectedOperation = ""
        while selectedOperation != "-1":
            print("\nSelect operation to do:")
            print("\t1 to initialize database")
            print("\t2 to run Twitter analisys")
            print("\t-1 to exit")

            selectedOperation = input()

            # inizializzazione database
            if selectedOperation == "1":
                if args.database_type == "S":
                    r_du.initialise_database(setting_data['MariaDB'])
                elif args.database_type == "M":
                    m_du.initialise_cluster(setting_data['MongoDB']["Mongos_client"])

                print('Initialization completed!')

            # run twitter analisys
            elif selectedOperation == "2":
                if args.database_type == "S":
                    r_du.run_twitter_analisys(setting_data['MariaDB'])
                elif args.database_type == "M":
                    m_pa.run_twitter_analisys(setting_data)

    # se sono secondary, avvio il servizio che rimane in ascolto di eventuali richieste
    elif args.database_type == "M" and args.mongodb_nodetype == "S":

        secondary_index = int(args.mongodb_secondary_index)
        secondary_setting_data = setting_data['MongoDB']['SecondaryNodes'][secondary_index]

        m_sa.start_secondary_node(secondary_setting_data['ServicePort'],
                                  secondary_setting_data['Address'],
                                  secondary_setting_data['DBPort'])


