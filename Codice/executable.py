import json
import argparse
import time
import relational_db_utils as r_du, mongo_db_utils as m_du
import mongo_primary_node as m_pa, mongo_secondary_node as m_sa


def main_code():
    # Si specificano i parametri in input che modificano il comportamento del sistema
    parser = argparse.ArgumentParser()

    parser.add_argument('--database-type', help='Type of database: M = mongoDB, S = MariaDB')
    parser.add_argument('--mongodb-nodetype', help='Type of node: P = primary, S = secondary')
    parser.add_argument('--mongodb-secondary-index', help='Secondary node index')
    parser.add_argument('--setting-file', help='Path of the setting file', default='resources/setting.json')

    args = parser.parse_args()

    # lettura del file di impostazioni
    m_du.PATH_SETTING_FILE = args.setting_file
    with open(args.setting_file) as json_file:
        setting_data = json.load(json_file)

    if args.database_type == "S" or (args.database_type == "M" and args.mongodb_nodetype == "P"):

        # nodo primario
        selected_operation = ""
        while selected_operation != "-1":
            print("\nSelect operation to do:")
            print("\t1 to initialize database")
            print("\t2 to run Tweets analysis")
            print("\t3 to create word cloud")

            if args.database_type == "S":
                print("\t4 to show resources stats")
                print("\t5 to show words count plot")

            print("\t-1 to exit")

            selected_operation = input()

            # inizio a tracciare il tempo di esecuzione
            start_time = time.time()

            if args.database_type == "S":
                # MariaDB
                maria_db_setting = setting_data['MariaDB']

                if selected_operation == "1":
                    r_du.initialise_database(maria_db_setting)
                elif selected_operation == "2":
                    r_du.run_twitter_analysis(maria_db_setting)
                elif selected_operation == "3":
                    r_du.create_clouds(maria_db_setting)
                elif selected_operation == "4":
                    r_du.get_resources_stats(maria_db_setting)
                elif selected_operation == "5":
                    r_du.plot_counts(maria_db_setting)

            elif args.database_type == "M":
                # MongoDB
                mongo_db_setting = setting_data['MongoDB']

                if selected_operation == "1":
                    m_du.initialise_cluster(mongo_db_setting)
                elif selected_operation == "2":
                    m_pa.run_twitter_analysis(mongo_db_setting)
                elif selected_operation == "3":
                    m_du.create_clouds(mongo_db_setting)

            # mostro il tempo trascorso per l'esecuzione del task
            end_time = time.time()
            show_time(end_time - start_time)

    elif args.database_type == "M" and args.mongodb_nodetype == "S":

        # se sono nodi secondari, avvio il servizio che rimane in ascolto
        secondary_index = int(args.mongodb_secondary_index)
        secondary_setting_data = setting_data['MongoDB']['SecondaryNodes'][secondary_index]

        m_sa.start_secondary_node(secondary_setting_data['ServicePort'],
                                  secondary_setting_data['Address'],
                                  secondary_setting_data['DBPort'])


def show_time(sec):
    mins = sec // 60
    sec = sec % 60
    hours = mins // 60
    mins = mins % 60
    print("Time Lapsed = {0}:{1}:{2}".format(int(hours), int(mins), sec))


if __name__ == "__main__":
    try:
        main_code()
    except Exception as e:
        print("An error has occourred:")
        print(e)
        print()
        input("Press a key to exit...")
        raise e
