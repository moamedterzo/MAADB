import requests
import json
import argparse
import loadMongoDBResources, loadSQLResources, slave_application


from bson.py3compat import abc, string_type, PY3, text_type

class Code(str):
    """BSON's JavaScript code type.
    Raises :class:`TypeError` if `code` is not an instance of
    :class:`basestring` (:class:`str` in python 3) or `scope`
    is not ``None`` or an instance of :class:`dict`.
    Scope variables can be set by passing a dictionary as the `scope`
    argument or by using keyword arguments. If a variable is set as a
    keyword argument it will override any setting for that variable in
    the `scope` dictionary.
    :Parameters:
      - `code`: A string containing JavaScript code to be evaluated or another
        instance of Code. In the latter case, the scope of `code` becomes this
        Code's :attr:`scope`.
      - `scope` (optional): dictionary representing the scope in which
        `code` should be evaluated - a mapping from identifiers (as
        strings) to values. Defaults to ``None``. This is applied after any
        scope associated with a given `code` above.
      - `**kwargs` (optional): scope variables can also be passed as
        keyword arguments. These are applied after `scope` and `code`.
    .. versionchanged:: 3.4
      The default value for :attr:`scope` is ``None`` instead of ``{}``.
    """

    _type_marker = 13

    def __new__(cls, code, scope=None, **kwargs):
        if not isinstance(code, string_type):
            raise TypeError("code must be an "
                            "instance of %s" % (string_type.__name__))

        if not PY3 and isinstance(code, text_type):
            self = str.__new__(cls, code.encode('utf8'))
        else:
            self = str.__new__(cls, code)

        try:
            self.__scope = code.scope
        except AttributeError:
            self.__scope = None

        if scope is not None:
            if not isinstance(scope, abc.Mapping):
                raise TypeError("scope must be an instance of dict")
            if self.__scope is not None:
                self.__scope.update(scope)
            else:
                self.__scope = scope

        if kwargs:
            if self.__scope is not None:
                self.__scope.update(kwargs)
            else:
                self.__scope = kwargs

        return self

    @property
    def scope(self):
        """Scope dictionary for this instance or ``None``.
        """
        return self.__scope

    def __repr__(self):
        return "Code(%s, %r)" % (str.__repr__(self), self.__scope)

    def __eq__(self, other):
        if isinstance(other, Code):
            return (self.__scope, str(self)) == (other.__scope, str(other))
        return False

    __hash__ = None

    def __ne__(self, other):
        return not self == other


def try_mapreduce():


    import pymongo
    client = pymongo.MongoClient("localhost", 27019)
    db = client['TwitterEmotionsSlave']

    db.my_result.delete_many({})

    with open("prove\\map.js", 'r') as file:
        map = Code(file.read())

    with open("prove\\reduce.js", 'r') as file:
        reduce = Code(file.read())

    coll = db.Tweet

    print('starting...')
    coll.map_reduce(
        map,
        reduce, "my_result", limit=1000
    )

    print(coll)





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


    try_mapreduce()


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


