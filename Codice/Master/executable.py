import requests
import json


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


def provesql():
    import pyodbc
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=DESKTOP-D62CNUA\SQLEXPRESS01;"
                          "Database=TwitterEmotions;"
                          "Trusted_Connection=yes;")

    cursor = cnxn.cursor()
    cursor.execute('SELECT * FROM Slang')

    for row in cursor:
        print('row = %r' % (row,))



if __name__ == "__main__":
    main()