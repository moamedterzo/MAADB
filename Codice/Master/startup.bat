START CMD /C CALL python executable.py --database-type=M --mongodb-nodetype=S --mongodb-slave-number=0
START CMD /C CALL python executable.py --database-type=M --mongodb-nodetype=S --mongodb-slave-number=1
START /b "" python executable.py --database-type=M --mongodb-nodetype=M