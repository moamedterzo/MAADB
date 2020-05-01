START /b "" python executable.py --database-type=M --mongodb-nodetype=P
START CMD /C CALL python executable.py --database-type=M --mongodb-nodetype=S --mongodb-secondary-index=0
START CMD /C CALL python executable.py --database-type=M --mongodb-nodetype=S --mongodb-secondary-index=1