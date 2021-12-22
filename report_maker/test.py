import core

database=""
user=""
password=""
host=""
port=""

xlsx = core.Core(database, user, password,host, port)
xlsx.create_reports()

