# from flask import Flask, render_template


# app = Flask(__name__)

# @app.route("/hello")
# def hello():
#     return "Hello World"

# @app.route("/")    
# def newWorld():
#    return render_template('index.html')


# app.run(debug= True, port = 8111)




# import os 
# import sys
# import pathlib

# root_path = (
#     pathlib.Path(__file__)
# )

# print(__name__)
# print(__file__)
# print(pathlib.Path(__file__).parent.resolve())
# print("--------------------",sys.path)

"""


import socket               # Import socket module

s = socket.socket()         # Create a socket object
host = socket.gethostname() # Get local machine name
port = 12345                # Reserve a port for your service.

s.connect((host, port))
print(s.recv(1024))
s.close    
"""


import socket

host = socket.gethostname()
port = 5112 						#initiate port no above 1024 

server_socket  = socket.socket() 			# get instance

#The bind() function takes tuple as argument
server_socket.bind((host, port))

#configure how many client the server can listen simultaneously
server_socket.listen(2)
conn, address = server_socket.accept() 		# accept new connection
print("Connection from: " + str(address))

while True:
	# receive data stream. it won’t accept data packet greater than 1024 bytes
	data = conn.recv(1024).decode()
	if not data:
		#if data is not received break
		break
	print("from connected user: "+ str(data))
	data = input("->")
	
conn.close() 		#close the connection