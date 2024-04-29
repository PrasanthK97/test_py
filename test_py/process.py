import psutil, os 

"""
This module is very important in getting all necessary information about any processes running in the cpu.
The various details such as process id, cpu percentage the process consumes, memory info about the process, tcp connections of the process.
"""


p = psutil.Process(os.getpid())
print(p)
print(float(p.cpu_percent()))
print(psutil.cpu_count())
print(p.memory_full_info())
print(p.connections(kind= "tcp"))
print(p.num_threads)
print(p.num_fds)
# print((psutil.cpu_count), float(psutil.cpu_percent))

print("ffff" , os.path.dirname(__file__))
serviceName = "registration_services2"
buildNumFilePath = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + '/{}.version'.format(serviceName)
print(buildNumFilePath)
# with open(buildNumFilePath, 'r') as f:
#         content = f.read()
#         print(content)