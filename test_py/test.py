# import re 

# a = "11:10:in(Tech Team) 13:47:out(Tech Team) 14:29:in(Tech Team) 14:33:out(Tech Team) 14:35:in(Tech Team) 17:43:in(Tech Team) 18:14:out(Tech Team) 18:18:in(Tech Team) 18:46:out(Tech Team) 18:47:in(Tech Team) 19:00:in(Tech Team) 20:20:out(Tech Team) 20:22:in(Tech Team) 20:31:out(Tech Team)"

# hours = re.compile("(\d{2}):\d{2}:in")

# minutes = re.compile("\d{2}:(\d{2}):in")

# # hours = re.compile("(\d{2})[\d{2}:in(Tech Team)]$",flags= re.M)


# # print(pattern.findall(a))
# print(hours.findall(a))
# print(minutes.findall(a))


# inTime = re.compile("(\d{2}:\d{2}):in")
# outTime = re.compile( "(\d{2}:\d{2}):out")
# print("inTime", inTime.findall(a))
# print("outTime",outTime.findall(a))

# bothInOut = re.compile("\(Tech Team\)")
# bothTimesList = bothInOut.split(a)

# print(bothTimesList)
# for i in bothTimesList:
#     temp = re.compile("(\d{2}:\d{2}):")
#     print(temp.split(i)[1:])



# import requests
# r = requests.get('https://www.python.org')
# print(r.status_code)


# x = requests.get('https://w3schools.com/python/demopage.htm')

# print(x.text)



# import jinja2
# from jinja2 import Template, FileSystemLoader

# environment = jinja2.Environment()
# template =  environment.from_string("Hello, {{name}}")
# print(template.render(name= "World"))

# t = Template("Hello, {% if name %}, {{name}}, {%else%} stranger{% endif %}!")
# print(t.render())
# print(t.render(name= "World2"))


# write_messages.py


'''
Program to create multiple files in different names with same content using Jinja2
'''

from jinja2 import Environment, FileSystemLoader

'''
Jinja uses a central object called the template Environment. Instances of this class is used to store the configuration and global objects,
and are used to load global templates from the filesystem or other locations.
'''

max_score = 100
test_name = "Python Challenge"
students = [
    {"name": "Sandrine",  "score": 100},
    {"name": "Gergeley", "score": 87},
    {"name": "Frieda", "score": 92},
]

environment = Environment(loader=FileSystemLoader("templates/"))
template = environment.get_template("message.txt")

for student in students:
    filename = f"message_{student['name'].lower()}.txt"
    content = template.render(
        student,
        max_score=max_score,
        test_name=test_name
    )
    with open(filename, mode="w", encoding="utf-8") as message:
        message.write(content)
        print(f"... wrote {filename}")
        

    with open(filename, mode= "r") as message:
        print(message.read())




















































