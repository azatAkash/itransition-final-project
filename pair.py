template = "template = {0!r}\nimport json\njs = 'console.log(' + json.dumps(template.format(template)) + ');'\nprint(js)\n"
import json
js = 'console.log(' + json.dumps(template.format(template)) + ');'
print(js)

