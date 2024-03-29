import re


camel2snake_regex = re.compile(r'(?<!^)(?=[A-Z])')

def camel_to_snake(text):
    return camel2snake_regex.sub('_', text).lower()
