import os
import re

def sanitize_table_name(filename):
    name = os.path.splitext(filename)[0]
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    return name

def sanitize_header_name(header):
    name = re.sub(r'[^a-zA-Z0-9_]', '_', header)
    return name