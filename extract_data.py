import argparse
from tqdm import tqdm
import json
import os
import gzip
from fastwarc.warc import ArchiveIterator
from io import BytesIO

def finance_related():
    pass

def load_warc(warc, max_retries=3):
    for i in range(max_retries):
        try:
            with fsspec.open(warc_file, 'rb') as f:
                return f.read()
        except Exception as e:
            if i == max_retries-1:
                raise Exception(f"Load failed: {warc_file}")
            print(f"Retying load {warc_file}")
            time.sleep(2 ** i + random.uniform(0, 1))

def valid_html(doc):
    if not doc.headers or not doc.http_headers:
        return False
    if doc.headers.get('WARC-Type') != 'response':
        return False
    content_type = str(doc.http_content_type or "")
    if content_type.startswith(('text/html', 'application/xhtml+xml')):
        return True
    return False

def decode_html(html):
    """Decodes the html if possible.
    First try to decode with utf-8, then try to detect the encoding."""
    try:
        html = bytes_to_str(html, 'utf-8')
    except Exception as e:
        encoding = detect_encoding(html)
        if encoding is None or encoding == 'utf-8':
            return
        try:
            html = bytes_to_str(html, encoding)
        except Exception as e:
            return
    return html

def process_warc(warc):
    doc_count = defaultdict(int)
    with time_guard(timeout=60*5):
        try:
            loaded = load_warc(warc)
            try:
                stream = BytesIO(f)
            except:
                print(f"Failed to read WARC stream: {warc_file}")
                return
            total_parsed = 0
            total_finance = 0
            for doc in tqdm(ArchiveIterator(stream)):
                try:
                    doc_count['records']+=1
                    if not valid_html(doc): continue
                    doc_count['html']+=1
                    html = doc.reader.read()
                except:
        except:
    
        