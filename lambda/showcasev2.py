import boto3
import os
import urllib.parse
import json
import uuid
from trp import Document
import base64
import pymysql
from datetime import datetime

db_host = "texttract.ccri4role0i0.ap-southeast-1.rds.amazonaws.com"
name = "admin"
pas = "V1B1Cloud"
db_name = "textract"

print('Loading function')
textract = boto3.client('textract')

def store_data(result):
    try:
        conn = pymysql.connect(host=db_host, user=name, passwd=pas, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
        logger.error(e)
        sys.exit()
    cursor = conn.cursor()
    tglnot = result['formData']['Tanggal Nota :']
    lok = result['formData']['Lokasi Cabang :']
    no = result['formData']['No. Nota :']
    nama = result['formData']['Kepada Yth :']

    for item in result['tableData']:
        if item['Kode Barang'] is not None:
           # sql = "INSERT INTO nota_penjualan (no_nota, kepada_yth, tanggal_nota, lokasi, kode_barang, nama_barang, harga, jumlah, sub_total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
           # values = (no, nama, tglnot, lok, item['Kode Barang'], item['Nama Barang'], item['Harga'], item['Jumlah'], item['Sub Total'] )
           # cursor.execute(sql, values)
           # conn.commit()
           print(item['Kode Barang'])
        else:
            print("False")
        
#insert into `nota_penjualan` (no_nota, kepada_yth, tanggal_nota, lokasi, kode_barang, nama_barang, harga, jumlah, sub_total) value (10, "TEST", 13/08/2002, "JAKARTA", 100, "BURGER", 10000, 1, 100000)
def get_textract_data(imageBase64):
    print('Loading getTextractData')
    # Call Amazon Textract
    print(textract)
    
    response = textract.analyze_document(
        Document={
            'Bytes': base64.b64decode(imageBase64)
        },
        FeatureTypes=["TABLES", "FORMS"])
    return response


def process_document(doc):
    data = []
    form_dict = {}
    cell_len = 0

    # Parsing the response from trp
    for page in doc.pages:
        # Getting the form data
        for field in page.form.fields:
            form_dict[format(field.key)] = format(field.value)

        # Getting the table contents
        for table in page.tables:
            for r, row in enumerate(table.rows):
                cell_len = len(row.cells)
                for c, cell in enumerate(row.cells):
                    data.append(format(cell.text))

    final = [data[i * cell_len:(i + 1) * cell_len] for i in
             range((len(data) + cell_len - 1) // cell_len)]

    # Setting the table heading as keys
    keys = [k.strip() for k in final[0]]
    # Storing the table values in table_data
    table_data = [dict(zip(keys, values)) for values in final[1:]]
    # Generating the JSON
    result = {
        "formData": form_dict,
        "tableData": table_data
    }
    return result

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    eventBody = json.loads(json.dumps(event))['body']
    
    imageBase64 = json.loads(eventBody)['Image']
   
    response = get_textract_data(imageBase64)
    doc = Document(response)
    result = process_document(doc)
    store_data(result)
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }