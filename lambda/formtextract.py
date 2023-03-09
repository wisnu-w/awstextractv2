import boto3
import os
import urllib.parse
import json
import uuid
from trp import Document
import base64
from datetime import datetime

print('Loading function')
textract = boto3.client('textract')



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

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }