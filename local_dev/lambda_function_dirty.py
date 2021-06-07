import json
import pandas as pd
import urllib.parse
import boto3

topic = "arn:aws:sns:us-east-1:792659744406:nydig-bi-project"

def parse_transaction_file(json_obj, filename):
    print("in parse_transaction_file")
    #json_file = open(filename)
    #json_obj = json.load(json_file)
    #if not len(json_obj) == txCount: raise Exception("Transcation count mismatch")
    #print(len(json_obj))
    #print(json_obj[0]['vout'][3])
    #print(json_obj[0]['locktime'])
    #print(json_obj[595]['vout'][0])
    #print(len(json_obj[592]['vout']))
    #df = pd.DataFrame.from_dict(json_obj[595]['vin'])
    
    transaction_data        = []
    transaction_input_data  = []
    transaction_output_data = []
    transaction_count = 0
    
    for x in range(len(json_obj)):
        transaction_count += 1
        #Coinbase
        tx, tx_in, = {}, {}
        if x == 0:
            tx['tx_id']          = json_obj[0]['txid']
            tx['vjoinsplit']     = json_obj[0]['vjoinsplit']
            tx['blockhash']      = json_obj[0]['blockhash']
            tx['blockheight']    = json_obj[0]['blockheight']
            tx['blocktime']      = json_obj[0]['blocktime']
            tx['confirmations']  = json_obj[0]['confirmations']
            tx['isCoinBase']     = json_obj[0]['isCoinBase']
            tx['fees']           = None
            tx['locktime']       = json_obj[0]['locktime']
            tx['size']           = json_obj[0]['size']
            tx['time']           = json_obj[0]['time']
            tx['valueIn']        = None
            tx['valueOut']       = json_obj[0]['valueOut']
            tx['version']        = json_obj[0]['version']
            
            tx_in['tx_id']       = json_obj[0]['txid']
            tx_in['coinbase']    = json_obj[0]['vin'][0]['coinbase']
            tx_in['sequence']    = json_obj[0]['vin'][0]['sequence']
            tx_in['n']           = json_obj[0]['vin'][0]['n']
        else:
            tx['tx_id']          = json_obj[x]['txid']
            tx['vjoinsplit']     = json_obj[x]['vjoinsplit']
            tx['blockhash']      = json_obj[x]['blockhash']
            tx['blockheight']    = json_obj[x]['blockheight']
            tx['blocktime']      = json_obj[x]['blocktime']
            tx['confirmations']  = json_obj[x]['confirmations']
            tx['isCoinBase']     = False
            tx['fees']           = json_obj[x]['fees']
            tx['locktime']       = json_obj[x]['locktime']
            tx['size']           = json_obj[x]['size']
            tx['time']           = json_obj[x]['time']
            tx['valueIn']        = json_obj[x]['valueIn']
            tx['valueOut']       = json_obj[x]['valueOut']
            tx['version']        = json_obj[x]['version']
            
            tx_in['tx_id']            = json_obj[x]['txid']
            tx_in['in_tx_id']         = json_obj[x]['vin'][0]['txid']
            tx_in['vout']             = json_obj[x]['vin'][0]['vout']
            tx_in['coinbase']         = None
            tx_in['sequence']         = json_obj[x]['vin'][0]['sequence']
            tx_in['n']                = json_obj[x]['vin'][0]['n']
            tx_in['scriptSig_hex']    = json_obj[x]['vin'][0]['scriptSig']['hex']
            tx_in['scriptSig_asm']    = json_obj[x]['vin'][0]['scriptSig']['asm']
            tx_in['addr']             = json_obj[x]['vin'][0]['addr']
            tx_in['valueSat']         = json_obj[x]['vin'][0]['valueSat']
            tx_in['value']            = json_obj[x]['vin'][0]['value']
            tx_in['doubleSpentTxID']  = json_obj[x]['vin'][0]['doubleSpentTxID']
                                                   
        for y in range(len(json_obj[x]['vout'])):
            tx_out = {}
            tx_out['tx_id']                    = json_obj[x]['txid']
            tx_out['spentTxId']                = json_obj[x]['vout'][y]['spentTxId']
            tx_out['spentIndex']               = json_obj[x]['vout'][y]['spentIndex']
            tx_out['spentHeight']              = json_obj[x]['vout'][y]['spentHeight']
            tx_out['value']                    = json_obj[x]['vout'][y]['value']
            tx_out['n']                        = json_obj[x]['vout'][y]['n']
            tx_out['scriptPubKey_hex']         = json_obj[x]['vout'][y]['scriptPubKey']['hex']
            tx_out['scriptPubKey_asm']         = json_obj[x]['vout'][y]['scriptPubKey']['asm']
            tx_out['scriptPubKey_addresses']   = json_obj[x]['vout'][y]['scriptPubKey']['addresses']
            tx_out['scriptPubKey_type']        = json_obj[x]['vout'][y]['scriptPubKey']['type']
            transaction_output_data.append(tx_out)
            
            
        transaction_data.append(tx)
        transaction_input_data.append(tx_in)
    
    print(transaction_count)
    if transaction_count > 500:
        publish_sns_message(topic, "Transaction count > 500 for block: " + filename)
    
    tx_df     = pd.DataFrame(transaction_data)
    tx_in_df  = pd.DataFrame(transaction_input_data)
    tx_out_df = pd.DataFrame(transaction_output_data)
    
    #tx_df.to_csv("csv/" + "test_tx.csv", encoding = "utf-8", index = True)
    #tx_in_df.to_csv("csv/" + "test_tx_in.csv", encoding = "utf-8", index = True)
    #tx_out_df.to_csv("csv/" + "test_tx_out.csv", encoding = "utf-8", index = True)

    convert_to_csv(tx_df, filename)
    convert_to_csv(tx_in_df, filename + "_in")
    convert_to_csv(tx_out_df, filename + "_out")



def parse_block_file(json_obj, filename):
    print("in parse_block_file")
    #json_file = open(filename)
    #json_obj = json.load(json_file)
    txCount = (json_obj['txCount'])
    df = pd.DataFrame.from_dict(json_obj)

    convert_to_csv(df, filename)

    return df, txCount


def convert_to_csv(df, filename):
    print("In convert_to_csv")
    lambda_path = "/tmp/" + filename + ".csv"
    df.to_csv(lambda_path, encoding = "utf-8", index = False, sep='|', header=True)
    s3 = boto3.resource("s3")
    s3.meta.client.upload_file(lambda_path, "nydig-bi-csv-data", filename + ".csv")

def get_file(event):
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    filename = key.split('/', 1)[1][:-5]
    print(key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        return response['Body'], filename
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


def publish_sns_message(topic, message):
    client = boto3.client('sns')
    response = client.publish(
            TargetArn=topic,
            Message=json.dumps({'default': json.dumps(message)}),
            Subject='Blockchain Analysis Alert',
            MessageStructure='json')


def lambda_handler(event, context):

    response, filename = get_file(event)
    print("Got file: " + filename)

    json_file = response.read().decode()
    print("Decoded s3 response")
    json_obj = json.loads(json_file)
    print("Loaded json object")



    if "trans" in filename:
        print("calling parse_transaction_file now")
        parse_transaction_file(json_obj, filename)
        print("line159")

    if "block" in filename:
        parse_block_file(json_obj, filename)
        print("line155")






    return {
        'statusCode': 200,
        'body': json.dumps('Hello from test123123!')
    }
