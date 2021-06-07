import json
import sys
import pandas as pd
import urllib.parse
import boto3

topic = "arn:aws:sns:us-east-1:792659744406:nydig-bi-project"

def parse_transaction_file(json_obj, filename):
    print("Starting to parse the transaction file: " + filename)

    transaction_data        = []
    transaction_input_data  = []
    transaction_output_data = []
    transaction_count = 0
    
    try: 
        for x in range(len(json_obj)):
            transaction_count += 1
            #Coinbase
            tx = {}
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
                for y in range(len(json_obj[x]['vin'])):
                    tx_in = {}
                    tx_in['tx_id']       = json_obj[0]['txid']
                    tx_in['coinbase']    = json_obj[0]['vin'][0]['coinbase']
                    tx_in['sequence']    = json_obj[0]['vin'][0]['sequence']
                    tx_in['n']           = json_obj[0]['vin'][0]['n']
    
                    tx_in['tx_id']            = json_obj[x]['txid']
                    tx_in['in_tx_id']         = json_obj[x]['vin'][y]['txid']
                    tx_in['vout']             = json_obj[x]['vin'][y]['vout']
                    tx_in['coinbase']         = None
                    tx_in['sequence']         = json_obj[x]['vin'][y]['sequence']
                    tx_in['n']                = json_obj[x]['vin'][y]['n']
                    tx_in['scriptSig_hex']    = json_obj[x]['vin'][y]['scriptSig']['hex']
                    tx_in['scriptSig_asm']    = json_obj[x]['vin'][y]['scriptSig']['asm']
                    tx_in['addr']             = json_obj[x]['vin'][y]['addr']
                    tx_in['valueSat']         = json_obj[x]['vin'][y]['valueSat']
                    tx_in['value']            = json_obj[x]['vin'][y]['value']
                    tx_in['doubleSpentTxID']  = json_obj[x]['vin'][y]['doubleSpentTxID']
                    transaction_input_data.append(tx_in)
                                                       
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

    except Exception as e:
        print(e)
        trace_back = sys.exc_info()[2]
        line = trace_back.tb_lineno
        print(line)
        print("Error while trying to parse the json object for transaction file: " + filename)
    
    print("Transaction count: " + str(transaction_count))
    
    tx_df     = pd.DataFrame(transaction_data)
    tx_in_df  = pd.DataFrame(transaction_input_data)
    tx_out_df = pd.DataFrame(transaction_output_data)

    if transaction_count != len(tx_df):
        publish_sns_message(topic, "Transaction count mismatch for block: " + filename)

    convert_to_csv(tx_df, filename)
    convert_to_csv(tx_in_df, filename + "_in")
    convert_to_csv(tx_out_df, filename + "_out")

    return(tx_df, tx_in_df, tx_out_df)

def transaction_analysis(tx_df, tx_in_df, tx_out_df):
    #Block
    block = tx_df['blockheight'][0]
    #Amount of transactions
    amount_of_transaction_ids = len(tx_df)
    amount_of_inputs          = len(tx_in_df)
    amount_of_outputs         = len(tx_out_df)
    #Amount of input transactions vs output transactions

    #Amount of bitcoin 
    btc_in                    = tx_df['valueIn'].sum()
    btc_out                   = tx_df['valueOut'].sum()
    btc_val_of_inputs         = tx_in_df['value'].sum()
    btc_val_of_outputs        = tx_out_df['value'].sum()
    #Total amount of fees as a percent of bitcoin
    sum_of_fees = tx_df['fees'].sum()

    print(amount_of_transaction_ids)
    print(amount_of_inputs)
    print(amount_of_outputs)
    print(btc_in)
    print(btc_out)
    print(btc_val_of_inputs)
    print(btc_val_of_outputs)
    print(sum_of_fees)

    message = "Amount of transactions: {} \n \
    Amount of transaction inputs: {} \n \
    Amount of transaction outputs: {} \n \
    Value of Bitcoin in: {} \n \
    Value of Bitcoin out: {} \n \
    Sum of fees : {} \n ".format(amount_of_transaction_ids, amount_of_inputs, amount_of_outputs, btc_in, btc_out, sum_of_fees)


    publish_sns_message(topic, message, subject = "Blockchain Analysis Alert for Block: " + str(block))

def parse_block_file(json_obj, filename):
    print("Starting to parse the block file: " + filename)
    try:
        txCount = (json_obj['txCount'])
        df = pd.DataFrame.from_dict(json_obj)
    except Exception as e:
        print(e)
        print("Error while processing the block file: " + filename)
    
    convert_to_csv(df, filename)

    return df, txCount


def convert_to_csv(df, filename):
    print("Converting " + filename + " to CSV.")
    lambda_path = "/tmp/" + filename + ".csv"
    df.to_csv(lambda_path, encoding = "utf-8", index = False, sep='|', header=True)
    print("Uploading " + filename + " to s3 bucket nydig-bi-csv-data.")
    s3 = boto3.resource("s3")
    s3.meta.client.upload_file(lambda_path, "nydig-bi-csv-data", filename + ".csv")

def get_file(event):
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    filename = key.split('/', 1)[1][:-5]
    print("S3 file key: " + key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        return response['Body'], filename
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}.'.format(key, bucket))
        raise e


def publish_sns_message(topic, message, subject = "NYDIG Blockchain Project"):
    try:
        client = boto3.client('sns')
        response = client.publish(
                TargetArn=topic,
                Message=message,#json.dumps({'default': json.dumps(message)}),
                Subject=subject)
                #MessageStructure='json')
    except Exception as e:
        print(e)
        print("Error while publishing SNS message to: " + topic)


def lambda_handler(event, context):

    print("Starting Lambda Execution")

    response, filename = get_file(event)
    print("Got file: " + filename)

    try:
        json_file = response.read().decode()
        print("Decoded s3 response")
        json_obj = json.loads(json_file)
        print("Loaded json object")
    except Exception as e:
        print(e)
        print("Error decoding the json file: " + filename)

    if "trans" in filename:
        try:
            tx_df, tx_in_df, tx_out_df = parse_transaction_file(json_obj, filename)
            print("Succesfully processed the transaction file: " + filename)
            transaction_analysis(tx_df, tx_in_df, tx_out_df)
            print("Succesfully analyzed the transaction file: " + filename)
        except Exception as e:
            print(e)
            trace_back = sys.exc_info()[2]
            line = trace_back.tb_lineno
            print(line)
            print("Error while trying to process the transaction file: " + filename)
    if "block" in filename:
        try:
            parse_block_file(json_obj, filename)
            print("Succesfully processed the block file: " + filename)
        except Exception as e:
            print(e)
            print("Error while trying to process the block file: " + filename)

    return {
        'statusCode': 200,
        'body': json.dumps('Ending Lambda')
    }
