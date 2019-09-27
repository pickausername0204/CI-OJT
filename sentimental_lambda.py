"""TECH-4098 [追加課題] Lambdaの利用その2(S3/SQS/Comprehend)でLambdaに書いたコードです。
本システムでは tech-kadai-comprehend-sent-inputバケットにファイルをアップロードすると裏でSQS→lambda→Amazon comprehendがうごいて
tech-kadai-comprehend-sent-outputバケットに感情分析された結果のファイルが出力される内容となっています。
なお、最初にアプロードするファイルはJSONファイルを想定しています。
"""
import boto3,json

def lambda_handler(event, context):#メイン関数
    target = get_file(event)#返り値がタプル型
    target = list(target)
    target[0] = sentimental_analysis(target[0])
    upload_file(target[0],target[1])
    return 

def get_file(lambda_event):#s3から目的のファイルの中身と名前を抽出する関数
    s3 = boto3.client("s3")
    bucket_name = "tech-kadai-comprehend-sent-input"
    lambda_event = json.loads(lambda_event['Records'][0]['body'])
    object_name = lambda_event['Records'][0]['s3']['object']['key']
    file = s3.get_object(Bucket=bucket_name,Key=object_name)
    paragraph = str(file['Body'].read())
    return paragraph,object_name
    
def sentimental_analysis(text):#Amazon comprehendを呼び出して、感情分析をする関数
    comprehend = boto3.client("comprehend")
    response = comprehend.detect_sentiment(Text = text,LanguageCode = "en")
    return(response)

def upload_file(text_analyzed,upload_file_name):#s3に目的のファイルをアップロードする関数
    s3 = boto3.resource("s3")
    bucket = "tech-kadai-comprehend-sent-output"
    key = upload_file_name
    obj = s3.Object(bucket,key)
    obj.put(Body=json.dumps(text_analyzed))
    return
