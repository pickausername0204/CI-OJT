import boto3,json

def lambda_handler(event, context):#main function
    target = get_file(event)#Tuple type
    target = list(target)
    target[0] = sentimental_analysis(target[0])
    upload_file(target[0],target[1])
    return 

def get_file(lambda_event):#Function to get a target file from s3
    s3 = boto3.client("s3")
    bucket_name = "tech-kadai-comprehend-sent-input"
    lambda_event = json.loads(lambda_event['Records'][0]['body'])
    object_name = lambda_event['Records'][0]['s3']['object']['key']
    file = s3.get_object(Bucket=bucket_name,Key=object_name)
    paragraph = str(file['Body'].read())
    return paragraph,object_name
    
def sentimental_analysis(text):#Function for emotion analysis using Amazon comprehend
    comprehend = boto3.client("comprehend")
    response = comprehend.detect_sentiment(Text = text,LanguageCode = "en")
    return(response)

def upload_file(text_analyzed,upload_file_name):#Function to upload a target file to s3
    s3 = boto3.resource("s3")
    bucket = "tech-kadai-comprehend-sent-output"
    key = upload_file_name
    obj = s3.Object(bucket,key)
    obj.put(Body=json.dumps(text_analyzed))
    return