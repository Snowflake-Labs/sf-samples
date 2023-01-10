from flask import Flask, request, make_response
import boto3
import datetime
import uuid
import json
import os

app = Flask(__name__)

# set variables used later
cookie_id = "user"
stream_name = os.environ['STREAM_NAME']

# set up connection to firehose using default AWS config
session = boto3.session.Session()
impressionClient = session.client('firehose')

@app.route("/", methods=["GET"])
def log_request_and_return():
    # Obviously, cookies can be problematic for a variety of reasons.
    # Here, this is standing in for whatever forms of identity you may use.
    existing_cookie = request.cookies.get(cookie_id)
    cookie = existing_cookie if existing_cookie else str(uuid.uuid1())
    # Write data from HTTP headers plus cookie to Firehose stream
    response = impressionClient.put_record(
           DeliveryStreamName=stream_name,
           Record={
                'Data': json.dumps({
                    'event_time': datetime.datetime.now().isoformat(),
                    'cookie': cookie,
                    'args': request.args,
                    'headers': dict(request.headers),
                    'browser': request.user_agent.browser,
                    'platform': request.user_agent.platform,
                    'remote_addr': request.remote_addr,
                })
            }
        )
    
    # Return a 204 non content so we don't waste bytes over the wire
    resp = make_response('', 204)
    if (not existing_cookie):
        resp.set_cookie(cookie_id, cookie)
    return resp