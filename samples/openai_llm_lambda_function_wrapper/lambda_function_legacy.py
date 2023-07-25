import json
import openai
import os

def lambda_handler(event, context):
    # 200 is the HTTP status code for "ok".
    status_code = 200

    # The return value will contain an array of arrays (one inner array per input row).
    array_of_rows_to_return = [ ]

    # From the input parameter named "event", get the body, which contains
    # the input rows.
    event_body = event["body"]
    openai.api_key = os.environ['open_ai_key']
    
    # Convert the input from a JSON string into a JSON object.
    payload = json.loads(event_body)
    # This is basically an array of arrays. The inner array contains the
    # row number, and a value for each parameter passed to the function.
    rows = payload["data"]

    # For each input row in the JSON object...
    for row in rows:
        row_number = row[0]
        prompt = row[1]

        openai_response = openai.Completion.create(
            model="text-davinci-003",
            prompt=prompt,
            temperature=0.3,
            max_tokens=60,
            top_p=1.0,
            frequency_penalty=0.0,
            presence_penalty=0.0
        )
    
        # Compose the output based on the input. This simple example
        # merely echoes the input by collecting the values into an array that
        # will be treated as a single VARIANT value.
        query = openai_response['choices'][0]['text']
        query = query.split()
        query = " ".join(query)

        # Put the returned row number and the returned value into an array.
        row_to_return = [row_number,query]

        # ... and add that array to the main array.
        array_of_rows_to_return.append(row_to_return)

    json_compatible_string_to_return = json.dumps({"data" : array_of_rows_to_return})
    
    # try:
    # except Exception as err:
    #     # 400 implies some type of error.
    #     status_code = 400
    #     # Tell caller what this function could not handle.
    #     json_compatible_string_to_return = event_body

    # Return the return value and HTTP status code.
    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }
