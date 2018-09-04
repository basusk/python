from twilio.rest import TwilioRestClient

account_sid = "AC2c9b62ab0ec3932425a521dde185103c"
auth_token = "0f19f1be91fdb3e4a10a1bdfdd2ab913"
client = TwilioRestClient(account_sid, auth_token)

message = client.sms.messages.create(
    body="Happy Birthday Kasturi",
    to="+16503039293",
    from_="+16503037035")
print (message.sid)
