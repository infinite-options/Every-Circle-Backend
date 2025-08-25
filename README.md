# Every Circle Backend

Zappa Base URL: https://o7t5ikn907.execute-api.us-west-1.amazonaws.com/dev

# README: if conn error make sure password is set properly in RDS PASSWORD section

# README: Debug Mode may need to be set to False when deploying live (although it seems to be working through Zappa)

# README: if there are errors, make sure you have all requirements are loaded

# pip3 install -r requirements.txt

Endpoints go live when code is pushed to MASTER

To run the SEARCH Endpoint in Postman run either:
http://127.0.0.1:5000/api/search/<profile_id>
https://ioec2testsspm.infiniteoptions.com/api/search/<profile_id>
