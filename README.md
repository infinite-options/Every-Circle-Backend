# Every Circle Backend

Endpoints go live when code is pushed to MASTER

To upload additional data to Elastic Cloud:
1. Modify mysql_to_elastic.py to specify which table should be queried and where it in Elastic Cloud should be uploaded
   NOTE:  For business and ratings, you just have to comment/uncomment code.  For other tables you may need to copy/modify code
2. Run python3 mysql_to_elastic.py ==> For loading data into the elastic cloud

To run the SEARCH Endpoint in Postman run either: 
  	http://127.0.0.1:5000/api/business_results?query=%22Chinese%20Food%22
	https://ioec2testsspm.infiniteoptions.com/api/business_results?query=%22Chinese%20Food%22