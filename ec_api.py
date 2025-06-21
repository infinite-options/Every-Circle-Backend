# MANIFEST MY space_dev (PROPERTY MANAGEMENT) BACKEND PYTHON FILE
# https://l0h6a9zi1e.execute-api.us-west-1.amazonaws.com/dev/<enter_endpoint_details>
# https://qn4agnb0v9.execute-api.us-west-1.amazonaws.com/production/<enter_endpoint_details>


# To run program:  python3 ec_api.py

# README:  if conn error make sure password is set properly in RDS PASSWORD section
# README:  Debug Mode may need to be set to False when deploying live (although it seems to be working through Zappa)
# README:  if there are errors, make sure you have all requirements are loaded
# pip3 install -r requirements.txt


# SECTION 1:  IMPORT FILES AND FUNCTIONS
from data_ec import connect, uploadImage, s3
from users import UserInfo
from user_profile import Profile
from business import Business, Business_v2, Businesses
from business_v3 import Business_v3
from ratings import Ratings, Ratings_v2
from ratings_v3 import Ratings_v3
from search import Search, Search_v2
from lists import Lists
from middle_layer import Business_Results
from charges import Charges
from business_budget import Business_Budget
from business_revenue import BusinessRevenue
from feed import Feed
from category_list import CategoryList
from chatbot import ChatbotAPI
from user_connections import Connections
from aisearch import AISearch
from aisearch_v2 import AISearchTag, AISearchTag_v3
from tag_generator_api import TagGeneratorAPI
from sambanovasearch import AIDirectBusinessSearch
from user_profile_info import UserProfileInfo
from business_info import BusinessInfo
from transactions import Transactions
from user_path_connection import ConnectionsPath
from network_connection import NetworkPath
from tagSearch_direct import TagSearchDirect
from tagCategorySearch import TagCategorySearch
from profile_details import ProfileDetails
from profile_wish import ProfileWishInfo
from tagSearch_split import TagSplitSearch
from tagSearch_split_nlp import TagSplitNLPSearch
from alltagSearch_split import SplitSearch
# from middle_layer_opensearch import BusinessResults
from middle_layer_ec2opensearch import BusinessResults
from transaction_cost import TransactionCost
# from jwtToken import JwtToken
from functools import wraps
import jwt

# from flask import Request

import os
import boto3
import json
import pytz

import calendar
from dotenv import load_dotenv
from datetime import datetime, date, timedelta, timezone
from flask import Flask, request, render_template, url_for, redirect, jsonify, abort
from flask_restful import Resource, Api
from flask_cors import CORS
from flask_mail import Mail, Message  # used for email
from flask_jwt_extended import JWTManager, verify_jwt_in_request, get_jwt_identity, jwt_required, create_access_token 
from pytz import timezone as ptz  # Not sure what the difference is
from decimal import Decimal
from hashlib import sha512
from twilio.rest import Client
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
# from google_auth_oauthlib.flow import InstalledAppFlow
from urllib.parse import urlparse
from io import BytesIO
from dateutil.relativedelta import relativedelta
from dateutil.relativedelta import *
from math import ceil
from werkzeug.exceptions import BadRequest, NotFound
from werkzeug.datastructures import FileStorage  # For file handling
from werkzeug.datastructures import ImmutableMultiDict

# used for serializer email and error handling
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature

# from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
# from cryptography.hazmat.primitives.padding import PKCS7
# from cryptography.hazmat.backends import default_backend
import json
import base64

load_dotenv()

print(f"-------------------- New Program Run ( {os.getenv('RDS_DB')} ) --------------------")

# == Using Cryptography library for AES encryption ==

# load_dotenv()
# AES_SECRET_KEY = os.getenv('AES_SECRET_KEY')
# # print("AES Secret Key: ", AES_SECRET_KEY)
# AES_KEY = AES_SECRET_KEY.encode('utf-8')
# BLOCK_SIZE = int(os.getenv('BLOCK_SIZE'))
# # print("Block Size: ", BLOCK_SIZE)
# POSTMAN_SECRET = os.getenv('POSTMAN_SECRET')
# # print("POSTMAN_SECRET: ", POSTMAN_SECRET)


# Encrypt dictionary - Currently commented
# def encrypt_dict(data_dict):
#     try:
#         print("In encrypt_dict: ", data_dict)
#         # Convert dictionary to JSON string
#         json_data = json.dumps(data_dict).encode()

#         # Pad the JSON data
#         padder = PKCS7(BLOCK_SIZE * 8).padder()
#         padded_data = padder.update(json_data) + padder.finalize()

#         # Generate a random initialization vector (IV)
#         iv = os.urandom(BLOCK_SIZE)

#         # Create a new AES cipher
#         cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv), backend=default_backend())
#         encryptor = cipher.encryptor()

#         # Encrypt the padded data
#         encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

#         # Combine IV and encrypted data, then Base64 encode
#         encrypted_blob = base64.b64encode(iv + encrypted_data).decode()
#         return encrypted_blob
#     except Exception as e:
#         print(f"Encryption error: {e}")
#         return None

# Decrypt dictionary - Currently commented
# def decrypt_dict(encrypted_blob):
#     print("Actual decryption started")
#     try:
#         # Base64 decode the encrypted blob
#         encrypted_data = base64.b64decode(encrypted_blob)

#         # Extract the IV (first BLOCK_SIZE bytes) and the encrypted content
#         iv = encrypted_data[:BLOCK_SIZE]
#         encrypted_content = encrypted_data[BLOCK_SIZE:]

#         # Create a new AES cipher
#         cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv), backend=default_backend())
#         decryptor = cipher.decryptor()

#         # Decrypt the encrypted content
#         decrypted_padded_data = decryptor.update(encrypted_content) + decryptor.finalize()

#         # Unpad the decrypted content
#         unpadder = PKCS7(BLOCK_SIZE * 8).unpadder()
#         decrypted_data = unpadder.update(decrypted_padded_data) + unpadder.finalize()

#         # Convert the JSON string back to a dictionary
#         return json.loads(decrypted_data.decode())
#     except Exception as e:
#         print(f"Decryption error: {e}")
#         return None



# NEED to figure out where the NotFound or InternalServerError is displayed
# from werkzeug.exceptions import BadRequest, InternalServerError

#  NEED TO SOLVE THIS
# from NotificationHub import Notification
# from NotificationHub import NotificationHub

# BING API KEY
# Import Bing API key into bing_api_key.py

#  NEED TO SOLVE THIS
# from env_keys import BING_API_KEY, RDS_PW




app = Flask(__name__)
api = Api(app)

CORS(app)

# Set this to false when deploying to live application
app.config['DEBUG'] = True

# Setup the Flask-JWT-Extended extension
app.config["JWT_SECRET_KEY"] = os.getenv('JWT_SECRET_KEY')
app.config['JWT_TOKEN_LOCATION'] = ['headers'] 
app.config['JWT_HEADER_NAME'] = 'Authorization' 
app.config['JWT_HEADER_TYPE'] = 'Bearer'

jwtManager = JWTManager(app)


# --------------- Google Scopes and Credentials------------------
# SCOPES = "https://www.googleapis.com/auth/calendar"
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
# CLIENT_SECRET_FILE = "credentials.json"
# APPLICATION_NAME = "nitya-ayurveda"
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg'])
s = URLSafeTimedSerializer('thisisaverysecretkey')


# --------------- Stripe Variables ------------------
# STRIPE KEYS
stripe_public_test_key = os.getenv("stripe_public_test_key")
stripe_secret_test_key = os.getenv("stripe_secret_test_key")

stripe_public_live_key = os.getenv("stripe_public_live_key")
stripe_secret_live_key = os.getenv("stripe_secret_live_key")


# --------------- Twilio Setting ------------------
# Twilio's settings
# from twilio.rest import Client
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')



# --------------- Mail Variables ------------------
# Mail username and password loaded in .env file
app.config['MAIL_USERNAME'] = os.getenv('SUPPORT_EMAIL')
app.config['MAIL_PASSWORD'] = os.getenv('SUPPORT_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER')
# print("Sender: ", app.config['MAIL_DEFAULT_SENDER'])


# Setting for mydomain.com
app.config["MAIL_SERVER"] = "smtp.mydomain.com"
app.config["MAIL_PORT"] = 465

# Setting for gmail
# app.config['MAIL_SERVER'] = 'smtp.gmail.com'
# app.config['MAIL_PORT'] = 465

app.config["MAIL_USE_TLS"] = False
app.config["MAIL_USE_SSL"] = True


# Set this to false when deploying to live application
app.config["DEBUG"] = True
# app.config["DEBUG"] = False

# MAIL  -- This statement has to be below the Mail Variables
mail = Mail(app)




# --------------- Time Variables ------------------
# convert to UTC time zone when testing in local time zone
utc = pytz.utc

# # These statment return Day and Time in GMT
# def getToday(): return datetime.strftime(datetime.now(utc), "%Y-%m-%d")
# def getNow(): return datetime.strftime(datetime.now(utc), "%Y-%m-%d %H:%M:%S")

# # These statment return Day and Time in Local Time - Not sure about PST vs PDT
def getToday():
    return datetime.strftime(datetime.now(), "%Y-%m-%d")

def getNow():
    return datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")


# NOTIFICATIONS - NEED TO INCLUDE NOTIFICATION HUB FILE IN SAME DIRECTORY
# from NotificationHub import AzureNotification
# from NotificationHub import AzureNotificationHub
# from NotificationHub import Notification
# from NotificationHub import NotificationHub
# For Push notification
# isDebug = False
# NOTIFICATION_HUB_KEY = os.environ.get('NOTIFICATION_HUB_KEY')
# NOTIFICATION_HUB_NAME = os.environ.get('NOTIFICATION_HUB_NAME')
# NOTIFICATION_HUB_NAME = os.environ.get('NOTIFICATION_HUB_NAME'




# -- Send Email Endpoints start here -------------------------------------------------------------------------------

def sendEmail(recipient, subject, body):
    with app.app_context():
        print("In sendEmail: ", recipient, subject, body)
        sender="support@manifestmy.space"
        print("sender: ", sender)
        msg = Message(
            sender=sender,
            recipients=[recipient],
            subject=subject,
            body=body
        )
        print("sender: ", sender)
        # print("Email message: ", msg)
        mail.send(msg)
        # print("email sent")

# app.sendEmail = sendEmail

    
class SendEmail(Resource):
    def post(self):
        payload = request.get_json()
        print(payload)

        # Check if each field in the payload is not null
        if all(field is not None for field in payload.values()):
            sendEmail(payload["receiver"], payload["email_subject"], payload["email_body"])
            return "Email Sent"
        else:
            return "Some fields are missing in the payload", 400


class SendEmail_CLASS(Resource):
    def get(self):
        print("In Send EMail CRON get")
        try:
            conn = connect()

            recipient = "pmarathay@gmail.com"
            subject = "MySpace CRON Jobs Completed"
            body = "The Following CRON Jobs Ran:"
            # mail.send(msg)
            sendEmail(recipient, subject, body)

            return "Email Sent", 200

        except:
            raise BadRequest("Request failed, please try again later.")
        finally:
            print("exit SendEmail")


def SendEmail_CRON(self):
        print("In Send EMail CRON get")
        try:
            conn = connect()

            recipient = "pmarathay@gmail.com"
            subject = "MySpace CRON Jobs Completed"
            body = "The Following CRON Jobs Ran:"
            # mail.send(msg)
            sendEmail(recipient, subject, body)

            return "Email Sent", 200

        except:
            raise BadRequest("Request failed, please try again later.")
        finally:
            print("exit SendEmail")


def Send_Twilio_SMS(message, phone_number):
    # print("In Twilio: ", message, phone_number)
    items = {}
    numbers = phone_number
    message = message
    numbers = list(set(numbers.split(',')))
    # print("TWILIO_ACCOUNT_SID: ", TWILIO_ACCOUNT_SID)
    # print("TWILIO_AUTH_TOKEN: ", TWILIO_AUTH_TOKEN)
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    # print("Client Info: ", client)
    for destination in numbers:
        message = client.messages.create(
            body=message,
            from_='+19254815757',
            to="+1" + destination
        )
    items['code'] = 200
    items['Message'] = 'SMS sent successfully to the recipient'
    return items




class stripe_key(Resource):
    def get(self, desc):
        print(desc)
        if desc == "PMTEST":
            return {"publicKey": stripe_public_test_key}
        else:
            return {"publicKey": stripe_public_live_key}

class Refer(Resource):
    def post(self):
        print("In Refer POST")
        # current user_id, email or number of the person being referred
        payload = request.get_json()
        response = {}

        if 'profile_uid' not in payload:
            response['message'] = 'profile_uid is required to refer a friend'
            response['code'] = 400
            return response, 400
        
        if  'user_referred_email' not in payload and 'user_referred_number' not in payload:
            response['message'] = 'Either user_referred_email or user_referred_number is required'
            response['code'] = 400
            return response, 400
        
        try:
            with connect() as db:
                profile_exists_query = db.select('every_circle.profile', where={'profile_uid': payload['profile_uid']})
                if not profile_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404
                
                print(profile_exists_query)
                user_profile_details = profile_exists_query['result'][0]

                # user_profile_query = db.select('every_circle.profile', where={'profile_user_id': payload['user_uid']})
                # if not user_profile_query['result']:
                #     response['message'] = 'User exists in User table but does not exists in the profile table'
                #     response['code'] = 404
                #     return response, 404
                
                # print(user_profile_query)
                # user_profile_details = user_profile_query['result'][0]
            
            if 'message' in payload:
                message = payload['message']
                message = message + f" Please click on the link to sign up. https://everycircle.netlify.app?referral_id={payload['profile_uid']}"
            else:
                message = f"Hi, {user_profile_details['profile_first_name']} {user_profile_details['profile_last_name']} has referred you to Every-Circle.  Please click on the link to sign up. https://everycircle.netlify.app?referral_id={payload['user_uid']}"
            
            if 'user_referred_email' in payload and payload['user_referred_email']:
                # send email
                # print(payload['user_referred_email'], type(payload['user_referred_email']))
                recipient = payload['user_referred_email']
                subject = "Every-Circle referreal from a friend"
                body = message

                try:
                    print("Now about to send email")
                    sendEmail(recipient, subject, body)
                    response['Email Status'] = 'Sent'
                except:
                    response['Email Status'] = 'Failed'

            if 'user_referred_number' in payload and payload['user_referred_number']:
                # send SMS
                # message = message
                phone_number = payload['user_referred_number']

                try:
                    Send_Twilio_SMS(message, phone_number)
                    response['SMS Status'] = 'Sent'
                except:
                    response['SMS Status'] = "Failed"
            
            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            return response, 500



#  -- ACTUAL ENDPOINTS    -----------------------------------------

api.add_resource(stripe_key, "/stripe_key/<string:desc>")
api.add_resource(UserInfo, "/userinfo", "/userinfo/<string:user_id>")
api.add_resource(Profile, "/profile", "/profile/<string:uid>")
api.add_resource(Business, "/business", "/business/<string:uid>")
api.add_resource(Business_v2, "/api/v2/business", "/api/v2/business/<string:uid>")
api.add_resource(Businesses, "/businesses")
api.add_resource(Ratings, "/ratings", "/ratings/<string:uid>")
api.add_resource(Ratings_v2, "/api/v2/ratings", "/api/v2/ratings/<string:uid>")
api.add_resource(Search, "/search/<string:profile_id>")
api.add_resource(Search_v2, "/api/v2/search/<string:profile_id>")
api.add_resource(Refer, "/refer-a-friend")
api.add_resource(Lists, "/lists")
api.add_resource(Charges, "/charges")
api.add_resource(Business_Budget, "/business-budget/<string:business_id>")
api.add_resource(Feed, "/feed/<string:profile_id>")
api.add_resource(CategoryList, "/category_list/<string:uid>")
api.add_resource(ChatbotAPI, "/api/v1/chatbot")
api.add_resource(Connections, '/api/v1/connections/<string:profile_id>')
api.add_resource(BusinessRevenue, '/api/v1/businessrevenue/<string:business_id>')
api.add_resource(AISearch, '/api/v1/aisearch/<string:profile_id>')
api.add_resource(Business_v3, '/api/v3/business_v3', '/api/v3/business_v3/<string:uid>')
api.add_resource(AISearchTag, '/api/v2/AITagSearch/<string:profile_id>')
api.add_resource(AISearchTag_v3, '/api/v3/AITagSearch/<string:profile_id>')
api.add_resource(TagGeneratorAPI, '/api/v1/taggenerator')
api.add_resource(Ratings_v3, '/api/v3/ratings_v3', '/api/v3/ratings_v3/<string:uid>')
api.add_resource(AIDirectBusinessSearch, '/api/v1/aidirectbusinesssearch/<string:profile_id>')
api.add_resource(UserProfileInfo, '/api/v1/userprofileinfo', '/api/v1/userprofileinfo/<string:uid>')
api.add_resource(BusinessInfo, '/api/v1/businessinfo','/api/v1/businessinfo/<string:uid>')
api.add_resource(Transactions, '/api/v1/transactions','/api/v1/transactions/<string:uid>')

api.add_resource(Business_Results, '/api/business_results')
api.add_resource(ConnectionsPath, '/api/connections_path/<string:first_uid>/<string:second_uid>')
api.add_resource(NetworkPath, "/api/network/<string:target_uid>/<int:degree>")
api.add_resource(TagSearchDirect, "/api/tagsearchdistinct/<string:query>")
api.add_resource(TagCategorySearch, "/api/tagcategorydistinct/<string:query>")
api.add_resource(ProfileDetails, "/api/profiledetails/<string:query>")
api.add_resource(ProfileWishInfo, "/api/profilewishinfo/<string:query>")
api.add_resource(TagSplitSearch, "/api/tagsplitsearchdistinct/<string:query>")
api.add_resource(TagSplitNLPSearch, "/api/tagsplitnlpsearch/<string:query>")
api.add_resource(SplitSearch, "/api/tagsplitcategorysearch/<string:query>")
api.add_resource(BusinessResults, '/api/businessresults/<string:query>')
api.add_resource(TransactionCost, '/api/transactioncost/<string:user_uid>/<string:ts_uid>')
# @app.route('/decrypt', methods=['POST'])
# def decrypt_data():
#     try:
#         # Get the encrypted data from the request body
#         encrypted_data_base64 = request.json.get('encrypted_data')
#         # print("encrypted_data: ", encrypted_data_base64)
#         decrypted_data = decrypt_dict(encrypted_data_base64)
#         print("Decrypted Data: ", decrypt_data)
        
#         return jsonify({"decrypted_data": decrypted_data})
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


# def check_jwt_token():
#     if request.path == '/auth/refreshToken' or request.path.startswith('/userInfo/') or request.path.startswith('/businessProfile'):
#         return jsonify({'message': 'JWT not required!'}), 201
#     try:
#         print('Request Headers:', request.headers['Authorization'])
#         verify_jwt_in_request()
#         current_user = get_jwt_identity() 
#         print(f"Current User ID: {current_user}")
#         return jsonify({'message': 'JWT is present!'}), 201
#     except jwt.ExpiredSignatureError:
#         print('JWT Expired')
#         return jsonify({'message': 'Token is expired!'}), 401

#     except jwt.InvalidTokenError:
#         print('JWT Invalid')
#         return jsonify({'message': 'Invalid token!'}), 404

#     except Exception as e:
#         # This will catch any other exception, including missing token
#         print('JWT Missing')
#         return jsonify({'message': 'Missing token!'}), 404


# Middleware for decrypting incoming request data
# def decrypt_request():
#     if request.is_json:
#         print('Inside is_json')

#         encrypted_data = request.get_json().get('encrypted_data')
#         form_data = request.get_json().get('data_type') # True = Form data, False = JSON data
#         if encrypted_data and form_data == False:
#             decrypted_data = decrypt_dict(encrypted_data)
#             # print('decrypted data', decrypted_data)

#             # Override request.get_json() to return decrypted data
#             def get_json_override(*args, **kwargs):
#                 return decrypted_data
            
#             request.get_json = get_json_override
#         else:
#             print("Data issue")
#     elif request.content_type and request.content_type.startswith('multipart/form-data'):
#         # For FormData directly in the request
#         encrypted_data = request.form.get('encrypted_data')

#         if encrypted_data:
#             decrypted_data = decrypt_dict(encrypted_data)
#             # print("decrypted_data: ", decrypted_data)
#             fields = {}
#             files = {}

#             for key, value in decrypted_data.items():
#                 if isinstance(value, dict) and 'fileName' in value and 'fileType' in value:
#                     # Handle file-specific data
#                     # print("image - ", value)
#                     file_binary = base64.b64decode(value['fileData'])
#                     file_stream = BytesIO(file_binary)
#                     files[key] = FileStorage(
#                         stream=file_stream,
#                         filename=value['fileName'],
#                         content_type=value['fileType']
#                     )
#                 else:
#                     fields[key] = value

#             # Update `request.form` and `request.files`

#             # print(" Fields: ", fields)
#             request.form = ImmutableMultiDict(fields)
#             request.files = ImmutableMultiDict(files)

#             # print("Updated Form Data:", request.form)
#             # print("Updated Files:", request.files)
#         else:
#             print("No encrypted data found in multipart/form-data request")
#     else:
#         print("GET Request, no JSON object received")

# Middleware to encrypt response data
# def encrypt_response(data):
#     print("data: ",data)
#     encrypted_data = encrypt_dict(data)
#     return jsonify({'encrypted_data': encrypted_data})


# Health check route (optional)
# @app.route('/')
# def health_check():
#     print("In Health Check")
#     return jsonify({"message": "API is running!"})


# Actual middleware.  Commands before request (check JWT and then decrypt data) and after request (encrypt response before passing to FrontEnd)
# @app.before_request 
# def before_request():
#     # all_headers = dict(request.headers)
#     # print(all_headers)
#     # print("Before Postman Secret: ", request.headers.get("Postman-Secret"))
#     if request.headers.get("Postman-Secret") != POSTMAN_SECRET:
#         if request.method != 'OPTIONS':  
#             print("In Middleware before_request")
#             response,code = check_jwt_token()
#             if code == 201:
#                 decrypt_request()

#             else:
#                 print("Response Code: ", code)
#                 response = encrypt_response(response.get_json()) if response.is_json else response
#                 response.status_code = code
#                 return response

# @app.after_request
# def after_request(response):
#     # print("After Postman Secret: ", request.headers.get("Postman-Secret"))
#     if request.headers.get("Postman-Secret") != POSTMAN_SECRET:
#         print("In Middleware after_request")
#         # print("Actual endpoint response: ", type(response))
#         # print("Actual endpoint response2: ", type(response.get_json()))
#         original_status_code = response.status_code

#         response = encrypt_response(response.get_json()) if response.is_json else response
        
#         response.status_code = original_status_code

#     return response
    

# Apply middlewares
# setup_middlewares(app)

#This method is to refresh the jwt token from the FrontEnd
# @app.route('/auth/refreshToken', methods=['GET'])
# @jwt_required(refresh=True)  # This ensures that only refresh tokens can be used here
# def refreshToken():
#     try:
#         print('Inside refresh token')
#         current_user = get_jwt_identity()  # Get user identity from refresh token
#         new_access_token = create_access_token(identity=current_user)  # Create new access token
#         print('New token is', new_access_token)
#         return jsonify(access_token=new_access_token)
#     except Exception as e:
#         print('Error refreshing token:', e)
#         return jsonify({'message': 'Could not refresh token'}), 401
    



if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4090)