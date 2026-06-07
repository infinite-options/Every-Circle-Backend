# EVERY CIRCLE BACKEND PYTHON FILE
# https://o7t5ikn907.execute-api.us-west-1.amazonaws.com/dev /<enter_endpoint_details>
# No production endpoint yet


# To run program:  python3 ec_api.py
# Check README.md for more information


import os

# # Explicitly override Hugging Face cache directories
# os.environ["HF_HOME"] = "/home/ec2-user/.cache/huggingface"
# os.environ["TRANSFORMERS_CACHE"] = "/home/ec2-user/.cache/huggingface"

# Load environment variables first, before any imports
from dotenv import load_dotenv
load_dotenv()

# SECTION 1:  IMPORT FILES AND FUNCTIONS
from data_ec import connect, uploadImage, s3
from users import UserInfo
from business import Business, Business_v2, BusinessDetails, Businesses, BusinessTagSearch, BusinessServicePurchase, BusinessClaim  
from business_v3 import Business_v3
from ratings import Ratings
from lists import Lists
from charges import Charges
from business_budget import Business_Budget
from business_revenue import BusinessRevenue
from category_list import CategoryList
from chatbot import ChatbotAPI
from tag_generator_api import TagGeneratorAPI
from user_profile_info import UserProfileInfo
from business_info import BusinessInfo
from business_services_options import BusinessServiceOptions
from transactions import (
    Transactions,
    SellerTransactions,
    DeclinedReturns,
    ReturnTransaction,
)
from user_path_connection import ConnectionsPath
from network_connection import NetworkPath
from profile_details import ProfileDetails
from profile_wish import ProfileWishInfo
from bounty_results import BountyResults, BusinessBountyResults
from transaction_receipt import TransactionReceipt
from account_screen import AccountScreenPersonal, AccountScreenBusiness
from circles import Circles
from nearby import NearbyLocation, NearbyUsers
from chat import Conversations, Messages
from feedback import Feedback
from search_referral import SearchReferral
from profile_views import ProfileViews
# from jwtToken import JwtToken
from functools import wraps

# from flask import Request

# import os
import boto3
import json
import pytz

from ably_auth import AblyToken
import calendar
from datetime import datetime, date, timedelta, timezone
from flask import Flask, request, render_template, url_for, redirect, jsonify, abort
from flask_restful import Resource, Api
from flask_cors import CORS
from flask_mail import Mail, Message  # used for email
from flask_jwt_extended import JWTManager, verify_jwt_in_request, get_jwt_identity, jwt_required, create_access_token, create_refresh_token
from pytz import timezone as ptz  # Not sure what the difference is
from decimal import Decimal
from hashlib import sha512
from twilio.rest import Client
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
# from google_auth_oauthlib.flow import InstalledAppFlow
from urllib.parse import urlparse
from io import BytesIO
from dateutil.relativedelta import relativedelta
from math import ceil
from werkzeug.exceptions import BadRequest, NotFound
from werkzeug.datastructures import FileStorage  # For file handling
from werkzeug.datastructures import ImmutableMultiDict

# used for serializer email and error handling
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend
import base64
import googlemaps

# import awsgi
# def lambda_handler(event, context):
#    return awsgi.response(app, event, context, base64_content_types={"image/png"})

print(f"-------------------- New Program Run ( {os.getenv('RDS_DB')} ) --------------------")

# == Using Cryptography library for AES encryption ==

AES_SECRET_KEY = os.getenv('AES_SECRET_KEY')
AES_KEY = AES_SECRET_KEY.encode('utf-8')
BLOCK_SIZE = int(os.getenv('BLOCK_SIZE'))
POSTMAN_SECRET = os.getenv('POSTMAN_SECRET')

ENCRYPTION_ENABLED = True if os.getenv('ENCRYPTION_ENABLED') == "True" else False
print("ENCRYPTION_ENABLED: ", ENCRYPTION_ENABLED)

decrypted_data = {}


def encrypt_dict(data_dict):
    try:
        print("In encrypt_dict: ", data_dict)
        json_data = json.dumps(data_dict).encode()
        padder = PKCS7(BLOCK_SIZE * 8).padder()
        padded_data = padder.update(json_data) + padder.finalize()
        iv = os.urandom(BLOCK_SIZE)
        cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
        encrypted_blob = base64.b64encode(iv + encrypted_data).decode()
        return encrypted_blob
    except Exception as e:
        print(f"Encryption error: {e}")
        return None


def decrypt_dict(encrypted_blob):
    print("Actual decryption started")
    try:
        encrypted_data = base64.b64decode(encrypted_blob)
        iv = encrypted_data[:BLOCK_SIZE]
        encrypted_content = encrypted_data[BLOCK_SIZE:]
        cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_padded_data = decryptor.update(encrypted_content) + decryptor.finalize()
        unpadder = PKCS7(BLOCK_SIZE * 8).unpadder()
        decrypted_data = unpadder.update(decrypted_padded_data) + unpadder.finalize()
        return json.loads(decrypted_data.decode())
    except Exception as e:
        print(f"Decryption error: {e}")
        return None



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

# Logging Info
import logging
import os

# Check if we're running in AWS Lambda (read-only file system)
is_lambda = os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None

if not is_lambda:
    # Only set up file logging when not running in Lambda
    try:
        from logging.handlers import RotatingFileHandler
        
        LOG_FILE = "logs/ec_api.log"
        
        # Only create directory if path includes one
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        # Set up rotating log handler
        log_handler = RotatingFileHandler(
            LOG_FILE, maxBytes=2 * 1024 * 1024, backupCount=3
        )
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s"
        ))

        logging.basicConfig(
            level=logging.INFO,
            handlers=[
                log_handler,
                logging.StreamHandler()
            ]
        )
        
        logging.info("🚀 ec_api.py has started successfully (with file logging).")
    except Exception as e:
        # Fallback to console-only logging if file logging fails
        logging.basicConfig(
            level=logging.INFO,
            handlers=[logging.StreamHandler()]
        )
        logging.info("🚀 ec_api.py has started successfully (console logging only).")
        logging.warning(f"File logging setup failed: {e}")
else:
    # Lambda environment - use console logging only
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.StreamHandler()]
    )
    logging.info("🚀 ec_api.py has started successfully in Lambda environment.")


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
        if desc == "ECTEST":
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
                profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_uid': payload['profile_uid']})
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

# -- CRON ENDPOINTS start here -------------------------------------------------------------------------------

# -- CURRENT CRON JOB


class Lists_CLASS(Resource):
    def get(self):
        print("In Lists CLASS JOB")

        response = {}

        try:
            # Run query to find all APPROVED Contracts
            with connect() as db:       
                generic_query = db.execute("""
                    SELECT * FROM every_circle.lists;
                    """)

                generic_list = generic_query['result']
                print("\nApproved List Contents: ", generic_list)
                response["Lists CRON Job completed"] = {'message': f'Lists CRON Job completed' ,
                        'code': 200}

                    
        except:
                response["Lists CRON Job failed"] = {'message': f'Lists CRON Job failed' ,
                        'code': 500}

        return response

def Lists_CRON(Resource):
        print("In Lists CRON JOB")

        response = {}

        try:
            # Run query to find all APPROVED Contracts
            with connect() as db:    
                generic_query = db.execute("""
                    SELECT * FROM every_circle.lists;
                    """)

                generic_list = generic_query['result']
                print("\nApproved List Contents: ", generic_list)
                response["Lists CRON Job completed"] = {'message': f'Lists CRON Job completed' ,
                        'code': 200}

                    
        except:
                response["Lists CRON Job failed"] = {'message': f'Lists CRON Job failed' ,
                        'code': 500}

        return response

#  -- ACTUAL ENDPOINTS    -----------------------------------------

api.add_resource(stripe_key, "/stripe_key/<string:desc>")
api.add_resource(UserInfo, "/userinfo", "/userinfo/<string:user_id>")
api.add_resource(Business, "/business", "/business/<string:uid>")
api.add_resource(Business_v2, "/api/v2/business", "/api/v2/business/<string:uid>")
api.add_resource(Businesses, "/businesses")
api.add_resource(Ratings, "/ratings", "/ratings/<string:uid>")
api.add_resource(Refer, "/refer-a-friend")
api.add_resource(Lists, "/lists")
api.add_resource(Charges, "/charges")
api.add_resource(Business_Budget, "/business-budget/<string:business_id>")
api.add_resource(CategoryList, "/category_list/<string:uid>")
api.add_resource(ChatbotAPI, "/api/v1/chatbot")
api.add_resource(BusinessRevenue, '/api/v1/businessrevenue/<string:business_id>')
api.add_resource(Business_v3, '/api/v3/business_v3', '/api/v3/business_v3/<string:uid>')
api.add_resource(TagGeneratorAPI, '/api/v1/taggenerator')
api.add_resource(UserProfileInfo, '/api/v1/userprofileinfo', '/api/v1/userprofileinfo/<string:uid>')
api.add_resource(BusinessInfo, '/api/v1/businessinfo','/api/v1/businessinfo/<string:uid>')
# Static paths must register before `/api/v1/transactions/<profile_id>` so `return` is not captured as profile_id.
api.add_resource(ReturnTransaction, '/api/v1/transactions/return')
api.add_resource(Transactions, '/api/v1/transactions', '/api/v1/transactions/<string:profile_id>')
api.add_resource(SellerTransactions,'/api/v1/transactions/seller/<string:profile_id>')

api.add_resource(ConnectionsPath, '/api/connections_path/<string:first_uid>/<string:second_uid>')
api.add_resource(NetworkPath, "/api/network/<string:target_uid>/<int:degree>")
api.add_resource(ProfileDetails, "/api/profiledetails/<string:query>")
api.add_resource(ProfileWishInfo,  "/api/profilewishinfo", "/api/profilewishinfo/<string:profile_wish_id>")
api.add_resource(TransactionReceipt, '/api/transactionreceipt/<string:profile_id>/<string:transaction_uid>')
api.add_resource(BountyResults, '/api/bountyresults/<string:profile_id>')
api.add_resource(BusinessBountyResults, '/api/business-bountyresults/<string:business_id>')
api.add_resource(
    AccountScreenPersonal,
    '/api/v1/account-screen/personal/<string:profile_id>',
)
api.add_resource(
    AccountScreenBusiness,
    '/api/v1/account-screen/business/<string:business_uid>',
)
api.add_resource(Circles, '/api/v1/circles/<string:circle_id>', '/api/v1/circles')
api.add_resource(NearbyLocation,  '/api/v1/nearby/location')
api.add_resource(NearbyUsers,     '/api/v1/nearby/<string:profile_uid>')
api.add_resource(Conversations,   '/api/v1/chat/conversations', '/api/v1/chat/conversations/<string:profile_uid>')
api.add_resource(Messages,        '/api/v1/chat/messages', '/api/v1/chat/messages/<string:conversation_uid>')
api.add_resource(Feedback, '/api/feedback')
api.add_resource(SearchReferral, '/api/search_referral')
api.add_resource(BusinessDetails, '/api/v1/business_details')
# api.add_resource(BusinessMaxBounty, '/api/v1/businessmaxbounty')
api.add_resource(BusinessTagSearch, '/api/v1/businesstagsearch')
api.add_resource(AblyToken, '/api/v1/ably/token')
api.add_resource(DeclinedReturns, '/api/v1/transactions/returns/declined')
api.add_resource(ProfileViews, '/api/v1/profile_views', '/api/v1/profile_views/<string:profile_uid>')
api.add_resource(BusinessServicePurchase, "/business/service/purchase")
api.add_resource(BusinessServiceOptions, '/api/business_service_options/<string:bs_uid>')
api.add_resource(BusinessClaim, "/api/v1/business_claim")
api.add_resource(Lists_CLASS, "/api/v1/lists_cron")


class GooglePlacesInfo(Resource):
    def post(self):
        try:
            data = request.get_json()
            place_id = data.get('place_id')
            user_uid = data.get('user_uid')
            
            if not place_id:
                return {'error': 'place_id is required'}, 400
                
            if not user_uid:
                return {'error': 'user_uid is required'}, 400
            
            # Create instance of BusinessInfo and call the method
            business_info = BusinessInfo()
            return business_info.get_google_places_info(place_id, user_uid)
            
        except Exception as e:
            print(f"Error in GooglePlacesInfo: {str(e)}")
            return {'error': str(e)}, 500

# Add the new endpoint to the API
api.add_resource(GooglePlacesInfo, '/api/google-places')


#  -- ENCRYPTION MIDDLEWARE    -----------------------------------------

def check_jwt_token():
    if (
        request.path == '/auth/refreshToken'
        or request.path == '/auth/accessToken'
        or request.path.startswith('/userinfo')
        or request.path.startswith('/api/v1/userprofileinfo')
    ):
        return jsonify({'message': 'JWT not required!'}), 201
    try:
        print('Request Headers:', request.headers['Authorization'])
        verify_jwt_in_request()
        current_user = get_jwt_identity()
        print(f"Current User ID: {current_user}")
        return jsonify({'message': 'JWT is present!'}), 201
    except Exception as e:
        exc_name = type(e).__name__
        if exc_name == 'ExpiredSignatureError':
            print('JWT Expired')
            return jsonify({'message': 'Token is expired!'}), 401
        if exc_name == 'InvalidTokenError':
            print('JWT Invalid')
            return jsonify({'message': 'Invalid token!'}), 404
        print('JWT Missing')
        return jsonify({'message': 'Missing token!'}), 404


def decrypt_request():
    if not ENCRYPTION_ENABLED:
        return

    if request.is_json:
        global decrypted_data
        print(f"Inside is_json - {os.getenv('RDS_DB')}")
        print(request.get_json().get('encrypted_data'))
        encrypted_data = request.get_json().get('encrypted_data')
        if encrypted_data:
            decrypted_data = decrypt_dict(encrypted_data)
            print('decrypted data', decrypted_data, type(decrypted_data))

            def get_json_override(*args, **kwargs):
                global decrypted_data
                print("In function: ", decrypted_data, type(decrypted_data))
                if isinstance(decrypted_data, str):
                    decrypted_data = json.loads(decrypted_data)
                    print("JSON Announcement Payload: ", decrypted_data, type(decrypted_data))
                return decrypted_data

            request.get_json = get_json_override
        else:
            print("Data issue")
    elif request.content_type and request.content_type.startswith('multipart/form-data'):
        print(f"Inside multipart - {os.getenv('RDS_DB')}")
        encrypted_data = request.form.get('encrypted_data')

        if encrypted_data:
            decrypted_data = decrypt_dict(encrypted_data)
            fields = {}
            files = {}

            for key, value in decrypted_data.items():
                if isinstance(value, dict) and 'fileName' in value and 'fileType' in value:
                    file_binary = base64.b64decode(value['fileData'])
                    file_stream = BytesIO(file_binary)
                    files[key] = FileStorage(
                        stream=file_stream,
                        filename=value['fileName'],
                        content_type=value['fileType']
                    )
                else:
                    fields[key] = value

            request.form = ImmutableMultiDict(fields)
            request.files = ImmutableMultiDict(files)
        else:
            print("No encrypted data found in multipart/form-data request")
    else:
        print("GET Request, no JSON object received")


def encrypt_response(data):
    if not ENCRYPTION_ENABLED:
        return jsonify(data)

    encrypted_data = encrypt_dict(data)
    return jsonify({'encrypted_data': encrypted_data})


@app.route('/')
def health_check():
    print("In Health Check")
    return jsonify({"message": "API is running!"})


@app.route('/decode', methods=['POST'])
def decode():
    print("In decode")

    decrypt_request()

    if request.is_json:
        response = jsonify({'decode': request.get_json(force=True)})
    else:
        decode_files = {}
        for key, value in request.form.items():
            decode_files[key] = value
        for file_key, file_storage in request.files.items():
            print(f"Key: {file_key}, Filename: {file_storage.filename}")
            decode_files[file_key] = file_storage.filename
        response = jsonify(decode_files)
    return response


@app.before_request
def before_request():
    if request.headers.get("Postman-Secret") != POSTMAN_SECRET:
        if request.method != 'OPTIONS':
            print("In Middleware before_request")
            response, code = check_jwt_token()
            if code == 201:
                decrypt_request()
            else:
                print("Response Code: ", code)
                response = encrypt_response(response.get_json()) if response.is_json else response
                response.status_code = code
                return response


@app.after_request
def after_request(response):
    if request.headers.get("Postman-Secret") != POSTMAN_SECRET:
        print("In Middleware after_request")
        original_status_code = response.status_code
        response = encrypt_response(response.get_json()) if response.is_json else response
        response.status_code = original_status_code
    return response


@app.route('/auth/accessToken', methods=['POST'])
def issue_access_token():
    """Issue JWT for Every-Circle app API calls. Auth login lambda does not return JWT yet."""
    try:
        payload = request.get_json(force=True) or {}
        user_uid = payload.get('user_uid')
        if not user_uid:
            return jsonify({'message': 'user_uid required'}), 400
        with connect() as db:
            response = db.select('users', {'user_uid': user_uid})
        if not response.get('result'):
            return jsonify({'message': 'User not found'}), 404
        access_token = create_access_token(identity=user_uid)
        refresh_token = create_refresh_token(identity=user_uid)
        return jsonify(access_token=access_token, refresh_token=refresh_token)
    except Exception as e:
        print('Error issuing access token:', e)
        return jsonify({'message': 'Could not issue access token'}), 500


@app.route('/auth/refreshToken', methods=['GET'])
@jwt_required(refresh=True)
def refreshToken():
    try:
        print('Inside refresh token')
        current_user = get_jwt_identity()
        new_access_token = create_access_token(identity=current_user)
        print('New token is', new_access_token)
        return jsonify(access_token=new_access_token)
    except Exception as e:
        print('Error refreshing token:', e)
        return jsonify({'message': 'Could not refresh token'}), 401


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4090)