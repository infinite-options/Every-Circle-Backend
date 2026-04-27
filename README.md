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
https://o7t5ikn907.execute-api.us-west-1.amazonaws.com/dev/api/v1/userprofileinfo/110-000007

SHOW PROCESSLIST;

KILL 276116;

To RESET Database
SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE every_circle.business;
TRUNCATE TABLE every_circle.business_category;
TRUNCATE TABLE every_circle.business_link;
TRUNCATE TABLE every_circle.business_monthly_cap;
TRUNCATE TABLE every_circle.business_services;
TRUNCATE TABLE every_circle.business_tags;
TRUNCATE TABLE every_circle.business_type;
TRUNCATE TABLE every_circle.business_user;

TRUNCATE TABLE every_circle.category;
TRUNCATE TABLE every_circle.charges;
TRUNCATE TABLE every_circle.circles;
TRUNCATE TABLE every_circle.conversations;
TRUNCATE TABLE every_circle.feedback;
TRUNCATE TABLE every_circle.lists;
TRUNCATE TABLE every_circle.messages;

TRUNCATE TABLE every_circle.`profile-DNU`; -- special case (hyphen)
TRUNCATE TABLE every_circle.profile_education;
TRUNCATE TABLE every_circle.profile_experience;
TRUNCATE TABLE every_circle.profile_expertise;
TRUNCATE TABLE every_circle.profile_link;
TRUNCATE TABLE every_circle.profile_personal;
TRUNCATE TABLE every_circle.profile_views;
TRUNCATE TABLE every_circle.profile_wish;

TRUNCATE TABLE every_circle.ratings;
TRUNCATE TABLE every_circle.recommendation;
TRUNCATE TABLE every_circle.recommendation_used;
TRUNCATE TABLE every_circle.social_link;
TRUNCATE TABLE every_circle.tags;

TRUNCATE TABLE every_circle.transactions;
TRUNCATE TABLE every_circle.transactions_bounty;
TRUNCATE TABLE every_circle.transactions_items;

TRUNCATE TABLE every_circle.types;
TRUNCATE TABLE every_circle.users;
TRUNCATE TABLE every_circle.wish_response;

SET FOREIGN_KEY_CHECKS = 1;
