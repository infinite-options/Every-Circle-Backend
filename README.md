# Every Circle Backend

Zappa Base URL for development: https://o7t5ikn907.execute-api.us-west-1.amazonaws.com/dev
Zappa Base URL for production: https://ml7xmrvue6.execute-api.us-west-1.amazonaws.com/production

Database for development: every_circle_test
Database for production: every_circle

zappa update dev updates Backend code to /dev
zappa update production updates Backend code to /production
NOTE: Make sure database is set propertly in zappa_settings.json

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

-- TO RESET ENTIRE DATABASE
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

-- TO DELETE ALL TRANSACTION AND WALLET ACCOUNTS ONLY
SET FOREIGN_KEY_CHECKS = 0;
-- Ledger / wallet (depends on transactions)
TRUNCATE TABLE every_circle.wallet_transactions;
TRUNCATE TABLE every_circle.wallet;
-- Return restock audit (depends on transaction_return_requests)
TRUNCATE TABLE every_circle.profile_expertise_restocks;
TRUNCATE TABLE every_circle.business_service_restocks;
-- Returns (depends on transactions / items)
TRUNCATE TABLE every_circle.transaction_return_requests;
-- Transaction children
TRUNCATE TABLE every_circle.transactions_bounty;
TRUNCATE TABLE every_circle.transactions_shipping;
TRUNCATE TABLE every_circle.transactions_items;
-- Parent orders (includes sale + return transaction rows)
TRUNCATE TABLE every_circle.transactions;
SET FOREIGN_KEY_CHECKS = 1;

SEEKING ALLOCATION
BUYER is the one who posted the Seeking ad (the one paying the unit cost _ qty)
SELLER is the one who will provide the product or service (the one receiving the unit cost _ qty)
RECOMMENDER is the one who put the Seller and Buyer in touch with each other

There are two scenarios:

1.  The Recommender is the Seller
2.  The Recommender is NOT the Seller

3.  The Recommender is the Seller
    a. Recommender/Seller gets 40%
    b. The Nodes between the Recommender and the BUYER (excluding the Recommender and the BUYER) get 40% with a maximum 20% per node. Any excess goes to Charity
    c. Every Circle gets 20%

    EXAMPLED: B - N1 - N2 - N3 - R/S

4.  The Recommender is NOT the Seller
    a. Recommender gets 40%
    b. The Nodes between the Recommender and the BUYER (excluding the Recommender and the BUYER) get 40% with a maximum 20% per node. Any excess goes to Charity. NOTE: This may include the Seller depending on how the network is connected.  
    c. Every Circle gets 20%

    EXAMPLE
    B - N1 - N2 - N3 - R S does not get any of the bounty
    B - N1 - S - N3 - R S is in the chain so gets part of the bounty

FULFILLMENT CSV FILES
For scenarios of how order-return-cancel are handled, see fulfillment_states.csv and fulfillment_fix_status.csv
