/* Model: fact_transactions_enriched
    Purpose: Adding distance and digitalization metrics to analyze each operation.
    Author: Elif
*/

{{ config(materialized='table') }}

WITH master_table AS ( SELECT * FROM {{ ref('int_transaction_leftjoin') }} )

SELECT
      transaction_id
    , user_id
    , transaction_date
    , use_chip

    , CASE 
         WHEN use_chip = 'Online Transaction' THEN 'Online'
         ELSE 'Physical Transaction'
      END AS channel_type
    
    /* I measured the distance between the user's house and the seller in meters, 
    then divided by 1000 to convert it to kilometers.*/
    , ST_DISTANCE(
        ST_GEOGPOINT(user_home_lon, user_home_lat), 
        ST_GEOGPOINT(merchant_lon, merchant_lat)) / 1000 AS distance_km

    , user_home_lon
    , user_home_lat
    , merchant_lon
    , merchant_lat
    , merchant_city
    , merchant_state
    , category_name 
    , amount
    , card_on_dark_web
    , errors
    , gender
    , current_age
    , user_state_name
    , users_state 
    , credit_score

    /* To enhance the clarity of our analysis, 
    I consolidated the 108 specific categories from the fact table into broader, 
    high-level groups. I leveraged AI to assist in this classification, 
    ensuring a logical structure that makes the data easier to interpret. */
    , CASE 
        WHEN category_name IN (
            'Pottery and Ceramics', 
            'Family Clothing Stores', 
            'Book Stores', 
            'Artist Supply Stores, Craft Shops', 
            "Women's Ready-To-Wear Stores", 
            'Cosmetic Stores', 
            'Electronics Stores', 
            'Computers, Computer Peripheral Equipment', 
            'Discount Stores', 
            'Sports Apparel, Riding Apparel Stores', 
            'Antique Shops', 
            'Automotive Parts and Accessories Stores', 
            'Floor Covering Stores', 
            'Department Stores', 
            'Shoe Stores', 
            'Miscellaneous Home Furnishing Stores', 
            'Gift, Card, Novelty Stores', 
            'Leather Goods', 
            'Precious Stones and Metals', 
            'Furniture, Home Furnishings, and Equipment Stores', 
            'Music Stores - Musical Instruments',  
            'Lighting, Fixtures, Electrical Supplies',  
            'Wholesale Clubs', 
            'Sporting Goods Stores',  
            'Household Appliance Stores', 
            'Books, Periodicals, Newspapers',  
            'Florists Supplies, Nursery Stock and Flowers' 
        ) THEN 'Retail & Shopping'

        WHEN category_name IN (
            'Fast Food Restaurants',
            'Eating Places and Restaurants', 
            'Drinking Places (Alcoholic Beverages)', 
            'Miscellaneous Food Stores', 
            'Grocery Stores, Supermarkets',
            'Package Stores, Beer, Wine, Liquor'
        ) THEN 'Food & Dining'

        WHEN category_name IN (
            'Taxicabs and Limousines', 
            'Tolls and Bridge Fees', 
            'Railroad Passenger Transport', 
            'Automotive Body Repair Shops', 
            'Travel Agencies', 'Automotive Service Shops', 
            'Towing Services', 
            'Railroad Freight', 
            'Cruise Lines', 
            'Motor Freight Carriers and Trucking', 
            'Lodging - Hotels, Motels, Resorts', 
            'Service Stations', 
            'Car Washes', 
            'Local and Suburban Commuter Transportation', 
            'Passenger Railways', 
            'Bus Lines', 
            'Airlines'
        ) THEN 'Travel, Transportation & Automotive'

        WHEN category_name IN (
            'Doctors, Physicians', 
            'Dentists and Orthodontists', 
            'Chiropractors', 
            'Podiatrists', 
            'Hospitals', 
            'Drug Stores and Pharmacies', 
            'Medical Services', 
            'Optometrists, Optical Goods and Eyeglasses'
        ) THEN 'Health & Medical'

        WHEN category_name IN (
            'Beauty and Barber Shops',
            'Cleaning and Maintenance Services', 
            'Postal Services - Government Only', 
            'Insurance Sales, Underwriting', 
            'Accounting, Auditing, and Bookkeeping Services', 
            'Computer Network Services', 
            'Money Transfer', 
            'Laundry Services', 
            'Tax Preparation Services', 
            'Detective Agencies, Security Services', 
            'Legal Services and Attorneys'
        ) THEN 'Professional & Personal Services'

        WHEN category_name IN (
            'Motion Picture Theaters', 
            'Digital Goods - Games', 
            'Theatrical Producers', 
            'Amusement Parks, Carnivals, Circuses', 
            'Cable, Satellite, and Other Pay Television Services', 
            'Digital Goods - Media, Books, Apps', 
            'Betting (including Lottery Tickets, Casinos)', 
            'Recreational Sports, Clubs', 
            'Athletic Fields, Commercial Sports'
        ) THEN 'Entertainment & Recreation'

        WHEN category_name IN (
            'Ship Chandlers', 
            'Brick, Stone, and Related Materials', 
            'Miscellaneous Metals', 
            'Welding Repair', 
            'Non-Precious Metal Services', 
            'Fabricated Structural Metal Products', 
            'Miscellaneous Metal Fabrication', 
            'Industrial Equipment and Supplies', 
            'Electroplating, Plating, Polishing Services', 
            'Ironwork', 
            'Miscellaneous Metalwork', 
            'Heat Treating Metal Services', 
            'Tools, Parts, Supplies Manufacturing', 
            'Miscellaneous Fabricated Metal Products', 
            'Steel Drums and Barrels', 
            'Miscellaneous Machinery and Parts Manufacturing', 
            'Steelworks', 
            'Coated and Laminated Products', 
            'Non-Ferrous Metal Foundries', 
            'Semiconductors and Related Devices', 
            'Steel Products Manufacturing', 
            'Bolt, Nut, Screw, Rivet Manufacturing'
        ) THEN 'Manufacturing & Industrial'

        WHEN category_name IN (
            'Upholstery and Drapery Stores', 
            'Heating, Plumbing, Air Conditioning Contractors', 
            'Hardware Stores', 
            'Lawn and Garden Supply Stores', 
            'Lumber and Building Materials', 
            'Telecommunication Services', 
            'Utilities - Electric, Gas, Water, Sanitary', 
            'Gardening Supplies'
        ) THEN 'Home, Utilities & Construction'

        ELSE 'Other' 
    END AS general_group

FROM master_table