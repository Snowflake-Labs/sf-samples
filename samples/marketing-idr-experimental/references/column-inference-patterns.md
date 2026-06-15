# Column Inference Patterns

Map arbitrary source column names to canonical identity fields. Use case-insensitive matching against these patterns.

## Canonical Field Mapping

### EMAIL
```
Patterns: email, email_address, emailaddr, e_mail, contact_email, 
          primary_email, work_email, personal_email, subscriber_email,
          user_email, account_email, login_email
Vendor-specific:
  - Salesforce: Email, PersonEmail, npe01__WorkEmail__c
  - HubSpot: email, hs_email_address
  - Klaviyo: $email, email
  - Braze: email_address, email
  - Segment: email, traits_email
Cleansing: LOWER(TRIM(<col>))
Skip if: fill_rate < 0.05 (less than 5% populated)
```

### PHONE
```
Patterns: phone, phone_number, mobile, cell, telephone, phone_no,
          mobile_phone, home_phone, work_phone, primary_phone,
          contact_phone, sms_number, mobile_number, tel
Vendor-specific:
  - Salesforce: Phone, MobilePhone, PersonMobilePhone
  - HubSpot: phone, mobilephone, hs_calculated_phone_number
  - Klaviyo: $phone_number, phone_number
  - Braze: phone
  - Segment: phone, traits_phone
Cleansing: REGEXP_REPLACE(<col>, '[^0-9]', '')
Validation: LENGTH(phone_clean) >= 10 (discard shorter fragments)
Skip if: fill_rate < 0.05
```

### LOYALTY_ID
```
Patterns: loyalty_id, membership_id, loyalty_number, rewards_id,
          member_id, loyalty_card, rewards_number, club_id,
          membership_number, program_id, subscriber_id
Vendor-specific:
  - Custom POS: loyalty_id, member_number, card_number
Cleansing: UPPER(TRIM(<col>))
Skip if: fill_rate < 0.05 OR >90% of values are sequential integers (likely a surrogate PK, not a business identifier)
```

### DEVICE_ID
```
Patterns: device_id, maid, idfa, gaid, advertising_id, ad_id,
          mobile_ad_id, device_identifier, hardware_id
Vendor-specific:
  - Segment: context_device_advertising_id, anonymous_id (when mobile)
  - Braze: device_id
  - mParticle: device_identities
Cleansing: LOWER(TRIM(<col>))
Note: Device IDs are WEAK signals — require corroboration for deterministic matching
Skip if: fill_rate < 0.05
```

### COOKIE_ID
```
Patterns: cookie_id, anonymous_id, visitor_id, session_id,
          browser_id, client_id, tracking_id, ga_client_id
Vendor-specific:
  - Segment: anonymous_id
  - Google Analytics: client_id, ga_client_id
  - Adobe: visitor_id, mcid
Cleansing: TRIM(<col>)
Note: Cookie IDs are the WEAKEST signal — require strong corroboration (email or loyalty match)
Skip if: fill_rate < 0.05
```

### FIRST_NAME
```
Patterns: first_name, fname, given_name, forename, first,
          contact_first_name, billing_first_name
Vendor-specific:
  - Salesforce: FirstName, PersonFirstName
  - HubSpot: firstname
  - Stripe: name (split on space, take first)
Cleansing: UPPER(TRIM(<col>))
```

### LAST_NAME
```
Patterns: last_name, lname, surname, family_name, last,
          contact_last_name, billing_last_name
Vendor-specific:
  - Salesforce: LastName, PersonLastName
  - HubSpot: lastname
Cleansing: UPPER(TRIM(<col>))
```

### Composite Name Handling
```
If source has ONLY a single name field (full_name, name, customer_name, cardholder_name):
  - If contains '/': SPLIT_PART(col, '/', 2) = first_name, SPLIT_PART(col, '/', 1) = last_name
  - If contains ',': TRIM(SPLIT_PART(col, ',', 2)) = first_name, TRIM(SPLIT_PART(col, ',', 1)) = last_name
  - Otherwise: first word = first_name, remaining = last_name
    SPLIT_PART(TRIM(col), ' ', 1) = first_name
    SUBSTR(TRIM(col), LEN(SPLIT_PART(TRIM(col), ' ', 1)) + 2) = last_name
```

### ADDRESS_LINE_1
```
Patterns: address, address_line_1, street, street_address,
          shipping_address, mailing_address, billing_address,
          address1, addr, address_1, home_address
Vendor-specific:
  - Salesforce: MailingStreet, BillingStreet, PersonMailingStreet
  - Shopify: address1
Cleansing: UPPER(TRIM(<col>))
```

### CITY
```
Patterns: city, town, municipality, locality, shipping_city,
          billing_city, mailing_city
Cleansing: UPPER(TRIM(<col>))
```

### STATE
```
Patterns: state, province, region, state_code, state_province,
          shipping_state, billing_state, mailing_state
Cleansing: UPPER(TRIM(<col>))
Note: If full state name, consider mapping to 2-letter code for consistency
```

### ZIP5
```
Patterns: zip, postal_code, zipcode, zip_code, postcode,
          shipping_zip, billing_zip, mailing_zip
Cleansing: LEFT(REGEXP_REPLACE(<col>, '[^0-9]', ''), 5)
Note: Handles ZIP+4 (12345-6789 -> 12345) and international formats
```

### DATE_OF_BIRTH
```
Patterns: dob, date_of_birth, birth_date, birthday, birthdate,
          date_birth, born_on
Cleansing: TRY_TO_DATE(<col>) — handle multiple date formats gracefully
Skip if: fill_rate < 0.03
```

### GENDER
```
Patterns: gender, sex
Cleansing: UPPER(LEFT(TRIM(<col>), 1)) — normalize to M/F/O
```

### SOURCE_UPDATED_AT
```
Patterns (priority order):
  1. updated_at, modified_at, last_modified, last_updated
  2. created_at, created_date, insert_date
  3. _sdc_received_at, _fivetran_synced (CDC timestamps)
  4. received_at (Segment)
  5. SystemModstamp (Salesforce)
Fallback: CURRENT_TIMESTAMP() if no timestamp column exists (will mean "recency" is always tied)
```

## Primary Key Detection

The source_id field should be the primary key of the source table. Detection logic:

1. Look for columns named: `id`, `customer_id`, `user_id`, `record_id`, `<table_name>_id`
2. Validate uniqueness: `SELECT COUNT(*) = COUNT(DISTINCT <col>) FROM <table>`
3. If multiple candidates, prefer the one with the shortest name or the one that's NOT NULL in all rows
4. If no natural PK found, use `ROW_NUMBER() OVER (ORDER BY <timestamp>)` as a synthetic key

## Decision Rules

- If a column matches multiple canonical fields, prefer the more specific match (e.g., `mobile_phone` -> PHONE, not DEVICE_ID)
- If fill_rate < 5%, skip the mapping (too sparse to be useful for matching)
- If a column has >50% duplicate values AND is not email/phone/loyalty_id, it's likely a category, not an identifier — skip it
- Always present the proposed mapping to the user for confirmation before generating DDL
