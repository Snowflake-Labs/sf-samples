# Meta Standard Events Reference

## Supported Events (17 Total)

| Event Name | Description | Required Fields |
|------------|-------------|-----------------|
| `AddPaymentInfo` | Payment info added during checkout | event_time, user_data |
| `AddToCart` | Item added to shopping cart | event_time, user_data, value, currency |
| `AddToWishlist` | Item added to wishlist | event_time, user_data |
| `CompleteRegistration` | User registration completed | event_time, user_data |
| `Contact` | Contact form submitted | event_time, user_data |
| `CustomizeProduct` | Product customization | event_time, user_data |
| `Donate` | Donation made | event_time, user_data, value, currency |
| `FindLocation` | Store locator used | event_time, user_data |
| `InitiateCheckout` | Checkout process started | event_time, user_data, value, currency |
| `Lead` | Lead form submitted | event_time, user_data |
| `PageView` | Page viewed | event_time, user_data |
| `Purchase` | Purchase completed | event_time, user_data, value, currency |
| `Schedule` | Appointment scheduled | event_time, user_data |
| `Search` | Search performed | event_time, user_data, search_string |
| `StartTrial` | Free trial started | event_time, user_data |
| `SubmitApplication` | Application submitted | event_time, user_data |
| `Subscribe` | Subscription started | event_time, user_data, value, currency |
| `ViewContent` | Content viewed | event_time, user_data |

## User Data Fields (PII)

All PII must be **SHA256 hashed** before sending.

| Field | Description | Format |
|-------|-------------|--------|
| `em` | Email address | SHA256(lowercase(trim(email))) |
| `ph` | Phone number | SHA256(digits only, with country code) |
| `fn` | First name | SHA256(lowercase(trim(name))) |
| `ln` | Last name | SHA256(lowercase(trim(name))) |
| `ct` | City | SHA256(lowercase(trim(city))) |
| `st` | State | SHA256(2-letter code) |
| `zp` | Zip code | SHA256(first 5 digits) |
| `country` | Country | 2-letter ISO code (not hashed) |
| `external_id` | Customer ID | SHA256(id) |

## Non-PII Fields

| Field | Description | Example |
|-------|-------------|---------|
| `client_ip_address` | User's IP | `192.168.1.1` |
| `client_user_agent` | Browser UA | `Mozilla/5.0...` |
| `fbc` | Click ID cookie | `fb.1.1554763741205...` |
| `fbp` | Browser ID cookie | `fb.1.1558571054389...` |

## Custom Data Fields

| Field | Description | Type |
|-------|-------------|------|
| `value` | Monetary value | Float |
| `currency` | Currency code | String (ISO 4217) |
| `content_ids` | Product SKUs | Array of strings |
| `content_name` | Product name | String |
| `content_type` | Product category | String |
| `contents` | Product details | Array of objects |
| `num_items` | Quantity | Integer |
| `search_string` | Search query | String |

## Action Sources

| Value | Description |
|-------|-------------|
| `website` | Browser-based events |
| `app` | Mobile app events |
| `phone_call` | Phone call conversions |
| `chat` | Chat/messaging conversions |
| `physical_store` | In-store conversions |
| `system_generated` | Automated/backend events |
| `other` | Other sources |
