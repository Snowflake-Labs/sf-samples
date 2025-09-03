# About the Data

The dataset used in this project is publicly available and cited in *Discovering Knowledge in Data* by Daniel T. Larose.  
It originates from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets.php), as acknowledged by the author.  

To ensure an **apples-to-apples comparison**, I’ve chosen the **same dataset** featured in the official [AWS SageMaker example](https://sagemaker-examples.readthedocs.io/en/latest/introduction_to_applying_machine_learning/xgboost_customer_churn/xgboost_customer_churn_outputs.html#Data).  
Let’s go ahead and download it.

---

### Feature Descriptions

- **State**: The U.S. state in which the customer resides (two-letter abbreviation, e.g., `OH`, `NJ`)
- **Account Length**: Number of days the account has been active
- **Area Code**: The three-digit area code of the customer's phone number
- **Phone**: Remaining seven digits of the phone number (non-informative)
- **Int’l Plan**: Whether the customer has an international calling plan (`yes`/`no`)
- **VMail Plan**: Whether the customer has a voice mail feature (`yes`/`no`)
- **VMail Message**: Average number of voice mail messages per month
- **Day Mins**: Total calling minutes used during the day
- **Day Calls**: Total number of calls placed during the day
- **Day Charge**: Billed cost of daytime calls
- **Eve Mins / Eve Calls / Eve Charge**: Evening call usage and billing
- **Night Mins / Night Calls / Night Charge**: Nighttime call usage and billing
- **Intl Mins / Intl Calls / Intl Charge**: International call usage and billing
- **CustServ Calls**: Number of calls placed to Customer Service
- **Churn?**: Whether the customer left the service (`true`/`false`)
