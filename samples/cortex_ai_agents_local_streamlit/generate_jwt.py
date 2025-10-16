# To run this on the command line, enter:
# python3 generate_jwt.py --account=<account_identifier> --user=<username> --private_key_file_path=<path_to_private_key_file>

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PublicFormat
from cryptography.hazmat.backends import default_backend
from datetime import timedelta, timezone, datetime
import argparse
import base64
from getpass import getpass
import hashlib
import logging
import sys

# This class relies on the PyJWT module (https://pypi.org/project/PyJWT/).
import jwt

logger = logging.getLogger(__name__)

try:
    from typing import Text
except ImportError:
    logger.debug('# Python 3.5.0 and 3.5.1 have incompatible typing modules.', exc_info=True)
    from typing_extensions import Text

ISSUER = "iss"
EXPIRE_TIME = "exp"
ISSUE_TIME = "iat"
SUBJECT = "sub"

# If you generated an encrypted private key, implement this method to return
# the passphrase for decrypting your private key. As an example, this function
# prompts the user for the passphrase.
def get_private_key_passphrase():
    return getpass('Passphrase for private key: ')

class JWTGenerator(object):
    """
    Creates and signs a JWT with the specified private key file, username, and account identifier. The JWTGenerator keeps the
    generated token and only regenerates the token if a specified period of time has passed.
    """
    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minute lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes
    ALGORITHM = "RS256"  # Tokens will be generated using RSA with SHA256

    def __init__(self, account: Text, user: Text, private_key_file_path: Text, private_key_passphrase: Text,
                 lifetime: timedelta = LIFETIME, renewal_delay: timedelta = RENEWAL_DELTA):
        """
        __init__ creates an object that generates JWTs for the specified user, account identifier, and private key.
        :param account: Your Snowflake account identifier. See https://docs.snowflake.com/en/user-guide/admin-account-identifier.html. Note that if you are using the account locator, exclude any region information from the account locator.
        :param user: The Snowflake username.
        :param private_key_file_path: Path to the private key file used for signing the JWTs.
        :param lifetime: The number of minutes (as a timedelta) during which the key will be valid.
        :param renewal_delay: The number of minutes (as a timedelta) from now after which the JWT generator should renew the JWT.
        """

        logger.info(
            """Creating JWTGenerator with arguments
            account : %s, user : %s, lifetime : %s, renewal_delay : %s""",
            account, user, lifetime, renewal_delay)

        # Construct the fully qualified name of the user in uppercase.
        self.account = self.prepare_account_name_for_jwt(account)
        self.user = user.upper()
        self.qualified_username = self.account + "." + self.user

        self.lifetime = lifetime
        self.renewal_delay = renewal_delay
        self.private_key_file_path = private_key_file_path
        self.renew_time = datetime.now(timezone.utc)
        self.token = None

        # Load the private key from the specified file.
        with open(self.private_key_file_path, 'rb') as pem_in:
            pemlines = pem_in.read()
            try:
                # Try to access the private key without a passphrase.
                self.private_key = load_pem_private_key(pemlines, None, default_backend())
            except TypeError:
                # If that fails, provide the passphrase returned from get_private_key_passphrase().
                self.private_key = load_pem_private_key(pemlines, private_key_passphrase.encode(), default_backend())

    def prepare_account_name_for_jwt(self, raw_account: Text) -> Text:
        """
        Prepare the account identifier for use in the JWT.
        For the JWT, the account identifier must not include the subdomain or any region or cloud provider information.
        :param raw_account: The specified account identifier. 
        :return: The account identifier in a form that can be used to generate JWT.
        """
        account = raw_account
        if not '.global' in account:
            # Handle the general case.
            idx = account.find('.')
            if idx > 0:
                account = account[0:idx]
        else:
            # Handle the replication case.
            idx = account.find('-')
            if idx > 0:
                account = account[0:idx]
        # Use uppercase for the account identifier.
        return account.upper()

    def get_token(self) -> Text:
        """
        Generates a new JWT. If a JWT has been already been generated earlier, return the previously generated token unless the
        specified renewal time has passed.
        :return: the new token
        """
        now = datetime.now(timezone.utc)  # Fetch the current time

        # If the token has expired or doesn't exist, regenerate the token.
        if self.token is None or self.renew_time <= now:
            logger.info("Generating a new token because the present time (%s) is later than the renewal time (%s)",
                        now, self.renew_time)
            # Calculate the next time we need to renew the token.
            self.renew_time = now + self.renewal_delay

            # Prepare the fields for the payload.
            # Generate the public key fingerprint for the issuer in the payload.
            public_key_fp = self.calculate_public_key_fingerprint(self.private_key)

            # Create our payload
            payload = {
                # Set the issuer to the fully qualified username concatenated with the public key fingerprint.
                ISSUER: self.qualified_username + '.' + public_key_fp,

                # Set the subject to the fully qualified username.
                SUBJECT: self.qualified_username,

                # Set the issue time to now.
                ISSUE_TIME: now,

                # Set the expiration time, based on the lifetime specified for this object.
                EXPIRE_TIME: now + self.lifetime
            }

            # Regenerate the actual token
            token = jwt.encode(payload, key=self.private_key, algorithm=JWTGenerator.ALGORITHM)
            # If you are using a version of PyJWT prior to 2.0, jwt.encode returns a byte string, rather than a string.
            # If the token is a byte string, convert it to a string.
            if isinstance(token, bytes):
              token = token.decode('utf-8')
            self.token = token
            logger.info("Generated a JWT with the following payload: %s", jwt.decode(self.token, key=self.private_key.public_key(), algorithms=[JWTGenerator.ALGORITHM]))

        return self.token

    def calculate_public_key_fingerprint(self, private_key: Text) -> Text:
        """
        Given a private key in PEM format, return the public key fingerprint.
        :param private_key: private key string
        :return: public key fingerprint
        """
        # Get the raw bytes of public key.
        public_key_raw = private_key.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)

        # Get the sha256 hash of the raw bytes.
        sha256hash = hashlib.sha256()
        sha256hash.update(public_key_raw)

        # Base64-encode the value and prepend the prefix 'SHA256:'.
        public_key_fp = 'SHA256:' + base64.b64encode(sha256hash.digest()).decode('utf-8')
        logger.info("Public key fingerprint is %s", public_key_fp)

        return public_key_fp

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--account', required=True, help='The account identifier (e.g. "myorganization-myaccount" for "myorganization-myaccount.snowflakecomputing.com").')
    cli_parser.add_argument('--user', required=True, help='The user name.')
    cli_parser.add_argument('--private_key_file_path', required=True, help='Path to the private key file used for signing the JWT.')
    cli_parser.add_argument('--lifetime', type=int, default=59, help='The number of minutes that the JWT should be valid for.')
    cli_parser.add_argument('--renewal_delay', type=int, default=54, help='The number of minutes before the JWT generator should produce a new JWT.')
    args = cli_parser.parse_args()

    token = JWTGenerator(args.account, args.user, args.private_key_file_path, timedelta(minutes=args.lifetime), timedelta(minutes=args.renewal_delay)).get_token()
    print('JWT:')
    print(token)

if __name__ == "__main__":
    main()
