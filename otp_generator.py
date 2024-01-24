import pyotp

"""
Retrieves the authenticator one-time 
password using the key value
"""


def get_otp_for_key(key: str) -> str:
    totp = pyotp.TOTP(key)
    return totp.now()
