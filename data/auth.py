import requests
import pyotp
import re
import json
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect

# ========== CREDENTIALS ==========
API_KEY = 'cbdso8ok9ebl7nzc'
API_SECRET = 'zsgetfe2lwyhn78uyz8fr8yzb7ycia4g'
USER_ID = 'DWF302'
PASSWORD = 'Integration@2025'
TOTP_KEY = 'your_totp_key_here'

# Token file path
TOKEN_FILE = 'zerodha_token.json'

def save_token(access_token, user_data):
    """Save access token to file with timestamp"""
    token_data = {
        'access_token': access_token,
        'user_id': user_data.get('user_id'),
        'user_name': user_data.get('user_name'),
        'email': user_data.get('email'),
        'saved_at': datetime.now().isoformat(),
        'expires_at': (datetime.now() + timedelta(hours=24)).isoformat()  # Zerodha tokens expire daily
    }
    
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token_data, f, indent=4)
    
    print(f"✓ Token saved to {TOKEN_FILE}")

def load_token():
    """Load access token from file"""
    if not os.path.exists(TOKEN_FILE):
        return None
    
    try:
        with open(TOKEN_FILE, 'r') as f:
            token_data = json.load(f)
        return token_data
    except Exception as e:
        print(f"⚠️  Error loading token: {e}")
        return None

def verify_token(access_token):
    """Verify if token is still valid"""
    try:
        kite = KiteConnect(api_key=API_KEY)
        kite.set_access_token(access_token)
        
        # Try to fetch profile - if successful, token is valid
        profile = kite.profile()
        print("✓ Token is valid!")
        return True, kite, profile
    except Exception as e:
        print(f"✗ Token expired or invalid: {str(e)}")
        return False, None, None

def zerodha_login():
    """Automatic login to Zerodha Kite API with 2FA"""
    
    try:
        # Step 1: Initialize KiteConnect and Session
        kite = KiteConnect(api_key=API_KEY)
        session = requests.Session()
        
        # Get login page first to set cookies
        session.get(kite.login_url())
        
        # Step 2: Login with credentials
        login_payload = {
            "user_id": USER_ID,
            "password": PASSWORD
        }
        
        response = session.post("https://kite.zerodha.com/api/login", data=login_payload)
        login_data = response.json()
        
        if login_data['status'] != 'success':
            print(f"❌ Login failed: {login_data.get('message', 'Unknown error')}")
            return None, None
        
        print("✓ Step 1: Login successful")
        
        # Step 3: Handle 2FA with TOTP
        totp = input("Enter TOTP: ")  # Manual TOTP input
        
        twofa_payload = {
            "user_id": USER_ID,
            "request_id": login_data["data"]["request_id"],
            "twofa_value": totp,
            "twofa_type": "totp",
            "skip_session": True
        }
        
        response = session.post("https://kite.zerodha.com/api/twofa", data=twofa_payload)
        twofa_data = response.json()
        
        if twofa_data['status'] != 'success':
            print(f"❌ 2FA failed: {twofa_data.get('message', 'Unknown error')}")
            return None, None
        
        print("✓ Step 2: 2FA successful")
        
        # Step 4: Extract request_token
        request_token = None
        
        try:
            response = session.get(kite.login_url(), allow_redirects=True)
            parse_result = urlparse(response.url)
            query_params = parse_qs(parse_result.query)
            
            if 'request_token' in query_params:
                request_token = query_params['request_token'][0]
                print("✓ Step 3: Request token extracted from URL")
        
        except Exception as e:
            error_str = str(e)
            pattern = r'request_token=([A-Za-z0-9]+)'
            matches = re.findall(pattern, error_str)
            
            if matches:
                request_token = matches[0]
                print("✓ Step 3: Request token extracted from exception")
        
        if not request_token and hasattr(response, 'history'):
            for resp in response.history:
                if 'request_token' in resp.url:
                    request_token = parse_qs(urlparse(resp.url).query)['request_token'][0]
                    print("✓ Step 3: Request token found in history")
                    break
        
        if not request_token:
            print("❌ Could not extract request_token")
            return None, None
        
        print(f"✓ Request Token: {request_token}")
        
        # Step 5: Generate session and access token
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"]
        kite.set_access_token(access_token)
        
        print("\n" + "="*50)
        print("🎉 LOGIN SUCCESSFUL!")
        print("="*50)
        print(f"✓ Access Token: {access_token}")
        print(f"✓ User ID: {data['user_id']}")
        print(f"✓ User Name: {data['user_name']}")
        print("="*50 + "\n")
        
        return kite, data
    
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None


def auth_run():
    """Main authentication function with token caching"""
    
    print("\n" + "="*50)
    print("🔐 ZERODHA AUTHENTICATION")
    print("="*50 + "\n")
    
    # Step 1: Try to load saved token
    token_data = load_token()
    
    if token_data:
        print(f"📂 Found saved token from {token_data['saved_at']}")
        print(f"👤 User: {token_data['user_name']} ({token_data['user_id']})")
        print("🔍 Verifying token...\n")
        
        # Step 2: Verify token
        is_valid, kite, profile = verify_token(token_data['access_token'])
        
        if is_valid:
            print("\n✅ Using existing token - Login skipped!")
            print(f"👤 Logged in as: {profile['user_name']}")
            print(f"📧 Email: {profile['email']}")
            print(f"🏢 Broker: {profile['broker']}\n")
            return kite
        else:
            print("\n⚠️  Token expired. Performing fresh login...\n")
    else:
        print("📂 No saved token found. Performing fresh login...\n")
    
    # Step 3: Fresh login if token not valid
    kite, user_data = zerodha_login()
    
    if kite and user_data:
        # Save the new token
        save_token(user_data['access_token'], user_data)
        
        # Test the connection
        try:
            profile = kite.profile()
            print(f"\n✅ AUTHENTICATION COMPLETE")
            print(f"👤 Full Name: {profile['user_name']}")
            print(f"📧 Email: {profile['email']}")
            print(f"🏢 Broker: {profile['broker']}\n")
            
            return kite
            
        except Exception as e:
            print(f"Error fetching profile: {e}")
            return None
    else:
        print("\n⚠️  Login failed. Please check your credentials.")
        return None


# ========== MAIN EXECUTION ==========
# if __name__ == "__main__":
#     kite = auth_run()
    