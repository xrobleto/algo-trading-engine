"""
Generate hashed passwords for user authentication.

Usage:
    python generate_password.py mypassword123

The output hash can be added to config.yaml
"""

import sys

def generate_hash(password: str) -> str:
    """Generate bcrypt hash for a password."""
    try:
        # Try newer API first (streamlit-authenticator >= 0.3.0)
        try:
            import bcrypt
            hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
            return hashed
        except ImportError:
            pass

        # Fallback to streamlit_authenticator's Hasher
        import streamlit_authenticator as stauth
        hasher = stauth.Hasher()
        hashed = hasher.hash(password)
        return hashed

    except Exception as e:
        print(f"Error: {e}")
        print("\nTrying alternative method...")

        # Direct bcrypt approach
        try:
            import bcrypt
            hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
            return hashed
        except ImportError:
            print("Please install bcrypt: pip install bcrypt")
            sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_password.py <password>")
        print("Example: python generate_password.py mysecretpassword")
        sys.exit(1)

    password = sys.argv[1]
    hashed = generate_hash(password)

    print(f"\nPassword: {password}")
    print(f"Hash:     {hashed}")
    print(f"\nAdd this to config.yaml under credentials > usernames > <user> > password")
