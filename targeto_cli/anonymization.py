import base64
import pysodium
import hashlib

# generate collection key
def generate_collection_key():
    scalar = pysodium.crypto_core_ristretto255_scalar_random()
    base64_encoded_scalar = base64.urlsafe_b64encode(scalar).decode('utf-8')
    collection_key = base64_encoded_scalar[:43]
    return collection_key

def get_blinded_value(collection_key, data):
    """
    Generates a blinded value for the given data using the collection key.

    """
    # Calculate the SHA-256 hash of the data
    sha256_hash = hashlib.sha256(data.encode('utf-8'))
    hash_result = sha256_hash.hexdigest()

    # Convert the hash result to a point value using Ristretto255
    point_value = pysodium.crypto_core_ristretto255_from_hash(hash_result.encode())

    # Perform multiplication of the collection key and the point value
    blind = pysodium.crypto_scalarmult_ristretto255(collection_key, point_value)

    # Encode the blinded value using base64
    blind_value = base64.urlsafe_b64encode(blind).decode('utf-8')

    return blind_value
